/*
Copyright 2025-present Julian Gerhards

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import CISAL
import Foundation

/// Thin wrapper over Intel ISA-L's Reed-Solomon GF(256) primitives. Stateless: every call
/// rebuilds its coefficient matrix locally from (k, m) alone, so this is safe to call
/// concurrently from multiple stripes with no shared state.
enum ReedSolomonEngine {
    enum EngineError: Error, CustomStringConvertible {
        case invalidShardCounts(dataCount: Int, parityCount: Int)
        case shardSizeMismatch
        case tooFewSurvivingShards(needed: Int, available: Int)
        case shardIndexOutOfRange(Int)
        case indexAlreadyAvailable(Int)
        case singularMatrix

        var description: String {
            switch self {
            case .invalidShardCounts(let k, let m):
                "Invalid shard counts: dataCount=\(k), parityCount=\(m) (both must be >= 1)"
            case .shardSizeMismatch:
                "All shards passed to the erasure coding engine must be the same length"
            case .tooFewSurvivingShards(let needed, let available):
                "Need at least \(needed) surviving shards to reconstruct, only \(available) available"
            case .shardIndexOutOfRange(let index):
                "Shard index \(index) is out of range"
            case .indexAlreadyAvailable(let index):
                "Shard index \(index) was requested for reconstruction but is already available"
            case .singularMatrix:
                "Erasure coding matrix is singular - should never happen with a Cauchy matrix"
            }
        }
    }

    /// Owns one raw `unsigned char *` buffer for the lifetime of an encode/decode call - ISA-L's
    /// C API needs real pointers, not Swift `Data`.
    private final class ShardBuffer {
        let pointer: UnsafeMutablePointer<UInt8>
        let count: Int

        init(count: Int) {
            self.count = count
            pointer = .allocate(capacity: max(count, 1))
        }

        init(data: Data) {
            count = data.count
            pointer = .allocate(capacity: max(count, 1))
            data.withUnsafeBytes { raw in
                if let base = raw.baseAddress, count > 0 {
                    pointer.update(from: base.assumingMemoryBound(to: UInt8.self), count: count)
                }
            }
        }

        deinit { pointer.deallocate() }

        var data: Data { Data(bytes: pointer, count: count) }
    }

    /// Cauchy [k+m x k] coefficient matrix: rows 0..<k are the identity (systematic code - the
    /// first k encoded outputs ARE the original data), rows k..<k+m are parity coefficients.
    /// Deterministic from (k, m) alone, so encode and reconstruct each recompute it independently.
    private static func encodeMatrix(dataCount k: Int, parityCount m: Int) -> [UInt8] {
        var matrix = [UInt8](repeating: 0, count: (k + m) * k)
        matrix.withUnsafeMutableBufferPointer { buf in
            gf_gen_cauchy1_matrix(buf.baseAddress, Int32(k + m), Int32(k))
        }
        return matrix
    }

    /// Splits `dataShards` (exactly `k` equal-length pieces, length may be 0) into `parityCount`
    /// parity shards.
    static func encode(dataShards: [Data], parityCount: Int) throws -> [Data] {
        let k = dataShards.count
        let m = parityCount
        guard k >= 1, m >= 1 else {
            throw EngineError.invalidShardCounts(dataCount: k, parityCount: m)
        }
        guard let len = dataShards.first?.count, dataShards.allSatisfy({ $0.count == len }) else {
            throw EngineError.shardSizeMismatch
        }

        var matrix = encodeMatrix(dataCount: k, parityCount: m)
        var gftbls = [UInt8](repeating: 0, count: 32 * k * m)
        matrix.withUnsafeMutableBufferPointer { matBuf in
            gftbls.withUnsafeMutableBufferPointer { tblBuf in
                ec_init_tables(
                    Int32(k), Int32(m), matBuf.baseAddress!.advanced(by: k * k), tblBuf.baseAddress)
            }
        }

        let dataBuffers = dataShards.map { ShardBuffer(data: $0) }
        let parityBuffers = (0..<m).map { _ in ShardBuffer(count: len) }
        var dataPointers = dataBuffers.map { $0.pointer as UnsafeMutablePointer<UInt8>? }
        var codingPointers = parityBuffers.map { $0.pointer as UnsafeMutablePointer<UInt8>? }

        dataPointers.withUnsafeMutableBufferPointer { dataBuf in
            codingPointers.withUnsafeMutableBufferPointer { codingBuf in
                gftbls.withUnsafeMutableBufferPointer { tblBuf in
                    ec_encode_data(
                        Int32(len), Int32(k), Int32(m), tblBuf.baseAddress,
                        dataBuf.baseAddress, codingBuf.baseAddress)
                }
            }
        }

        return parityBuffers.map { $0.data }
    }

    /// Reconstructs the shards at `missingIndices` (each in `0..<k+m`) from `availableShards`,
    /// which must hold at least `k` entries - any `k` of the `k+m` shards, data or parity.
    static func reconstruct(
        availableShards: [Int: Data],
        missingIndices: [Int],
        dataCount k: Int,
        parityCount m: Int
    ) throws -> [Int: Data] {
        guard k >= 1, m >= 1 else {
            throw EngineError.invalidShardCounts(dataCount: k, parityCount: m)
        }
        let total = k + m
        guard availableShards.count >= k else {
            throw EngineError.tooFewSurvivingShards(needed: k, available: availableShards.count)
        }
        guard let len = availableShards.values.first?.count,
            availableShards.values.allSatisfy({ $0.count == len })
        else {
            throw EngineError.shardSizeMismatch
        }
        for index in availableShards.keys {
            guard (0..<total).contains(index) else { throw EngineError.shardIndexOutOfRange(index) }
        }
        for index in missingIndices {
            guard (0..<total).contains(index) else { throw EngineError.shardIndexOutOfRange(index) }
            guard availableShards[index] == nil else { throw EngineError.indexAlreadyAvailable(index) }
        }
        guard !missingIndices.isEmpty else { return [:] }

        // Any index not in `availableShards` is unusable as a systematic source, whether or not
        // the caller actually asked to recover it right now.
        let erred = Set(0..<total).subtracting(availableShards.keys)
        let matrix = encodeMatrix(dataCount: k, parityCount: m)

        var decodeIndex = [Int](repeating: 0, count: k)
        var subMatrix = [UInt8](repeating: 0, count: k * k)
        var row = 0
        for i in 0..<k {
            while erred.contains(row) { row += 1 }
            decodeIndex[i] = row
            for col in 0..<k {
                subMatrix[k * i + col] = matrix[k * row + col]
            }
            row += 1
        }

        var invertMatrix = [UInt8](repeating: 0, count: k * k)
        let inverted = subMatrix.withUnsafeMutableBufferPointer { subBuf in
            invertMatrix.withUnsafeMutableBufferPointer { invBuf in
                gf_invert_matrix(subBuf.baseAddress, invBuf.baseAddress, Int32(k))
            }
        }
        guard inverted == 0 else { throw EngineError.singularMatrix }

        // Missing data-shard rows come straight from the inverse; missing parity-shard rows are
        // the inverse multiplied by that parity row of the original encode matrix.
        let p = missingIndices.count
        var decodeMatrix = [UInt8](repeating: 0, count: k * p)
        for (outRow, missing) in missingIndices.enumerated() {
            if missing < k {
                for col in 0..<k {
                    decodeMatrix[k * outRow + col] = invertMatrix[k * missing + col]
                }
            } else {
                for col in 0..<k {
                    var sum: UInt8 = 0
                    for j in 0..<k {
                        sum ^= gf_mul(invertMatrix[j * k + col], matrix[k * missing + j])
                    }
                    decodeMatrix[k * outRow + col] = sum
                }
            }
        }

        var gftbls = [UInt8](repeating: 0, count: 32 * k * p)
        decodeMatrix.withUnsafeMutableBufferPointer { dmBuf in
            gftbls.withUnsafeMutableBufferPointer { tblBuf in
                ec_init_tables(Int32(k), Int32(p), dmBuf.baseAddress, tblBuf.baseAddress)
            }
        }

        let sourceBuffers = decodeIndex.map { ShardBuffer(data: availableShards[$0]!) }
        let outputBuffers = (0..<p).map { _ in ShardBuffer(count: len) }
        var sourcePointers = sourceBuffers.map { $0.pointer as UnsafeMutablePointer<UInt8>? }
        var outputPointers = outputBuffers.map { $0.pointer as UnsafeMutablePointer<UInt8>? }

        sourcePointers.withUnsafeMutableBufferPointer { srcBuf in
            outputPointers.withUnsafeMutableBufferPointer { outBuf in
                gftbls.withUnsafeMutableBufferPointer { tblBuf in
                    ec_encode_data(
                        Int32(len), Int32(k), Int32(p), tblBuf.baseAddress,
                        srcBuf.baseAddress, outBuf.baseAddress)
                }
            }
        }

        var result: [Int: Data] = [:]
        for (i, index) in missingIndices.enumerated() {
            result[index] = outputBuffers[i].data
        }
        return result
    }
}
