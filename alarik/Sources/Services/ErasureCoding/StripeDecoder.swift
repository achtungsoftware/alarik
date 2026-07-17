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

import Foundation

enum StripeDecoderError: Error, CustomStringConvertible {
    case tooFewHealthyShards(needed: Int, available: Int)
    case stripeUnrecoverable(stripeIndex: Int, needed: Int, available: Int)

    var description: String {
        switch self {
        case .tooFewHealthyShards(let needed, let available):
            "Need at least \(needed) healthy shards to decode, only \(available) opened cleanly"
        case .stripeUnrecoverable(let stripeIndex, let needed, let available):
            "Stripe \(stripeIndex): need \(needed) healthy copies to reconstruct, only \(available) checksummed correctly"
        }
    }
}

/// What a decode observed, beyond the reconstructed bytes themselves.
struct StripeDecodeResult {
    let meta: ObjectMeta
    /// Shard indices that were gathered but failed a per-stripe checksum during this decode - i.e.
    /// silently corrupt on disk. Drives read-repair: these copies are treated as damaged and
    /// rebuilt from healthy survivors. Only ever covers shards (and stripes) this decode actually
    /// read, so a ranged read reports only what it touched.
    let corruptShardIndices: Set<Int>
}

/// Reconstructs an object's bytes from its `.ecshard` files, streaming output stripe by stripe -
/// bounded memory regardless of object size. Needs only `dataShards` shards to be healthy;
/// missing files, unreadable headers, and per-stripe checksum failures are all treated as
/// "unavailable" and reconstructed the same way via `ReedSolomonEngine`.
enum StripeDecoder {
    /// `shardPaths` may include indices whose file doesn't exist or won't open - those are
    /// silently dropped rather than failing the whole decode, as long as `dataShards` others
    /// remain healthy. `onChunk` receives the reconstructed payload in order, already trimmed to
    /// the real (unpadded) logical size. When `range` is given, only the requested inclusive byte
    /// range is emitted - and stripes lying entirely outside it are skipped without being read or
    /// decoded, so a small ranged read of a large object doesn't pay to reconstruct the whole thing.
    @discardableResult
    static func decode(
        shardPaths: [Int: String],
        range: (start: Int, end: Int)? = nil,
        onChunk: (Data) throws -> Void
    ) throws -> StripeDecodeResult {
        var readers: [Int: ErasureCodedShardReader] = [:]
        defer { for reader in readers.values { reader.close() } }

        for (index, path) in shardPaths {
            guard let reader = try? ErasureCodedShardReader(path: path) else { continue }
            readers[index] = reader
        }

        guard let reference = readers.values.first?.header else {
            throw StripeDecoderError.tooFewHealthyShards(needed: 1, available: 0)
        }
        let k = reference.dataShards
        let m = reference.parityShards
        let stripeUnitSize = reference.stripeUnitSize
        let stripeCount = reference.stripeCount
        let objectMeta = reference.objectMeta

        // Drop any reader whose header disagrees with the reference - a mismatched/corrupt
        // header is treated exactly like a missing shard, not a fatal decode error.
        readers = readers.filter { _, reader in
            reader.header.dataShards == k && reader.header.parityShards == m
                && reader.header.stripeUnitSize == stripeUnitSize
                && reader.header.stripeCount == stripeCount
        }
        guard readers.count >= k else {
            throw StripeDecoderError.tooFewHealthyShards(needed: k, available: readers.count)
        }

        let stripeDataSize = k * stripeUnitSize
        var corruptShardIndices: Set<Int> = []
        var logicalOffset = 0

        for stripeIndex in 0..<stripeCount {
            // The real (unpadded) logical span this stripe contributes - computable without
            // decoding, so out-of-range stripes are skipped before any I/O.
            let stripeLogicalSize = Swift.min(stripeDataSize, objectMeta.size - logicalOffset)
            let stripeStart = logicalOffset
            let stripeEnd = logicalOffset + stripeLogicalSize
            logicalOffset = stripeEnd

            if let range {
                if stripeEnd <= range.start { continue }  // entirely before the range
                if stripeStart > range.end { break }  // entirely after the range - done
            }

            let assembled = try decodeStripe(
                readers: readers, stripeIndex: stripeIndex, dataShards: k, parityShards: m,
                stripeUnitSize: stripeUnitSize, corruptShardIndices: &corruptShardIndices)

            // Slice the decoded stripe down to the portion that's both real (unpadded) and within
            // the requested range.
            let emitFrom = range.map { Swift.max($0.start, stripeStart) - stripeStart } ?? 0
            let emitTo = range.map { Swift.min($0.end + 1, stripeEnd) - stripeStart } ?? stripeLogicalSize
            if emitTo > emitFrom {
                try onChunk(assembled.subdata(in: emitFrom..<emitTo))
            }
        }

        return StripeDecodeResult(meta: objectMeta, corruptShardIndices: corruptShardIndices)
    }

    /// Fast path: if every data shard (indices `0..<k`) is present and checksums clean, this is
    /// the systematic code's identity property - just concatenate, no GF math. Falls back to
    /// `ReedSolomonEngine.reconstruct` only for the stripes that actually need it. Any shard that
    /// fails its checksum is recorded in `corruptShardIndices` so the read path can rebuild it.
    private static func decodeStripe(
        readers: [Int: ErasureCodedShardReader], stripeIndex: Int, dataShards k: Int, parityShards m: Int,
        stripeUnitSize: Int, corruptShardIndices: inout Set<Int>
    ) throws -> Data {
        var fastPath = Data(capacity: k * stripeUnitSize)
        var fastPathOK = true
        for i in 0..<k {
            guard let reader = readers[i] else { fastPathOK = false; break }
            do {
                fastPath.append(try reader.readStripe(stripeIndex))
            } catch let error as ErasureCodedObjectHandlerError {
                if case .checksumMismatch(let shardIndex, _) = error {
                    corruptShardIndices.insert(shardIndex)
                }
                fastPathOK = false
                break
            }
        }
        if fastPathOK { return fastPath }

        var available: [Int: Data] = [:]
        for (index, reader) in readers {
            guard available.count < k else { break }
            do {
                available[index] = try reader.readStripe(stripeIndex)
            } catch let error as ErasureCodedObjectHandlerError {
                if case .checksumMismatch(let shardIndex, _) = error {
                    corruptShardIndices.insert(shardIndex)
                }
            } catch {
                // A non-checksum read error (short read, etc.) - treat as unavailable, but don't
                // flag it as corrupt data the way a checksum mismatch is.
            }
        }
        guard available.count >= k else {
            throw StripeDecoderError.stripeUnrecoverable(
                stripeIndex: stripeIndex, needed: k, available: available.count)
        }

        let missingDataIndices = (0..<k).filter { available[$0] == nil }
        let recovered =
            missingDataIndices.isEmpty
            ? [:]
            : try ReedSolomonEngine.reconstruct(
                availableShards: available, missingIndices: missingDataIndices,
                dataCount: k, parityCount: m)

        var assembled = Data()
        for i in 0..<k {
            assembled.append(available[i] ?? recovered[i]!)
        }
        return assembled
    }
}
