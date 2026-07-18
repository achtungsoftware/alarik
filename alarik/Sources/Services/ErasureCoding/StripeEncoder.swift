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

enum StripeEncoderError: Error, CustomStringConvertible {
    case openFailed(path: String, errno: Int32)
    case truncatedSource(path: String)
    case sizeMismatch(declared: Int, actual: Int)

    var description: String {
        switch self {
        case .openFailed(let path, let code):
            "Could not open payload source '\(path)' (errno \(code))"
        case .truncatedSource(let path):
            "Payload source '\(path)' ended before its declared size"
        case .sizeMismatch(let declared, let actual):
            "objectMeta.size (\(declared)) doesn't match the sum of payloadSources sizes (\(actual))"
        }
    }
}

/// Reads bytes sequentially across several `(path, offset, size)` file regions as one stream,
/// ignoring source boundaries - the same `payloadSources` shape `ObjectFileHandler.writeStreamed`
/// already uses, so single-shot PUT and CompleteMultipartUpload drive `StripeEncoder` identically.
private struct PayloadSourceReader {
    private let sources: [(path: String, offset: Int, size: Int)]
    private var sourceIndex = 0
    private var fd: Int32 = -1
    private var remainingInSource = 0

    init(sources: [(path: String, offset: Int, size: Int)]) {
        self.sources = sources
    }

    /// Reads exactly `count` real bytes into `buffer` starting at `bufferOffset`. Callers only
    /// ever ask for bytes known to exist (bounded by the total declared source size), so a short
    /// read here means a source file is shorter than it claimed - a genuine error, not padding.
    mutating func read(into buffer: inout [UInt8], bufferOffset: Int, count: Int) throws {
        var totalRead = 0
        while totalRead < count {
            if remainingInSource == 0 {
                if fd >= 0 {
                    _ = POSIXFile.close(fd)
                    fd = -1
                }
                guard sourceIndex < sources.count else {
                    throw StripeEncoderError.truncatedSource(path: sources.last?.path ?? "<none>")
                }
                let source = sources[sourceIndex]
                sourceIndex += 1
                let opened = POSIXFile.open(source.path, O_RDONLY)
                guard opened >= 0 else {
                    throw StripeEncoderError.openFailed(path: source.path, errno: errno)
                }
                fd = opened
                _ = POSIXFile.lseek(fd, off_t(source.offset), SEEK_SET)
                remainingInSource = source.size
                continue
            }
            let toRead = Swift.min(count - totalRead, remainingInSource)
            let bytesRead = buffer.withUnsafeMutableBytes { raw -> Int in
                POSIXFile.read(fd, raw.baseAddress!.advanced(by: bufferOffset + totalRead), toRead)
            }
            guard bytesRead > 0 else {
                throw StripeEncoderError.truncatedSource(path: sources[sourceIndex - 1].path)
            }
            remainingInSource -= bytesRead
            totalRead += bytesRead
        }
    }

    mutating func closeIfNeeded() {
        if fd >= 0 {
            _ = POSIXFile.close(fd)
            fd = -1
        }
    }
}

/// Streams `payloadSources` through Reed-Solomon encoding, one stripe (`dataShards *
/// stripeUnitSize` bytes) at a time, writing `dataShards + parityShards` local `.ecshard` files
/// as it goes - bounded memory regardless of object size, same ethos as
/// `ObjectFileHandler.writeStreamed`. The final stripe is zero-padded to a full stripe boundary;
/// `objectMeta.size` (the real, unpadded logical size) tells `StripeDecoder` how much of it is
/// real data.
enum StripeEncoder {
    /// Encodes `payloadSources` into shard files at the paths `shardPath(0)..<shardPath(k+m-1)`
    /// returns. On any failure, every shard file from this attempt is removed - never leaves a
    /// partial, misleadingly-quorum-satisfying shard set behind.
    static func encode(
        objectMeta: ObjectMeta,
        payloadSources: [(path: String, offset: Int, size: Int)],
        dataShards: Int,
        parityShards: Int,
        stripeUnitSize: Int = Constants.erasureCodingStripeUnitSize,
        shardPath: (Int) -> String
    ) throws -> [String] {
        let totalShardCount = dataShards + parityShards
        let shardPaths = (0..<totalShardCount).map(shardPath)

        let declaredTotal = payloadSources.reduce(0) { $0 + $1.size }
        guard declaredTotal == objectMeta.size else {
            throw StripeEncoderError.sizeMismatch(declared: objectMeta.size, actual: declaredTotal)
        }

        let stripeDataSize = dataShards * stripeUnitSize
        let stripeCount = declaredTotal == 0 ? 0 : (declaredTotal + stripeDataSize - 1) / stripeDataSize

        var writers: [ErasureCodedShardWriter] = []
        do {
            for shardIndex in 0..<totalShardCount {
                let header = ErasureCodedShardHeader(
                    shardIndex: shardIndex, dataShards: dataShards, parityShards: parityShards,
                    stripeUnitSize: stripeUnitSize, stripeCount: stripeCount, objectMeta: objectMeta)
                writers.append(try ErasureCodedShardWriter(path: shardPaths[shardIndex], header: header))
            }
        } catch {
            for i in writers.indices { writers[i].abort() }
            throw error
        }

        var reader = PayloadSourceReader(sources: payloadSources)
        defer { reader.closeIfNeeded() }

        var stripeBuffer = [UInt8](repeating: 0, count: stripeDataSize)
        var bytesRemaining = declaredTotal

        do {
            for _ in 0..<stripeCount {
                let realBytesThisStripe = Swift.min(stripeBuffer.count, bytesRemaining)
                if realBytesThisStripe > 0 {
                    try reader.read(into: &stripeBuffer, bufferOffset: 0, count: realBytesThisStripe)
                }
                if realBytesThisStripe < stripeBuffer.count {
                    for i in realBytesThisStripe..<stripeBuffer.count { stripeBuffer[i] = 0 }
                }
                bytesRemaining -= realBytesThisStripe

                var dataSlices: [Data] = []
                dataSlices.reserveCapacity(dataShards)
                for d in 0..<dataShards {
                    let start = d * stripeUnitSize
                    dataSlices.append(Data(stripeBuffer[start..<(start + stripeUnitSize)]))
                }
                let parityChunks = try ReedSolomonEngine.encode(
                    dataShards: dataSlices, parityCount: parityShards)

                for d in 0..<dataShards {
                    try writers[d].appendStripe(dataSlices[d])
                }
                for p in 0..<parityShards {
                    try writers[dataShards + p].appendStripe(parityChunks[p])
                }
            }

            for i in writers.indices { try writers[i].finish() }
        } catch {
            for i in writers.indices { writers[i].abort() }
            for path in shardPaths { try? FileManager.default.removeItem(atPath: path) }
            throw error
        }

        return shardPaths
    }
}
