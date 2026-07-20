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
import Testing

@testable import Alarik

@Suite("StripeEncoder + StripeDecoder tests", .serialized)
struct StripeEncoderDecoderTests {
    private func tempDir(_ label: String) -> String {
        "\(NSTemporaryDirectory())ec-\(label)-\(UUID().uuidString)/"
    }

    private func writeSourceFile(_ data: Data, in dir: String) -> String {
        try? FileManager.default.createDirectory(
            atPath: dir, withIntermediateDirectories: true)
        let path = "\(dir)src-\(UUID().uuidString)"
        FileManager.default.createFile(atPath: path, contents: data)
        return path
    }

    private func randomData(_ count: Int, seed: UInt8 = 0) -> Data {
        Data((0..<count).map { UInt8(truncatingIfNeeded: $0 + Int(seed)) })
    }

    private func meta(size: Int, key: String = "k") -> ObjectMeta {
        ObjectMeta(
            bucketName: "b", key: key, size: size, contentType: "application/octet-stream",
            etag: "etag", updatedAt: Date(timeIntervalSince1970: 1_700_000_000))
    }

    private func encodeAndCollectPaths(
        data: Data, dataShards: Int, parityShards: Int, stripeUnitSize: Int,
        scratchDir: String
    ) throws -> [Int: String] {
        let srcDir = tempDir("src")
        defer { try? FileManager.default.removeItem(atPath: srcDir) }
        let srcPath = writeSourceFile(data, in: srcDir)

        let paths = try StripeEncoder.encode(
            objectMeta: meta(size: data.count),
            payloadSources: [(path: srcPath, offset: 0, size: data.count)],
            dataShards: dataShards, parityShards: parityShards, stripeUnitSize: stripeUnitSize,
            shardPath: { "\(scratchDir)\($0).ecshard" })

        var result: [Int: String] = [:]
        for (i, path) in paths.enumerated() { result[i] = path }
        return result
    }

    private func decodeAll(_ shardPaths: [Int: String]) throws -> Data {
        var out = Data()
        _ = try StripeDecoder.decode(shardPaths: shardPaths) { out.append($0) }
        return out
    }

    // MARK: - Round trips across sizes

    @Test(
        "encode + decode round-trips byte-identically across a range of sizes",
        arguments: [0, 1, 7, 32, 33, 100, 1000]
    )
    func roundTripsAcrossSizes(size: Int) throws {
        let scratchDir = tempDir("shards")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let data = randomData(size, seed: 11)
        let shardPaths = try encodeAndCollectPaths(
            data: data, dataShards: 4, parityShards: 2, stripeUnitSize: 8, scratchDir: scratchDir)

        let decoded = try decodeAll(shardPaths)
        #expect(decoded == data)
    }

    @Test(
        "k=1/m=0 (zero parity shards - MetadataStore's standalone, non-networked case) round-trips",
        arguments: [0, 1, 7, 1000]
    )
    func zeroParityShardsRoundTrips(size: Int) throws {
        let scratchDir = tempDir("shards-zero-parity")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let data = randomData(size, seed: 5)
        let shardPaths = try encodeAndCollectPaths(
            data: data, dataShards: 1, parityShards: 0, stripeUnitSize: 64, scratchDir: scratchDir)

        #expect(shardPaths.count == 1)
        let decoded = try decodeAll(shardPaths)
        #expect(decoded == data)
    }

    @Test("multi-source payload (simulating CompleteMultipartUpload) round-trips correctly")
    func multiSourcePayloadRoundTrips() throws {
        let srcDir = tempDir("src")
        let scratchDir = tempDir("shards")
        defer {
            try? FileManager.default.removeItem(atPath: srcDir)
            try? FileManager.default.removeItem(atPath: scratchDir)
        }

        let part1 = randomData(50, seed: 1)
        let part2 = randomData(77, seed: 2)
        let part1Path = writeSourceFile(part1, in: srcDir)
        let part2Path = writeSourceFile(part2, in: srcDir)

        let paths = try StripeEncoder.encode(
            objectMeta: meta(size: part1.count + part2.count),
            payloadSources: [
                (path: part1Path, offset: 0, size: part1.count),
                (path: part2Path, offset: 0, size: part2.count),
            ],
            dataShards: 3, parityShards: 2, stripeUnitSize: 16,
            shardPath: { "\(scratchDir)\($0).ecshard" })

        var shardPaths: [Int: String] = [:]
        for (i, path) in paths.enumerated() { shardPaths[i] = path }

        let decoded = try decodeAll(shardPaths)
        #expect(decoded == part1 + part2)
    }

    // MARK: - Missing shards

    @Test("decode succeeds with exactly k surviving shards, including an all-parity survivor set")
    func decodesFromExactlyKSurvivors() throws {
        let scratchDir = tempDir("shards")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let data = randomData(500, seed: 3)
        let allPaths = try encodeAndCollectPaths(
            data: data, dataShards: 4, parityShards: 4, stripeUnitSize: 32, scratchDir: scratchDir)

        // Only the 4 parity shards survive - k == m == 4 here, so this is exactly enough.
        let survivors = allPaths.filter { $0.key >= 4 }
        #expect(survivors.count == 4)

        let decoded = try decodeAll(survivors)
        #expect(decoded == data)
    }

    @Test("decode throws a clear error with fewer than k surviving shards")
    func throwsBelowQuorum() throws {
        let scratchDir = tempDir("shards")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let data = randomData(200, seed: 4)
        let allPaths = try encodeAndCollectPaths(
            data: data, dataShards: 4, parityShards: 2, stripeUnitSize: 16, scratchDir: scratchDir)

        // Only 3 of the 6 shards survive - below k=4.
        let survivors = allPaths.filter { $0.key < 3 }
        #expect(throws: StripeDecoderError.self) {
            try self.decodeAll(survivors)
        }
    }

    @Test("decode tolerates a shard path that doesn't exist on disk (dropped, not fatal)")
    func tolerantOfMissingFile() throws {
        let scratchDir = tempDir("shards")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let data = randomData(300, seed: 5)
        var shardPaths = try encodeAndCollectPaths(
            data: data, dataShards: 3, parityShards: 2, stripeUnitSize: 16, scratchDir: scratchDir)
        shardPaths[1] = "\(scratchDir)does-not-exist.ecshard"

        let decoded = try decodeAll(shardPaths)
        #expect(decoded == data)
    }

    // MARK: - Corruption

    @Test("decode reconstructs a single corrupted stripe from survivors automatically")
    func reconstructsCorruptedStripe() throws {
        let scratchDir = tempDir("shards")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let data = randomData(400, seed: 6)
        let shardPaths = try encodeAndCollectPaths(
            data: data, dataShards: 4, parityShards: 2, stripeUnitSize: 32, scratchDir: scratchDir)

        // Corrupt the last byte of shard 0's file - lands inside its first stripe's payload.
        let corruptPath = shardPaths[0]!
        var raw = try Data(contentsOf: URL(fileURLWithPath: corruptPath))
        raw[raw.count - 1] ^= 0xFF
        try raw.write(to: URL(fileURLWithPath: corruptPath))

        let decoded = try decodeAll(shardPaths)
        #expect(decoded == data)
    }

    // MARK: - Validation

    @Test("encode rejects payloadSources whose total size disagrees with objectMeta.size")
    func encodeRejectsSizeMismatch() throws {
        let srcDir = tempDir("src")
        let scratchDir = tempDir("shards")
        defer {
            try? FileManager.default.removeItem(atPath: srcDir)
            try? FileManager.default.removeItem(atPath: scratchDir)
        }
        let data = randomData(10)
        let srcPath = writeSourceFile(data, in: srcDir)

        #expect(throws: StripeEncoderError.self) {
            try StripeEncoder.encode(
                objectMeta: self.meta(size: 999),
                payloadSources: [(path: srcPath, offset: 0, size: data.count)],
                dataShards: 2, parityShards: 1, stripeUnitSize: 8,
                shardPath: { "\(scratchDir)\($0).ecshard" })
        }
    }

    @Test("a failed encode leaves no shard files behind at all")
    func failedEncodeLeavesNoFiles() throws {
        let srcDir = tempDir("src")
        let scratchDir = tempDir("shards")
        defer {
            try? FileManager.default.removeItem(atPath: srcDir)
            try? FileManager.default.removeItem(atPath: scratchDir)
        }
        let data = randomData(10)
        let srcPath = writeSourceFile(data, in: srcDir)

        _ = try? StripeEncoder.encode(
            objectMeta: meta(size: 999),
            payloadSources: [(path: srcPath, offset: 0, size: data.count)],
            dataShards: 2, parityShards: 1, stripeUnitSize: 8,
            shardPath: { "\(scratchDir)\($0).ecshard" })

        let leftovers = (try? FileManager.default.contentsOfDirectory(atPath: scratchDir)) ?? []
        #expect(leftovers.isEmpty)
    }

    // MARK: - Ranged decode

    private func decodeRange(_ shardPaths: [Int: String], start: Int, end: Int) throws -> Data {
        var out = Data()
        _ = try StripeDecoder.decode(shardPaths: shardPaths, range: (start: start, end: end)) {
            out.append($0)
        }
        return out
    }

    @Test(
        "ranged decode returns exactly the requested byte slice, across boundaries and edges",
        arguments: [
            (0, 0), (0, 7), (7, 8), (0, 511), (511, 512), (500, 700), (1023, 1023),
            (0, 1023), (300, 300), (1, 1022),
        ]
    )
    func rangedDecodeReturnsExactSlice(start: Int, end: Int) throws {
        let scratchDir = tempDir("shards")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        // 1024 bytes over k=4 * stripeUnitSize=64 = 256-byte stripes -> 4 stripes, so the ranges
        // above deliberately straddle stripe boundaries (256, 512, 768).
        let data = randomData(1024, seed: 21)
        let shardPaths = try encodeAndCollectPaths(
            data: data, dataShards: 4, parityShards: 2, stripeUnitSize: 64, scratchDir: scratchDir)

        let sliced = try decodeRange(shardPaths, start: start, end: end)
        #expect(sliced == data.subdata(in: start..<(end + 1)))
    }

    @Test("a ranged decode still reconstructs from survivors when a data shard is missing")
    func rangedDecodeReconstructsFromSurvivors() throws {
        let scratchDir = tempDir("shards")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let data = randomData(1000, seed: 22)
        var shardPaths = try encodeAndCollectPaths(
            data: data, dataShards: 4, parityShards: 2, stripeUnitSize: 32, scratchDir: scratchDir)
        // Drop two data shards - forces a real matrix-solve decode, even for the range.
        shardPaths[0] = "\(scratchDir)missing-0.ecshard"
        shardPaths[1] = "\(scratchDir)missing-1.ecshard"

        let sliced = try decodeRange(shardPaths, start: 100, end: 799)
        #expect(sliced == data.subdata(in: 100..<800))
    }

    @Test("a suffix-style range (last N bytes) decodes correctly")
    func rangedDecodeSuffix() throws {
        let scratchDir = tempDir("shards")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let data = randomData(777, seed: 23)
        let shardPaths = try encodeAndCollectPaths(
            data: data, dataShards: 3, parityShards: 2, stripeUnitSize: 40, scratchDir: scratchDir)

        let sliced = try decodeRange(shardPaths, start: 777 - 100, end: 776)
        #expect(sliced == data.suffix(100))
    }

    // MARK: - Corrupt-shard reporting (drives read-repair)

    @Test("a clean decode reports no corrupt shard indices")
    func cleanDecodeReportsNoCorruption() throws {
        let scratchDir = tempDir("shards")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let data = randomData(500, seed: 24)
        let shardPaths = try encodeAndCollectPaths(
            data: data, dataShards: 4, parityShards: 2, stripeUnitSize: 32, scratchDir: scratchDir)

        var out = Data()
        let result = try StripeDecoder.decode(shardPaths: shardPaths) { out.append($0) }
        #expect(out == data)
        #expect(result.corruptShardIndices.isEmpty)
    }

    @Test("decode reports the specific shard index whose checksum failed")
    func decodeReportsCorruptShardIndex() throws {
        let scratchDir = tempDir("shards")
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let data = randomData(400, seed: 25)
        let shardPaths = try encodeAndCollectPaths(
            data: data, dataShards: 4, parityShards: 2, stripeUnitSize: 32, scratchDir: scratchDir)

        // Flip a byte inside shard 2's first stripe payload.
        let corruptPath = shardPaths[2]!
        var raw = try Data(contentsOf: URL(fileURLWithPath: corruptPath))
        raw[raw.count - 1] ^= 0xFF
        try raw.write(to: URL(fileURLWithPath: corruptPath))

        var out = Data()
        let result = try StripeDecoder.decode(shardPaths: shardPaths) { out.append($0) }
        // Still decodes correctly (reconstructed from survivors)...
        #expect(out == data)
        // ...and flags shard 2 as the corrupt copy that read-repair should rebuild.
        #expect(result.corruptShardIndices.contains(2))
    }
}
