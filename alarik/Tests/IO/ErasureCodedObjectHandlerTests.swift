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

@Suite("ErasureCodedObjectHandler tests", .serialized)
struct ErasureCodedObjectHandlerTests {
    private func createTempPath() -> String {
        let temp = NSTemporaryDirectory()
        let uuid = UUID().uuidString
        return "\(temp)ec-test-\(uuid)/0.ecshard"
    }

    private func cleanup(path: String) {
        let fileURL = URL(fileURLWithPath: path)
        try? FileManager.default.removeItem(at: fileURL.deletingLastPathComponent())
    }

    private func testMeta(size: Int) -> ObjectMeta {
        ObjectMeta(
            bucketName: "b", key: "k", size: size, contentType: "application/octet-stream",
            etag: "etag", updatedAt: Date(timeIntervalSince1970: 1_700_000_000))
    }

    // MARK: - Path scheme

    @Test("shardPath and versionedShardPath produce distinct, extension-correct paths")
    func pathSchemeIsWellFormed() {
        let plain = ErasureCodedObjectHandler.shardPath(bucketName: "b", key: "k", shardIndex: 2)
        #expect(plain.hasSuffix("2.ecshard"))
        #expect(plain.contains("k.ecshards/"))

        let versioned = ErasureCodedObjectHandler.versionedShardPath(
            bucketName: "b", key: "k", versionId: "v1", shardIndex: 2)
        #expect(versioned.hasSuffix("2.ecshard"))
        #expect(versioned.contains("k.versions/v1.ecshards/"))
        #expect(plain != versioned)
    }

    @Test("shardPath sanitizes path traversal in the key")
    func pathSchemeSanitizesTraversal() {
        let path = ErasureCodedObjectHandler.shardPath(
            bucketName: "b", key: "../../etc/passwd", shardIndex: 0)
        #expect(!path.contains(".."))
    }

    @Test("versionedShardBasePath sanitizes path traversal in a client-supplied versionId")
    func versionedPathSanitizesVersionIdTraversal() {
        // A client can pass an arbitrary ?versionId= on GET/DELETE; it must never escape the
        // key's .versions/ directory (this path is removeItem'd recursively on delete).
        let base = ErasureCodedObjectHandler.versionedShardBasePath(
            bucketName: "b", key: "k", versionId: "../../other-bucket/victim")
        #expect(!base.contains(".."))
        #expect(base.contains("k.versions/"))
        // The sanitized remainder stays inside the .versions directory.
        #expect(base.hasSuffix(".ecshards/"))

        // The legacy .obj path builder shares the same guarantee.
        let legacy = ObjectFileHandler.versionedPath(
            for: "b", key: "k", versionId: "../../../etc/passwd")
        #expect(!legacy.contains(".."))
        #expect(legacy.contains("k.versions/"))
        #expect(legacy.hasSuffix(".obj"))

        // Legitimate ids (32-hex, and the literal "null" sentinel) pass through untouched.
        #expect(ObjectFileHandler.sanitizedVersionId("null") == "null")
        let real = ObjectMeta.generateVersionId()
        #expect(ObjectFileHandler.sanitizedVersionId(real) == real)
    }

    @Test("a corrupt 4-byte length prefix is rejected as an invalid header, not a giant allocation")
    func corruptLengthPrefixRejected() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }
        try FileManager.default.createDirectory(
            atPath: (path as NSString).deletingLastPathComponent, withIntermediateDirectories: true)

        // 0xFFFFFFFF length prefix + a little garbage - simulates bit rot in the first bytes.
        // Without the cap this attempted a ~4 GiB allocation on open (and the scrubber opens
        // every local shard, so one rotted byte could OOM the process).
        var bytes = Data([0xFF, 0xFF, 0xFF, 0xFF])
        bytes.append(Data(repeating: 0xAB, count: 64))
        try bytes.write(to: URL(fileURLWithPath: path))

        #expect(throws: ErasureCodedObjectHandlerError.self) {
            _ = try ErasureCodedShardReader(path: path)
        }
    }

    @Test("a decodable header with out-of-band field values is rejected as invalid")
    func corruptHeaderFieldsRejected() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }
        try FileManager.default.createDirectory(
            atPath: (path as NSString).deletingLastPathComponent, withIntermediateDirectories: true)

        // Valid JSON, hostile values: a negative stripeUnitSize would crash readStripe's
        // allocation; a huge one would OOM it. Both must read as "this copy can't be trusted".
        let badHeader = ErasureCodedShardHeader(
            shardIndex: 0, dataShards: 2, parityShards: 2, stripeUnitSize: -64, stripeCount: 1,
            objectMeta: testMeta(size: 128))
        let json = try JSONEncoder().encode(badHeader)
        var bytes = Data()
        withUnsafeBytes(of: UInt32(json.count).bigEndian) { bytes.append(contentsOf: $0) }
        bytes.append(json)
        try bytes.write(to: URL(fileURLWithPath: path))

        #expect(throws: ErasureCodedObjectHandlerError.self) {
            _ = try ErasureCodedShardReader(path: path)
        }
    }

    @Test("the version-aware path overloads select the plain vs versioned path by nil-ness")
    func versionAwarePathOverloads() {
        // nil versionId -> the plain, non-versioned path (identical to the base overload).
        let plain = ErasureCodedObjectHandler.shardPath(
            bucketName: "b", key: "k", versionId: nil, shardIndex: 3)
        #expect(
            plain == ErasureCodedObjectHandler.shardPath(bucketName: "b", key: "k", shardIndex: 3))
        #expect(plain.contains("k.ecshards/"))

        // A concrete versionId -> the versioned path (identical to versionedShardPath).
        let versioned = ErasureCodedObjectHandler.shardPath(
            bucketName: "b", key: "k", versionId: "v9", shardIndex: 3)
        #expect(
            versioned
                == ErasureCodedObjectHandler.versionedShardPath(
                    bucketName: "b", key: "k", versionId: "v9", shardIndex: 3))
        #expect(versioned.contains("k.versions/v9.ecshards/"))

        let plainBase = ErasureCodedObjectHandler.shardBasePath(
            bucketName: "b", key: "k", versionId: nil)
        #expect(plainBase == ErasureCodedObjectHandler.shardBasePath(bucketName: "b", key: "k"))
        let versionedBase = ErasureCodedObjectHandler.shardBasePath(
            bucketName: "b", key: "k", versionId: "v9")
        #expect(
            versionedBase
                == ErasureCodedObjectHandler.versionedShardBasePath(
                    bucketName: "b", key: "k", versionId: "v9"))
    }

    // MARK: - Writer/reader round trip

    @Test("writer + reader round-trips a header and several stripes")
    func writerReaderRoundTrips() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let header = ErasureCodedShardHeader(
            shardIndex: 3, dataShards: 4, parityShards: 2, stripeUnitSize: 16, stripeCount: 3,
            objectMeta: testMeta(size: 48))

        var writer = try ErasureCodedShardWriter(path: path, header: header)
        let stripes = (0..<3).map { i in Data((0..<16).map { UInt8(($0 + i * 16) % 256) }) }
        for stripe in stripes {
            try writer.appendStripe(stripe)
        }
        try writer.finish()

        #expect(FileManager.default.fileExists(atPath: path))

        let reader = try ErasureCodedShardReader(path: path)
        #expect(reader.header.shardIndex == 3)
        #expect(reader.header.dataShards == 4)
        #expect(reader.header.parityShards == 2)
        #expect(reader.header.stripeCount == 3)
        #expect(reader.header.objectMeta.size == 48)

        for (i, expected) in stripes.enumerated() {
            #expect(try reader.readStripe(i) == expected)
        }
        reader.close()
    }

    @Test("reading a corrupted stripe throws checksumMismatch instead of returning bad bytes")
    func corruptedStripeThrows() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let header = ErasureCodedShardHeader(
            shardIndex: 0, dataShards: 2, parityShards: 1, stripeUnitSize: 8, stripeCount: 1,
            objectMeta: testMeta(size: 16))
        var writer = try ErasureCodedShardWriter(path: path, header: header)
        try writer.appendStripe(Data(repeating: 0xAB, count: 8))
        try writer.finish()

        // Flip a byte inside the stripe payload (after the 4-byte length prefix, header JSON,
        // and 32-byte checksum) without touching the checksum itself.
        var raw = try Data(contentsOf: URL(fileURLWithPath: path))
        let corruptOffset = raw.count - 1
        raw[corruptOffset] = raw[corruptOffset] ^ 0xFF
        try raw.write(to: URL(fileURLWithPath: path))

        let reader = try ErasureCodedShardReader(path: path)
        #expect(throws: ErasureCodedObjectHandlerError.self) {
            try reader.readStripe(0)
        }
        reader.close()
    }

    @Test("reading an out-of-range stripe index throws")
    func outOfRangeStripeThrows() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let header = ErasureCodedShardHeader(
            shardIndex: 0, dataShards: 2, parityShards: 1, stripeUnitSize: 8, stripeCount: 1,
            objectMeta: testMeta(size: 16))
        var writer = try ErasureCodedShardWriter(path: path, header: header)
        try writer.appendStripe(Data(repeating: 0, count: 8))
        try writer.finish()

        let reader = try ErasureCodedShardReader(path: path)
        #expect(throws: ErasureCodedObjectHandlerError.self) {
            try reader.readStripe(5)
        }
        reader.close()
    }

    @Test("a failed write (aborted mid-way) never leaves a visible file behind")
    func abortedWriteLeavesNoFile() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let header = ErasureCodedShardHeader(
            shardIndex: 0, dataShards: 2, parityShards: 1, stripeUnitSize: 8, stripeCount: 1,
            objectMeta: testMeta(size: 8))
        var writer = try ErasureCodedShardWriter(path: path, header: header)
        try writer.appendStripe(Data(repeating: 0, count: 8))
        writer.abort()

        #expect(!FileManager.default.fileExists(atPath: path))
    }
}
