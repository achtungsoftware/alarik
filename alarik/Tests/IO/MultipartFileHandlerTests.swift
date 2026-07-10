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

import Crypto
import Foundation
import Testing

@testable import Alarik

@Suite("MultipartFileHandler tests", .serialized)
struct MultipartFileHandlerTests {

    private func cleanupMultipart() throws {
        let fm = FileManager.default
        let multipartPath = MultipartFileHandler.rootPath
        if fm.fileExists(atPath: multipartPath) {
            let contents = try fm.contentsOfDirectory(atPath: multipartPath)
            for item in contents {
                if item != ".gitkeep" {
                    try fm.removeItem(atPath: "\(multipartPath)\(item)")
                }
            }
        }
    }

    @Test("generateUploadId returns valid unique IDs")
    func testGenerateUploadId() throws {
        let id1 = MultipartFileHandler.generateUploadId()
        let id2 = MultipartFileHandler.generateUploadId()

        #expect(id1 != id2)
        #expect(id1.count == 32)  // UUID without dashes
        #expect(id2.count == 32)
        #expect(!id1.contains("-"))
        #expect(id1 == id1.lowercased())
    }

    /// A syntactically-valid upload id (32 lowercase hex chars, matching `generateUploadId()`'s
    /// format) for tests that need a fixed literal rather than a freshly-generated one - anything
    /// not matching this shape is now rejected before it ever reaches a path.
    private static let validUploadId = "0123456789abcdef0123456789abcdef"

    @Test("uploadPath generates correct path structure")
    func testUploadPath() throws {
        let path = try MultipartFileHandler.uploadPath(
            for: "test-bucket", uploadId: Self.validUploadId)
        #expect(path == "Storage/multipart/test-bucket/\(Self.validUploadId)/")
    }

    @Test("metadataPath generates correct path")
    func testMetadataPath() throws {
        let path = try MultipartFileHandler.metadataPath(
            for: "test-bucket", uploadId: Self.validUploadId)
        #expect(path == "Storage/multipart/test-bucket/\(Self.validUploadId)/meta.json")
    }

    @Test("partPath generates correct path for part numbers")
    func testPartPath() throws {
        let path1 = try MultipartFileHandler.partPath(
            for: "bucket", uploadId: Self.validUploadId, partNumber: 1)
        let path5 = try MultipartFileHandler.partPath(
            for: "bucket", uploadId: Self.validUploadId, partNumber: 5)

        #expect(path1 == "Storage/multipart/bucket/\(Self.validUploadId)/part-1")
        #expect(path5 == "Storage/multipart/bucket/\(Self.validUploadId)/part-5")
    }

    @Test("partMetaPath generates correct path")
    func testPartMetaPath() throws {
        let path = try MultipartFileHandler.partMetaPath(
            for: "bucket", uploadId: Self.validUploadId, partNumber: 3)
        #expect(path == "Storage/multipart/bucket/\(Self.validUploadId)/part-3.meta")
    }

    @Test("uploadPath rejects an upload id that isn't exactly 32 lowercase hex characters")
    func testUploadPathRejectsMalformedUploadId() throws {
        // The exact shape this review found exploitable: a traversal payload disguised as an
        // upload id must never reach path construction, regardless of what's actually on disk.
        for malformed in [
            "../../etc/passwd", "short", String(repeating: "a", count: 33),
            "ABCDEF0123456789ABCDEF0123456789", "0123456789abcdef0123456789abcd/",
        ] {
            #expect(throws: (any Error).self) {
                try MultipartFileHandler.uploadPath(for: "bucket", uploadId: malformed)
            }
        }
    }

    @Test("createUpload creates directory and metadata file")
    func testCreateUpload() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: "test-bucket",
            key: "test/file.txt",
            contentType: "text/plain",
            metadata: ["custom": "value"]
        )

        #expect(uploadId.count == 32)

        // Verify directory exists
        let uploadDir = try MultipartFileHandler.uploadPath(for: "test-bucket", uploadId: uploadId)
        #expect(FileManager.default.fileExists(atPath: uploadDir))

        // Verify metadata exists
        let metaPath = try MultipartFileHandler.metadataPath(for: "test-bucket", uploadId: uploadId)
        #expect(FileManager.default.fileExists(atPath: metaPath))

        // Verify metadata content
        let meta = try MultipartFileHandler.getUploadMeta(
            bucketName: "test-bucket", uploadId: uploadId)
        #expect(meta.bucketName == "test-bucket")
        #expect(meta.key == "test/file.txt")
        #expect(meta.contentType == "text/plain")
        #expect(meta.metadata["custom"] == "value")
    }

    @Test("uploadExists returns correct values")
    func testUploadExists() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        #expect(!MultipartFileHandler.uploadExists(bucketName: "bucket", uploadId: "nonexistent"))

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: "bucket",
            key: "file.txt"
        )

        #expect(MultipartFileHandler.uploadExists(bucketName: "bucket", uploadId: uploadId))
    }

    @Test("writePart stores part data and returns ETag")
    func testWritePart() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: "bucket",
            key: "file.txt"
        )

        let partData = Data("Hello, World!".utf8)
        let etag = try MultipartFileHandler.writePart(
            bucketName: "bucket",
            uploadId: uploadId,
            partNumber: 1,
            data: partData
        )

        // Verify ETag is MD5 hash
        let expectedEtag = Insecure.MD5.hash(data: partData).hex
        #expect(etag == expectedEtag)

        // Verify part file exists
        let partPath = try MultipartFileHandler.partPath(
            for: "bucket", uploadId: uploadId, partNumber: 1)
        #expect(FileManager.default.fileExists(atPath: partPath))

        // Verify part data
        let storedData = try Data(contentsOf: URL(fileURLWithPath: partPath))
        #expect(storedData == partData)
    }

    @Test("writePart validates part number range")
    func testWritePartValidatesPartNumber() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: "bucket",
            key: "file.txt"
        )

        let data = Data("test".utf8)

        // Part 0 should fail
        #expect(throws: (any Error).self) {
            try MultipartFileHandler.writePart(
                bucketName: "bucket",
                uploadId: uploadId,
                partNumber: 0,
                data: data
            )
        }

        // Part 10001 should fail
        #expect(throws: (any Error).self) {
            try MultipartFileHandler.writePart(
                bucketName: "bucket",
                uploadId: uploadId,
                partNumber: 10001,
                data: data
            )
        }

        // Part 1 should succeed
        _ = try MultipartFileHandler.writePart(
            bucketName: "bucket",
            uploadId: uploadId,
            partNumber: 1,
            data: data
        )

        // Part 10000 should succeed
        _ = try MultipartFileHandler.writePart(
            bucketName: "bucket",
            uploadId: uploadId,
            partNumber: 10000,
            data: data
        )
    }

    @Test("writePart fails for non-existent upload")
    func testWritePartNonExistentUpload() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        #expect(throws: (any Error).self) {
            try MultipartFileHandler.writePart(
                bucketName: "bucket",
                uploadId: "nonexistent",
                partNumber: 1,
                data: Data("test".utf8)
            )
        }
    }

    @Test("listParts returns uploaded parts in order")
    func testListParts() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: "bucket",
            key: "file.txt"
        )

        // Upload parts out of order
        _ = try MultipartFileHandler.writePart(
            bucketName: "bucket", uploadId: uploadId, partNumber: 3, data: Data("part3".utf8))
        _ = try MultipartFileHandler.writePart(
            bucketName: "bucket", uploadId: uploadId, partNumber: 1, data: Data("part1".utf8))
        _ = try MultipartFileHandler.writePart(
            bucketName: "bucket", uploadId: uploadId, partNumber: 2, data: Data("part2".utf8))

        let (parts, isTruncated, _) = try MultipartFileHandler.listParts(
            bucketName: "bucket",
            uploadId: uploadId
        )

        #expect(parts.count == 3)
        #expect(!isTruncated)
        #expect(parts[0].partNumber == 1)
        #expect(parts[1].partNumber == 2)
        #expect(parts[2].partNumber == 3)
    }

    @Test("listParts respects maxParts limit")
    func testListPartsMaxParts() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: "bucket",
            key: "file.txt"
        )

        for i in 1...5 {
            _ = try MultipartFileHandler.writePart(
                bucketName: "bucket", uploadId: uploadId, partNumber: i, data: Data("part\(i)".utf8)
            )
        }

        let (parts, isTruncated, nextMarker) = try MultipartFileHandler.listParts(
            bucketName: "bucket",
            uploadId: uploadId,
            maxParts: 2
        )

        #expect(parts.count == 2)
        #expect(isTruncated)
        #expect(nextMarker == 2)
    }

    @Test("listParts respects partNumberMarker")
    func testListPartsMarker() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: "bucket",
            key: "file.txt"
        )

        for i in 1...5 {
            _ = try MultipartFileHandler.writePart(
                bucketName: "bucket", uploadId: uploadId, partNumber: i, data: Data("part\(i)".utf8)
            )
        }

        let (parts, _, _) = try MultipartFileHandler.listParts(
            bucketName: "bucket",
            uploadId: uploadId,
            partNumberMarker: 2  // Start after part 2
        )

        #expect(parts.count == 3)
        #expect(parts[0].partNumber == 3)
        #expect(parts[1].partNumber == 4)
        #expect(parts[2].partNumber == 5)
    }

    @Test("abortUpload removes upload directory")
    func testAbortUpload() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: "bucket",
            key: "file.txt"
        )

        _ = try MultipartFileHandler.writePart(
            bucketName: "bucket", uploadId: uploadId, partNumber: 1, data: Data("part1".utf8))

        let uploadDir = try MultipartFileHandler.uploadPath(for: "bucket", uploadId: uploadId)
        #expect(FileManager.default.fileExists(atPath: uploadDir))

        try MultipartFileHandler.abortUpload(bucketName: "bucket", uploadId: uploadId)

        #expect(!FileManager.default.fileExists(atPath: uploadDir))
    }

    @Test("abortUpload fails for non-existent upload")
    func testAbortNonExistentUpload() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        #expect(throws: (any Error).self) {
            try MultipartFileHandler.abortUpload(bucketName: "bucket", uploadId: "nonexistent")
        }
    }

    @Test("listUploads returns all uploads for bucket")
    func testListUploads() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        _ = try MultipartFileHandler.createUpload(bucketName: "bucket", key: "file1.txt")
        _ = try MultipartFileHandler.createUpload(bucketName: "bucket", key: "file2.txt")
        _ = try MultipartFileHandler.createUpload(bucketName: "other-bucket", key: "file3.txt")

        let (uploads, isTruncated, _, _) = try MultipartFileHandler.listUploads(
            bucketName: "bucket")

        #expect(uploads.count == 2)
        #expect(!isTruncated)

        let keys = uploads.map { $0.key }.sorted()
        #expect(keys == ["file1.txt", "file2.txt"])
    }

    @Test("listUploads filters by prefix")
    func testListUploadsWithPrefix() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        _ = try MultipartFileHandler.createUpload(bucketName: "bucket", key: "docs/file1.txt")
        _ = try MultipartFileHandler.createUpload(bucketName: "bucket", key: "docs/file2.txt")
        _ = try MultipartFileHandler.createUpload(bucketName: "bucket", key: "images/photo.jpg")

        let (uploads, _, _, _) = try MultipartFileHandler.listUploads(
            bucketName: "bucket",
            prefix: "docs/"
        )

        #expect(uploads.count == 2)
        #expect(uploads.allSatisfy { $0.key.hasPrefix("docs/") })
    }

    @Test("listUploads returns empty for bucket with no uploads")
    func testListUploadsEmpty() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        let (uploads, isTruncated, _, _) = try MultipartFileHandler.listUploads(
            bucketName: "empty-bucket"
        )

        #expect(uploads.isEmpty)
        #expect(!isTruncated)
    }

    @Test("completeUpload concatenates parts and creates object")
    func testCompleteUpload() throws {
        try cleanupMultipart()
        try StorageHelper.cleanStorage()
        defer {
            try? cleanupMultipart()
            try? StorageHelper.cleanStorage()
        }

        // Create bucket directory
        try FileManager.default.createDirectory(
            atPath: BucketHandler.rootPath + "test-bucket/",
            withIntermediateDirectories: true
        )

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: "test-bucket",
            key: "complete-test.txt",
            contentType: "text/plain"
        )

        let part1Data = Data("Hello, ".utf8)
        let part2Data = Data("World!".utf8)

        let etag1 = try MultipartFileHandler.writePart(
            bucketName: "test-bucket", uploadId: uploadId, partNumber: 1, data: part1Data)
        let etag2 = try MultipartFileHandler.writePart(
            bucketName: "test-bucket", uploadId: uploadId, partNumber: 2, data: part2Data)

        let (finalEtag, size, _) = try MultipartFileHandler.completeUpload(
            bucketName: "test-bucket",
            uploadId: uploadId,
            parts: [(1, etag1), (2, etag2)],
            versioningStatus: .disabled
        )

        // Verify size
        #expect(size == part1Data.count + part2Data.count)

        // Verify ETag format (MD5 of concatenated ETags + part count)
        #expect(finalEtag.contains("-2"))

        // Verify multipart upload directory was cleaned up
        let uploadDir = try MultipartFileHandler.uploadPath(for: "test-bucket", uploadId: uploadId)
        #expect(!FileManager.default.fileExists(atPath: uploadDir))

        // Verify object was created
        let objectPath = ObjectFileHandler.storagePath(for: "test-bucket", key: "complete-test.txt")
        #expect(FileManager.default.fileExists(atPath: objectPath))

        // Verify object content
        let (meta, data) = try ObjectFileHandler.read(from: objectPath)
        #expect(meta.key == "complete-test.txt")
        #expect(meta.contentType == "text/plain")
        #expect(String(data: data!, encoding: .utf8) == "Hello, World!")
    }

    @Test("completeUpload fails with ETag mismatch")
    func testCompleteUploadETagMismatch() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: "bucket",
            key: "file.txt"
        )

        _ = try MultipartFileHandler.writePart(
            bucketName: "bucket", uploadId: uploadId, partNumber: 1, data: Data("test".utf8))

        #expect(throws: (any Error).self) {
            try MultipartFileHandler.completeUpload(
                bucketName: "bucket",
                uploadId: uploadId,
                parts: [(1, "wrongetag")],
                versioningStatus: .disabled
            )
        }
    }

    @Test("completeUpload fails for non-existent upload")
    func testCompleteNonExistentUpload() throws {
        try cleanupMultipart()
        defer { try? cleanupMultipart() }

        #expect(throws: (any Error).self) {
            try MultipartFileHandler.completeUpload(
                bucketName: "bucket",
                uploadId: "nonexistent",
                parts: [(1, "etag")],
                versioningStatus: .disabled
            )
        }
    }
}
