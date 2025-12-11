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

@Suite("ObjectFileHandler Versioning Tests", .serialized)
struct ObjectFileHandlerVersioningTests {

    private func createTestMetadata(key: String = "test-file.txt") -> ObjectMeta {
        ObjectMeta(
            bucketName: "test-bucket",
            key: key,
            size: 1024,
            contentType: "text/plain",
            etag: "abc123",
            metadata: [:],
            updatedAt: Date()
        )
    }

    private func createTestData(_ content: String = "Test content") -> Data {
        content.data(using: .utf8)!
    }

    private func setupTestBucket() {
        let bucketURL = BucketHandler.bucketURL(for: "test-bucket")
        try? FileManager.default.createDirectory(
            at: bucketURL, withIntermediateDirectories: true)
    }

    private func cleanupTestBucket() {
        let bucketURL = BucketHandler.bucketURL(for: "test-bucket")
        try? FileManager.default.removeItem(at: bucketURL)
    }

    @Test("writeVersioned - Enabled versioning creates unique version ID")
    func testWriteVersionedEnabled() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()
        let data = createTestData("Version 1")

        let versionId = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: data,
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        // Version ID should be valid format (32 hex chars)
        #expect(versionId.count == 32)
        #expect(versionId.allSatisfy { $0.isHexDigit })
        #expect(versionId == versionId.lowercased())

        // File should exist at versioned path
        let versionedPath = ObjectFileHandler.versionedPath(
            for: "test-bucket", key: "test-file.txt", versionId: versionId)
        #expect(FileManager.default.fileExists(atPath: versionedPath))
    }

    @Test("writeVersioned - Multiple versions create unique IDs")
    func testWriteVersionedMultipleVersions() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        let versionId1 = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Version 1"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        let versionId2 = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Version 2"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        let versionId3 = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Version 3"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        // All version IDs should be unique
        #expect(versionId1 != versionId2)
        #expect(versionId2 != versionId3)
        #expect(versionId1 != versionId3)

        // All files should exist
        #expect(
            FileManager.default.fileExists(
                atPath: ObjectFileHandler.versionedPath(
                    for: "test-bucket", key: "test-file.txt", versionId: versionId1)))
        #expect(
            FileManager.default.fileExists(
                atPath: ObjectFileHandler.versionedPath(
                    for: "test-bucket", key: "test-file.txt", versionId: versionId2)))
        #expect(
            FileManager.default.fileExists(
                atPath: ObjectFileHandler.versionedPath(
                    for: "test-bucket", key: "test-file.txt", versionId: versionId3)))
    }

    @Test("writeVersioned - Suspended versioning uses null version ID")
    func testWriteVersionedSuspended() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()
        let data = createTestData("Suspended version")

        let versionId = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: data,
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .suspended
        )

        #expect(versionId == "null")

        // File should exist at null version path
        let nullVersionPath = ObjectFileHandler.versionedPath(
            for: "test-bucket", key: "test-file.txt", versionId: "null")
        #expect(FileManager.default.fileExists(atPath: nullVersionPath))
    }

    @Test("writeVersioned - Disabled versioning uses non-versioned path")
    func testWriteVersionedDisabled() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()
        let data = createTestData("Non-versioned content")

        let versionId = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: data,
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .disabled
        )

        #expect(versionId == "null")

        // File should exist at non-versioned path
        let normalPath = ObjectFileHandler.storagePath(for: "test-bucket", key: "test-file.txt")
        #expect(FileManager.default.fileExists(atPath: normalPath))
    }

    @Test("writeVersioned - Updates .latest pointer")
    func testWriteVersionedUpdatesLatestPointer() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        let versionId1 = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Version 1"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        // Check .latest pointer points to version 1
        let latestVersionId1 = try ObjectFileHandler.getLatestVersionId(
            bucketName: "test-bucket", key: "test-file.txt")
        #expect(latestVersionId1 == versionId1)

        // Add version 2
        let versionId2 = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Version 2"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        // Check .latest pointer updated to version 2
        let latestVersionId2 = try ObjectFileHandler.getLatestVersionId(
            bucketName: "test-bucket", key: "test-file.txt")
        #expect(latestVersionId2 == versionId2)
    }

    @Test("writeVersioned - Marks previous versions as not latest")
    func testWriteVersionedMarksPreviousNotLatest() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        let versionId1 = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Version 1"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        let versionId2 = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Version 2"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        // Read both versions
        let (meta1, _) = try ObjectFileHandler.read(
            from: ObjectFileHandler.versionedPath(
                for: "test-bucket", key: "test-file.txt", versionId: versionId1),
            loadData: false
        )
        let (meta2, _) = try ObjectFileHandler.read(
            from: ObjectFileHandler.versionedPath(
                for: "test-bucket", key: "test-file.txt", versionId: versionId2),
            loadData: false
        )

        #expect(meta1.isLatest == false)
        #expect(meta2.isLatest == true)
    }

    @Test("readVersion - Reads latest version by default")
    func testReadVersionLatestByDefault() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Version 1"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Latest Version"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        let (_, readData) = try ObjectFileHandler.readVersion(
            bucketName: "test-bucket",
            key: "test-file.txt",
            versionId: nil
        )

        #expect(readData != nil)
        #expect(String(data: readData!, encoding: .utf8) == "Latest Version")
    }

    @Test("readVersion - Reads specific version by ID")
    func testReadVersionSpecificById() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        let versionId1 = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("First Version Content"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Second Version Content"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        // Read specific version
        let (meta, readData) = try ObjectFileHandler.readVersion(
            bucketName: "test-bucket",
            key: "test-file.txt",
            versionId: versionId1
        )

        #expect(readData != nil)
        #expect(String(data: readData!, encoding: .utf8) == "First Version Content")
        #expect(meta.versionId == versionId1)
    }

    @Test("readVersion - Throws for non-existent version")
    func testReadVersionNonExistent() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Some content"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        // Try to read non-existent version
        let fakeVersionId = "00000000000000000000000000000000"

        #expect(throws: (any Error).self) {
            _ = try ObjectFileHandler.readVersion(
                bucketName: "test-bucket",
                key: "test-file.txt",
                versionId: fakeVersionId
            )
        }
    }

    @Test("readVersion - Falls back to non-versioned path")
    func testReadVersionFallbackToNonVersioned() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        // Create non-versioned file directly
        let metadata = createTestMetadata()
        let data = createTestData("Non-versioned file")
        let path = ObjectFileHandler.storagePath(for: "test-bucket", key: "test-file.txt")
        try ObjectFileHandler.write(metadata: metadata, data: data, to: path)

        // Read without versionId should find it
        let (_, readData) = try ObjectFileHandler.readVersion(
            bucketName: "test-bucket",
            key: "test-file.txt",
            versionId: nil
        )

        #expect(readData != nil)
        #expect(String(data: readData!, encoding: .utf8) == "Non-versioned file")
    }

    @Test("listVersions - Returns all versions of a key")
    func testListVersionsAllVersions() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("V1"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("V2"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("V3"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        let versions = try ObjectFileHandler.listVersions(
            bucketName: "test-bucket", key: "test-file.txt")

        #expect(versions.count == 3)

        // Should be sorted by date descending (newest first)
        for i in 0..<versions.count - 1 {
            #expect(versions[i].updatedAt >= versions[i + 1].updatedAt)
        }
    }

    @Test("listVersions - Returns empty for non-existent key")
    func testListVersionsNonExistentKey() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let versions = try ObjectFileHandler.listVersions(
            bucketName: "test-bucket", key: "nonexistent.txt")

        #expect(versions.isEmpty)
    }

    @Test("listVersions - Includes non-versioned file")
    func testListVersionsIncludesNonVersioned() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        // Create non-versioned file
        let metadata = createTestMetadata()
        let data = createTestData("Non-versioned")
        let path = ObjectFileHandler.storagePath(for: "test-bucket", key: "test-file.txt")
        try ObjectFileHandler.write(metadata: metadata, data: data, to: path)

        let versions = try ObjectFileHandler.listVersions(
            bucketName: "test-bucket", key: "test-file.txt")

        #expect(versions.count == 1)
    }

    @Test("listAllVersions - Returns versions across multiple keys")
    func testListAllVersionsMultipleKeys() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let meta1 = createTestMetadata(key: "file1.txt")
        let meta2 = createTestMetadata(key: "file2.txt")

        _ = try ObjectFileHandler.writeVersioned(
            metadata: meta1,
            data: createTestData("File1 V1"),
            bucketName: "test-bucket",
            key: "file1.txt",
            versioningStatus: .enabled
        )

        _ = try ObjectFileHandler.writeVersioned(
            metadata: meta1,
            data: createTestData("File1 V2"),
            bucketName: "test-bucket",
            key: "file1.txt",
            versioningStatus: .enabled
        )

        _ = try ObjectFileHandler.writeVersioned(
            metadata: meta2,
            data: createTestData("File2 V1"),
            bucketName: "test-bucket",
            key: "file2.txt",
            versioningStatus: .enabled
        )

        let (versions, deleteMarkers, _, _, _, _) = try ObjectFileHandler.listAllVersions(
            bucketName: "test-bucket")

        #expect(versions.count == 3)
        #expect(deleteMarkers.count == 0)

        let keys = versions.map { $0.key }
        #expect(keys.filter { $0 == "file1.txt" }.count == 2)
        #expect(keys.filter { $0 == "file2.txt" }.count == 1)
    }

    @Test("listAllVersions - Respects prefix filter")
    func testListAllVersionsWithPrefix() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metaDocs = createTestMetadata(key: "docs/readme.txt")
        let metaImages = createTestMetadata(key: "images/photo.jpg")

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metaDocs,
            data: createTestData("Doc content"),
            bucketName: "test-bucket",
            key: "docs/readme.txt",
            versioningStatus: .enabled
        )

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metaImages,
            data: createTestData("Image content"),
            bucketName: "test-bucket",
            key: "images/photo.jpg",
            versioningStatus: .enabled
        )

        let (versions, _, _, _, _, _) = try ObjectFileHandler.listAllVersions(
            bucketName: "test-bucket", prefix: "docs/")

        #expect(versions.count == 1)
        #expect(versions.first?.key == "docs/readme.txt")
    }

    @Test("deleteVersion - Deletes specific version")
    func testDeleteVersionSpecific() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        let versionId1 = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("V1"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        let versionId2 = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("V2"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        // Delete version 1
        try ObjectFileHandler.deleteVersion(
            bucketName: "test-bucket", key: "test-file.txt", versionId: versionId1)

        // Version 1 should be gone
        let path1 = ObjectFileHandler.versionedPath(
            for: "test-bucket", key: "test-file.txt", versionId: versionId1)
        #expect(!FileManager.default.fileExists(atPath: path1))

        // Version 2 should still exist
        let path2 = ObjectFileHandler.versionedPath(
            for: "test-bucket", key: "test-file.txt", versionId: versionId2)
        #expect(FileManager.default.fileExists(atPath: path2))
    }

    @Test("deleteVersion - Throws for non-existent version")
    func testDeleteVersionNonExistent() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        #expect(throws: (any Error).self) {
            try ObjectFileHandler.deleteVersion(
                bucketName: "test-bucket", key: "test-file.txt",
                versionId: "00000000000000000000000000000000")
        }
    }

    @Test("createDeleteMarker - Creates delete marker")
    func testCreateDeleteMarker() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Original"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        let deleteMarkerId = try ObjectFileHandler.createDeleteMarker(
            bucketName: "test-bucket", key: "test-file.txt")

        #expect(deleteMarkerId.versionId!.count == 32)

        // Delete marker should be the latest
        let latestVersionId = try ObjectFileHandler.getLatestVersionId(
            bucketName: "test-bucket", key: "test-file.txt")
        #expect(latestVersionId == deleteMarkerId.versionId)

        // Read delete marker and verify isDeleteMarker flag
        let (deleteMeta, _) = try ObjectFileHandler.read(
            from: ObjectFileHandler.versionedPath(
                for: "test-bucket", key: "test-file.txt", versionId: deleteMarkerId.versionId!),
            loadData: false
        )
        #expect(deleteMeta.isDeleteMarker == true)
        #expect(deleteMeta.isLatest == true)
    }

    @Test("isVersioned - Returns true for versioned objects")
    func testIsVersionedTrue() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Content"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        #expect(
            ObjectFileHandler.isVersioned(bucketName: "test-bucket", key: "test-file.txt") == true)
    }

    @Test("isVersioned - Returns false for non-versioned objects")
    func testIsVersionedFalse() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()
        let data = createTestData("Content")
        let path = ObjectFileHandler.storagePath(for: "test-bucket", key: "test-file.txt")
        try ObjectFileHandler.write(metadata: metadata, data: data, to: path)

        #expect(
            ObjectFileHandler.isVersioned(bucketName: "test-bucket", key: "test-file.txt") == false)
    }

    @Test("isVersioned - Returns false for non-existent key")
    func testIsVersionedNonExistent() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        #expect(
            ObjectFileHandler.isVersioned(bucketName: "test-bucket", key: "nonexistent.txt")
                == false)
    }

    @Test("keyExistsVersioned - Returns true for versioned key")
    func testKeyExistsVersionedTrue() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Content"),
            bucketName: "test-bucket",
            key: "test-file.txt",
            versioningStatus: .enabled
        )

        #expect(
            ObjectFileHandler.versionedKeyExists(bucketName: "test-bucket", key: "test-file.txt")
                == true)
    }

    @Test("keyExistsVersioned - Returns true for non-versioned key")
    func testKeyExistsVersionedNonVersionedTrue() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = createTestMetadata()
        let data = createTestData("Content")
        let path = ObjectFileHandler.storagePath(for: "test-bucket", key: "test-file.txt")
        try ObjectFileHandler.write(metadata: metadata, data: data, to: path)

        #expect(
            ObjectFileHandler.versionedKeyExists(bucketName: "test-bucket", key: "test-file.txt")
                == true)
    }

    @Test("keyExistsVersioned - Returns false for non-existent key")
    func testKeyExistsVersionedFalse() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        #expect(
            ObjectFileHandler.versionedKeyExists(bucketName: "test-bucket", key: "nonexistent.txt")
                == false)
    }

    @Test("versionedBasePath - Generates correct path")
    func testVersionedBasePath() {
        let basePath = ObjectFileHandler.versionedBasePath(for: "my-bucket", key: "folder/file.txt")

        #expect(basePath.contains("my-bucket"))
        #expect(basePath.contains("folder/file.txt.versions/"))
        #expect(basePath.hasSuffix("/"))
    }

    @Test("versionedPath - Generates correct path")
    func testVersionedPath() {
        let path = ObjectFileHandler.versionedPath(
            for: "my-bucket", key: "file.txt", versionId: "abc123")

        #expect(path.contains("my-bucket"))
        #expect(path.contains("file.txt.versions/"))
        #expect(path.contains("abc123.obj"))
        #expect(path.hasSuffix(".obj"))
    }

    @Test("latestPointerPath - Generates correct path")
    func testLatestPointerPath() {
        let path = ObjectFileHandler.latestPointerPath(for: "my-bucket", key: "file.txt")

        #expect(path.contains("my-bucket"))
        #expect(path.contains("file.txt.versions/"))
        #expect(path.contains(".latest"))
    }

    @Test("Path traversal prevention in key")
    func testPathTraversalPreventionInKey() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let metadata = ObjectMeta(
            bucketName: "test-bucket",
            key: "../../../etc/passwd",  // Malicious key
            size: 100,
            contentType: "text/plain",
            etag: "test",
            updatedAt: Date()
        )

        _ = try ObjectFileHandler.writeVersioned(
            metadata: metadata,
            data: createTestData("Malicious content"),
            bucketName: "test-bucket",
            key: "../../../etc/passwd",
            versioningStatus: .enabled
        )

        // File should be created in bucket directory, not outside
        let basePath = ObjectFileHandler.versionedBasePath(
            for: "test-bucket", key: "../../../etc/passwd")
        #expect(basePath.contains("test-bucket"))
        // Should have sanitized the path (removed ..)
    }

    @Test("generateVersionId - Creates valid hex string")
    func testGenerateVersionId() {
        let versionId = ObjectMeta.generateVersionId()

        #expect(versionId.count == 32)
        #expect(versionId.allSatisfy { $0.isHexDigit })
        #expect(versionId == versionId.lowercased())
        #expect(!versionId.contains("-"))
    }

    @Test("generateVersionId - Creates unique IDs")
    func testGenerateVersionIdUnique() {
        var ids = Set<String>()
        for _ in 0..<1000 {
            ids.insert(ObjectMeta.generateVersionId())
        }
        // All 1000 should be unique
        #expect(ids.count == 1000)
    }
}
