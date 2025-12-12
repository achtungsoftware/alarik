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

@Suite("ObjectFileHandler Tests", .serialized)
struct ObjectFileHandlerTests {

    private func createTestMetadata() -> ObjectMeta {
        ObjectMeta(
            bucketName: "test-bucket",
            key: "test-key.txt",
            size: 1024,
            contentType: "text/plain",
            etag: "abc123",
            metadata: ["author": "test-user", "version": "1.0"],
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    private func createTestData() -> Data {
        "Hello, World! This is test data.".data(using: .utf8)!
    }

    private func createTempPath() -> String {
        let temp = NSTemporaryDirectory()
        let uuid = UUID().uuidString
        return "\(temp)test-\(uuid)/file.obj"
    }

    private func cleanup(path: String) {
        let fileURL = URL(fileURLWithPath: path)
        let folderURL = fileURL.deletingLastPathComponent()
        try? FileManager.default.removeItem(at: folderURL)
    }

    @Test("Write and read basic file")
    func writeAndReadBasicFile() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let metadata = createTestMetadata()
        let data = createTestData()

        // Write
        try ObjectFileHandler.write(metadata: metadata, data: data, to: path)

        // Verify file exists
        #expect(FileManager.default.fileExists(atPath: path))

        // Read
        let (readMeta, readData) = try ObjectFileHandler.read(from: path)

        // Verify metadata
        #expect(readMeta.bucketName == metadata.bucketName)
        #expect(readMeta.key == metadata.key)
        #expect(readMeta.size == metadata.size)
        #expect(readMeta.contentType == metadata.contentType)
        #expect(readMeta.etag == metadata.etag)
        #expect(readMeta.metadata == metadata.metadata)
        #expect(
            readMeta.updatedAt.timeIntervalSince1970 == metadata.updatedAt.timeIntervalSince1970)

        // Verify data
        #expect(readData == data)
    }

    @Test("Write with empty data")
    func writeWithEmptyData() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let metadata = createTestMetadata()
        let emptyData = Data()

        try ObjectFileHandler.write(metadata: metadata, data: emptyData, to: path)
        let (_, readData) = try ObjectFileHandler.read(from: path)

        #expect(readData?.isEmpty == true)
    }

    @Test("Write with 1MB of data")
    func writeWith1MBData() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let metadata = createTestMetadata()
        // Create 1MB of data
        let largeData = Data(repeating: 0x42, count: 1024 * 1024)

        try ObjectFileHandler.write(metadata: metadata, data: largeData, to: path)
        let (readMeta, readData) = try ObjectFileHandler.read(from: path)

        #expect(readMeta.key == metadata.key)
        #expect(readData == largeData)
    }

    @Test("Write with 10MB of data")
    func writeWith10MBData() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let metadata = createTestMetadata()
        // Create 1MB of data
        let largeData = Data(repeating: 0x42, count: 1024 * 1024 * 10)

        try ObjectFileHandler.write(metadata: metadata, data: largeData, to: path)
        let (readMeta, readData) = try ObjectFileHandler.read(from: path)

        #expect(readMeta.key == metadata.key)
        #expect(readData == largeData)
    }

    @Test("Write with 100MB of data")
    func writeWith100MBData() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let metadata = createTestMetadata()
        // Create 1MB of data
        let largeData = Data(repeating: 0x42, count: 1024 * 1024 * 100)

        try ObjectFileHandler.write(metadata: metadata, data: largeData, to: path)
        let (readMeta, readData) = try ObjectFileHandler.read(from: path)

        #expect(readMeta.key == metadata.key)
        #expect(readData == largeData)
    }

    @Test("Write with 1GB of data")
    func writeWith1GBData() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let metadata = createTestMetadata()
        // Create 1MB of data
        let largeData = Data(repeating: 0x42, count: 1024 * 1024 * 1000)

        try ObjectFileHandler.write(metadata: metadata, data: largeData, to: path)
        let (readMeta, readData) = try ObjectFileHandler.read(from: path)

        #expect(readMeta.key == metadata.key)
        #expect(readData == largeData)
    }

    @Test("Write creates intermediate directories")
    func writeCreatesIntermediateDirectories() throws {
        let temp = NSTemporaryDirectory()
        let uuid = UUID().uuidString
        let path = "\(temp)test-\(uuid)/deeply/nested/path/file.obj"
        defer { cleanup(path: path) }

        let metadata = createTestMetadata()
        let data = createTestData()

        try ObjectFileHandler.write(metadata: metadata, data: data, to: path)

        #expect(FileManager.default.fileExists(atPath: path))
    }

    @Test("Write overwrites existing file")
    func writeOverwritesExistingFile() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let metadata1 = createTestMetadata()
        let data1 = "First data".data(using: .utf8)!

        try ObjectFileHandler.write(metadata: metadata1, data: data1, to: path)

        // Overwrite with new data
        var metadata2 = createTestMetadata()
        metadata2.key = "updated-key.txt"
        metadata2.etag = "xyz789"
        let data2 = "Second data - updated".data(using: .utf8)!

        try ObjectFileHandler.write(metadata: metadata2, data: data2, to: path)

        let (readMeta, readData) = try ObjectFileHandler.read(from: path)

        #expect(readMeta.key == "updated-key.txt")
        #expect(readMeta.etag == "xyz789")
        #expect(readData == data2)
    }

    @Test("Read metadata only (HEAD)")
    func readMetadataOnly() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let metadata = createTestMetadata()
        let largeData = Data(repeating: 0xFF, count: 10 * 1024 * 1024)  // 10MB

        try ObjectFileHandler.write(metadata: metadata, data: largeData, to: path)

        // Read metadata only
        let (readMeta, readData) = try ObjectFileHandler.read(from: path, loadData: false)

        #expect(readMeta.bucketName == metadata.bucketName)
        #expect(readMeta.key == metadata.key)
        #expect(readData == nil)
    }

    @Test("Read non-existent file throws error")
    func readNonExistentFileThrowsError() throws {
        let path = "/nonexistent/path/file.obj"

        #expect(throws: (any Error).self) {
            try ObjectFileHandler.read(from: path)
        }
    }

    @Test("Read corrupted file (too short) throws error")
    func readCorruptedFileTooShort() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        // Create file with only 2 bytes (needs at least 4 for length prefix)
        let fileURL = URL(fileURLWithPath: path)
        try FileManager.default.createDirectory(
            at: fileURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try Data([0x00, 0x01]).write(to: fileURL)

        #expect(throws: (any Error).self) {
            try ObjectFileHandler.read(from: path)
        }
    }

    @Test("Read corrupted file (truncated metadata) throws error")
    func readCorruptedFileTruncatedMetadata() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        // Create file with length prefix claiming 1000 bytes but only containing 10
        let fileURL = URL(fileURLWithPath: path)
        try FileManager.default.createDirectory(
            at: fileURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )

        var data = Data()
        let fakeLength = UInt32(1000).bigEndian
        withUnsafeBytes(of: fakeLength) { data.append(contentsOf: $0) }
        data.append(Data(repeating: 0x00, count: 10))

        try data.write(to: fileURL)

        #expect(throws: (any Error).self) {
            try ObjectFileHandler.read(from: path)
        }
    }

    @Test("Storage path generation basic")
    func storagePathGenerationBasic() {
        let path = ObjectFileHandler.storagePath(for: "my-bucket", key: "file.txt")

        #expect(path.contains("my-bucket"))
        #expect(path.hasSuffix("file.txt.obj"))
    }

    @Test("Storage path handles nested keys")
    func storagePathHandlesNestedKeys() {
        let path = ObjectFileHandler.storagePath(for: "bucket", key: "folder/subfolder/file.txt")

        #expect(path.contains("folder/subfolder/file.txt.obj"))
    }

    @Test("Storage path sanitizes parent directory traversal")
    func storagePathSanitizesParentDirectoryTraversal() {
        let path = ObjectFileHandler.storagePath(for: "bucket", key: "../../../etc/passwd")

        #expect(!path.contains(".."))
        #expect(path.contains("///etc/passwd.obj"))
    }

    @Test("Storage path encodes special characters in bucket name")
    func storagePathEncodesSpecialCharacters() {
        let path = ObjectFileHandler.storagePath(for: "my bucket", key: "file.txt")

        #expect(path.contains("my%20bucket") || path.contains("my+bucket"))
    }

    @Test("Storage path handles multiple .. sequences")
    func storagePathHandlesMultipleParentSequences() {
        let path = ObjectFileHandler.storagePath(
            for: "bucket",
            key: "folder/../another/../file.txt"
        )

        #expect(!path.contains(".."))
    }

    @Test("Handles metadata with empty dictionary")
    func handlesMetadataWithEmptyDictionary() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        var metadata = createTestMetadata()
        metadata.metadata = [:]

        try ObjectFileHandler.write(metadata: metadata, data: createTestData(), to: path)
        let (readMeta, _) = try ObjectFileHandler.read(from: path)

        #expect(readMeta.metadata.isEmpty)
    }

    @Test("Handles metadata with special characters")
    func handlesMetadataWithSpecialCharacters() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        var metadata = createTestMetadata()
        metadata.key = "file with spaces & special!@#.txt"
        metadata.metadata = ["emoji": "🚀", "unicode": "こんにちは"]

        try ObjectFileHandler.write(metadata: metadata, data: createTestData(), to: path)
        let (readMeta, _) = try ObjectFileHandler.read(from: path)

        #expect(readMeta.key == "file with spaces & special!@#.txt")
        #expect(readMeta.metadata["emoji"] == "🚀")
        #expect(readMeta.metadata["unicode"] == "こんにちは")
    }

    @Test("Handles binary data correctly")
    func handlesBinaryDataCorrectly() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let metadata = createTestMetadata()
        // Create binary data with all byte values
        let binaryData = Data((0...255).map { UInt8($0) })

        try ObjectFileHandler.write(metadata: metadata, data: binaryData, to: path)
        let (_, readData) = try ObjectFileHandler.read(from: path)

        #expect(readData == binaryData)
    }

    @Test("Concurrent writes to different files")
    func concurrentWritesToDifferentFiles() async throws {
        let paths = (0..<10).map { _ in createTempPath() }
        defer { paths.forEach { cleanup(path: $0) } }

        try await withThrowingTaskGroup(of: Void.self) { group in
            for (index, path) in paths.enumerated() {
                group.addTask {
                    var metadata = self.createTestMetadata()
                    metadata.key = "file-\(index).txt"
                    let data = "Data for file \(index)".data(using: .utf8)!

                    try ObjectFileHandler.write(metadata: metadata, data: data, to: path)
                }
            }

            try await group.waitForAll()
        }

        // Verify all files were written correctly
        for (index, path) in paths.enumerated() {
            let (readMeta, _) = try ObjectFileHandler.read(from: path)
            #expect(readMeta.key == "file-\(index).txt")
        }
    }

    @Test("keyExists returns true for existing object file")
    func keyExistsReturnsTrueForExistingFile() throws {
        let path = createTempPath()
        defer { cleanup(path: path) }

        let metadata = createTestMetadata()
        let data = createTestData()
        try ObjectFileHandler.write(metadata: metadata, data: data, to: path)

        let exists = ObjectFileHandler.keyExists(
            for: metadata.bucketName,
            key: metadata.key,
            path: path  // explicit path
        )
        #expect(exists == true)

        // Also test without explicit path (uses storagePath)
        let existsViaStoragePath = ObjectFileHandler.keyExists(
            for: metadata.bucketName,
            key: metadata.key
        )
        #expect(existsViaStoragePath == false)  // because storagePath goes to rootPath, not temp
    }

    @Test("keyExists returns false for non-existent key")
    func keyExistsReturnsFalseForMissingKey() throws {
        let exists = ObjectFileHandler.keyExists(
            for: "nonexistent-bucket",
            key: "nonexistent/key.txt"
        )
        #expect(exists == false)
    }

    @Test("keyExists returns false when file does not exist at custom path")
    func keyExistsReturnsFalseForMissingCustomPath() throws {
        let path = "/tmp/alarik-test-this-file-does-not-exist-\(UUID().uuidString).obj"
        let exists = ObjectFileHandler.keyExists(
            for: "any-bucket",
            key: "any-key",
            path: path
        )
        #expect(exists == false)
    }

    @Test("keyExists returns true only for regular files (not directories)")
    func keyExistsIgnoresDirectories() throws {
        let tempDir = NSTemporaryDirectory()
        let uuid = UUID().uuidString
        let dirPath = "\(tempDir)alarik-test-dir-\(uuid)/"
        let filePath = "\(dirPath)file.obj"

        defer {
            try? FileManager.default.removeItem(atPath: dirPath)
        }

        try FileManager.default.createDirectory(atPath: dirPath, withIntermediateDirectories: true)

        // Directory exists, but no .obj file
        let existsForDir = ObjectFileHandler.keyExists(
            for: "test", key: "some/key", path: dirPath + "file.obj")
        #expect(existsForDir == false)

        // Now create actual file
        let metadata = createTestMetadata()
        try ObjectFileHandler.write(metadata: metadata, data: Data(), to: filePath)

        let existsForFile = ObjectFileHandler.keyExists(
            for: "test", key: "some/key", path: filePath)
        #expect(existsForFile == true)
    }

    @Test("keyExists works correctly with sanitized keys containing ..")
    func keyExistsWithParentTraversalInKey() throws {
        let bucket = "safe-bucket"
        let maliciousKey = "../../../etc/passwd"
        let expectedPath = ObjectFileHandler.storagePath(for: bucket, key: maliciousKey)

        // The storagePath function removes ".." so the final path should NOT allow traversal
        #expect(!expectedPath.contains(".."))

        // Since no file was written, keyExists should return false even with malicious key
        let exists = ObjectFileHandler.keyExists(for: bucket, key: maliciousKey)
        #expect(exists == false)

        // Even if a file existed at the sanitized location, it would be checked there — but we didn't write one
    }

    @Test("keyExists returns true when file exists at sanitized storage path")
    func keyExistsWithSanitizedPathActuallyExists() throws {
        let bucket = "test-bucket"
        let originalKey = "normal/../safe/key.txt"
        let sanitizedPath = ObjectFileHandler.storagePath(for: bucket, key: originalKey)

        // Write using a clean key that resolves to same sanitized path
        let cleanKey = "safe/key.txt"
        let metadata = ObjectMeta(
            bucketName: bucket,
            key: cleanKey,
            size: 100,
            contentType: "text/plain",
            etag: "123",
            metadata: [:],
            updatedAt: Date()
        )

        defer { cleanup(path: sanitizedPath) }

        try ObjectFileHandler.write(metadata: metadata, data: Data(), to: sanitizedPath)

        // Now check with the original dangerous key — should still find it because path is sanitized
        let exists = ObjectFileHandler.keyExists(for: bucket, key: originalKey)
        #expect(exists == true)
    }

    @Test("keyExists with custom path bypasses storagePath logic")
    func keyExistsCustomPathBypassesStoragePath() throws {
        let customPath = createTempPath()
        defer { cleanup(path: customPath) }

        try ObjectFileHandler.write(
            metadata: createTestMetadata(),
            data: Data(),
            to: customPath
        )

        // This would normally go through rootPath + bucket + key → different location
        // But with explicit path, it checks exactly where we wrote
        let exists = ObjectFileHandler.keyExists(
            for: "ignored-bucket",
            key: "ignored-key",
            path: customPath
        )
        #expect(exists == true)
    }

    private func createTestBucket() throws -> String {
        let bucketName = "test-bucket-\(UUID().uuidString)"
        try BucketHandler.create(name: bucketName)
        return bucketName
    }

    private func cleanupBucket(name: String) {
        try? BucketHandler.delete(name: name, force: false)
    }

    private func createTestObject(
        bucketName: String,
        key: String,
        content: String = "test content",
        contentType: String = "text/plain"
    ) throws {
        let data = content.data(using: .utf8) ?? Data()
        let etag = "etag-\(UUID().uuidString)"
        let metadata = ObjectMeta(
            bucketName: bucketName,
            key: key,
            size: data.count,
            contentType: contentType,
            etag: etag,
            metadata: [:],
            updatedAt: Date()
        )

        let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
        try ObjectFileHandler.write(metadata: metadata, data: data, to: path)
    }

    @Test("List objects in empty bucket returns empty list")
    func listObjectsEmptyBucket() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        let (objects, commonPrefixes, isTruncated, nextMarker) = try ObjectFileHandler.listObjects(
            bucketName: bucketName
        )

        #expect(objects.isEmpty)
        #expect(commonPrefixes.isEmpty)
        #expect(isTruncated == false)
        #expect(nextMarker == nil)
    }

    @Test("List objects in non-existent bucket returns empty list")
    func listObjectsNonExistentBucket() throws {
        let (objects, commonPrefixes, isTruncated, nextMarker) = try ObjectFileHandler.listObjects(
            bucketName: "non-existent-bucket-\(UUID().uuidString)"
        )

        #expect(objects.isEmpty)
        #expect(commonPrefixes.isEmpty)
        #expect(isTruncated == false)
        #expect(nextMarker == nil)
    }

    @Test("List objects returns single object")
    func listObjectsSingleObject() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "file.txt")

        let (objects, commonPrefixes, isTruncated, nextMarker) = try ObjectFileHandler.listObjects(
            bucketName: bucketName
        )

        #expect(objects.count == 1)
        #expect(objects[0].key == "file.txt")
        #expect(objects[0].bucketName == bucketName)
        #expect(objects[0].contentType == "text/plain")
        #expect(commonPrefixes.isEmpty)
        #expect(isTruncated == false)
        #expect(nextMarker == nil)
    }

    @Test("List objects returns multiple objects sorted by key")
    func listObjectsMultipleSorted() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "zebra.txt")
        try createTestObject(bucketName: bucketName, key: "alpha.txt")
        try createTestObject(bucketName: bucketName, key: "beta.txt")

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName
        )

        #expect(objects.count == 3)
        #expect(objects[0].key == "alpha.txt")
        #expect(objects[1].key == "beta.txt")
        #expect(objects[2].key == "zebra.txt")
    }

    @Test("List objects with prefix returns only matching objects")
    func listObjectsWithPrefix() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "docs/file1.txt")
        try createTestObject(bucketName: bucketName, key: "docs/file2.txt")
        try createTestObject(bucketName: bucketName, key: "images/photo.jpg")
        try createTestObject(bucketName: bucketName, key: "video.mp4")

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            prefix: "docs/"
        )

        #expect(objects.count == 2)
        #expect(objects[0].key == "docs/file1.txt")
        #expect(objects[1].key == "docs/file2.txt")
    }

    @Test("List objects with prefix that matches nothing returns empty")
    func listObjectsWithNonMatchingPrefix() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "file.txt")

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            prefix: "nonexistent/"
        )

        #expect(objects.isEmpty)
    }

    @Test("List objects with empty prefix returns all objects")
    func listObjectsWithEmptyPrefix() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "file1.txt")
        try createTestObject(bucketName: bucketName, key: "folder/file2.txt")

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            prefix: ""
        )

        #expect(objects.count == 2)
    }

    @Test("List objects with delimiter groups directories")
    func listObjectsWithDelimiter() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "root.txt")
        try createTestObject(bucketName: bucketName, key: "docs/file1.txt")
        try createTestObject(bucketName: bucketName, key: "docs/file2.txt")
        try createTestObject(bucketName: bucketName, key: "images/photo.jpg")

        let (objects, commonPrefixes, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            delimiter: "/"
        )

        #expect(objects.count == 1)
        #expect(objects[0].key == "root.txt")
        #expect(commonPrefixes.count == 2)
        #expect(commonPrefixes.contains("docs/"))
        #expect(commonPrefixes.contains("images/"))
    }

    @Test("List objects with delimiter and prefix")
    func listObjectsWithDelimiterAndPrefix() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "docs/api/file1.txt")
        try createTestObject(bucketName: bucketName, key: "docs/api/file2.txt")
        try createTestObject(bucketName: bucketName, key: "docs/guides/guide1.txt")
        try createTestObject(bucketName: bucketName, key: "docs/readme.txt")

        let (objects, commonPrefixes, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            prefix: "docs/",
            delimiter: "/"
        )

        #expect(objects.count == 1)
        #expect(objects[0].key == "docs/readme.txt")
        #expect(commonPrefixes.count == 2)
        #expect(commonPrefixes.contains("docs/api/"))
        #expect(commonPrefixes.contains("docs/guides/"))
    }

    @Test("List objects with delimiter handles nested directories")
    func listObjectsWithDelimiterNested() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "a/b/c/file.txt")
        try createTestObject(bucketName: bucketName, key: "a/b/file2.txt")
        try createTestObject(bucketName: bucketName, key: "a/file3.txt")

        let (objects, commonPrefixes, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            prefix: "a/",
            delimiter: "/"
        )

        #expect(objects.count == 1)
        #expect(objects[0].key == "a/file3.txt")
        #expect(commonPrefixes.count == 1)
        #expect(commonPrefixes.contains("a/b/"))
    }

    @Test("List objects with delimiter returns sorted common prefixes")
    func listObjectsWithDelimiterSortedPrefixes() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "zebra/file.txt")
        try createTestObject(bucketName: bucketName, key: "alpha/file.txt")
        try createTestObject(bucketName: bucketName, key: "beta/file.txt")

        let (_, commonPrefixes, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            delimiter: "/"
        )

        #expect(commonPrefixes.count == 3)
        #expect(commonPrefixes[0] == "alpha/")
        #expect(commonPrefixes[1] == "beta/")
        #expect(commonPrefixes[2] == "zebra/")
    }

    @Test("List objects respects maxKeys limit")
    func listObjectsMaxKeys() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        for i in 1...10 {
            try createTestObject(bucketName: bucketName, key: "file\(i).txt")
        }

        let (objects, _, isTruncated, nextMarker) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            maxKeys: 5
        )

        #expect(objects.count == 5)
        #expect(isTruncated == true)
        #expect(nextMarker != nil)
        #expect(nextMarker == objects.last?.key)
    }

    @Test("List objects pagination with marker")
    func listObjectsWithMarker() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "file1.txt")
        try createTestObject(bucketName: bucketName, key: "file2.txt")
        try createTestObject(bucketName: bucketName, key: "file3.txt")
        try createTestObject(bucketName: bucketName, key: "file4.txt")

        // First page
        let (page1, _, isTruncated1, marker1) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            maxKeys: 2
        )

        #expect(page1.count == 2)
        #expect(isTruncated1 == true)
        #expect(marker1 != nil)

        // Second page
        let (page2, _, isTruncated2, marker2) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            maxKeys: 2,
            marker: marker1
        )

        #expect(page2.count == 2)
        #expect(isTruncated2 == false)
        #expect(marker2 == nil)

        // Verify no overlap
        #expect(page1[0].key != page2[0].key)
        #expect(page1[1].key != page2[0].key)
    }

    @Test("List objects with marker at last key returns empty")
    func listObjectsMarkerAtEnd() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "file1.txt")
        try createTestObject(bucketName: bucketName, key: "file2.txt")

        let (objects, _, isTruncated, nextMarker) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            marker: "file2.txt"
        )

        #expect(objects.isEmpty)
        #expect(isTruncated == false)
        #expect(nextMarker == nil)
    }

    @Test("List objects with maxKeys larger than available objects")
    func listObjectsMaxKeysExceedsAvailable() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "file1.txt")
        try createTestObject(bucketName: bucketName, key: "file2.txt")

        let (objects, _, isTruncated, nextMarker) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            maxKeys: 100
        )

        #expect(objects.count == 2)
        #expect(isTruncated == false)
        #expect(nextMarker == nil)
    }

    @Test("List objects with special characters in keys")
    func listObjectsWithSpecialCharacters() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "file with spaces.txt")
        try createTestObject(bucketName: bucketName, key: "file-with-dashes.txt")
        try createTestObject(bucketName: bucketName, key: "file_with_underscores.txt")
        try createTestObject(bucketName: bucketName, key: "file+with+plus.txt")

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName
        )

        #expect(objects.count == 4)
        let keys = objects.map { $0.key }
        #expect(keys.contains("file with spaces.txt"))
        #expect(keys.contains("file-with-dashes.txt"))
        #expect(keys.contains("file_with_underscores.txt"))
        #expect(keys.contains("file+with+plus.txt"))
    }

    @Test("List objects with unicode characters in keys")
    func listObjectsWithUnicode() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "文件.txt")
        try createTestObject(bucketName: bucketName, key: "файл.txt")
        try createTestObject(bucketName: bucketName, key: "🚀rocket.txt")

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName
        )

        #expect(objects.count == 3)
        let keys = objects.map { $0.key }
        #expect(keys.contains("文件.txt"))
        #expect(keys.contains("файл.txt"))
        #expect(keys.contains("🚀rocket.txt"))
    }

    @Test("List objects preserves metadata fields")
    func listObjectsPreservesMetadata() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        let testData = "test content with specific size".data(using: .utf8)!
        let testEtag = "test-etag-123"
        let testContentType = "application/json"

        let metadata = ObjectMeta(
            bucketName: bucketName,
            key: "test.json",
            size: testData.count,
            contentType: testContentType,
            etag: testEtag,
            metadata: ["custom": "value"],
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )

        let path = ObjectFileHandler.storagePath(for: bucketName, key: "test.json")
        try ObjectFileHandler.write(metadata: metadata, data: testData, to: path)

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName
        )

        #expect(objects.count == 1)
        let obj = objects[0]
        #expect(obj.key == "test.json")
        #expect(obj.size == testData.count)
        #expect(obj.contentType == testContentType)
        #expect(obj.etag == testEtag)
        #expect(obj.updatedAt.timeIntervalSince1970 == 1_700_000_000)
    }

    @Test("List objects skips corrupted files")
    func listObjectsSkipsCorruptedFiles() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        // Create valid object
        try createTestObject(bucketName: bucketName, key: "valid.txt")

        // Create corrupted object by writing invalid data directly
        let corruptedPath = ObjectFileHandler.storagePath(for: bucketName, key: "corrupted.txt")
        let corruptedURL = URL(fileURLWithPath: corruptedPath)
        try FileManager.default.createDirectory(
            at: corruptedURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try Data([0x00, 0x01, 0x02]).write(to: corruptedURL)

        // Create another valid object
        try createTestObject(bucketName: bucketName, key: "valid2.txt")

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName
        )

        // Should only return the two valid objects
        #expect(objects.count == 2)
        #expect(objects[0].key == "valid.txt")
        #expect(objects[1].key == "valid2.txt")
    }

    @Test("List objects handles deeply nested keys")
    func listObjectsDeeplyNested() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        let deepKey = "a/b/c/d/e/f/g/h/i/j/file.txt"
        try createTestObject(bucketName: bucketName, key: deepKey)

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName
        )

        #expect(objects.count == 1)
        #expect(objects[0].key == deepKey)
    }

    @Test("List objects with very long key names")
    func listObjectsLongKeyNames() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        let longKey = String(repeating: "a", count: 100) + ".txt"
        try createTestObject(bucketName: bucketName, key: longKey)

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName
        )

        #expect(objects.count == 1)
        #expect(objects[0].key == longKey)
    }

    @Test("List objects with large number of objects")
    func listObjectsLargeNumberOfObjects() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        // Create 1000 objects
        for i in 0..<1000 {
            try createTestObject(
                bucketName: bucketName,
                key: String(format: "file%04d.txt", i)
            )
        }

        let start = Date()
        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            maxKeys: 100
        )
        let duration = Date().timeIntervalSince(start)

        #expect(objects.count == 100)
        #expect(duration < 1.0)  // Should complete within 1 second
    }

    @Test("List objects with prefix, delimiter, and maxKeys - prefixes first")
    func listObjectsCombinedFilters() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "docs/api/endpoint1.txt")
        try createTestObject(bucketName: bucketName, key: "docs/api/endpoint2.txt")
        try createTestObject(bucketName: bucketName, key: "docs/guides/guide1.txt")
        try createTestObject(bucketName: bucketName, key: "docs/readme.txt")
        try createTestObject(bucketName: bucketName, key: "images/photo.jpg")

        let (objects, commonPrefixes, isTruncated, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            prefix: "docs/",
            delimiter: "/",
            maxKeys: 2
        )

        // With maxKeys=2 and delimiter, we get the first 2 items in sorted order:
        // Sorted: docs/api/ < docs/guides/ < docs/readme.txt
        // With maxKeys=2: docs/api/, docs/guides/
        #expect(commonPrefixes.count == 2)
        #expect(commonPrefixes[0] == "docs/api/")
        #expect(commonPrefixes[1] == "docs/guides/")
        #expect(objects.count == 0)
        #expect(isTruncated == true)  // docs/readme.txt is still pending
    }

    @Test("List objects with marker and prefix")
    func listObjectsMarkerAndPrefix() throws {
        let bucketName = try createTestBucket()
        defer { cleanupBucket(name: bucketName) }

        try createTestObject(bucketName: bucketName, key: "docs/file1.txt")
        try createTestObject(bucketName: bucketName, key: "docs/file2.txt")
        try createTestObject(bucketName: bucketName, key: "docs/file3.txt")
        try createTestObject(bucketName: bucketName, key: "images/photo.jpg")

        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            prefix: "docs/",
            marker: "docs/file1.txt"
        )

        #expect(objects.count == 2)
        #expect(objects[0].key == "docs/file2.txt")
        #expect(objects[1].key == "docs/file3.txt")
    }
}
