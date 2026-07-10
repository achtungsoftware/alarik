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

/// `S3Service.deleteObject` is a pure filesystem operation (no DB/app needed) - these tests
/// exercise it directly, matching `ObjectFileHandlerVersioningTests`'s style.
@Suite("S3Service.deleteObject tests", .serialized)
struct S3ServiceDeleteObjectTests {
    private func setupTestBucket() {
        let bucketURL = BucketHandler.bucketURL(for: "delete-test-bucket")
        try? FileManager.default.createDirectory(at: bucketURL, withIntermediateDirectories: true)
    }

    private func cleanupTestBucket() {
        let bucketURL = BucketHandler.bucketURL(for: "delete-test-bucket")
        try? FileManager.default.removeItem(at: bucketURL)
    }

    private func metadata(key: String) -> ObjectMeta {
        ObjectMeta(
            bucketName: "delete-test-bucket", key: key, size: 2, contentType: "text/plain",
            etag: "etag", metadata: [:], updatedAt: Date())
    }

    @Test(
        "deleteObject on a SUSPENDED-versioning bucket preserves history behind a null-version delete marker"
    )
    func testDeleteOnSuspendedVersioningPreservesHistory() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let key = "important.txt"
        let v1 = try ObjectFileHandler.writeVersioned(
            metadata: metadata(key: key), data: Data("v1".utf8),
            bucketName: "delete-test-bucket", key: key, versioningStatus: .enabled)
        let v2 = try ObjectFileHandler.writeVersioned(
            metadata: metadata(key: key), data: Data("v2".utf8),
            bucketName: "delete-test-bucket", key: key, versioningStatus: .enabled)

        // Enable -> suspend -> delete with no version ID: the exact scenario that used to
        // permanently and silently destroy v1/v2 instead of only affecting the null version.
        let outcome = try S3Service.deleteObject(
            bucketName: "delete-test-bucket", key: key, versionId: nil,
            versioningStatus: .suspended)

        #expect(outcome.isDeleteMarker == true)
        #expect(outcome.versionId == "null")

        // Both real versions must have survived the delete.
        let (_, v1Data) = try ObjectFileHandler.readVersion(
            bucketName: "delete-test-bucket", key: key, versionId: v1)
        #expect(v1Data == Data("v1".utf8))
        let (_, v2Data) = try ObjectFileHandler.readVersion(
            bucketName: "delete-test-bucket", key: key, versionId: v2)
        #expect(v2Data == Data("v2".utf8))

        // A plain GetObject (no version ID) must now behave like the key is deleted - the
        // "latest" pointer resolves to the delete marker, not to v1 or v2.
        let latestVersionId = try ObjectFileHandler.getLatestVersionId(
            bucketName: "delete-test-bucket", key: key)
        #expect(latestVersionId == "null")
    }

    @Test("deleteObject on a bucket that's never had versioning enabled is a genuine permanent delete")
    func testDeleteOnNeverVersionedBucketIsPermanent() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let key = "plain.txt"
        let path = ObjectFileHandler.storagePath(for: "delete-test-bucket", key: key)
        try ObjectFileHandler.write(metadata: metadata(key: key), data: Data("plain".utf8), to: path)
        #expect(FileManager.default.fileExists(atPath: path))

        let outcome = try S3Service.deleteObject(
            bucketName: "delete-test-bucket", key: key, versionId: nil,
            versioningStatus: .disabled)

        #expect(outcome.isDeleteMarker == false)
        #expect(outcome.versionId == nil)
        #expect(!FileManager.default.fileExists(atPath: path))
    }

    @Test("deleteObject on an ENABLED-versioning bucket creates a fresh, uniquely-versioned delete marker")
    func testDeleteOnEnabledVersioningCreatesFreshMarker() throws {
        setupTestBucket()
        defer { cleanupTestBucket() }

        let key = "versioned.txt"
        let v1 = try ObjectFileHandler.writeVersioned(
            metadata: metadata(key: key), data: Data("v1".utf8),
            bucketName: "delete-test-bucket", key: key, versioningStatus: .enabled)

        let outcome = try S3Service.deleteObject(
            bucketName: "delete-test-bucket", key: key, versionId: nil, versioningStatus: .enabled)

        #expect(outcome.isDeleteMarker == true)
        #expect(outcome.versionId != nil)
        #expect(outcome.versionId != "null")

        let (_, v1Data) = try ObjectFileHandler.readVersion(
            bucketName: "delete-test-bucket", key: key, versionId: v1)
        #expect(v1Data == Data("v1".utf8))
    }
}
