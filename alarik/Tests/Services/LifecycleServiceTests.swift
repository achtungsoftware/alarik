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
import Vapor

@testable import Alarik

@Suite("LifecycleService tests", .serialized)
struct LifecycleServiceTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await test(app)
        } catch {
            try? StorageHelper.cleanStorage()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    /// Creates the DB row + on-disk directory for a bucket with the given lifecycle rules,
    /// owned by a freshly-created user (to satisfy the FK).
    private func createBucketWithRules(
        _ app: Application, name: String, rules: [LifecycleRule],
        versioningStatus: VersioningStatus = .disabled
    ) async throws {
        let user = User(
            name: "Lifecycle Test User", username: UUID().uuidString,
            passwordHash: try Bcrypt.hash("TestPass123!"), isAdmin: false)
        try await user.create(app: app)

        let bucket = Bucket(name: name, userId: user.id, versioningStatus: versioningStatus)
        bucket.lifecycleRules = LifecycleConfiguration(rules: rules).toJSON()
        try await bucket.save(app: app)

        try BucketHandler.create(name: name)
        await BucketVersioningCache.shared.setStatus(for: name, status: versioningStatus)
    }

    private func writeObject(
        bucketName: String, key: String, content: String = "data", daysOld: Int = 0
    ) throws {
        let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
        let data = Data(content.utf8)
        let meta = ObjectMeta(
            bucketName: bucketName, key: key, size: data.count, contentType: "text/plain",
            etag: Insecure.MD5.hash(data: data).hex,
            updatedAt: Date().addingTimeInterval(-Double(daysOld) * 86400))
        try ObjectFileHandler.write(metadata: meta, data: data, to: path)
    }

    @discardableResult
    private func writeVersionedObject(
        bucketName: String, key: String, content: String = "data", daysOld: Int = 0
    ) throws -> String {
        let data = Data(content.utf8)
        let meta = ObjectMeta(
            bucketName: bucketName, key: key, size: data.count, contentType: "text/plain",
            etag: Insecure.MD5.hash(data: data).hex,
            updatedAt: Date().addingTimeInterval(-Double(daysOld) * 86400))
        return try ObjectFileHandler.writeVersioned(
            metadata: meta, data: data, bucketName: bucketName, key: key,
            versioningStatus: .enabled)
    }

    private func createStaleMultipartUpload(bucketName: String, key: String, daysOld: Int) throws
        -> String
    {
        let uploadId = try MultipartFileHandler.createUpload(bucketName: bucketName, key: key)
        let backdated = MultipartUploadMeta(
            uploadId: uploadId, bucketName: bucketName, key: key,
            contentType: "application/octet-stream", metadata: [:],
            initiated: Date().addingTimeInterval(-Double(daysOld) * 86400))
        let metaPath = try MultipartFileHandler.metadataPath(for: bucketName, uploadId: uploadId)
        try JSONEncoder().encode(backdated).write(to: URL(fileURLWithPath: metaPath))
        return uploadId
    }

    // MARK: - Expiration

    @Test("runSweep - Expires a current object older than Expiration.Days")
    func testExpiresOldCurrentObject() async throws {
        try await withApp { app in
            try await createBucketWithRules(
                app, name: "lifecycle-expire-bucket",
                rules: [
                    LifecycleRule(
                        id: "rule1", enabled: true, prefix: "", expirationDays: 7)
                ])
            try writeObject(bucketName: "lifecycle-expire-bucket", key: "old.txt", daysOld: 10)
            try writeObject(bucketName: "lifecycle-expire-bucket", key: "new.txt", daysOld: 1)

            try await LifecycleService.runSweep(app: app)

            #expect(
                !ObjectFileHandler.keyExists(
                    for: "lifecycle-expire-bucket", key: "old.txt",
                    path: ObjectFileHandler.storagePath(
                        for: "lifecycle-expire-bucket", key: "old.txt")))
            #expect(
                ObjectFileHandler.keyExists(
                    for: "lifecycle-expire-bucket", key: "new.txt",
                    path: ObjectFileHandler.storagePath(
                        for: "lifecycle-expire-bucket", key: "new.txt")))
        }
    }

    @Test("runSweep - Expiration in a versioned bucket creates a delete marker, not a permanent delete")
    func testExpirationInVersionedBucketCreatesDeleteMarker() async throws {
        try await withApp { app in
            try await createBucketWithRules(
                app, name: "lifecycle-expire-versioned-bucket",
                rules: [
                    LifecycleRule(
                        id: "rule1", enabled: true, prefix: "", expirationDays: 7)
                ],
                versioningStatus: .enabled)
            _ = try writeVersionedObject(
                bucketName: "lifecycle-expire-versioned-bucket", key: "old.txt", daysOld: 10)

            try await LifecycleService.runSweep(app: app)

            let (meta, _) = try ObjectFileHandler.readVersion(
                bucketName: "lifecycle-expire-versioned-bucket", key: "old.txt", versionId: nil,
                loadData: false)
            #expect(meta.isDeleteMarker)
        }
    }

    @Test("runSweep - Does not expire an object newer than Expiration.Days")
    func testDoesNotExpireRecentObject() async throws {
        try await withApp { app in
            try await createBucketWithRules(
                app, name: "lifecycle-no-expire-bucket",
                rules: [
                    LifecycleRule(
                        id: "rule1", enabled: true, prefix: "", expirationDays: 30)
                ])
            try writeObject(bucketName: "lifecycle-no-expire-bucket", key: "recent.txt", daysOld: 5)

            try await LifecycleService.runSweep(app: app)

            #expect(
                ObjectFileHandler.keyExists(
                    for: "lifecycle-no-expire-bucket", key: "recent.txt",
                    path: ObjectFileHandler.storagePath(
                        for: "lifecycle-no-expire-bucket", key: "recent.txt")))
        }
    }

    @Test("runSweep - Prefix filter only expires matching keys")
    func testPrefixFilterOnlyAffectsMatchingKeys() async throws {
        try await withApp { app in
            try await createBucketWithRules(
                app, name: "lifecycle-prefix-bucket",
                rules: [
                    LifecycleRule(
                        id: "rule1", enabled: true, prefix: "logs/", expirationDays: 1)
                ])
            try writeObject(
                bucketName: "lifecycle-prefix-bucket", key: "logs/old.txt", daysOld: 10)
            try writeObject(
                bucketName: "lifecycle-prefix-bucket", key: "documents/old.txt", daysOld: 10)

            try await LifecycleService.runSweep(app: app)

            #expect(
                !ObjectFileHandler.keyExists(
                    for: "lifecycle-prefix-bucket", key: "logs/old.txt",
                    path: ObjectFileHandler.storagePath(
                        for: "lifecycle-prefix-bucket", key: "logs/old.txt")))
            #expect(
                ObjectFileHandler.keyExists(
                    for: "lifecycle-prefix-bucket", key: "documents/old.txt",
                    path: ObjectFileHandler.storagePath(
                        for: "lifecycle-prefix-bucket", key: "documents/old.txt")))
        }
    }

    @Test("runSweep - A Disabled rule is never evaluated")
    func testDisabledRuleNeverEvaluated() async throws {
        try await withApp { app in
            try await createBucketWithRules(
                app, name: "lifecycle-disabled-bucket",
                rules: [
                    LifecycleRule(
                        id: "rule1", enabled: false, prefix: "", expirationDays: 1)
                ])
            try writeObject(bucketName: "lifecycle-disabled-bucket", key: "old.txt", daysOld: 100)

            try await LifecycleService.runSweep(app: app)

            #expect(
                ObjectFileHandler.keyExists(
                    for: "lifecycle-disabled-bucket", key: "old.txt",
                    path: ObjectFileHandler.storagePath(
                        for: "lifecycle-disabled-bucket", key: "old.txt")))
        }
    }

    @Test("runSweep - A bucket with no lifecycle configuration is skipped without error")
    func testBucketWithoutLifecycleRulesIsSkipped() async throws {
        try await withApp { app in
            try await createBucketWithRules(app, name: "lifecycle-none-bucket", rules: [])
            try writeObject(bucketName: "lifecycle-none-bucket", key: "old.txt", daysOld: 1000)

            try await LifecycleService.runSweep(app: app)

            #expect(
                ObjectFileHandler.keyExists(
                    for: "lifecycle-none-bucket", key: "old.txt",
                    path: ObjectFileHandler.storagePath(
                        for: "lifecycle-none-bucket", key: "old.txt")))
        }
    }

    // MARK: - NoncurrentVersionExpiration

    @Test("runSweep - Expires noncurrent versions that became noncurrent past NoncurrentDays")
    func testExpiresOldNoncurrentVersions() async throws {
        try await withApp { app in
            try await createBucketWithRules(
                app, name: "lifecycle-noncurrent-bucket",
                rules: [
                    LifecycleRule(
                        id: "rule1", enabled: true, prefix: "",
                        noncurrentVersionExpirationDays: 3)
                ],
                versioningStatus: .enabled)

            let firstVersionId = try writeVersionedObject(
                bucketName: "lifecycle-noncurrent-bucket", key: "file.txt", content: "v1",
                daysOld: 20)
            // Superseding it 5 days ago makes the first version "noncurrent" as of 5 days ago
            _ = try writeVersionedObject(
                bucketName: "lifecycle-noncurrent-bucket", key: "file.txt", content: "v2",
                daysOld: 5)

            try await LifecycleService.runSweep(app: app)

            let remaining = try ObjectFileHandler.listVersions(
                bucketName: "lifecycle-noncurrent-bucket", key: "file.txt")
            #expect(!remaining.contains { $0.versionId == firstVersionId })
            #expect(remaining.count == 1)
        }
    }

    @Test("runSweep - Does not expire a noncurrent version that hasn't been noncurrent long enough")
    func testDoesNotExpireRecentlyNoncurrentVersion() async throws {
        try await withApp { app in
            try await createBucketWithRules(
                app, name: "lifecycle-noncurrent-recent-bucket",
                rules: [
                    LifecycleRule(
                        id: "rule1", enabled: true, prefix: "",
                        noncurrentVersionExpirationDays: 30)
                ],
                versioningStatus: .enabled)

            let firstVersionId = try writeVersionedObject(
                bucketName: "lifecycle-noncurrent-recent-bucket", key: "file.txt", content: "v1",
                daysOld: 20)
            _ = try writeVersionedObject(
                bucketName: "lifecycle-noncurrent-recent-bucket", key: "file.txt", content: "v2",
                daysOld: 5)

            try await LifecycleService.runSweep(app: app)

            let remaining = try ObjectFileHandler.listVersions(
                bucketName: "lifecycle-noncurrent-recent-bucket", key: "file.txt")
            #expect(remaining.contains { $0.versionId == firstVersionId })
            #expect(remaining.count == 2)
        }
    }

    @Test("runSweep - NoncurrentVersionExpiration never removes the current version")
    func testNoncurrentVersionExpirationLeavesCurrentVersionAlone() async throws {
        try await withApp { app in
            try await createBucketWithRules(
                app, name: "lifecycle-noncurrent-current-bucket",
                rules: [
                    LifecycleRule(
                        id: "rule1", enabled: true, prefix: "",
                        noncurrentVersionExpirationDays: 1)
                ],
                versioningStatus: .enabled)

            _ = try writeVersionedObject(
                bucketName: "lifecycle-noncurrent-current-bucket", key: "file.txt", content: "v1",
                daysOld: 200)
            let currentVersionId = try writeVersionedObject(
                bucketName: "lifecycle-noncurrent-current-bucket", key: "file.txt", content: "v2",
                daysOld: 100)

            try await LifecycleService.runSweep(app: app)

            let remaining = try ObjectFileHandler.listVersions(
                bucketName: "lifecycle-noncurrent-current-bucket", key: "file.txt")
            #expect(remaining.count == 1)
            #expect(remaining.first?.versionId == currentVersionId)
            #expect(remaining.first?.isLatest == true)
        }
    }

    // MARK: - AbortIncompleteMultipartUpload

    @Test("runSweep - Aborts a stale incomplete multipart upload")
    func testAbortsStaleMultipartUpload() async throws {
        try await withApp { app in
            try await createBucketWithRules(
                app, name: "lifecycle-mpu-bucket",
                rules: [
                    LifecycleRule(
                        id: "rule1", enabled: true, prefix: "",
                        abortIncompleteMultipartUploadDays: 3)
                ])

            let uploadId = try createStaleMultipartUpload(
                bucketName: "lifecycle-mpu-bucket", key: "stale.txt", daysOld: 10)

            try await LifecycleService.runSweep(app: app)

            #expect(!MultipartFileHandler.uploadExists(
                bucketName: "lifecycle-mpu-bucket", uploadId: uploadId))
        }
    }

    @Test("runSweep - Leaves a fresh incomplete multipart upload alone")
    func testLeavesFreshMultipartUploadAlone() async throws {
        try await withApp { app in
            try await createBucketWithRules(
                app, name: "lifecycle-mpu-fresh-bucket",
                rules: [
                    LifecycleRule(
                        id: "rule1", enabled: true, prefix: "",
                        abortIncompleteMultipartUploadDays: 7)
                ])

            let uploadId = try MultipartFileHandler.createUpload(
                bucketName: "lifecycle-mpu-fresh-bucket", key: "fresh.txt")

            try await LifecycleService.runSweep(app: app)

            #expect(MultipartFileHandler.uploadExists(
                bucketName: "lifecycle-mpu-fresh-bucket", uploadId: uploadId))
        }
    }
}
