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
import Fluent
import Foundation
import Testing
import Vapor

@testable import Alarik

@Suite("BucketService tests", .serialized)
struct BucketServiceTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()
            try await test(app)
            try await app.autoRevert()
        } catch {
            try? StorageHelper.cleanStorage()
            try? await app.autoRevert()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    private func createUser(_ app: Application) async throws -> UUID {
        let user = User(
            name: "Bucket Service Test User", username: UUID().uuidString,
            passwordHash: try Bcrypt.hash("TestPass123!"), isAdmin: false)
        try await user.save(on: app.db)
        return user.id!
    }

    @Test("create - Succeeds and registers the versioning cache entry")
    func testCreateSuccess() async throws {
        try await withApp { app in
            let userId = try await createUser(app)

            try await BucketService.create(
                on: app.db, bucketName: "create-success-bucket", userId: userId)

            let bucket = try await Bucket.query(on: app.db)
                .filter(\.$name == "create-success-bucket")
                .first()
            #expect(bucket != nil)
            #expect(
                await BucketVersioningCache.shared.getStatus(for: "create-success-bucket")
                    == .disabled)
            #expect(
                FileManager.default.fileExists(
                    atPath: BucketHandler.bucketURL(for: "create-success-bucket").path))
        }
    }

    @Test(
        "create - Failing to create a bucket whose name already exists does not destroy the existing bucket's files"
    )
    func testCreateDuplicateNameDoesNotDestroyExistingBucket() async throws {
        try await withApp { app in
            let userId = try await createUser(app)

            try await BucketService.create(on: app.db, bucketName: "dup-bucket", userId: userId)

            // Put a real file in the legitimately-created bucket
            let path = ObjectFileHandler.storagePath(for: "dup-bucket", key: "important.txt")
            let data = Data("important data".utf8)
            try ObjectFileHandler.write(
                metadata: ObjectMeta(
                    bucketName: "dup-bucket", key: "important.txt", size: data.count,
                    contentType: "text/plain", etag: Insecure.MD5.hash(data: data).hex,
                    updatedAt: Date()),
                data: data, to: path)
            #expect(FileManager.default.fileExists(atPath: path))

            // Attempt to create a second bucket with the same name - bypasses any
            // controller-level pre-check and hits the DB's unique constraint on `name`
            // directly, which is exactly the failure this regression covers.
            await #expect(throws: (any Error).self) {
                try await BucketService.create(on: app.db, bucketName: "dup-bucket", userId: userId)
            }

            // The original bucket and its file must be completely untouched
            #expect(FileManager.default.fileExists(atPath: path))
            let readBack = try ObjectFileHandler.read(from: path, loadData: true)
            #expect(readBack.1 == data)

            let buckets = try await Bucket.query(on: app.db)
                .filter(\.$name == "dup-bucket")
                .all()
            #expect(buckets.count == 1)
        }
    }

    @Test("create - Rolls back the DB row and cache entry when the directory can't be created")
    func testCreateRollsBackOnDirectoryCreationFailure() async throws {
        try await withApp { app in
            let userId = try await createUser(app)

            // Pre-create a plain *file* at the exact path the bucket directory would need to
            // go - BucketHandler.create's createDirectory(at:) fails when a non-directory
            // already occupies that path, simulating an unrelated filesystem failure (not a
            // duplicate bucket name) without leaving any prior valid bucket behind.
            let bucketURL = BucketHandler.bucketURL(for: "rollback-fail-bucket")
            try FileManager.default.createDirectory(
                at: bucketURL.deletingLastPathComponent(), withIntermediateDirectories: true)
            try Data("blocking file".utf8).write(to: bucketURL)

            await #expect(throws: (any Error).self) {
                try await BucketService.create(
                    on: app.db, bucketName: "rollback-fail-bucket", userId: userId)
            }

            // The bucket row must have been rolled back, not left dangling
            let bucket = try await Bucket.query(on: app.db)
                .filter(\.$name == "rollback-fail-bucket")
                .first()
            #expect(bucket == nil)

            // The versioning cache must not retain an entry for a bucket that doesn't exist
            let versioningMap = await BucketVersioningCache.shared.getMap()
            #expect(versioningMap["rollback-fail-bucket"] == nil)
        }
    }
}
