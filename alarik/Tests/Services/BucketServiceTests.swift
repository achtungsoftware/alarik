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

@Suite("BucketService tests", .serialized)
struct BucketServiceTests {
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

    private func createUser(_ app: Application) async throws -> UUID {
        let user = User(
            name: "Bucket Service Test User", username: UUID().uuidString,
            passwordHash: try Bcrypt.hash("TestPass123!"), isAdmin: false)
        try await user.create(app: app)
        return user.id
    }

    @Test("create - Succeeds and registers the versioning cache entry")
    func testCreateSuccess() async throws {
        try await withApp { app in
            let userId = try await createUser(app)

            try await BucketService.create(
                app: app, bucketName: "create-success-bucket", userId: userId)

            let bucket = try await Bucket.find(app: app, name: "create-success-bucket")
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

            try await BucketService.create(app: app, bucketName: "dup-bucket", userId: userId)

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
                try await BucketService.create(app: app, bucketName: "dup-bucket", userId: userId)
            }

            // The original bucket and its file must be completely untouched
            #expect(FileManager.default.fileExists(atPath: path))
            let readBack = try ObjectFileHandler.read(from: path, loadData: true)
            #expect(readBack.1 == data)

            let buckets = try await Bucket.all(app: app).filter { $0.name == "dup-bucket" }
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
                    app: app, bucketName: "rollback-fail-bucket", userId: userId)
            }

            // The bucket row must have been rolled back, not left dangling
            let bucket = try await Bucket.find(app: app, name: "rollback-fail-bucket")
            #expect(bucket == nil)

            // The versioning cache must not retain an entry for a bucket that doesn't exist
            let versioningMap = await BucketVersioningCache.shared.getMap()
            #expect(versioningMap["rollback-fail-bucket"] == nil)
        }
    }

    @Test(
        "delete - Force-deleting a non-empty bucket removes its objects, so recreating it under the same name doesn't resurrect them"
    )
    func testForceDeleteRemovesObjectsSoRecreatedBucketStartsEmpty() async throws {
        try await withApp { app in
            let userId = try await createUser(app)

            try await BucketService.create(app: app, bucketName: "ghost-bucket", userId: userId)

            // A real object in the bucket before force-deleting it - the exact scenario this
            // regression covers: force-delete used to only wipe this node's local directory via
            // a raw filesystem remove, without ever running the object through a real delete -
            // in a cluster, every *other* node's copy was left completely untouched, and the
            // on-disk path being derived purely from the bucket name (not a unique id) meant a
            // bucket recreated under the same name silently reused the old, never-cleaned data.
            let path = ObjectFileHandler.storagePath(for: "ghost-bucket", key: "leftover.txt")
            let data = Data("should not survive".utf8)
            try ObjectFileHandler.write(
                metadata: ObjectMeta(
                    bucketName: "ghost-bucket", key: "leftover.txt", size: data.count,
                    contentType: "text/plain", etag: Insecure.MD5.hash(data: data).hex,
                    updatedAt: Date()),
                data: data, to: path)
            #expect(FileManager.default.fileExists(atPath: path))

            let req = Request(application: app, on: app.eventLoopGroup.next())
            try await BucketService.delete(
                req: req, bucketName: "ghost-bucket", userId: userId, force: true)

            #expect(!FileManager.default.fileExists(atPath: path))
            let deletedBucket = try await Bucket.find(app: app, name: "ghost-bucket")
            #expect(deletedBucket == nil)

            // Recreate a bucket under the exact same name - it must start genuinely empty.
            try await BucketService.create(app: app, bucketName: "ghost-bucket", userId: userId)

            #expect(!FileManager.default.fileExists(atPath: path))
            let (objects, _, _, _) = try ObjectFileHandler.listObjects(bucketName: "ghost-bucket")
            #expect(objects.isEmpty)
        }
    }

    @Test(
        "delete - Purges pending internal cluster replication tasks for the bucket, not just what's currently visible"
    )
    func testForceDeletePurgesPendingClusterReplicationTasks() async throws {
        try await withApp { app in
            let userId = try await createUser(app)

            try await BucketService.create(app: app, bucketName: "straggler-bucket", userId: userId)

            // Simulates the completely normal (not an error case) outcome of a quorum write: 2 of
            // 3 responsible nodes ack in time, satisfying quorum, while the 3rd is left with a
            // durable catch-up task to deliver later. From that snapshot in time, the straggler
            // node genuinely doesn't have the object yet, so a cluster-wide listing at delete time
            // can never find (and therefore never delete) anything there - only purging this row
            // directly stops the dispatcher from delivering it after the bucket is gone.
            let task = ClusterReplicationTask(
                bucketName: "straggler-bucket", key: "in-flight.txt", versionId: nil,
                operation: .put, targetNodeId: UUID(), reason: .write,
                ownerNodeId: OutboxMailbox.selfNodeId(app: app))
            try OutboxMailbox.update(task, collection: OutboxCollections.clusterReplicationTasks)

            let req = Request(application: app, on: app.eventLoopGroup.next())
            try await BucketService.delete(
                req: req, bucketName: "straggler-bucket", userId: userId, force: true)

            let remaining = OutboxMailbox.allOwnedTasks(
                ClusterReplicationTask.self, app: app, collection: OutboxCollections.clusterReplicationTasks
            ).filter { $0.bucketName == "straggler-bucket" }.count
            #expect(remaining == 0)
        }
    }
}
