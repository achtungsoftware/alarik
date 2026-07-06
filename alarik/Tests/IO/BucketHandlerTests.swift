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
import Vapor
import VaporTesting

@testable import Alarik

@Suite("BucketHandler Tests", .serialized)
struct BucketHandlerTests {
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

    @Test("Bucket list")
    func testList() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()
            var buckets = try BucketHandler.list()
            #expect(buckets.isEmpty)
            try BucketHandler.create(name: "bucketA")
            try BucketHandler.create(name: "bucketB")
            try BucketHandler.create(name: "bucketC")
            buckets = try BucketHandler.list()
            #expect(buckets.sorted() == ["bucketA", "bucketB", "bucketC"])
            try BucketHandler.delete(name: "bucketA", force: false)
            try BucketHandler.delete(name: "bucketB", force: false)
            try BucketHandler.delete(name: "bucketC", force: false)
            buckets = try BucketHandler.list()
            #expect(buckets.isEmpty)
        }
    }

    @Test("Force delete bucket with contents")
    func testForceDelete() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()

            // Create bucket with files
            try BucketHandler.create(name: "testbucket")
            let bucketURL = BucketHandler.bucketURL(for: "testbucket")

            // Add root file
            let file1 = bucketURL.appendingPathComponent("test.txt")
            try "content".write(to: file1, atomically: true, encoding: .utf8)

            // Add nested file
            let subDir = bucketURL.appendingPathComponent("mykey/sub")
            try FileManager.default.createDirectory(at: subDir, withIntermediateDirectories: true)
            let file2 = subDir.appendingPathComponent("test.txt")
            try "content".write(to: file2, atomically: true, encoding: .utf8)

            // Verify files exist
            #expect(FileManager.default.fileExists(atPath: file1.path))
            #expect(FileManager.default.fileExists(atPath: file2.path))
            #expect(try BucketHandler.countKeys(name: "testbucket") == 2)

            // Force delete should succeed even with contents
            try BucketHandler.forceDelete(name: "testbucket")

            // Verify bucket is deleted
            #expect(!FileManager.default.fileExists(atPath: bucketURL.path))
            #expect(try BucketHandler.list().isEmpty)
        }
    }

    @Test("Force delete empty bucket")
    func testForceDeleteEmptyBucket() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()

            // Create empty bucket
            try BucketHandler.create(name: "emptybucket")
            let bucketURL = BucketHandler.bucketURL(for: "emptybucket")
            #expect(FileManager.default.fileExists(atPath: bucketURL.path))

            // Force delete should work on empty bucket
            try BucketHandler.forceDelete(name: "emptybucket")

            // Verify bucket is deleted
            #expect(!FileManager.default.fileExists(atPath: bucketURL.path))
        }
    }

    @Test("Force delete non-existent bucket")
    func testForceDeleteNonExistentBucket() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()

            // Force delete on non-existent bucket should not throw
            try BucketHandler.forceDelete(name: "nonexistent")
        }
    }

    @Test("Regular delete fails on non-empty bucket")
    func testDeleteFailsOnNonEmptyBucket() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()

            // Create bucket with a real object (only .obj files count as bucket contents -
            // that's the only thing an object write can ever produce)
            try BucketHandler.create(name: "testbucket")
            let bucketURL = BucketHandler.bucketURL(for: "testbucket")
            let meta = ObjectMeta(
                bucketName: "testbucket", key: "test.txt", size: 7, contentType: "text/plain",
                etag: "abc", updatedAt: Date())
            try ObjectFileHandler.write(
                metadata: meta, data: Data("content".utf8),
                to: ObjectFileHandler.storagePath(for: "testbucket", key: "test.txt"))

            // Regular delete should fail
            #expect(throws: S3Error.self) {
                try BucketHandler.delete(name: "testbucket", force: false)
            }

            // Bucket should still exist
            #expect(FileManager.default.fileExists(atPath: bucketURL.path))

            // Clean up with force delete
            try BucketHandler.forceDelete(name: "testbucket")
        }
    }

    @Test("Regular delete succeeds when only empty directory skeletons remain")
    func testDeleteSucceedsWithEmptyDirectorySkeletons() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()

            // Deleting a nested key leaves its empty parent directories behind - S3 has no
            // directories, so those leftovers must never block deleting the bucket (found
            // via `mc rb --force`, which deletes every object and then expects DeleteBucket
            // to succeed).
            try BucketHandler.create(name: "skeleton-bucket")
            let bucketURL = BucketHandler.bucketURL(for: "skeleton-bucket")
            let nested = bucketURL.appendingPathComponent("deep/nested/path")
            try FileManager.default.createDirectory(at: nested, withIntermediateDirectories: true)

            try BucketHandler.delete(name: "skeleton-bucket", force: false)
            #expect(!FileManager.default.fileExists(atPath: bucketURL.path))
        }
    }

    @Test("Regular delete fails while versioned objects remain (hidden .versions dir)")
    func testDeleteFailsWithVersionedObjects() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()

            // Versioned objects live under hidden .versions directories - a bucket holding
            // only those must still count as non-empty (S3 refuses to delete a bucket
            // while any version or delete marker remains).
            try BucketHandler.create(name: "versioned-remains")
            let meta = ObjectMeta(
                bucketName: "versioned-remains", key: "v.txt", size: 1, contentType: "text/plain",
                etag: "abc", updatedAt: Date())
            _ = try ObjectFileHandler.writeVersioned(
                metadata: meta, data: Data("x".utf8), bucketName: "versioned-remains",
                key: "v.txt", versioningStatus: .enabled)

            #expect(ObjectFileHandler.hasBucketObjects(bucketName: "versioned-remains"))
            #expect(throws: S3Error.self) {
                try BucketHandler.delete(name: "versioned-remains", force: false)
            }

            try BucketHandler.forceDelete(name: "versioned-remains")
        }
    }

    @Test("Bucket count keys")
    func testCountKeys() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()
            // Non-existent bucket
            #expect(try BucketHandler.countKeys(name: "testbucket") == 0)
            // Create bucket
            try BucketHandler.create(name: "testbucket")
            #expect(try BucketHandler.countKeys(name: "testbucket") == 0)
            let bucketURL = BucketHandler.bucketURL(for: "testbucket")
            // Add root file
            let file1 = bucketURL.appendingPathComponent("test.txt")
            try "content".write(to: file1, atomically: true, encoding: .utf8)
            #expect(try BucketHandler.countKeys(name: "testbucket") == 1)
            // Add subdir file
            let subDir = bucketURL.appendingPathComponent("mykey/sub")
            try FileManager.default.createDirectory(at: subDir, withIntermediateDirectories: true)
            let file2 = subDir.appendingPathComponent("test.txt")
            try "content".write(to: file2, atomically: true, encoding: .utf8)
            #expect(try BucketHandler.countKeys(name: "testbucket") == 2)
            // Add another root file
            let file3 = bucketURL.appendingPathComponent("another.txt")
            try "content".write(to: file3, atomically: true, encoding: .utf8)
            #expect(try BucketHandler.countKeys(name: "testbucket") == 3)
            // Delete one file
            try FileManager.default.removeItem(at: file1)
            #expect(try BucketHandler.countKeys(name: "testbucket") == 2)
            // Clean up files and dirs to allow delete
            try FileManager.default.removeItem(at: file2)
            try FileManager.default.removeItem(at: file3)
            try FileManager.default.removeItem(at: subDir)
            try FileManager.default.removeItem(at: bucketURL.appendingPathComponent("mykey"))
            // Delete bucket
            try BucketHandler.delete(name: "testbucket", force: false)
            #expect(try BucketHandler.countKeys(name: "testbucket") == 0)
        }
    }

    // MARK: - calculateStats

    @Test("calculateStats - Non-existent bucket reports zero")
    func testCalculateStatsNonExistentBucket() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()
            let stats = BucketHandler.calculateStats(bucketName: "does-not-exist")
            #expect(stats.sizeBytes == 0)
            #expect(stats.objectCount == 0)
        }
    }

    @Test("calculateStats - Empty bucket reports zero")
    func testCalculateStatsEmptyBucket() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()
            try BucketHandler.create(name: "empty-stats")
            let stats = BucketHandler.calculateStats(bucketName: "empty-stats")
            #expect(stats.sizeBytes == 0)
            #expect(stats.objectCount == 0)
        }
    }

    @Test("calculateStats - Sums size of every file but only counts .obj files as objects")
    func testCalculateStatsMixedFiles() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()
            try BucketHandler.create(name: "stats-bucket")
            let bucketURL = BucketHandler.bucketURL(for: "stats-bucket")

            // Two real objects (5 + 10 bytes) plus a non-.obj sidecar file (3 bytes) that
            // should count toward size but not objectCount
            try "12345".write(
                to: bucketURL.appendingPathComponent("a.txt.obj"), atomically: true,
                encoding: .utf8)
            try "1234567890".write(
                to: bucketURL.appendingPathComponent("b.txt.obj"), atomically: true,
                encoding: .utf8)
            try "xyz".write(
                to: bucketURL.appendingPathComponent("notes.meta"), atomically: true,
                encoding: .utf8)

            let stats = BucketHandler.calculateStats(bucketName: "stats-bucket")
            #expect(stats.objectCount == 2)
            #expect(stats.sizeBytes == 18)  // 5 + 10 + 3
        }
    }

    @Test("calculateStats - Recurses into nested folders")
    func testCalculateStatsNested() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()
            try BucketHandler.create(name: "nested-stats")
            let bucketURL = BucketHandler.bucketURL(for: "nested-stats")

            try "12345".write(
                to: bucketURL.appendingPathComponent("root.txt.obj"), atomically: true,
                encoding: .utf8)

            let subDir = bucketURL.appendingPathComponent("dir/sub")
            try FileManager.default.createDirectory(at: subDir, withIntermediateDirectories: true)
            try "1234567890".write(
                to: subDir.appendingPathComponent("deep.txt.obj"), atomically: true,
                encoding: .utf8)

            let stats = BucketHandler.calculateStats(bucketName: "nested-stats")
            #expect(stats.objectCount == 2)
            #expect(stats.sizeBytes == 15)
        }
    }

    @Test("calculateStats - A non-empty prefix scopes the walk to just that folder")
    func testCalculateStatsScopedToPrefix() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()
            try BucketHandler.create(name: "scoped-stats")
            let bucketURL = BucketHandler.bucketURL(for: "scoped-stats")

            try "12345".write(
                to: bucketURL.appendingPathComponent("root.txt.obj"), atomically: true,
                encoding: .utf8)

            let folderA = bucketURL.appendingPathComponent("folderA")
            try FileManager.default.createDirectory(at: folderA, withIntermediateDirectories: true)
            try "1234567890".write(
                to: folderA.appendingPathComponent("a.txt.obj"), atomically: true, encoding: .utf8)
            try "12".write(
                to: folderA.appendingPathComponent("b.txt.obj"), atomically: true, encoding: .utf8)

            let folderB = bucketURL.appendingPathComponent("folderB")
            try FileManager.default.createDirectory(at: folderB, withIntermediateDirectories: true)
            try "1".write(
                to: folderB.appendingPathComponent("c.txt.obj"), atomically: true, encoding: .utf8)

            // Whole bucket sees everything
            let wholeBucket = BucketHandler.calculateStats(bucketName: "scoped-stats")
            #expect(wholeBucket.objectCount == 4)
            #expect(wholeBucket.sizeBytes == 18)  // 5 + 10 + 2 + 1

            // Scoped to folderA only sees its own two objects
            let scoped = BucketHandler.calculateStats(bucketName: "scoped-stats", prefix: "folderA")
            #expect(scoped.objectCount == 2)
            #expect(scoped.sizeBytes == 12)  // 10 + 2
        }
    }

    @Test("calculateStats - A non-existent prefix within a real bucket reports zero, not the whole bucket")
    func testCalculateStatsNonExistentPrefix() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()
            try BucketHandler.create(name: "prefix-miss-stats")
            let bucketURL = BucketHandler.bucketURL(for: "prefix-miss-stats")
            try "12345".write(
                to: bucketURL.appendingPathComponent("root.txt.obj"), atomically: true,
                encoding: .utf8)

            let stats = BucketHandler.calculateStats(
                bucketName: "prefix-miss-stats", prefix: "no-such-folder")
            #expect(stats.sizeBytes == 0)
            #expect(stats.objectCount == 0)
        }
    }

    @Test("calculateStats - Path traversal in prefix is sanitized, never escaping the bucket")
    func testCalculateStatsPrefixTraversalSanitized() async throws {
        try await withApp { _ in
            try StorageHelper.cleanStorage()
            try BucketHandler.create(name: "traversal-stats")
            let bucketURL = BucketHandler.bucketURL(for: "traversal-stats")
            try "12345".write(
                to: bucketURL.appendingPathComponent("root.txt.obj"), atomically: true,
                encoding: .utf8)

            // "../../etc" sanitizes down to "etc" (a nonexistent folder inside the bucket),
            // never escapes to a real path outside the bucket directory
            let stats = BucketHandler.calculateStats(
                bucketName: "traversal-stats", prefix: "../../etc")
            #expect(stats.sizeBytes == 0)
            #expect(stats.objectCount == 0)
        }
    }
}
