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
            try BucketHandler.delete(name: "bucketA")
            try BucketHandler.delete(name: "bucketB")
            try BucketHandler.delete(name: "bucketC")
            buckets = try BucketHandler.list()
            #expect(buckets.isEmpty)
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
            try BucketHandler.delete(name: "testbucket")
            #expect(try BucketHandler.countKeys(name: "testbucket") == 0)
        }
    }
}
