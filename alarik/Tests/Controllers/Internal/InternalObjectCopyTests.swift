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
import VaporTesting

@testable import Alarik

/// `POST /api/v1/objects/copy`: the console's copy, move/rename and version restore.
@Suite("Internal object copy tests", .serialized)
struct InternalObjectCopyTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            let loadCacheLifecycle = LoadCacheLifecycle()
            try await loadCacheLifecycle.didBootAsync(app)
            try await test(app)
        } catch {
            try? StorageHelper.cleanStorage()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    private func createBucket(
        _ app: Application, token: String, name: String, versioningEnabled: Bool = false
    ) async throws {
        try await app.test(
            .POST, "/api/v1/buckets",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(Bucket.Create(name: name, versioningEnabled: versioningEnabled))
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func meta(bucket: String, key: String, data: Data) -> ObjectMeta {
        ObjectMeta(
            bucketName: bucket, key: key, size: data.count, contentType: "text/plain",
            etag: Insecure.MD5.hash(data: data).hex, metadata: ["owner": "tests"],
            updatedAt: Date(), tags: ["env": "test"])
    }

    private func putObject(bucket: String, key: String, content: String) throws {
        let data = Data(content.utf8)
        try ObjectFileHandler.write(
            metadata: meta(bucket: bucket, key: key, data: data), data: data,
            to: ObjectFileHandler.storagePath(for: bucket, key: key))
    }

    @discardableResult
    private func putVersionedObject(bucket: String, key: String, content: String) throws -> String {
        let data = Data(content.utf8)
        return try ObjectFileHandler.writeVersioned(
            metadata: meta(bucket: bucket, key: key, data: data), data: data,
            bucketName: bucket, key: key, versioningStatus: .enabled)
    }

    private func currentContent(bucket: String, key: String) throws -> String? {
        guard let (_, data) = try ObjectFileHandler.readCurrentObject(
            bucketName: bucket, key: key, loadData: true)
        else { return nil }
        return data.map { String(decoding: $0, as: UTF8.self) }
    }

    private func copy(
        _ app: Application, token: String,
        _ body: InternalObjectController.ObjectCopyRequestDTO,
        _ check: (TestingHTTPResponse) throws -> Void
    ) async throws {
        try await app.test(
            .POST, "/api/v1/objects/copy",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(body)
            },
            afterResponse: { res in try check(res) })
    }

    private func request(
        from source: String, to destination: String, bucket: String, toBucket: String? = nil,
        versionId: String? = nil, deleteSource: Bool? = nil
    ) -> InternalObjectController.ObjectCopyRequestDTO {
        .init(
            sourceBucket: bucket, sourceKey: source, sourceVersionId: versionId,
            destinationBucket: toBucket ?? bucket, destinationKey: destination,
            deleteSource: deleteSource)
    }

    @Test("Copy keeps the source and carries bytes, metadata and tags over")
    func testCopy() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "copy-src")
            try putObject(bucket: "copy-src", key: "a/one.txt", content: "hello")

            try await copy(app, token: token, request(from: "a/one.txt", to: "b/two.txt", bucket: "copy-src")) { res in
                #expect(res.status == .ok)
                let dto = try res.content.decode(ObjectMeta.ResponseDTO.self)
                #expect(dto.key == "b/two.txt")
                #expect(dto.size == 5)
            }

            #expect(try currentContent(bucket: "copy-src", key: "a/one.txt") == "hello")
            #expect(try currentContent(bucket: "copy-src", key: "b/two.txt") == "hello")
            let copied = try #require(
                try ObjectFileHandler.readCurrentObject(bucketName: "copy-src", key: "b/two.txt", loadData: false)).meta
            #expect(copied.metadata["owner"] == "tests")
            #expect(copied.tags?["env"] == "test")
            #expect(copied.contentType == "text/plain")
        }
    }

    @Test("Copy across two owned buckets")
    func testCopyAcrossBuckets() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "copy-a")
            try await createBucket(app, token: token, name: "copy-b")
            try putObject(bucket: "copy-a", key: "doc.txt", content: "across")

            try await copy(app, token: token, request(from: "doc.txt", to: "moved/doc.txt", bucket: "copy-a", toBucket: "copy-b")) { res in
                #expect(res.status == .ok)
            }
            #expect(try currentContent(bucket: "copy-b", key: "moved/doc.txt") == "across")
        }
    }

    @Test("Move removes the source after the copy")
    func testMove() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "move-plain")
            try putObject(bucket: "move-plain", key: "old.txt", content: "payload")

            try await copy(app, token: token, request(from: "old.txt", to: "new.txt", bucket: "move-plain", deleteSource: true)) { res in
                #expect(res.status == .ok)
            }
            #expect(try currentContent(bucket: "move-plain", key: "old.txt") == nil)
            #expect(try currentContent(bucket: "move-plain", key: "new.txt") == "payload")
        }
    }

    @Test("Move on a versioned bucket leaves a delete marker on the source")
    func testMoveVersioned() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "move-versioned", versioningEnabled: true)
            try putVersionedObject(bucket: "move-versioned", key: "old.txt", content: "payload")

            try await copy(app, token: token, request(from: "old.txt", to: "new.txt", bucket: "move-versioned", deleteSource: true)) { res in
                #expect(res.status == .ok)
                let dto = try res.content.decode(ObjectMeta.ResponseDTO.self)
                #expect(dto.versionId != nil)
            }
            let sourceVersions = try ObjectFileHandler.listVersions(bucketName: "move-versioned", key: "old.txt")
            #expect(sourceVersions.count == 2)
            #expect(sourceVersions.first(where: \.isLatest)?.isDeleteMarker == true)
            #expect(try currentContent(bucket: "move-versioned", key: "old.txt") == nil)
            #expect(try currentContent(bucket: "move-versioned", key: "new.txt") == "payload")
        }
    }

    @Test("Restoring a version copies it onto the key as a new current version")
    func testRestoreVersion() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "restore", versioningEnabled: true)
            let v1 = try putVersionedObject(bucket: "restore", key: "doc.txt", content: "one")
            try putVersionedObject(bucket: "restore", key: "doc.txt", content: "two")
            #expect(try currentContent(bucket: "restore", key: "doc.txt") == "two")

            try await copy(app, token: token, request(from: "doc.txt", to: "doc.txt", bucket: "restore", versionId: v1)) { res in
                #expect(res.status == .ok)
                let dto = try res.content.decode(ObjectMeta.ResponseDTO.self)
                #expect(dto.versionId != nil)
                #expect(dto.versionId != v1)
            }

            #expect(try currentContent(bucket: "restore", key: "doc.txt") == "one")
            let versions = try ObjectFileHandler.listVersions(bucketName: "restore", key: "doc.txt")
            #expect(versions.count == 3)
            #expect(versions.filter(\.isLatest).count == 1)
        }
    }

    @Test("A delete marker cannot be the source of a copy")
    func testDeleteMarkerSource() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "marker-src", versioningEnabled: true)
            try putVersionedObject(bucket: "marker-src", key: "doc.txt", content: "one")
            let marker = try S3Service.deleteObject(
                bucketName: "marker-src", key: "doc.txt", versionId: nil, versioningStatus: .enabled)
            #expect(marker.isDeleteMarker)

            try await copy(app, token: token, request(from: "doc.txt", to: "doc.txt", bucket: "marker-src", versionId: marker.versionId)) { res in
                #expect(res.status == .badRequest)
            }
            // The implicit current object is the marker: nothing to copy.
            try await copy(app, token: token, request(from: "doc.txt", to: "copy.txt", bucket: "marker-src")) { res in
                #expect(res.status == .notFound)
            }
        }
    }

    @Test("Invalid requests are rejected before anything is written")
    func testValidation() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "copy-rules")
            try putObject(bucket: "copy-rules", key: "doc.txt", content: "x")

            try await copy(app, token: token, request(from: "doc.txt", to: "doc.txt", bucket: "copy-rules")) { res in
                #expect(res.status == .badRequest)
            }
            try await copy(app, token: token, request(from: "doc.txt", to: "doc.txt", bucket: "copy-rules", versionId: "null", deleteSource: true)) { res in
                #expect(res.status == .badRequest)
            }
            try await copy(app, token: token, request(from: "doc.txt", to: "folder/", bucket: "copy-rules")) { res in
                #expect(res.status == .badRequest)
            }
            try await copy(app, token: token, request(from: "folder/", to: "doc2.txt", bucket: "copy-rules")) { res in
                #expect(res.status == .badRequest)
            }
            try await copy(app, token: token, request(from: "missing.txt", to: "doc2.txt", bucket: "copy-rules")) { res in
                #expect(res.status == .notFound)
            }
            #expect(try currentContent(bucket: "copy-rules", key: "doc2.txt") == nil)
        }
    }

    @Test("Buckets of another user are not visible to a copy")
    func testForeignBucket() async throws {
        try await withApp { app in
            let owner = try await createUserAndLogin(app, username: "owner@example.com")
            let other = try await createUserAndLogin(app, username: "other@example.com")
            try await createBucket(app, token: owner, name: "owner-bucket")
            try await createBucket(app, token: other, name: "other-bucket")
            try putObject(bucket: "owner-bucket", key: "secret.txt", content: "s")
            try putObject(bucket: "other-bucket", key: "mine.txt", content: "m")

            try await copy(app, token: other, request(from: "secret.txt", to: "stolen.txt", bucket: "owner-bucket", toBucket: "other-bucket")) { res in
                #expect(res.status == .notFound)
            }
            try await copy(app, token: other, request(from: "mine.txt", to: "planted.txt", bucket: "other-bucket", toBucket: "owner-bucket")) { res in
                #expect(res.status == .notFound)
            }
            #expect(try currentContent(bucket: "other-bucket", key: "stolen.txt") == nil)
            #expect(try currentContent(bucket: "owner-bucket", key: "planted.txt") == nil)
        }
    }
}
