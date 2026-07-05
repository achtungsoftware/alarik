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

import Fluent
import Foundation
import Testing
import Vapor
import VaporTesting

@testable import Alarik

@Suite("SharedLinkController tests", .serialized)
struct SharedLinkControllerTests {
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

    /// Directly creates a user (no bucket/object DB rows are needed by SharedLinkController -
    /// only the underlying file on disk and a SharedLink row, mirroring how other tests write
    /// objects straight to disk via ObjectFileHandler rather than going through the full API).
    private func createUser(_ app: Application) async throws -> UUID {
        let user = User(
            name: "Share Test User",
            username: UUID().uuidString,
            passwordHash: try Bcrypt.hash("TestPass123!"),
            isAdmin: false
        )
        try await user.save(on: app.db)
        return user.id!
    }

    private func writeObject(bucketName: String, key: String, content: String) throws {
        let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
        let data = Data(content.utf8)
        let meta = ObjectMeta(
            bucketName: bucketName,
            key: key,
            size: data.count,
            contentType: "text/plain",
            etag: Insecure.MD5.hash(data: data).hex,
            updatedAt: Date()
        )
        try ObjectFileHandler.write(metadata: meta, data: data, to: path)
    }

    @Test("Serve - Valid token serves the file with zero authentication")
    func testServeValidToken() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            try writeObject(bucketName: "shared-bucket", key: "file.txt", content: "hello world")

            let link = SharedLink(
                userId: userId, bucketName: "shared-bucket", key: "file.txt",
                expiresAt: Date().addingTimeInterval(3600))
            try await link.save(on: app.db)

            try await app.test(
                .GET, "/api/v1/shared/\(link.id!.uuidString)",
                afterResponse: { res async in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "hello world")
                    #expect(
                        res.headers.first(name: "Content-Disposition")
                            == "attachment; filename=\"file.txt\"")
                })
        }
    }

    @Test("Serve - Content-Disposition uses only the last path segment of the key")
    func testServeContentDispositionUsesLastPathSegment() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            try writeObject(
                bucketName: "shared-bucket", key: "folder/sub/report.pdf", content: "%PDF-1.4")

            let link = SharedLink(
                userId: userId, bucketName: "shared-bucket", key: "folder/sub/report.pdf",
                expiresAt: Date().addingTimeInterval(3600))
            try await link.save(on: app.db)

            try await app.test(
                .GET, "/api/v1/shared/\(link.id!.uuidString)",
                afterResponse: { res async in
                    #expect(res.status == .ok)
                    #expect(
                        res.headers.first(name: "Content-Disposition")
                            == "attachment; filename=\"report.pdf\"")
                })
        }
    }

    @Test("Serve - Non-expiring link (nil expiresAt) works and survives the cleanup filter")
    func testServeNonExpiringLink() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            try writeObject(bucketName: "shared-bucket", key: "forever.txt", content: "no expiry")

            let link = SharedLink(
                userId: userId, bucketName: "shared-bucket", key: "forever.txt", expiresAt: nil)
            try await link.save(on: app.db)

            try await app.test(
                .GET, "/api/v1/shared/\(link.id!.uuidString)",
                afterResponse: { res async in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "no expiry")
                })

            // The hourly cleanup deletes rows where expires_at <= now - a NULL expiry must
            // never match that filter, or non-expiring links would silently vanish within an
            // hour of creation.
            let expired = try await SharedLink.query(on: app.db)
                .filter(\.$expiresAt <= Date.now)
                .all()
            #expect(expired.isEmpty)
            #expect(try await SharedLink.query(on: app.db).count() == 1)
        }
    }

    @Test("Serve - Expired link fails")
    func testServeExpiredLink() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            try writeObject(bucketName: "shared-bucket", key: "file.txt", content: "hello world")

            let link = SharedLink(
                userId: userId, bucketName: "shared-bucket", key: "file.txt",
                expiresAt: Date().addingTimeInterval(-3600))
            try await link.save(on: app.db)

            try await app.test(
                .GET, "/api/v1/shared/\(link.id!.uuidString)",
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Serve - Non-existent token fails")
    func testServeNonExistentToken() async throws {
        try await withApp { app in
            try await app.test(
                .GET, "/api/v1/shared/\(UUID().uuidString)",
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Serve - Malformed (non-UUID) token fails cleanly, not a 500")
    func testServeMalformedToken() async throws {
        try await withApp { app in
            try await app.test(
                .GET, "/api/v1/shared/not-a-valid-uuid",
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Serve - Object deleted after the link was created fails cleanly, not a 500")
    func testServeObjectDeletedAfterLinkCreated() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            try writeObject(bucketName: "shared-bucket", key: "file.txt", content: "hello world")

            let link = SharedLink(
                userId: userId, bucketName: "shared-bucket", key: "file.txt",
                expiresAt: Date().addingTimeInterval(3600))
            try await link.save(on: app.db)

            // Remove the underlying file, simulating it being deleted after the link was made
            let path = ObjectFileHandler.storagePath(for: "shared-bucket", key: "file.txt")
            try FileManager.default.removeItem(atPath: path)

            try await app.test(
                .GET, "/api/v1/shared/\(link.id!.uuidString)",
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Serve - Object deleted (delete marker) in a versioned bucket fails, not 200 with an empty body")
    func testServeDeleteMarkerFails() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            let data = Data("versioned content".utf8)
            let meta = ObjectMeta(
                bucketName: "shared-versioned-bucket", key: "file.txt", size: data.count,
                contentType: "text/plain", etag: Insecure.MD5.hash(data: data).hex,
                updatedAt: Date())
            _ = try ObjectFileHandler.writeVersioned(
                metadata: meta, data: data, bucketName: "shared-versioned-bucket",
                key: "file.txt", versioningStatus: .enabled)

            let link = SharedLink(
                userId: userId, bucketName: "shared-versioned-bucket", key: "file.txt",
                expiresAt: Date().addingTimeInterval(3600))
            try await link.save(on: app.db)

            // Delete the object - in a versioned bucket this creates a delete marker as the
            // new latest version, rather than removing the file outright.
            _ = try ObjectFileHandler.createDeleteMarker(
                bucketName: "shared-versioned-bucket", key: "file.txt")

            try await app.test(
                .GET, "/api/v1/shared/\(link.id!.uuidString)",
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }
}
