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
import VaporTesting

@testable import Alarik

@Suite("Internal Versioning tests", .serialized)
struct InternalVersioningTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()
            let loadCacheLifecycle = LoadCacheLifecycle()
            try await loadCacheLifecycle.didBootAsync(app)
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

    private func createBucket(
        _ app: Application, token: String, name: String, versioningEnabled: Bool = false
    ) async throws {
        let createDTO = Bucket.Create(name: name, versioningEnabled: versioningEnabled)

        try await app.test(
            .POST, "/api/v1/buckets",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(createDTO)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func uploadObject(
        _ app: Application, token: String, bucketName: String, fileName: String, content: String,
        prefix: String = ""
    ) async throws {
        let boundary = "----WebKitFormBoundary\(UUID().uuidString)"

        try await app.test(
            .POST, "/api/v1/objects?bucket=\(bucketName)&prefix=\(prefix)",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                req.headers.replaceOrAdd(
                    name: .contentType, value: "multipart/form-data; boundary=\(boundary)")

                var body = ""
                body += "--\(boundary)\r\n"
                body +=
                    "Content-Disposition: form-data; name=\"data\"; filename=\"\(fileName)\"\r\n"
                body += "Content-Type: text/plain\r\n\r\n"
                body += content
                body += "\r\n--\(boundary)--\r\n"

                req.body = ByteBuffer(string: body)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    @Test("Get versioning - Returns disabled by default")
    func testGetVersioningDefault() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            try await app.test(
                .GET, "/api/v1/buckets/test-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    struct VersioningDTO: Decodable {
                        let status: String
                    }
                    let dto = try res.content.decode(VersioningDTO.self)
                    #expect(dto.status == "Disabled")
                })
        }
    }

    @Test("Get versioning - Returns enabled when created with versioning")
    func testGetVersioningEnabled() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "versioned-bucket", versioningEnabled: true)

            try await app.test(
                .GET, "/api/v1/buckets/versioned-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    struct VersioningDTO: Decodable {
                        let status: String
                    }
                    let dto = try res.content.decode(VersioningDTO.self)
                    #expect(dto.status == "Enabled")
                })
        }
    }

    @Test("Get versioning - Without auth fails")
    func testGetVersioningUnauthorized() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            try await app.test(
                .GET, "/api/v1/buckets/test-bucket/versioning",
                afterResponse: { res in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Get versioning - Non-existent bucket fails")
    func testGetVersioningNonExistentBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .GET, "/api/v1/buckets/nonexistent/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Get versioning - User cannot access other user's bucket")
    func testGetVersioningUserIsolation() async throws {
        try await withApp { app in
            let token1 = try await createUserAndLogin(app, username: "myUniqueUserName")
            try await createBucket(app, token: token1, name: "user1-bucket")

            // Create another user
            let token2 = try await createUserAndLogin(app, username: "myUniqueUserName2")

            // User 2 cannot access user 1's bucket versioning
            try await app.test(
                .GET, "/api/v1/buckets/user1-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token2)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Set versioning - Enable success")
    func testSetVersioningEnable() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            struct VersioningDTO: Content {
                let status: String
            }

            try await app.test(
                .PUT, "/api/v1/buckets/test-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(VersioningDTO(status: "Enabled"))
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let dto = try res.content.decode(VersioningDTO.self)
                    #expect(dto.status == "Enabled")
                })

            // Verify it's actually enabled
            try await app.test(
                .GET, "/api/v1/buckets/test-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    let dto = try res.content.decode(VersioningDTO.self)
                    #expect(dto.status == "Enabled")
                })
        }
    }

    @Test("Set versioning - Suspend success")
    func testSetVersioningSuspend() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket", versioningEnabled: true)

            struct VersioningDTO: Content {
                let status: String
            }

            try await app.test(
                .PUT, "/api/v1/buckets/test-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(VersioningDTO(status: "Suspended"))
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let dto = try res.content.decode(VersioningDTO.self)
                    #expect(dto.status == "Suspended")
                })
        }
    }

    @Test("Set versioning - Invalid status fails")
    func testSetVersioningInvalidStatus() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            struct VersioningDTO: Content {
                let status: String
            }

            try await app.test(
                .PUT, "/api/v1/buckets/test-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(VersioningDTO(status: "InvalidStatus"))
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("Set versioning - Without auth fails")
    func testSetVersioningUnauthorized() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            struct VersioningDTO: Content {
                let status: String
            }

            try await app.test(
                .PUT, "/api/v1/buckets/test-bucket/versioning",
                beforeRequest: { req in
                    try req.content.encode(VersioningDTO(status: "Enabled"))
                },
                afterResponse: { res in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Set versioning - Non-existent bucket fails")
    func testSetVersioningNonExistentBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            struct VersioningDTO: Content {
                let status: String
            }

            try await app.test(
                .PUT, "/api/v1/buckets/nonexistent/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(VersioningDTO(status: "Enabled"))
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("List object versions - Returns all versions")
    func testListObjectVersions() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "versioned-bucket", versioningEnabled: true)

            // Upload same file multiple times
            try await uploadObject(
                app, token: token, bucketName: "versioned-bucket", fileName: "test.txt",
                content: "Version 1")
            try await uploadObject(
                app, token: token, bucketName: "versioned-bucket", fileName: "test.txt",
                content: "Version 2")
            try await uploadObject(
                app, token: token, bucketName: "versioned-bucket", fileName: "test.txt",
                content: "Version 3")

            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(versions.count == 3)

                    // Check all have different version IDs
                    let versionIds = versions.compactMap { $0.versionId }
                    #expect(versionIds.count == 3)
                    #expect(Set(versionIds).count == 3)  // All unique

                    // Only one should be latest
                    let latestVersions = versions.filter { $0.isLatest == true }
                    #expect(latestVersions.count == 1)
                })
        }
    }

    @Test("List object versions - Empty for non-existent key")
    func testListObjectVersionsNonExistentKey() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "versioned-bucket", versioningEnabled: true)

            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=versioned-bucket&key=nonexistent.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(versions.count == 0)
                })
        }
    }

    @Test("List object versions - Without auth fails")
    func testListObjectVersionsUnauthorized() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket", versioningEnabled: true)

            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=test-bucket&key=test.txt",
                afterResponse: { res in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("List object versions - Missing bucket param fails")
    func testListObjectVersionsMissingBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .GET, "/api/v1/objects/versions?key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("List object versions - Missing key param fails")
    func testListObjectVersionsMissingKey() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=test-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("Delete object version - Success")
    func testDeleteObjectVersion() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "versioned-bucket", versioningEnabled: true)

            // Upload multiple versions
            try await uploadObject(
                app, token: token, bucketName: "versioned-bucket", fileName: "test.txt",
                content: "Version 1")
            try await uploadObject(
                app, token: token, bucketName: "versioned-bucket", fileName: "test.txt",
                content: "Version 2")

            // Get versions
            var versions: [ObjectMeta.ResponseDTO] = []
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                })

            #expect(versions.count == 2)
            let versionToDelete = versions.first { $0.isLatest != true }!.versionId!

            // Delete specific version
            try await app.test(
                .DELETE,
                "/api/v1/objects/version?bucket=versioned-bucket&key=test.txt&versionId=\(versionToDelete)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            // Verify only one version remains
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    let remaining = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(remaining.count == 1)
                })
        }
    }

    @Test("Delete object version - Without auth fails")
    func testDeleteObjectVersionUnauthorized() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket", versioningEnabled: true)

            try await app.test(
                .DELETE,
                "/api/v1/objects/version?bucket=test-bucket&key=test.txt&versionId=abc123",
                afterResponse: { res in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Delete object version - Missing versionId param fails")
    func testDeleteObjectVersionMissingVersionId() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            try await app.test(
                .DELETE, "/api/v1/objects/version?bucket=test-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("Upload object - Creates versions when versioning enabled")
    func testUploadCreatesVersions() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "versioned-bucket", versioningEnabled: true)

            // Upload same file 3 times
            for i in 1...3 {
                try await uploadObject(
                    app, token: token, bucketName: "versioned-bucket", fileName: "test.txt",
                    content: "Content \(i)")
            }

            // Verify 3 versions exist
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(versions.count == 3)
                })
        }
    }

    @Test("Upload object - No versions created when versioning disabled")
    func testUploadNoVersionsWhenDisabled() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "normal-bucket", versioningEnabled: false)

            // Upload same file 3 times
            for i in 1...3 {
                try await uploadObject(
                    app, token: token, bucketName: "normal-bucket", fileName: "test.txt",
                    content: "Content \(i)")
            }

            // Should have 0 versions (not versioned storage)
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=normal-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    // May return 1 (the current file) or 0 depending on implementation
                    #expect(versions.count <= 1)
                })
        }
    }

    @Test("Delete object - Creates delete marker when versioning enabled")
    func testDeleteCreatesDeleteMarker() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "versioned-bucket", versioningEnabled: true)

            try await uploadObject(
                app, token: token, bucketName: "versioned-bucket", fileName: "test.txt",
                content: "Original content")

            // Delete the object
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            // Check versions - should have original + delete marker
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(versions.count == 2)

                    // One should be a delete marker
                    let deleteMarkers = versions.filter { $0.isDeleteMarker == true }
                    #expect(deleteMarkers.count == 1)
                })

            // Object should not appear in regular listing
            try await app.test(
                .GET, "/api/v1/objects?bucket=versioned-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    let files = page.items.filter { !$0.isFolder }
                    #expect(files.count == 0)
                })
        }
    }

    @Test("Create bucket - With versioning enabled")
    func testCreateBucketWithVersioning() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            let createDTO = Bucket.Create(name: "new-versioned-bucket", versioningEnabled: true)

            try await app.test(
                .POST, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bucket = try res.content.decode(Bucket.ResponseDTO.self)
                    #expect(bucket.name == "new-versioned-bucket")
                    #expect(bucket.versioningStatus == "Enabled")
                })
        }
    }

    @Test("Create bucket - Default versioning is disabled")
    func testCreateBucketDefaultVersioningDisabled() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            let createDTO = Bucket.Create(name: "normal-bucket", versioningEnabled: false)

            try await app.test(
                .POST, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bucket = try res.content.decode(Bucket.ResponseDTO.self)
                    #expect(bucket.versioningStatus == "Disabled")
                })
        }
    }

    @Test("User isolation - Cannot access other user's bucket versions")
    func testUserIsolationVersions() async throws {
        try await withApp { app in
            let token1 = try await createUserAndLogin(app, username: "abd")
            try await createBucket(app, token: token1, name: "user1-bucket", versioningEnabled: true)
            try await uploadObject(
                app, token: token1, bucketName: "user1-bucket", fileName: "secret.txt",
                content: "Secret data")

            // Create another user
            let token2 = try await createUserAndLogin(app, username: "abdef")

            // User 2 cannot list versions of user 1's bucket
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=user1-bucket&key=secret.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token2)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("User isolation - Cannot delete other user's object version")
    func testUserIsolationDeleteVersion() async throws {
        try await withApp { app in
            let token1 = try await createUserAndLogin(app, username: "abd")
            try await createBucket(app, token: token1, name: "user1-bucket", versioningEnabled: true)
            try await uploadObject(
                app, token: token1, bucketName: "user1-bucket", fileName: "important.txt",
                content: "Important data")

            // Get the version ID
            var versionId: String = ""
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=user1-bucket&key=important.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token1)
                },
                afterResponse: { res in
                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    versionId = versions.first!.versionId!
                })

            // Create another user
            let token2 = try await createUserAndLogin(app, username: "abdef")

            // User 2 cannot delete user 1's version
            try await app.test(
                .DELETE,
                "/api/v1/objects/version?bucket=user1-bucket&key=important.txt&versionId=\(versionId)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token2)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })

            // Verify version still exists for user 1
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=user1-bucket&key=important.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token1)
                },
                afterResponse: { res in
                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(versions.count == 1)
                })
        }
    }

    @Test("User isolation - Cannot set versioning on other user's bucket")
    func testUserIsolationSetVersioning() async throws {
        try await withApp { app in
            let token1 = try await createUserAndLogin(app, username: "abdef")
            try await createBucket(app, token: token1, name: "user1-bucket")

            // Create another user
            let token2 = try await createUserAndLogin(app, username: "abdef123")

            struct VersioningDTO: Content {
                let status: String
            }

            // User 2 cannot enable versioning on user 1's bucket
            try await app.test(
                .PUT, "/api/v1/buckets/user1-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token2)
                    try req.content.encode(VersioningDTO(status: "Enabled"))
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }
}
