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
import NIOCore
import NIOHTTP1
import SotoCore
import SotoS3
import SotoSignerV4
import Testing
import Vapor
import VaporTesting

@testable import Alarik

@Suite("S3 Versioning tests", .serialized)
struct S3VersioningTests {
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

    private func signedHeaders(
        for method: HTTPMethod,
        path: String,
        query: String? = nil,
        body: Data? = nil,
        additionalHeaders: [String: String] = [:]
    ) -> HTTPHeaders {
        var fullPath = path
        if let query = query, !query.isEmpty {
            fullPath += "?\(query)"
        }

        let urlString = "http://\(host)\(fullPath)"
        guard let url = URL(string: urlString) else {
            Issue.record("Invalid URL: \(urlString)")
            return HTTPHeaders()
        }

        let signer = AWSSigner(
            credentials: StaticCredential(accessKeyId: accessKey, secretAccessKey: secretKey),
            name: "s3",
            region: region
        )

        var headers: [(String, String)] = [("host", host)]
        for (key, value) in additionalHeaders {
            headers.append((key, value))
        }

        let signed = signer.signHeaders(
            url: url,
            method: method,
            headers: HTTPHeaders(headers),
            body: body != nil ? .data(body!) : .none
        )

        return signed
    }

    private func createBucket(_ app: Application, bucketName: String) async throws {
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)")
        try await app.test(
            .PUT, "/\(bucketName)",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func enableVersioning(_ app: Application, bucketName: String) async throws {
        let body = """
            <?xml version="1.0" encoding="UTF-8"?>
            <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Enabled</Status>
            </VersioningConfiguration>
            """
        let bodyData = body.data(using: .utf8)!
        let signed = signedHeaders(
            for: .PUT, path: "/\(bucketName)", query: "versioning", body: bodyData)

        try await app.test(
            .PUT, "/\(bucketName)?versioning",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: bodyData)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func suspendVersioning(_ app: Application, bucketName: String) async throws {
        let body = """
            <?xml version="1.0" encoding="UTF-8"?>
            <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Suspended</Status>
            </VersioningConfiguration>
            """
        let bodyData = body.data(using: .utf8)!
        let signed = signedHeaders(
            for: .PUT, path: "/\(bucketName)", query: "versioning", body: bodyData)

        try await app.test(
            .PUT, "/\(bucketName)?versioning",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: bodyData)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func putObject(
        _ app: Application, bucketName: String, key: String, content: String
    ) async throws -> String? {
        let data = content.data(using: .utf8)!
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)/\(key)", body: data)

        var versionId: String? = nil

        try await app.test(
            .PUT, "/\(bucketName)/\(key)",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: data)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
                versionId = res.headers.first(name: "x-amz-version-id")
            })

        return versionId
    }

    @Test("Get versioning - Disabled by default")
    func testGetVersioningDisabledByDefault() async throws {
        let bucketName = "test-versioning-default"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "versioning")

            try await app.test(
                .GET, "/\(bucketName)?versioning",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .xml)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<VersioningConfiguration"))
                    // Should not contain Status element when never enabled
                    #expect(!bodyString.contains("<Status>Enabled</Status>"))
                })
        }
    }

    @Test("Enable versioning - Success")
    func testEnableVersioning() async throws {
        let bucketName = "test-enable-versioning"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            // Verify versioning is enabled
            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "versioning")

            try await app.test(
                .GET, "/\(bucketName)?versioning",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Status>Enabled</Status>"))
                })
        }
    }

    @Test("Suspend versioning - Success")
    func testSuspendVersioning() async throws {
        let bucketName = "test-suspend-versioning"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)
            try await suspendVersioning(app, bucketName: bucketName)

            // Verify versioning is suspended
            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "versioning")

            try await app.test(
                .GET, "/\(bucketName)?versioning",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Status>Suspended</Status>"))
                })
        }
    }

    @Test("Enable versioning - Non-existent bucket fails")
    func testEnableVersioningNonExistentBucket() async throws {
        try await withApp { app in
            let body = """
                <?xml version="1.0" encoding="UTF-8"?>
                <VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>
                """
            let bodyData = body.data(using: .utf8)!
            let signed = signedHeaders(
                for: .PUT, path: "/nonexistent-bucket", query: "versioning", body: bodyData)

            try await app.test(
                .PUT, "/nonexistent-bucket?versioning",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .forbidden)
                })
        }
    }

    @Test("PUT object - Returns version ID when versioning enabled")
    func testPutObjectReturnsVersionId() async throws {
        let bucketName = "test-put-versioned"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            let content = "Hello, versioned world!"
            let data = content.data(using: .utf8)!
            let signed = signedHeaders(for: .PUT, path: "/\(bucketName)/test.txt", body: data)

            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: data)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let versionId = res.headers.first(name: "x-amz-version-id")
                    #expect(versionId != nil)
                    #expect(versionId != "null")
                    #expect(versionId!.count == 32)  // UUID without dashes
                })
        }
    }

    @Test("PUT object - No version ID when versioning disabled")
    func testPutObjectNoVersionIdWhenDisabled() async throws {
        let bucketName = "test-put-no-version"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let content = "Hello, non-versioned world!"
            let data = content.data(using: .utf8)!
            let signed = signedHeaders(for: .PUT, path: "/\(bucketName)/test.txt", body: data)

            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: data)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let versionId = res.headers.first(name: "x-amz-version-id")
                    #expect(versionId == nil)
                })
        }
    }

    @Test("PUT object - Multiple versions create unique IDs")
    func testPutObjectMultipleVersions() async throws {
        let bucketName = "test-multiple-versions"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            let versionId1 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Version 1")
            let versionId2 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Version 2")
            let versionId3 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Version 3")

            #expect(versionId1 != nil)
            #expect(versionId2 != nil)
            #expect(versionId3 != nil)
            #expect(versionId1 != versionId2)
            #expect(versionId2 != versionId3)
            #expect(versionId1 != versionId3)
        }
    }

    @Test("PUT object - Suspended versioning uses null version ID")
    func testPutObjectSuspendedVersioning() async throws {
        let bucketName = "test-suspended-put"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)
            try await suspendVersioning(app, bucketName: bucketName)

            let content = "Suspended version"
            let data = content.data(using: .utf8)!
            let signed = signedHeaders(for: .PUT, path: "/\(bucketName)/test.txt", body: data)

            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: data)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let versionId = res.headers.first(name: "x-amz-version-id")
                    #expect(versionId == "null")
                })
        }
    }

    @Test("GET object - Returns latest version by default")
    func testGetObjectReturnsLatestVersion() async throws {
        let bucketName = "test-get-latest"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Version 1")
            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Version 2")
            let latestVersionId = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Version 3 - Latest")

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)/test.txt")

            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString == "Version 3 - Latest")
                    let returnedVersionId = res.headers.first(name: "x-amz-version-id")
                    #expect(returnedVersionId == latestVersionId)
                })
        }
    }

    @Test("GET object - Specific version by versionId")
    func testGetObjectSpecificVersion() async throws {
        let bucketName = "test-get-specific"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            let versionId1 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Version 1 Content")
            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Version 2 Content")

            // Get specific version
            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt", query: "versionId=\(versionId1!)")

            try await app.test(
                .GET, "/\(bucketName)/test.txt?versionId=\(versionId1!)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString == "Version 1 Content")
                    let returnedVersionId = res.headers.first(name: "x-amz-version-id")
                    #expect(returnedVersionId == versionId1)
                })
        }
    }

    @Test("GET object - Non-existent version returns 404")
    func testGetObjectNonExistentVersion() async throws {
        let bucketName = "test-get-nonexistent-version"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Some content")

            let fakeVersionId = "00000000000000000000000000000000"
            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt", query: "versionId=\(fakeVersionId)")

            try await app.test(
                .GET, "/\(bucketName)/test.txt?versionId=\(fakeVersionId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("List versions - Empty bucket returns empty list")
    func testListVersionsEmpty() async throws {
        let bucketName = "test-list-versions-empty"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "versions")

            try await app.test(
                .GET, "/\(bucketName)?versions",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .xml)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<ListVersionsResult"))
                    #expect(bodyString.contains("<Name>\(bucketName)</Name>"))
                    #expect(bodyString.contains("<IsTruncated>false</IsTruncated>"))
                })
        }
    }

    @Test("List versions - Returns all versions of an object")
    func testListVersionsAllVersions() async throws {
        let bucketName = "test-list-all-versions"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            let versionId1 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "V1")
            let versionId2 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "V2")
            let versionId3 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "V3")

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "versions")

            try await app.test(
                .GET, "/\(bucketName)?versions",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<VersionId>\(versionId1!)</VersionId>"))
                    #expect(bodyString.contains("<VersionId>\(versionId2!)</VersionId>"))
                    #expect(bodyString.contains("<VersionId>\(versionId3!)</VersionId>"))

                    // Only one should be latest
                    let isLatestTrueCount =
                        bodyString.components(separatedBy: "<IsLatest>true</IsLatest>").count - 1
                    #expect(isLatestTrueCount == 1)
                })
        }
    }

    @Test("List versions - With prefix filter")
    func testListVersionsWithPrefix() async throws {
        let bucketName = "test-list-versions-prefix"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            _ = try await putObject(
                app, bucketName: bucketName, key: "docs/file1.txt", content: "Doc 1")
            _ = try await putObject(
                app, bucketName: bucketName, key: "docs/file2.txt", content: "Doc 2")
            _ = try await putObject(
                app, bucketName: bucketName, key: "images/photo.jpg", content: "Photo")

            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "versions&prefix=docs/")

            try await app.test(
                .GET, "/\(bucketName)?versions&prefix=docs/",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Key>docs/file1.txt</Key>"))
                    #expect(bodyString.contains("<Key>docs/file2.txt</Key>"))
                    #expect(!bodyString.contains("<Key>images/photo.jpg</Key>"))
                })
        }
    }

    @Test("DELETE object - Creates delete marker when versioning enabled")
    func testDeleteCreatesDeleteMarker() async throws {
        let bucketName = "test-delete-marker"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Original content")

            // Delete without specifying version
            let deleteSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)/test.txt")

            var deleteMarkerVersionId: String? = nil
            try await app.test(
                .DELETE, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                    deleteMarkerVersionId = res.headers.first(name: "x-amz-version-id")
                    let deleteMarkerHeader = res.headers.first(name: "x-amz-delete-marker")
                    #expect(deleteMarkerVersionId != nil)
                    #expect(deleteMarkerHeader == "true")
                })

            // Object should now return 404
            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/test.txt")
            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })

            // List versions should show delete marker
            let listSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "versions")
            try await app.test(
                .GET, "/\(bucketName)?versions",
                beforeRequest: { req in
                    req.headers.add(contentsOf: listSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<DeleteMarker>"))
                    #expect(bodyString.contains("<VersionId>\(deleteMarkerVersionId!)</VersionId>"))
                })
        }
    }

    @Test("DELETE object - Permanent delete with versionId")
    func testDeletePermanentWithVersionId() async throws {
        let bucketName = "test-permanent-delete"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            let versionId1 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Version 1")
            let versionId2 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Version 2")

            // Delete specific version (permanent)
            let deleteSigned = signedHeaders(
                for: .DELETE, path: "/\(bucketName)/test.txt", query: "versionId=\(versionId1!)")

            try await app.test(
                .DELETE, "/\(bucketName)/test.txt?versionId=\(versionId1!)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                    let returnedVersionId = res.headers.first(name: "x-amz-version-id")
                    #expect(returnedVersionId == versionId1)
                })

            // Version 1 should be gone
            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt", query: "versionId=\(versionId1!)")
            try await app.test(
                .GET, "/\(bucketName)/test.txt?versionId=\(versionId1!)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })

            // Version 2 should still exist
            let getV2Signed = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt", query: "versionId=\(versionId2!)")
            try await app.test(
                .GET, "/\(bucketName)/test.txt?versionId=\(versionId2!)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getV2Signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "Version 2")
                })
        }
    }

    @Test("DELETE object - Without versioning performs permanent delete")
    func testDeleteWithoutVersioningPermanent() async throws {
        let bucketName = "test-delete-no-versioning"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            // Versioning NOT enabled

            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Content")

            let deleteSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)/test.txt")
            try await app.test(
                .DELETE, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                    // Should NOT have version-id or delete-marker headers
                    let versionId = res.headers.first(name: "x-amz-version-id")
                    let deleteMarker = res.headers.first(name: "x-amz-delete-marker")
                    #expect(versionId == nil)
                    #expect(deleteMarker == nil)
                })

            // Object should be gone
            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/test.txt")
            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("List objects - Shows only latest version (not delete markers)")
    func testListObjectsShowsLatestOnly() async throws {
        let bucketName = "test-list-latest-only"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            // Create multiple versions of same key
            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "V1")
            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "V2")
            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "V3")

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)")

            try await app.test(
                .GET, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    // Should only have one <Key>test.txt</Key> entry
                    let keyCount =
                        bodyString.components(separatedBy: "<Key>test.txt</Key>").count - 1
                    #expect(keyCount == 1)
                })
        }
    }

    @Test("List objects - Deleted objects not shown")
    func testListObjectsDeletedNotShown() async throws {
        let bucketName = "test-list-deleted"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            _ = try await putObject(
                app, bucketName: bucketName, key: "keep.txt", content: "Keep this")
            _ = try await putObject(
                app, bucketName: bucketName, key: "delete.txt", content: "Delete this")

            // Delete one object
            let deleteSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)/delete.txt")
            try await app.test(
                .DELETE, "/\(bucketName)/delete.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            // List should only show keep.txt
            let listSigned = signedHeaders(for: .GET, path: "/\(bucketName)")
            try await app.test(
                .GET, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: listSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Key>keep.txt</Key>"))
                    #expect(!bodyString.contains("<Key>delete.txt</Key>"))
                })
        }
    }

    @Test("Versioning - Cache is updated when versioning enabled")
    func testVersioningCacheUpdated() async throws {
        let bucketName = "test-cache-update"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // First put should NOT have version ID (cache says disabled)
            let versionId1 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Before enabling")
            #expect(versionId1 == nil)

            // Enable versioning
            try await enableVersioning(app, bucketName: bucketName)

            // Second put SHOULD have version ID (cache updated)
            let versionId2 = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "After enabling")
            #expect(versionId2 != nil)
            #expect(versionId2 != "null")
        }
    }

    @Test("Versioning - Version IDs are valid format")
    func testVersionIdFormat() async throws {
        let bucketName = "test-version-id-format"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            let versionId = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Content")

            #expect(versionId != nil)
            // Should be lowercase hex (UUID without dashes)
            #expect(versionId!.count == 32)
            #expect(versionId!.allSatisfy { $0.isHexDigit })
            #expect(versionId! == versionId!.lowercased())
        }
    }

    @Test("Versioning - Cannot inject path traversal in versionId")
    func testVersionIdPathTraversalPrevention() async throws {
        let bucketName = "test-path-traversal"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Content")

            // Try to access with malicious versionId
            let maliciousVersionId = "../../../etc/passwd"
            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt",
                query: "versionId=\(maliciousVersionId)")

            try await app.test(
                .GET, "/\(bucketName)/test.txt?versionId=\(maliciousVersionId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    // Should return 404, not expose system files
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Versioning - Delete non-existent version is idempotent")
    func testDeleteNonExistentVersionIdempotent() async throws {
        let bucketName = "test-delete-idempotent"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            _ = try await putObject(
                app, bucketName: bucketName, key: "test.txt", content: "Content")

            let fakeVersionId = "00000000000000000000000000000000"
            let deleteSigned = signedHeaders(
                for: .DELETE, path: "/\(bucketName)/test.txt", query: "versionId=\(fakeVersionId)")

            // S3 returns success even for non-existent versions
            try await app.test(
                .DELETE, "/\(bucketName)/test.txt?versionId=\(fakeVersionId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })
        }
    }

    // MARK: - List Versions with Delimiter Tests

    @Test("List versions with delimiter - Returns common prefixes for folders")
    func testListVersionsWithDelimiter() async throws {
        let bucketName = "test-list-versions-delimiter"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            // Create files in different "folders"
            _ = try await putObject(
                app, bucketName: bucketName, key: "docs/readme.txt", content: "Readme")
            _ = try await putObject(
                app, bucketName: bucketName, key: "docs/guide.txt", content: "Guide")
            _ = try await putObject(
                app, bucketName: bucketName, key: "images/logo.png", content: "Logo")
            _ = try await putObject(
                app, bucketName: bucketName, key: "root.txt", content: "Root file")

            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "versions&delimiter=/")

            try await app.test(
                .GET, "/\(bucketName)?versions&delimiter=/",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    // Should have CommonPrefixes for docs/ and images/
                    #expect(bodyString.contains("<CommonPrefixes>"))
                    #expect(bodyString.contains("<Prefix>docs/</Prefix>"))
                    #expect(bodyString.contains("<Prefix>images/</Prefix>"))
                    // Should have the root file as a version entry
                    #expect(bodyString.contains("<Key>root.txt</Key>"))
                    // Should NOT have the nested files directly
                    #expect(!bodyString.contains("<Key>docs/readme.txt</Key>"))
                    #expect(!bodyString.contains("<Key>docs/guide.txt</Key>"))
                    #expect(!bodyString.contains("<Key>images/logo.png</Key>"))
                    // Should include delimiter in response
                    #expect(bodyString.contains("<Delimiter>/</Delimiter>"))
                })
        }
    }

    @Test("List versions with delimiter and prefix - Navigates into folder")
    func testListVersionsWithDelimiterAndPrefix() async throws {
        let bucketName = "test-list-versions-delimiter-prefix"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            // Create nested folder structure
            _ = try await putObject(
                app, bucketName: bucketName, key: "docs/readme.txt", content: "Readme")
            _ = try await putObject(
                app, bucketName: bucketName, key: "docs/api/endpoints.txt", content: "Endpoints")
            _ = try await putObject(
                app, bucketName: bucketName, key: "docs/api/auth.txt", content: "Auth")
            _ = try await putObject(
                app, bucketName: bucketName, key: "images/logo.png", content: "Logo")

            // List with prefix=docs/ and delimiter=/
            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "versions&delimiter=/&prefix=docs/")

            try await app.test(
                .GET, "/\(bucketName)?versions&delimiter=/&prefix=docs/",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    // Should have CommonPrefix for docs/api/
                    #expect(bodyString.contains("<Prefix>docs/api/</Prefix>"))
                    // Should have docs/readme.txt as a version entry
                    #expect(bodyString.contains("<Key>docs/readme.txt</Key>"))
                    // Should NOT have files from other folders
                    #expect(!bodyString.contains("<Key>images/logo.png</Key>"))
                    // Should NOT have nested api files directly
                    #expect(!bodyString.contains("<Key>docs/api/endpoints.txt</Key>"))
                    #expect(!bodyString.contains("<Key>docs/api/auth.txt</Key>"))
                })
        }
    }

    @Test("List versions without delimiter - Returns all files flat")
    func testListVersionsWithoutDelimiter() async throws {
        let bucketName = "test-list-versions-no-delimiter"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            _ = try await putObject(
                app, bucketName: bucketName, key: "docs/readme.txt", content: "Readme")
            _ = try await putObject(
                app, bucketName: bucketName, key: "images/logo.png", content: "Logo")
            _ = try await putObject(
                app, bucketName: bucketName, key: "root.txt", content: "Root")

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "versions")

            try await app.test(
                .GET, "/\(bucketName)?versions",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    // Should have all files directly listed
                    #expect(bodyString.contains("<Key>docs/readme.txt</Key>"))
                    #expect(bodyString.contains("<Key>images/logo.png</Key>"))
                    #expect(bodyString.contains("<Key>root.txt</Key>"))
                    // Should NOT have CommonPrefixes
                    #expect(!bodyString.contains("<CommonPrefixes>"))
                })
        }
    }

    @Test("List versions with delimiter - Empty bucket returns no common prefixes")
    func testListVersionsDelimiterEmptyBucket() async throws {
        let bucketName = "test-list-versions-delimiter-empty"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "versions&delimiter=/")

            try await app.test(
                .GET, "/\(bucketName)?versions&delimiter=/",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<ListVersionsResult"))
                    #expect(bodyString.contains("<Delimiter>/</Delimiter>"))
                    // Empty CommonPrefixes should NOT be present (S3 behavior)
                    #expect(!bodyString.contains("<CommonPrefixes>"))
                    // No versions or delete markers either
                    #expect(!bodyString.contains("<Version>"))
                    #expect(!bodyString.contains("<DeleteMarker>"))
                })
        }
    }

    @Test("List versions with delimiter - Deep nesting only shows immediate children")
    func testListVersionsDelimiterDeepNesting() async throws {
        let bucketName = "test-list-versions-deep-nesting"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            // Create deeply nested structure
            _ = try await putObject(
                app, bucketName: bucketName, key: "a/b/c/d/file.txt", content: "Deep")
            _ = try await putObject(
                app, bucketName: bucketName, key: "a/b/other.txt", content: "Other")
            _ = try await putObject(
                app, bucketName: bucketName, key: "a/top.txt", content: "Top")

            // List at root with delimiter - should only see "a/"
            let signedRoot = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "versions&delimiter=/")

            try await app.test(
                .GET, "/\(bucketName)?versions&delimiter=/",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signedRoot)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Prefix>a/</Prefix>"))
                    // Should NOT see deeper prefixes
                    #expect(!bodyString.contains("<Prefix>a/b/</Prefix>"))
                    #expect(!bodyString.contains("<Prefix>a/b/c/</Prefix>"))
                })

            // List at a/ with delimiter - should see a/b/ and a/top.txt
            let signedA = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "versions&delimiter=/&prefix=a/")

            try await app.test(
                .GET, "/\(bucketName)?versions&delimiter=/&prefix=a/",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signedA)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Prefix>a/b/</Prefix>"))
                    #expect(bodyString.contains("<Key>a/top.txt</Key>"))
                    // Should NOT see deeper prefixes
                    #expect(!bodyString.contains("<Prefix>a/b/c/</Prefix>"))
                })
        }
    }
}
