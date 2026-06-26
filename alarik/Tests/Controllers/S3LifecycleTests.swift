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

@Suite("S3 Lifecycle tests", .serialized)
struct S3LifecycleTests {
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

    private func lifecycleXML(prefix: String = "", expirationDays: Int = 30) -> String {
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
           <Rule>
              <ID>rule1</ID>
              <Filter><Prefix>\(prefix)</Prefix></Filter>
              <Status>Enabled</Status>
              <Expiration><Days>\(expirationDays)</Days></Expiration>
           </Rule>
        </LifecycleConfiguration>
        """
    }

    private func putLifecycle(_ app: Application, bucketName: String, xml: String) async throws
        -> TestingHTTPResponse
    {
        let bodyData = Data(xml.utf8)
        let signed = signedHeaders(
            for: .PUT, path: "/\(bucketName)", query: "lifecycle", body: bodyData)

        var captured: TestingHTTPResponse!
        try await app.test(
            .PUT, "/\(bucketName)?lifecycle",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: bodyData)
            },
            afterResponse: { res in
                captured = res
            })
        return captured
    }

    @Test("PUT/GET LifecycleConfiguration round-trip")
    func testPutGetLifecycleRoundTrip() async throws {
        let bucketName = "test-lifecycle-roundtrip"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let xml = lifecycleXML(prefix: "logs/", expirationDays: 30)
            let putResponse = try await putLifecycle(app, bucketName: bucketName, xml: xml)
            #expect(putResponse.status == .ok)

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "lifecycle")
            try await app.test(
                .GET, "/\(bucketName)?lifecycle",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<Prefix>logs/</Prefix>"))
                    #expect(res.body.string.contains("<Days>30</Days>"))
                })
        }
    }

    @Test("GET LifecycleConfiguration 404s when never configured")
    func testGetLifecycleNotConfigured() async throws {
        let bucketName = "test-lifecycle-unset"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "lifecycle")
            try await app.test(
                .GET, "/\(bucketName)?lifecycle",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    #expect(
                        res.body.string.contains("<Code>NoSuchLifecycleConfiguration</Code>"))
                })
        }
    }

    @Test("DELETE LifecycleConfiguration resets to unconfigured")
    func testDeleteLifecycleResetsToDefault() async throws {
        let bucketName = "test-lifecycle-delete"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            _ = try await putLifecycle(app, bucketName: bucketName, xml: lifecycleXML())

            let deleteSigned = signedHeaders(
                for: .DELETE, path: "/\(bucketName)", query: "lifecycle")
            try await app.test(
                .DELETE, "/\(bucketName)?lifecycle",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "lifecycle")
            try await app.test(
                .GET, "/\(bucketName)?lifecycle",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("PutBucketLifecycleConfiguration rejects unsupported elements (Transition)")
    func testPutLifecycleRejectsTransition() async throws {
        let bucketName = "test-lifecycle-reject-transition"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let xml = """
                <LifecycleConfiguration>
                  <Rule>
                    <ID>rule1</ID>
                    <Filter><Prefix>documents/</Prefix></Filter>
                    <Status>Enabled</Status>
                    <Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition>
                  </Rule>
                </LifecycleConfiguration>
                """

            let response = try await putLifecycle(app, bucketName: bucketName, xml: xml)
            #expect(response.status == .badRequest)
            #expect(response.body.string.contains("<Code>MalformedXML</Code>"))
        }
    }

    @Test("PutBucketLifecycleConfiguration rejects a Rule with no Rule elements")
    func testPutLifecycleRejectsEmptyConfiguration() async throws {
        let bucketName = "test-lifecycle-reject-empty"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let response = try await putLifecycle(
                app, bucketName: bucketName, xml: "<LifecycleConfiguration></LifecycleConfiguration>")
            #expect(response.status == .badRequest)
        }
    }

    @Test("Set lifecycle - Non-existent bucket fails")
    func testPutLifecycleNonExistentBucket() async throws {
        let bucketName = "test-lifecycle-no-bucket"
        try await withApp { app in
            let response = try await putLifecycle(
                app, bucketName: bucketName, xml: lifecycleXML())
            // Matches the existing convention (see testPutBucketPolicyNonExistentBucket): the
            // access-key/bucket cache check runs before the DB existence check, so an unmapped
            // bucket name yields 403, not 404.
            #expect(response.status == .forbidden)
        }
    }

    @Test("Set lifecycle - Without auth fails")
    func testPutLifecycleUnauthorized() async throws {
        let bucketName = "test-lifecycle-put-unauth"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let bodyData = Data(lifecycleXML().utf8)

            try await app.test(
                .PUT, "/\(bucketName)?lifecycle",
                beforeRequest: { req in
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }

    @Test("Get lifecycle - Without auth fails")
    func testGetLifecycleUnauthorized() async throws {
        let bucketName = "test-lifecycle-get-unauth"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            try await app.test(
                .GET, "/\(bucketName)?lifecycle",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }

    @Test("Delete lifecycle - Without auth fails")
    func testDeleteLifecycleUnauthorized() async throws {
        let bucketName = "test-lifecycle-delete-unauth"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            try await app.test(
                .DELETE, "/\(bucketName)?lifecycle",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }
}
