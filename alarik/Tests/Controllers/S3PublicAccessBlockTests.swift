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

@Suite("S3 Public Access Block tests", .serialized)
struct S3PublicAccessBlockTests {
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

    private func putObject(_ app: Application, bucketName: String, key: String, content: String)
        async throws
    {
        let data = content.data(using: .utf8)!
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)/\(key)", body: data)

        try await app.test(
            .PUT, "/\(bucketName)/\(key)",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: data)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func putPolicy(_ app: Application, bucketName: String, policyJSON: String) async throws
        -> TestingHTTPResponse
    {
        let bodyData = Data(policyJSON.utf8)
        let signed = signedHeaders(
            for: .PUT, path: "/\(bucketName)", query: "policy", body: bodyData)

        var captured: TestingHTTPResponse!
        try await app.test(
            .PUT, "/\(bucketName)?policy",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: bodyData)
            },
            afterResponse: { res in
                captured = res
            })
        return captured
    }

    private func publicReadPolicy(bucketName: String) -> String {
        """
        {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::\(bucketName)/*"
            }]
        }
        """
    }

    private func publicAccessBlockXML(
        blockPublicAcls: Bool = false, ignorePublicAcls: Bool = false,
        blockPublicPolicy: Bool = false, restrictPublicBuckets: Bool = false
    ) -> String {
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
           <BlockPublicAcls>\(blockPublicAcls)</BlockPublicAcls>
           <IgnorePublicAcls>\(ignorePublicAcls)</IgnorePublicAcls>
           <BlockPublicPolicy>\(blockPublicPolicy)</BlockPublicPolicy>
           <RestrictPublicBuckets>\(restrictPublicBuckets)</RestrictPublicBuckets>
        </PublicAccessBlockConfiguration>
        """
    }

    private func putPublicAccessBlock(_ app: Application, bucketName: String, xml: String)
        async throws -> TestingHTTPResponse
    {
        let bodyData = Data(xml.utf8)
        let signed = signedHeaders(
            for: .PUT, path: "/\(bucketName)", query: "publicAccessBlock", body: bodyData)

        var captured: TestingHTTPResponse!
        try await app.test(
            .PUT, "/\(bucketName)?publicAccessBlock",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: bodyData)
            },
            afterResponse: { res in
                captured = res
            })
        return captured
    }

    @Test("PUT/GET PublicAccessBlock round-trip")
    func testPutGetPublicAccessBlockRoundTrip() async throws {
        let bucketName = "test-pab-roundtrip"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let xml = publicAccessBlockXML(
                blockPublicAcls: true, ignorePublicAcls: false, blockPublicPolicy: true,
                restrictPublicBuckets: false)
            let putResponse = try await putPublicAccessBlock(app, bucketName: bucketName, xml: xml)
            #expect(putResponse.status == .ok)

            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "publicAccessBlock")
            try await app.test(
                .GET, "/\(bucketName)?publicAccessBlock",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<BlockPublicAcls>true</BlockPublicAcls>"))
                    #expect(res.body.string.contains("<IgnorePublicAcls>false</IgnorePublicAcls>"))
                    #expect(
                        res.body.string.contains("<BlockPublicPolicy>true</BlockPublicPolicy>"))
                    #expect(
                        res.body.string.contains(
                            "<RestrictPublicBuckets>false</RestrictPublicBuckets>"))
                })
        }
    }

    @Test("GET PublicAccessBlock 404s when never configured")
    func testGetPublicAccessBlockNotConfigured() async throws {
        let bucketName = "test-pab-not-configured"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "publicAccessBlock")
            try await app.test(
                .GET, "/\(bucketName)?publicAccessBlock",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    #expect(
                        res.body.string.contains(
                            "<Code>NoSuchPublicAccessBlockConfiguration</Code>"))
                })
        }
    }

    @Test("DELETE PublicAccessBlock resets to unconfigured (404 again), not all-false-but-present")
    func testDeletePublicAccessBlockResetsToDefault() async throws {
        let bucketName = "test-pab-delete"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let xml = publicAccessBlockXML(restrictPublicBuckets: true)
            _ = try await putPublicAccessBlock(app, bucketName: bucketName, xml: xml)

            let deleteSigned = signedHeaders(
                for: .DELETE, path: "/\(bucketName)", query: "publicAccessBlock")
            try await app.test(
                .DELETE, "/\(bucketName)?publicAccessBlock",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "publicAccessBlock")
            try await app.test(
                .GET, "/\(bucketName)?publicAccessBlock",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("BlockPublicPolicy rejects PutBucketPolicy outright, regardless of policy content")
    func testBlockPublicPolicyRejectsPutBucketPolicy() async throws {
        let bucketName = "test-pab-block-policy"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let xml = publicAccessBlockXML(blockPublicPolicy: true)
            _ = try await putPublicAccessBlock(app, bucketName: bucketName, xml: xml)

            let response = try await putPolicy(
                app, bucketName: bucketName, policyJSON: publicReadPolicy(bucketName: bucketName))
            #expect(response.status == .forbidden)
            #expect(response.body.string.contains("<Code>AccessDenied</Code>"))

            // Confirm nothing was saved
            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "policy")
            try await app.test(
                .GET, "/\(bucketName)?policy",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test(
        "RestrictPublicBuckets blocks anonymous access even with a public policy, owner access unaffected"
    )
    func testRestrictPublicBucketsBlocksAnonymousAccess() async throws {
        let bucketName = "test-pab-restrict"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "file.txt", content: "hello")
            _ = try await putPolicy(
                app, bucketName: bucketName, policyJSON: publicReadPolicy(bucketName: bucketName))

            // Before RestrictPublicBuckets - anonymous access works
            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            let xml = publicAccessBlockXML(restrictPublicBuckets: true)
            _ = try await putPublicAccessBlock(app, bucketName: bucketName, xml: xml)

            // After RestrictPublicBuckets - anonymous access is denied, even though the policy
            // still grants it
            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })

            // The owner's authenticated access is unaffected
            let ownerSigned = signedHeaders(for: .GET, path: "/\(bucketName)/file.txt")
            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: ownerSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "hello")
                })
        }
    }

    @Test("Public access block configuration is reloaded into the cache on startup")
    func testPublicAccessBlockCacheLoadedOnStartup() async throws {
        let bucketName = "test-pab-startup-reload"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "file.txt", content: "hello")
            _ = try await putPolicy(
                app, bucketName: bucketName, policyJSON: publicReadPolicy(bucketName: bucketName))
            let xml = publicAccessBlockXML(restrictPublicBuckets: true)
            _ = try await putPublicAccessBlock(app, bucketName: bucketName, xml: xml)

            // Confirm it's actually enforced before simulating the restart
            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })

            // Clear the in-memory cache directly (without touching the DB row), simulating a
            // process restart where the cache starts empty and must be rebuilt from the DB.
            await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)

            // Without a reload, the restriction would now (incorrectly) appear lifted
            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            // Re-run the boot-time cache load, as a real process restart would
            try await LoadCacheLifecycle().didBootAsync(app)

            // The restriction is back, proving it was correctly reloaded from the DB
            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }

    @Test("Public access block is removed when the bucket is deleted")
    func testPublicAccessBlockRemovedOnBucketDelete() async throws {
        let bucketName = "test-pab-bucket-delete"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let xml = publicAccessBlockXML(blockPublicPolicy: true)
            _ = try await putPublicAccessBlock(app, bucketName: bucketName, xml: xml)

            let deleteBucketSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)")
            try await app.test(
                .DELETE, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteBucketSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            // Recreate a bucket with the same name - it must NOT inherit the deleted bucket's
            // public access block from a stale cache entry
            try await createBucket(app, bucketName: bucketName)
            let response = try await putPolicy(
                app, bucketName: bucketName, policyJSON: publicReadPolicy(bucketName: bucketName))
            #expect(response.status == .noContent)
        }
    }
}
