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

@Suite("S3 Bucket Policy tests", .serialized)
struct S3BucketPolicyTests {
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

    private func publicReadPolicy(bucketName: String, resourceSuffix: String = "*") -> String {
        """
        {
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "PublicRead",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::\(bucketName)/\(resourceSuffix)"
            }]
        }
        """
    }

    private func publicListPolicy(bucketName: String) -> String {
        """
        {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::\(bucketName)"
            }]
        }
        """
    }

    @Test("PutBucketPolicy - Valid public-read policy succeeds and GetBucketPolicy echoes it back")
    func testPutBucketPolicyValid() async throws {
        let bucketName = "test-policy-put-valid"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let policyJSON = publicReadPolicy(bucketName: bucketName)

            let putResponse = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)
            #expect(putResponse.status == .noContent)

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "policy")
            try await app.test(
                .GET, "/\(bucketName)?policy",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == policyJSON)
                })
        }
    }

    @Test("PutBucketPolicy - Effect: Deny is rejected")
    func testPutBucketPolicyEffectDenyRejected() async throws {
        let bucketName = "test-policy-deny-rejected"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let policyJSON = """
                {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::\(bucketName)/*"
                    }]
                }
                """

            let response = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)
            #expect(response.status == .badRequest)
            #expect(response.body.string.contains("<Code>MalformedPolicy</Code>"))
        }
    }

    @Test("PutBucketPolicy - Non-wildcard Principal is rejected")
    func testPutBucketPolicyNonWildcardPrincipalRejected() async throws {
        let bucketName = "test-policy-principal-rejected"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let policyJSON = """
                {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::\(bucketName)/*"
                    }]
                }
                """

            let response = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)
            #expect(response.status == .badRequest)
            #expect(response.body.string.contains("<Code>MalformedPolicy</Code>"))
        }
    }

    @Test("PutBucketPolicy - Disallowed Action is rejected")
    func testPutBucketPolicyDisallowedActionRejected() async throws {
        let bucketName = "test-policy-action-rejected"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let policyJSON = """
                {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:PutObject",
                        "Resource": "arn:aws:s3:::\(bucketName)/*"
                    }]
                }
                """

            let response = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)
            #expect(response.status == .badRequest)
            #expect(response.body.string.contains("<Code>MalformedPolicy</Code>"))
        }
    }

    @Test("PutBucketPolicy - Resource bucket name mismatch is rejected")
    func testPutBucketPolicyMismatchedResourceBucketRejected() async throws {
        let bucketName = "test-policy-resource-mismatch"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let policyJSON = """
                {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::some-other-bucket/*"
                    }]
                }
                """

            let response = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)
            #expect(response.status == .badRequest)
            #expect(response.body.string.contains("<Code>MalformedPolicy</Code>"))
        }
    }

    @Test("PutBucketPolicy - Empty Statement list is rejected")
    func testPutBucketPolicyEmptyStatementsRejected() async throws {
        let bucketName = "test-policy-empty-statements"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let policyJSON = """
                { "Version": "2012-10-17", "Statement": [] }
                """

            let response = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)
            #expect(response.status == .badRequest)
            #expect(response.body.string.contains("<Code>MalformedPolicy</Code>"))
        }
    }

    @Test("PutBucketPolicy - Malformed JSON is rejected")
    func testPutBucketPolicyMalformedJSONRejected() async throws {
        let bucketName = "test-policy-malformed-json"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let response = try await putPolicy(
                app, bucketName: bucketName, policyJSON: "{not valid json")
            #expect(response.status == .badRequest)
            #expect(response.body.string.contains("<Code>MalformedPolicy</Code>"))
        }
    }

    @Test("PutBucketPolicy - Non-existent bucket fails")
    func testPutBucketPolicyNonExistentBucket() async throws {
        let bucketName = "test-policy-no-bucket"
        try await withApp { app in
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            let response = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)
            // Matches the existing handleVersioningPut convention (see
            // testEnableVersioningNonExistentBucket): the access-key/bucket cache check runs
            // before the DB existence check, so an unmapped bucket name yields 403, not 404.
            #expect(response.status == .forbidden)
        }
    }

    @Test("PutBucketPolicy - Unauthorized request is rejected")
    func testPutBucketPolicyUnauthorized() async throws {
        let bucketName = "test-policy-put-unauth"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            let bodyData = Data(policyJSON.utf8)

            try await app.test(
                .PUT, "/\(bucketName)?policy",
                beforeRequest: { req in
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })

            // No policy should have been saved
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

    @Test("DeleteBucketPolicy - Removes the policy, GetBucketPolicy then 404s")
    func testDeleteBucketPolicy() async throws {
        let bucketName = "test-policy-delete"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            let deleteSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)", query: "policy")
            try await app.test(
                .DELETE, "/\(bucketName)?policy",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "policy")
            try await app.test(
                .GET, "/\(bucketName)?policy",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    #expect(res.body.string.contains("<Code>NoSuchBucketPolicy</Code>"))
                })
        }
    }

    @Test("Anonymous GetObject succeeds when granted by bucket policy")
    func testAnonymousGetObjectAllowedByPolicy() async throws {
        let bucketName = "test-policy-anon-get"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "public.txt", content: "hello")
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            // No Authorization header, no signed query params - fully anonymous
            try await app.test(
                .GET, "/\(bucketName)/public.txt",
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "hello")
                })
        }
    }

    @Test("Anonymous GetObject is denied without a policy")
    func testAnonymousGetObjectDeniedWithoutPolicy() async throws {
        let bucketName = "test-policy-anon-get-denied"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "private.txt", content: "secret")

            try await app.test(
                .GET, "/\(bucketName)/private.txt",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }

    @Test("Anonymous GetObject is denied for a key outside the granted prefix")
    func testAnonymousGetObjectDeniedWrongPrefix() async throws {
        let bucketName = "test-policy-anon-get-wrong-prefix"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(
                app, bucketName: bucketName, key: "private/secret.txt", content: "secret")
            let policyJSON = publicReadPolicy(bucketName: bucketName, resourceSuffix: "public/*")
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            try await app.test(
                .GET, "/\(bucketName)/private/secret.txt",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }

    @Test("Anonymous GetObject only allows keys matching the granted prefix")
    func testAnonymousGetObjectPrefixScoping() async throws {
        let bucketName = "test-policy-anon-prefix-scoping"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(
                app, bucketName: bucketName, key: "public/file.txt", content: "public content")
            try await putObject(
                app, bucketName: bucketName, key: "private/file.txt", content: "private content")
            let policyJSON = publicReadPolicy(bucketName: bucketName, resourceSuffix: "public/*")
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            try await app.test(
                .GET, "/\(bucketName)/public/file.txt",
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "public content")
                })

            try await app.test(
                .GET, "/\(bucketName)/private/file.txt",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }

    @Test("Anonymous HeadObject succeeds when granted by bucket policy")
    func testAnonymousHeadObjectAllowedByPolicy() async throws {
        let bucketName = "test-policy-anon-head"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "public.txt", content: "hello")
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            try await app.test(
                .HEAD, "/\(bucketName)/public.txt",
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("Anonymous ListBucket succeeds when granted by bucket policy")
    func testAnonymousListBucketAllowedByPolicy() async throws {
        let bucketName = "test-policy-anon-list"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "a.txt", content: "A")
            let policyJSON = publicListPolicy(bucketName: bucketName)
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            try await app.test(
                .GET, "/\(bucketName)",
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<Key>a.txt</Key>"))
                })
        }
    }

    @Test("Anonymous ListBucket is denied when only GetObject is granted")
    func testAnonymousListBucketDeniedWithoutGrant() async throws {
        let bucketName = "test-policy-anon-list-denied"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "a.txt", content: "A")
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            try await app.test(
                .GET, "/\(bucketName)",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }

    @Test("Anonymous PutObject is always rejected, regardless of policy")
    func testAnonymousPutObjectAlwaysRejected() async throws {
        let bucketName = "test-policy-anon-put-rejected"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            let data = Data("malicious".utf8)
            try await app.test(
                .PUT, "/\(bucketName)/new-file.txt",
                beforeRequest: { req in
                    req.body = ByteBuffer(data: data)
                },
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })

            try await app.test(
                .GET, "/\(bucketName)/new-file.txt",
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Anonymous DeleteObject is always rejected, regardless of policy")
    func testAnonymousDeleteObjectAlwaysRejected() async throws {
        let bucketName = "test-policy-anon-delete-rejected"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "keep.txt", content: "keep me")
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            try await app.test(
                .DELETE, "/\(bucketName)/keep.txt",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })

            try await app.test(
                .GET, "/\(bucketName)/keep.txt",
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "keep me")
                })
        }
    }

    @Test("An invalid Authorization header never falls back to anonymous policy access")
    func testGarbledAuthorizationHeaderNeverFallsBackToAnonymous() async throws {
        let bucketName = "test-policy-garbled-auth"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "public.txt", content: "hello")
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            // A garbage Authorization header, even though the object is otherwise public
            try await app.test(
                .GET, "/\(bucketName)/public.txt",
                beforeRequest: { req in
                    req.headers.add(name: "Authorization", value: "AWS4-HMAC-SHA256 garbage")
                },
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }

    @Test("Bucket policy takes effect and stops taking effect immediately, without a restart")
    func testPolicyCacheUpdatesImmediately() async throws {
        let bucketName = "test-policy-cache-immediate"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "file.txt", content: "data")

            // Before any policy - denied
            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })

            // After PUT policy - immediately allowed
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            // After DELETE policy - immediately denied again
            let deleteSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)", query: "policy")
            try await app.test(
                .DELETE, "/\(bucketName)?policy",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }

    @Test("Bucket policy does not leak to a newly created bucket with the same name")
    func testPolicyDoesNotLeakAcrossBucketRecreation() async throws {
        let bucketName = "test-policy-recreate"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "file.txt", content: "data")
            let policyJSON = publicReadPolicy(bucketName: bucketName)
            _ = try await putPolicy(app, bucketName: bucketName, policyJSON: policyJSON)

            // Confirm the policy is active before deleting anything
            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            // Delete the object, then the bucket itself (not just the policy)
            let deleteObjectSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)/file.txt")
            try await app.test(
                .DELETE, "/\(bucketName)/file.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteObjectSigned)
                })

            let deleteBucketSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)")
            try await app.test(
                .DELETE, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteBucketSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            // Recreate a bucket with the same name - it must NOT inherit the deleted
            // bucket's policy from a stale cache entry
            try await createBucket(app, bucketName: bucketName)
            try await putObject(
                app, bucketName: bucketName, key: "file.txt", content: "different data")

            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }
}
