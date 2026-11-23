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

/// `/api/v1/buckets/:bucketName/cors`: the console's CORS editor.
@Suite("Internal bucket CORS tests", .serialized)
struct InternalBucketCORSTests {
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

    private func createBucket(_ app: Application, token: String, name: String) async throws {
        try await app.test(
            .POST, "/api/v1/buckets",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(Bucket.Create(name: name, versioningEnabled: false))
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func putRules(
        _ app: Application, token: String, bucket: String, _ rules: [CORSRule],
        expect status: HTTPStatus
    ) async throws {
        try await app.test(
            .PUT, "/api/v1/buckets/\(bucket)/cors",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(InternalBucketController.CORSDTO(rules: rules))
            },
            afterResponse: { res in
                #expect(res.status == status, "\(res.body.string)")
            })
    }

    @Test("Get, put and delete a bucket's CORS rules, cache included")
    func testRoundTrip() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "cors-bucket")

            try await app.test(
                .GET, "/api/v1/buckets/cors-bucket/cors",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(InternalBucketController.CORSDTO.self)
                    #expect(dto.rules.isEmpty)
                })

            // Methods are normalised, blanks dropped, an empty id becomes nil.
            let sent = CORSRule(
                id: " ", allowedOrigins: ["https://app.example.com", " "],
                allowedMethods: ["get", "Put"], allowedHeaders: ["*"], exposeHeaders: ["ETag"],
                maxAgeSeconds: 60)
            try await putRules(app, token: token, bucket: "cors-bucket", [sent], expect: .ok)

            let expected = CORSRule(
                id: nil, allowedOrigins: ["https://app.example.com"], allowedMethods: ["GET", "PUT"],
                allowedHeaders: ["*"], exposeHeaders: ["ETag"], maxAgeSeconds: 60)
            let stored = try #require(try await Bucket.find(app: app, name: "cors-bucket"))
            #expect(stored.corsRules == [expected])
            #expect(await BucketCORSCache.shared.config(for: "cors-bucket")?.rules == [expected])

            try await app.test(
                .GET, "/api/v1/buckets/cors-bucket/cors",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    let dto = try res.content.decode(InternalBucketController.CORSDTO.self)
                    #expect(dto.rules == [expected])
                })

            try await app.test(
                .DELETE, "/api/v1/buckets/cors-bucket/cors",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })
            let cleared = try #require(try await Bucket.find(app: app, name: "cors-bucket"))
            #expect(cleared.corsRules == nil)
            #expect(await BucketCORSCache.shared.config(for: "cors-bucket")?.rules.isEmpty == true)
        }
    }

    @Test("Invalid rule sets are rejected and leave the bucket untouched")
    func testValidation() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "cors-rules")
            let good = CORSRule(allowedOrigins: ["*"], allowedMethods: ["GET"])
            try await putRules(app, token: token, bucket: "cors-rules", [good], expect: .ok)

            try await putRules(app, token: token, bucket: "cors-rules", [], expect: .badRequest)
            try await putRules(
                app, token: token, bucket: "cors-rules",
                [CORSRule(allowedOrigins: [], allowedMethods: ["GET"])], expect: .badRequest)
            try await putRules(
                app, token: token, bucket: "cors-rules",
                [CORSRule(allowedOrigins: ["*"], allowedMethods: ["PATCH"])], expect: .badRequest)
            try await putRules(
                app, token: token, bucket: "cors-rules",
                [CORSRule(allowedOrigins: ["*"], allowedMethods: ["GET"], maxAgeSeconds: -5)],
                expect: .badRequest)

            let stored = try #require(try await Bucket.find(app: app, name: "cors-rules"))
            #expect(stored.corsRules == [good])
        }
    }

    @Test("Another user's bucket is not found")
    func testForeignBucket() async throws {
        try await withApp { app in
            let owner = try await createUserAndLogin(app, username: "owner@example.com")
            let other = try await createUserAndLogin(app, username: "other@example.com")
            try await createBucket(app, token: owner, name: "owner-cors")

            try await putRules(
                app, token: other, bucket: "owner-cors",
                [CORSRule(allowedOrigins: ["*"], allowedMethods: ["GET"])], expect: .notFound)
            try await app.test(
                .GET, "/api/v1/buckets/owner-cors/cors",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: other)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }
}
