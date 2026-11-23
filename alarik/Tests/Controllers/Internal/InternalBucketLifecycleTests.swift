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

/// `/api/v1/buckets/:bucketName/lifecycle`: the console's lifecycle editor.
@Suite("Internal bucket lifecycle tests", .serialized)
struct InternalBucketLifecycleTests {
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

    private func rule(
        id: String = "expire", prefix: String = "logs/", days: Int? = 30,
        noncurrentDays: Int? = nil, abortDays: Int? = nil
    ) -> LifecycleRule {
        LifecycleRule(
            id: id, enabled: true, prefix: prefix, expirationDays: days,
            noncurrentVersionExpirationDays: noncurrentDays,
            abortIncompleteMultipartUploadDays: abortDays)
    }

    private func putRules(
        _ app: Application, token: String, bucket: String, _ rules: [LifecycleRule],
        expect status: HTTPStatus
    ) async throws {
        try await app.test(
            .PUT, "/api/v1/buckets/\(bucket)/lifecycle",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(InternalBucketController.LifecycleDTO(rules: rules))
            },
            afterResponse: { res in
                #expect(res.status == status, "\(res.body.string)")
            })
    }

    @Test("Get, put and delete a bucket's lifecycle rules")
    func testRoundTrip() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "lifecycle-bucket")

            try await app.test(
                .GET, "/api/v1/buckets/lifecycle-bucket/lifecycle",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(InternalBucketController.LifecycleDTO.self)
                    #expect(dto.rules.isEmpty)
                })

            let rules = [
                rule(), rule(id: "", prefix: "", days: nil, noncurrentDays: 7, abortDays: 2),
            ]
            try await putRules(app, token: token, bucket: "lifecycle-bucket", rules, expect: .ok)

            let stored = try #require(try await Bucket.find(app: app, name: "lifecycle-bucket"))
            let storedRules = try #require(stored.lifecycleRules)
            #expect(storedRules.count == 2)
            #expect(storedRules[0] == rules[0])
            // A blank id is filled in, never stored empty.
            #expect(!storedRules[1].id.isEmpty)
            #expect(storedRules[1].noncurrentVersionExpirationDays == 7)
            #expect(storedRules[1].abortIncompleteMultipartUploadDays == 2)

            try await app.test(
                .GET, "/api/v1/buckets/lifecycle-bucket/lifecycle",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    let dto = try res.content.decode(InternalBucketController.LifecycleDTO.self)
                    #expect(dto.rules == storedRules)
                })

            try await app.test(
                .DELETE, "/api/v1/buckets/lifecycle-bucket/lifecycle",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })
            let cleared = try #require(try await Bucket.find(app: app, name: "lifecycle-bucket"))
            #expect(cleared.lifecycleRules == nil)
        }
    }

    @Test("Invalid rule sets are rejected and leave the bucket untouched")
    func testValidation() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "lifecycle-rules")
            try await putRules(app, token: token, bucket: "lifecycle-rules", [rule()], expect: .ok)

            try await putRules(app, token: token, bucket: "lifecycle-rules", [], expect: .badRequest)
            try await putRules(
                app, token: token, bucket: "lifecycle-rules", [rule(days: nil)], expect: .badRequest)
            try await putRules(
                app, token: token, bucket: "lifecycle-rules", [rule(days: 0)], expect: .badRequest)
            try await putRules(
                app, token: token, bucket: "lifecycle-rules", [rule(), rule()], expect: .badRequest)

            let stored = try #require(try await Bucket.find(app: app, name: "lifecycle-rules"))
            #expect(stored.lifecycleRules == [rule()])
        }
    }

    @Test("Another user's bucket is not found")
    func testForeignBucket() async throws {
        try await withApp { app in
            let owner = try await createUserAndLogin(app, username: "owner@example.com")
            let other = try await createUserAndLogin(app, username: "other@example.com")
            try await createBucket(app, token: owner, name: "owner-lifecycle")

            try await putRules(app, token: other, bucket: "owner-lifecycle", [rule()], expect: .notFound)
            try await app.test(
                .GET, "/api/v1/buckets/owner-lifecycle/lifecycle",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: other)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }
}
