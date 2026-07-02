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

@Suite("Notification delivery tests", .serialized)
struct NotificationDeliveryTests {

    // MARK: - Fake webhook receiver

    /// A real, listening HTTP server that records every delivery (body + signature header) and
    /// can be told to fail its first N requests - so these tests exercise the actual
    /// NotificationDispatcher HTTP + retry paths, not a stand-in.
    final class FakeReceiver: Sendable {
        actor State {
            var deliveries: [(body: String, signature: String?)] = []
            var failFirst = 0

            func record(body: String, signature: String?) -> Bool {
                if failFirst > 0 {
                    failFirst -= 1
                    return false  // signal the route to return 503
                }
                deliveries.append((body: body, signature: signature))
                return true
            }

            func setFailFirst(_ n: Int) { failFirst = n }
            func count() -> Int { deliveries.count }
            func all() -> [(body: String, signature: String?)] { deliveries }
        }

        let app: Application
        let state: State
        let url: String

        private init(app: Application, state: State, url: String) {
            self.app = app
            self.state = state
            self.url = url
        }

        static func start() async throws -> FakeReceiver {
            let app = try await Application.make(.testing)
            let state = State()

            app.post("hook") { req async throws -> Response in
                let body = req.body.string ?? ""
                let sig = req.headers.first(name: "X-Alarik-Signature-256")
                let ok = await state.record(body: body, signature: sig)
                return Response(status: ok ? .ok : .serviceUnavailable)
            }

            try await app.server.start(address: .hostname("127.0.0.1", port: 0))
            guard let port = app.http.server.shared.localAddress?.port else {
                throw Abort(.internalServerError, reason: "Fake receiver failed to bind a port.")
            }
            return FakeReceiver(app: app, state: state, url: "http://127.0.0.1:\(port)/hook")
        }

        func shutdown() async throws {
            await app.server.shutdown()
            try await app.asyncShutdown()
        }
    }

    // MARK: - Harness

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

    /// Creates a bucket owned by the default admin and installs a notification config (DB + cache)
    /// pointing at `receiver`.
    private func configureBucket(
        _ app: Application, bucket: String, receiver: FakeReceiver,
        events: [String] = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"], secret: String? = nil,
        prefix: String? = nil, suffix: String? = nil
    ) async throws -> NotificationRule {
        let admin = try await User.query(on: app.db).filter(\.$username == "alarik").first()!
        let bucketModel = Bucket(name: bucket, userId: admin.id!)
        let rule = NotificationRule(
            id: UUID(), url: receiver.url, secret: secret, events: events,
            prefix: prefix, suffix: suffix, enabled: true)
        let config = NotificationConfiguration(rules: [rule])
        bucketModel.notificationConfig = config.toJSON()
        try await bucketModel.save(on: app.db)
        await NotificationConfigCache.shared.setConfig(for: bucket, config: config)
        return rule
    }

    /// Waits until the receiver has at least `count` deliveries or the timeout elapses,
    /// driving the dispatcher each poll.
    private func waitForDeliveries(_ receiver: FakeReceiver, atLeast count: Int, timeout: Double = 5)
        async throws -> Bool
    {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            await NotificationDispatcher.shared.drain()
            if await receiver.state.count() >= count { return true }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        return await receiver.state.count() >= count
    }

    // MARK: - Tests

    @Test("emit delivers an ObjectCreated payload with a valid HMAC signature")
    func deliversSignedPayload() async throws {
        let receiver = try await FakeReceiver.start()
        defer { Task { try? await receiver.shutdown() } }

        try await withApp { app in
            _ = try await configureBucket(app, bucket: "notif-bucket", receiver: receiver, secret: "sup3rsecret")

            await NotificationService.emit(
                event: .objectCreatedPut, bucketName: "notif-bucket", key: "hello.txt",
                size: 5, etag: "etag123", versionId: nil, requestId: "REQ", sourceIP: "1.1.1.1",
                on: app.db)

            #expect(try await waitForDeliveries(receiver, atLeast: 1))

            let deliveries = await receiver.state.all()
            let delivery = try #require(deliveries.first)
            #expect(delivery.body.contains("\"eventName\":\"ObjectCreated:Put\""))
            #expect(delivery.body.contains("\"key\":\"hello.txt\""))

            // Signature must verify against the delivered body
            let sig = try #require(delivery.signature)
            let expected = NotificationService.signature(payload: delivery.body, secret: "sup3rsecret")
            #expect(sig == expected)
        }
    }

    @Test("no delivery header when the rule has no secret")
    func noSignatureWithoutSecret() async throws {
        let receiver = try await FakeReceiver.start()
        defer { Task { try? await receiver.shutdown() } }

        try await withApp { app in
            _ = try await configureBucket(app, bucket: "nosecret", receiver: receiver, secret: nil)
            await NotificationService.emit(
                event: .objectCreatedPut, bucketName: "nosecret", key: "a.txt", size: 1,
                etag: "e", versionId: nil, requestId: "R", sourceIP: nil, on: app.db)

            #expect(try await waitForDeliveries(receiver, atLeast: 1))
            let delivery = try #require(await receiver.state.all().first)
            #expect(delivery.signature == nil)
        }
    }

    @Test("buckets without a config emit nothing and never touch the outbox")
    func noConfigNoEmit() async throws {
        try await withApp { app in
            await NotificationService.emit(
                event: .objectCreatedPut, bucketName: "unconfigured", key: "a.txt", size: 1,
                etag: "e", versionId: nil, requestId: "R", sourceIP: nil, on: app.db)

            let count = try await NotificationDelivery.query(on: app.db).count()
            #expect(count == 0)
        }
    }

    @Test("prefix/suffix filters gate which events are delivered")
    func filtersApplied() async throws {
        let receiver = try await FakeReceiver.start()
        defer { Task { try? await receiver.shutdown() } }

        try await withApp { app in
            _ = try await configureBucket(
                app, bucket: "filtered", receiver: receiver,
                events: ["s3:ObjectCreated:*"], prefix: "images/", suffix: ".jpg")

            // Non-matching key -> no outbox row
            await NotificationService.emit(
                event: .objectCreatedPut, bucketName: "filtered", key: "docs/readme.txt",
                size: 1, etag: "e", versionId: nil, requestId: "R", sourceIP: nil, on: app.db)
            #expect(try await NotificationDelivery.query(on: app.db).count() == 0)

            // Matching key -> delivered
            await NotificationService.emit(
                event: .objectCreatedPut, bucketName: "filtered", key: "images/cat.jpg",
                size: 1, etag: "e", versionId: nil, requestId: "R", sourceIP: nil, on: app.db)
            #expect(try await waitForDeliveries(receiver, atLeast: 1))
        }
    }

    @Test("a delivery that first fails is retried and eventually succeeds")
    func retriesUntilSuccess() async throws {
        let receiver = try await FakeReceiver.start()
        defer { Task { try? await receiver.shutdown() } }

        try await withApp { app in
            _ = try await configureBucket(app, bucket: "retry-bucket", receiver: receiver)
            // Fail the first two delivery attempts
            await receiver.state.setFailFirst(2)

            await NotificationService.emit(
                event: .objectCreatedPut, bucketName: "retry-bucket", key: "r.txt", size: 1,
                etag: "e", versionId: nil, requestId: "R", sourceIP: nil, on: app.db)

            // First drain fails -> row stays pending with a future nextAttemptAt. Force it due
            // and drain again to simulate the backoff elapsing, twice.
            for _ in 0..<3 {
                await NotificationDispatcher.shared.drain()
                if let row = try await NotificationDelivery.query(on: app.db).first() {
                    row.nextAttemptAt = Date().addingTimeInterval(-1)
                    try await row.save(on: app.db)
                }
                try await Task.sleep(nanoseconds: 100_000_000)
            }
            #expect(try await waitForDeliveries(receiver, atLeast: 1))

            // Once delivered, the outbox row is gone
            #expect(try await NotificationDelivery.query(on: app.db).count() == 0)
        }
    }

    @Test("the test-event endpoint queues an s3:TestEvent delivery")
    func testEventEndpoint() async throws {
        let receiver = try await FakeReceiver.start()
        defer { Task { try? await receiver.shutdown() } }

        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)
            let rule = try await configureBucket(app, bucket: "test-evt", receiver: receiver, secret: "k")

            try await app.test(
                .POST, "/api/v1/buckets/test-evt/notifications/\(rule.id.uuidString)/test",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .accepted)
                })

            #expect(try await waitForDeliveries(receiver, atLeast: 1))
            let delivery = try #require(await receiver.state.all().first)
            #expect(delivery.body.contains("\"Event\":\"s3:TestEvent\""))
            #expect(delivery.body.contains("\"Bucket\":\"test-evt\""))
        }
    }

    @Test("deleting a bucket purges its pending outbox rows")
    func bucketDeletePurgesOutbox() async throws {
        let receiver = try await FakeReceiver.start()
        defer { Task { try? await receiver.shutdown() } }

        try await withApp { app in
            let admin = try await User.query(on: app.db).filter(\.$username == "alarik").first()!
            _ = try await configureBucket(app, bucket: "doomed", receiver: receiver)
            // BucketService.delete removes the bucket directory, so it must exist on disk
            try BucketHandler.create(name: "doomed")
            await receiver.state.setFailFirst(99)  // keep rows pending

            await NotificationService.emit(
                event: .objectCreatedPut, bucketName: "doomed", key: "a.txt", size: 1,
                etag: "e", versionId: nil, requestId: "R", sourceIP: nil, on: app.db)
            await NotificationDispatcher.shared.drain()
            #expect(try await NotificationDelivery.query(on: app.db).count() >= 1)

            try await BucketService.delete(
                on: app.db, bucketName: "doomed", userId: admin.id!, force: true)

            #expect(try await NotificationDelivery.query(on: app.db).count() == 0)
        }
    }

    // MARK: - Internal API validation

    @Test("setting a webhook config assigns ids and round-trips through GET")
    func setAndGetConfig() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)
            let admin = try await User.query(on: app.db).filter(\.$username == "alarik").first()!
            let bucketModel = Bucket(name: "api-bucket", userId: admin.id!)
            try await bucketModel.save(on: app.db)

            let body = #"{"rules":[{"id":"00000000-0000-0000-0000-000000000000","url":"https://example.com/h","events":["s3:ObjectCreated:*"],"enabled":true}]}"#

            try await app.test(
                .PUT, "/api/v1/buckets/api-bucket/notifications",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    req.headers.contentType = .json
                    req.body = ByteBuffer(string: body)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(InternalBucketController.NotificationConfigDTO.self)
                    #expect(dto.rules.count == 1)
                    // Server assigned a real id
                    #expect(dto.rules[0].id != NotificationRule.zeroUUID)
                })

            try await app.test(
                .GET, "/api/v1/buckets/api-bucket/notifications",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(InternalBucketController.NotificationConfigDTO.self)
                    #expect(dto.rules.count == 1)
                    #expect(dto.rules[0].url == "https://example.com/h")
                })
        }
    }

    @Test("removing a rule purges its queued deliveries; kept rules' rows survive")
    func removingRulePurgesItsQueue() async throws {
        let receiver = try await FakeReceiver.start()
        defer { Task { try? await receiver.shutdown() } }

        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)
            let admin = try await User.query(on: app.db).filter(\.$username == "alarik").first()!
            try await Bucket(name: "purge-bucket", userId: admin.id!).save(on: app.db)

            // Two enabled rules; keep the receiver failing so rows stay queued
            await receiver.state.setFailFirst(99)
            let ruleA = NotificationRule(
                id: UUID(), url: receiver.url, secret: nil, events: ["s3:ObjectCreated:*"],
                prefix: nil, suffix: nil, enabled: true)
            let ruleB = NotificationRule(
                id: UUID(), url: receiver.url, secret: nil, events: ["s3:ObjectCreated:*"],
                prefix: nil, suffix: nil, enabled: true)
            let config = NotificationConfiguration(rules: [ruleA, ruleB])
            let bucket = try await Bucket.query(on: app.db).filter(\.$name == "purge-bucket").first()!
            bucket.notificationConfig = config.toJSON()
            try await bucket.save(on: app.db)
            await NotificationConfigCache.shared.setConfig(for: "purge-bucket", config: config)

            // Queue one delivery per rule
            await NotificationService.emit(
                event: .objectCreatedPut, bucketName: "purge-bucket", key: "a.txt", size: 1,
                etag: "e", versionId: nil, requestId: "R", sourceIP: nil, on: app.db)
            await NotificationDispatcher.shared.drain()
            #expect(try await NotificationDelivery.query(on: app.db).count() == 2)

            // Re-save the config keeping only ruleA
            let body =
                #"{"rules":[{"id":""# + ruleA.id.uuidString
                + #"","url":""# + receiver.url + #"","events":["s3:ObjectCreated:*"],"enabled":true}]}"#
            try await app.test(
                .PUT, "/api/v1/buckets/purge-bucket/notifications",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    req.headers.contentType = .json
                    req.body = ByteBuffer(string: body)
                },
                afterResponse: { res async in #expect(res.status == .ok) })

            // ruleB's queued row is gone; ruleA's remains
            let remaining = try await NotificationDelivery.query(on: app.db).all()
            #expect(remaining.count == 1)
            #expect(remaining.first?.ruleId == ruleA.id)
        }
    }

    @Test("a non-admin cannot target a private/loopback webhook URL")
    func nonAdminBlockedFromPrivateURL() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app, username: "webhookuser@example.com")

            // Create a bucket for this non-admin user via the API
            try await app.test(
                .POST, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(Bucket.Create(name: "user-bucket", versioningEnabled: false))
                })

            let body = #"{"rules":[{"id":"00000000-0000-0000-0000-000000000000","url":"http://127.0.0.1:9000/h","events":["s3:ObjectCreated:*"],"enabled":true}]}"#

            try await app.test(
                .PUT, "/api/v1/buckets/user-bucket/notifications",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    req.headers.contentType = .json
                    req.body = ByteBuffer(string: body)
                },
                afterResponse: { res async in
                    #expect(res.status == .forbidden)
                })
        }
    }

    @Test("invalid webhook URLs and unknown events are rejected")
    func validationRejectsBadInput() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)
            let admin = try await User.query(on: app.db).filter(\.$username == "alarik").first()!
            try await Bucket(name: "val-bucket", userId: admin.id!).save(on: app.db)

            // Bad scheme
            try await app.test(
                .PUT, "/api/v1/buckets/val-bucket/notifications",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    req.headers.contentType = .json
                    req.body = ByteBuffer(
                        string: #"{"rules":[{"id":"00000000-0000-0000-0000-000000000000","url":"ftp://x/y","events":["s3:ObjectCreated:*"],"enabled":true}]}"#)
                },
                afterResponse: { res async in #expect(res.status == .badRequest) })

            // Unknown event
            try await app.test(
                .PUT, "/api/v1/buckets/val-bucket/notifications",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    req.headers.contentType = .json
                    req.body = ByteBuffer(
                        string: #"{"rules":[{"id":"00000000-0000-0000-0000-000000000000","url":"https://x/y","events":["s3:Nonsense"],"enabled":true}]}"#)
                },
                afterResponse: { res async in #expect(res.status == .badRequest) })
        }
    }
}
