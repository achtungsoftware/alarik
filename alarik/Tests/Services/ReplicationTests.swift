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
import NIOCore
import NIOHTTP1
import SotoCore
import SotoSignerV4
import Testing
import Vapor
import VaporTesting

@testable import Alarik

/// Replication is proven end-to-end against this same app's own S3 API, used as both the
/// source *and* the replication target (two different buckets on the one running instance).
/// Alarik's caches, dispatchers, and on-disk storage root are process-wide singletons
/// (`Storage/buckets/`, `ReplicationDispatcher.shared`, ...), so booting two independent
/// `configure(app:)` instances in one process - the way `NotificationDeliveryTests`' bare-bones
/// `FakeReceiver` avoids this - would silently corrupt each other's state. Using one app for
/// both roles sidesteps that entirely while still exercising the real path: `ReplicationClient`
/// builds a genuine Soto-signed HTTP request and sends it over a real bound TCP port (`app.test`
/// alone never opens a socket) to this same app's own `S3Controller`, which validates SigV4 and
/// writes through `ObjectFileHandler` exactly like a request from any other client would.
@Suite("Replication tests", .serialized)
struct ReplicationTests {

    private func withApp(_ test: (Application, String) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()
            try await LoadCacheLifecycle().didBootAsync(app)

            try await app.server.start(address: .hostname("127.0.0.1", port: 0))
            guard let port = app.http.server.shared.localAddress?.port else {
                await app.server.shutdown()
                throw Abort(.internalServerError, reason: "Test server failed to bind a port.")
            }

            do {
                try await test(app, "http://127.0.0.1:\(port)")
            } catch {
                await app.server.shutdown()
                throw error
            }
            await app.server.shutdown()

            try await app.autoRevert()
        } catch {
            try? StorageHelper.cleanStorage()
            try? await app.autoRevert()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    // MARK: - SigV4-signed S3 API helpers (mirrors S3ControllerTests)

    private func signedHeaders(
        for method: HTTPMethod, path: String, query: String? = nil, body: Data? = nil
    ) -> HTTPHeaders {
        var fullPath = path
        if let query, !query.isEmpty { fullPath += "?\(query)" }
        let url = URL(string: "http://127.0.0.1\(fullPath)")!

        let signer = AWSSigner(
            credentials: StaticCredential(accessKeyId: accessKey, secretAccessKey: secretKey),
            name: "s3", region: region)

        return signer.signHeaders(
            url: url, method: method, headers: HTTPHeaders([("host", "127.0.0.1")]),
            body: body != nil ? .data(body!) : .none)
    }

    private func createBucket(_ app: Application, bucketName: String) async throws {
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)")
        try await app.test(
            .PUT, "/\(bucketName)",
            beforeRequest: { req in req.headers.add(contentsOf: signed) },
            afterResponse: { res in #expect(res.status == .ok) })
    }

    private func enableVersioning(_ app: Application, bucketName: String) async throws {
        let body = Data(
            """
            <?xml version="1.0" encoding="UTF-8"?>
            <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Enabled</Status>
            </VersioningConfiguration>
            """.utf8)
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)", query: "versioning", body: body)
        try await app.test(
            .PUT, "/\(bucketName)?versioning",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: body)
            },
            afterResponse: { res in #expect(res.status == .ok) })
    }

    @discardableResult
    private func putObject(_ app: Application, bucketName: String, key: String, data: Data) async throws
        -> String?
    {
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)/\(key)", body: data)
        var versionId: String?
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

    private func deleteObject(_ app: Application, bucketName: String, key: String) async throws {
        let signed = signedHeaders(for: .DELETE, path: "/\(bucketName)/\(key)")
        try await app.test(
            .DELETE, "/\(bucketName)/\(key)",
            beforeRequest: { req in req.headers.add(contentsOf: signed) },
            afterResponse: { res in #expect(res.status == .noContent) })
    }

    // MARK: - Internal replication API helpers

    private func saveTargets(
        _ app: Application, token: String, bucketName: String, targets: [ReplicationTarget]
    ) async throws -> [ReplicationTarget] {
        var saved: [ReplicationTarget] = []
        try await app.test(
            .PUT, "/api/v1/buckets/\(bucketName)/replication/targets",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(InternalBucketController.ReplicationTargetsDTO(targets: targets))
            },
            afterResponse: { res async throws in
                #expect(res.status == .ok)
                saved = try res.content.decode(InternalBucketController.ReplicationTargetsDTO.self)
                    .targets
            })
        return saved
    }

    private func saveRules(
        _ app: Application, token: String, bucketName: String, rules: [ReplicationRule],
        expectedStatus: HTTPStatus = .ok
    ) async throws -> [ReplicationRule] {
        var saved: [ReplicationRule] = []
        try await app.test(
            .PUT, "/api/v1/buckets/\(bucketName)/replication/rules",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(InternalBucketController.ReplicationRulesDTO(rules: rules))
            },
            afterResponse: { res async throws in
                #expect(res.status == expectedStatus)
                if expectedStatus == .ok {
                    saved = try res.content.decode(InternalBucketController.ReplicationRulesDTO.self)
                        .rules
                }
            })
        return saved
    }

    /// Sets up one target (pointing at `endpoint`/`destBucket`) and one rule referencing it on
    /// `sourceBucket`, and returns both with their server-assigned ids.
    private func configureReplication(
        _ app: Application, token: String, sourceBucket: String, endpoint: String,
        destBucket: String, replicateDeletes: Bool = false, replicateExisting: Bool = false,
        prefix: String? = nil
    ) async throws -> (target: ReplicationTarget, rule: ReplicationRule) {
        let target = ReplicationTarget(
            id: ReplicationTarget.zeroUUID, endpoint: endpoint, targetBucket: destBucket,
            accessKeyId: accessKey, secretAccessKey: secretKey, region: region, enabled: true)
        let savedTargets = try await saveTargets(
            app, token: token, bucketName: sourceBucket, targets: [target])
        let savedTarget = try #require(savedTargets.first)

        let rule = ReplicationRule(
            id: ReplicationRule.zeroUUID, targetId: savedTarget.id, prefix: prefix,
            replicateDeletes: replicateDeletes, replicateExisting: replicateExisting, enabled: true)
        let savedRules = try await saveRules(app, token: token, bucketName: sourceBucket, rules: [rule])
        let savedRule = try #require(savedRules.first)

        return (savedTarget, savedRule)
    }

    /// Drains the dispatcher until `predicate` is true or the timeout elapses.
    private func waitUntil(timeout: Double = 10, _ predicate: () async throws -> Bool) async throws
        -> Bool
    {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            await ReplicationDispatcher.shared.drain()
            if try await predicate() { return true }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        return try await predicate()
    }

    private func readObject(bucketName: String, key: String) -> Data? {
        (try? ObjectFileHandler.readVersion(
            bucketName: bucketName, key: key, versionId: nil, loadData: true))?.1
    }

    // MARK: - Tests

    @Test("a PUT is replicated and byte-identical on the target")
    func putReplicatesByteIdentical() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-put")
            try await createBucket(app, bucketName: "repl-dst-put")
            try await enableVersioning(app, bucketName: "repl-src-put")
            try await enableVersioning(app, bucketName: "repl-dst-put")

            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-put", endpoint: baseURL,
                destBucket: "repl-dst-put")

            let content = Data("hello replication world".utf8)
            _ = try await putObject(app, bucketName: "repl-src-put", key: "hello.txt", data: content)

            let replicated = try await waitUntil {
                readObject(bucketName: "repl-dst-put", key: "hello.txt") == content
            }
            #expect(replicated)
            #expect(try await ReplicationTask.query(on: app.db).count() == 0)
        }
    }

    @Test("a large object goes through the multipart path and is byte-identical")
    func largeObjectMultipartByteIdentical() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-big")
            try await createBucket(app, bucketName: "repl-dst-big")
            try await enableVersioning(app, bucketName: "repl-src-big")
            try await enableVersioning(app, bucketName: "repl-dst-big")

            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-big", endpoint: baseURL,
                destBucket: "repl-dst-big")

            // Exceeds ReplicationClient.multipartThreshold (8 MiB)
            var content = Data("REPLICATION-LARGE-OBJECT-MARKER".utf8)
            content.append(Data(repeating: 0x42, count: 9 * 1024 * 1024))
            _ = try await putObject(app, bucketName: "repl-src-big", key: "big.bin", data: content)

            let replicated = try await waitUntil(timeout: 20) {
                readObject(bucketName: "repl-dst-big", key: "big.bin") == content
            }
            #expect(replicated)
        }
    }

    @Test("deletes are not replicated unless the rule opts in")
    func deleteNotReplicatedByDefault() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-del1")
            try await createBucket(app, bucketName: "repl-dst-del1")
            try await enableVersioning(app, bucketName: "repl-src-del1")
            try await enableVersioning(app, bucketName: "repl-dst-del1")

            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-del1", endpoint: baseURL,
                destBucket: "repl-dst-del1", replicateDeletes: false)

            let content = Data("will be deleted".utf8)
            _ = try await putObject(app, bucketName: "repl-src-del1", key: "a.txt", data: content)
            _ = try await waitUntil { readObject(bucketName: "repl-dst-del1", key: "a.txt") == content }

            try await deleteObject(app, bucketName: "repl-src-del1", key: "a.txt")
            await ReplicationDispatcher.shared.drain()
            try await Task.sleep(nanoseconds: 300_000_000)

            // No delete task was ever enqueued, and the object still exists on the target
            #expect(try await ReplicationTask.query(on: app.db).count() == 0)
            #expect(readObject(bucketName: "repl-dst-del1", key: "a.txt") == content)
        }
    }

    @Test("deletes are replicated when the rule opts in")
    func deleteReplicatedWhenOptedIn() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-del2")
            try await createBucket(app, bucketName: "repl-dst-del2")
            try await enableVersioning(app, bucketName: "repl-src-del2")
            try await enableVersioning(app, bucketName: "repl-dst-del2")

            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-del2", endpoint: baseURL,
                destBucket: "repl-dst-del2", replicateDeletes: true)

            let content = Data("will be deleted too".utf8)
            _ = try await putObject(app, bucketName: "repl-src-del2", key: "b.txt", data: content)
            _ = try await waitUntil { readObject(bucketName: "repl-dst-del2", key: "b.txt") == content }

            try await deleteObject(app, bucketName: "repl-src-del2", key: "b.txt")

            // Source versioning is enabled, so this created a delete marker rather than a
            // permanent delete - the current version on the target must become a delete marker.
            let replicated = try await waitUntil {
                guard
                    let (meta, _) = try? ObjectFileHandler.readVersion(
                        bucketName: "repl-dst-del2", key: "b.txt", versionId: nil, loadData: false)
                else { return false }
                return meta.isDeleteMarker
            }
            #expect(replicated)
        }
    }

    @Test("resync walks existing objects and enqueues them for replication")
    func resyncWalksExistingObjects() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-resync")
            try await createBucket(app, bucketName: "repl-dst-resync")
            try await enableVersioning(app, bucketName: "repl-src-resync")
            try await enableVersioning(app, bucketName: "repl-dst-resync")

            // Objects written before any replication rule exists are never auto-replicated
            let content = Data("pre-existing object".utf8)
            _ = try await putObject(app, bucketName: "repl-src-resync", key: "old.txt", data: content)

            let (_, rule) = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-resync", endpoint: baseURL,
                destBucket: "repl-dst-resync", replicateExisting: true)

            await ReplicationDispatcher.shared.drain()
            #expect(readObject(bucketName: "repl-dst-resync", key: "old.txt") == nil)

            // The walk runs in the background - the endpoint only confirms the rule/target are
            // valid and returns immediately, it never reports a synchronous count.
            try await app.test(
                .POST, "/api/v1/buckets/repl-src-resync/replication/rules/\(rule.id.uuidString)/resync",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .accepted)
                })

            let replicated = try await waitUntil {
                readObject(bucketName: "repl-dst-resync", key: "old.txt") == content
            }
            #expect(replicated)
        }
    }

    @Test("a task that fails is retried and eventually succeeds")
    func retryAfterFailure() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-retry")
            try await enableVersioning(app, bucketName: "repl-src-retry")

            // The destination bucket does not exist yet - the first attempt must fail
            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-retry", endpoint: baseURL,
                destBucket: "repl-dst-retry")

            let content = Data("retry me".utf8)
            _ = try await putObject(app, bucketName: "repl-src-retry", key: "c.txt", data: content)

            await ReplicationDispatcher.shared.drain()
            try await Task.sleep(nanoseconds: 300_000_000)

            let failing = try #require(try await ReplicationTask.query(on: app.db).first())
            #expect(failing.attempts >= 1)
            #expect(failing.lastError != nil)

            // Fix the underlying problem, then force the row due and retry
            try await createBucket(app, bucketName: "repl-dst-retry")
            try await enableVersioning(app, bucketName: "repl-dst-retry")
            failing.nextAttemptAt = Date().addingTimeInterval(-1)
            try await failing.save(on: app.db)

            let replicated = try await waitUntil {
                readObject(bucketName: "repl-dst-retry", key: "c.txt") == content
            }
            #expect(replicated)
            #expect(try await ReplicationTask.query(on: app.db).count() == 0)
        }
    }

    @Test("a task survives its target's deletion, using its snapshotted credentials")
    func taskSurvivesTargetDeletion() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-orphan")
            try await createBucket(app, bucketName: "repl-dst-orphan")
            try await enableVersioning(app, bucketName: "repl-src-orphan")
            try await enableVersioning(app, bucketName: "repl-dst-orphan")

            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-orphan", endpoint: baseURL,
                destBucket: "repl-dst-orphan")

            let content = Data("orphaned task".utf8)
            _ = try await putObject(app, bucketName: "repl-src-orphan", key: "d.txt", data: content)
            #expect(try await ReplicationTask.query(on: app.db).count() == 1)

            // Remove the target entirely - the rule referencing it is auto-disabled, but the
            // already-queued task must not be affected (it snapshotted its own credentials).
            _ = try await saveTargets(app, token: token, bucketName: "repl-src-orphan", targets: [])

            let rules = try await saveRules(
                app, token: token, bucketName: "repl-src-orphan", rules: [])
            #expect(rules.isEmpty)

            let replicated = try await waitUntil {
                readObject(bucketName: "repl-dst-orphan", key: "d.txt") == content
            }
            #expect(replicated)
        }
    }

    @Test("saving rules requires the bucket's versioning to be Enabled")
    func rulesRequireVersioning() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-noversion")
            try await createBucket(app, bucketName: "repl-dst-noversion")
            // Versioning deliberately left Disabled on the source

            let target = ReplicationTarget(
                id: ReplicationTarget.zeroUUID, endpoint: baseURL, targetBucket: "repl-dst-noversion",
                accessKeyId: accessKey, secretAccessKey: secretKey, region: region, enabled: true)
            let savedTargets = try await saveTargets(
                app, token: token, bucketName: "repl-src-noversion", targets: [target])

            let rule = ReplicationRule(
                id: ReplicationRule.zeroUUID, targetId: savedTargets[0].id, prefix: nil,
                replicateDeletes: false, replicateExisting: false, enabled: true)
            _ = try await saveRules(
                app, token: token, bucketName: "repl-src-noversion", rules: [rule],
                expectedStatus: .badRequest)
        }
    }

    @Test("saving a rule with an unknown target id is rejected")
    func unknownTargetIdRejected() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-unknown")
            try await enableVersioning(app, bucketName: "repl-src-unknown")

            let rule = ReplicationRule(
                id: ReplicationRule.zeroUUID, targetId: UUID(), prefix: nil,
                replicateDeletes: false, replicateExisting: false, enabled: true)
            _ = try await saveRules(
                app, token: token, bucketName: "repl-src-unknown", rules: [rule],
                expectedStatus: .badRequest)
        }
    }

    @Test("a non-admin cannot target a private/loopback replication endpoint")
    func nonAdminBlockedFromPrivateEndpoint() async throws {
        try await withApp { app, _ in
            let token = try await createUserAndLogin(app, username: "replicationuser@example.com")

            try await app.test(
                .POST, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(Bucket.Create(name: "user-repl-bucket", versioningEnabled: true))
                })

            let target = ReplicationTarget(
                id: ReplicationTarget.zeroUUID, endpoint: "http://127.0.0.1:9000",
                targetBucket: "whatever", accessKeyId: "a", secretAccessKey: "b", region: region,
                enabled: true)

            try await app.test(
                .PUT, "/api/v1/buckets/user-repl-bucket/replication/targets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(InternalBucketController.ReplicationTargetsDTO(targets: [target]))
                },
                afterResponse: { res async in
                    #expect(res.status == .forbidden)
                })
        }
    }

    @Test("deleting a bucket purges its pending replication tasks")
    func bucketDeletePurgesReplicationTasks() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            let admin = try await User.query(on: app.db).filter(\.$username == "alarik").first()!
            try await createBucket(app, bucketName: "repl-doomed")
            try await enableVersioning(app, bucketName: "repl-doomed")

            // Point the target at an unreachable destination so the task stays pending
            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-doomed", endpoint: baseURL,
                destBucket: "never-created")

            _ = try await putObject(app, bucketName: "repl-doomed", key: "a.txt", data: Data("x".utf8))
            #expect(try await ReplicationTask.query(on: app.db).count() == 1)

            try await BucketService.delete(
                on: app.db, bucketName: "repl-doomed", userId: admin.id!, force: true)

            #expect(try await ReplicationTask.query(on: app.db).count() == 0)
        }
    }

    // MARK: - Internal API validation (GET round-trip, caps, field validation)

    @Test("GET targets/rules return empty arrays for a bucket with no config, and round-trip after PUT")
    func getEndpointsRoundTrip() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-getset")
            try await createBucket(app, bucketName: "repl-dst-getset")
            try await enableVersioning(app, bucketName: "repl-src-getset")

            try await app.test(
                .GET, "/api/v1/buckets/repl-src-getset/replication/targets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(InternalBucketController.ReplicationTargetsDTO.self)
                    #expect(dto.targets.isEmpty)
                })

            try await app.test(
                .GET, "/api/v1/buckets/repl-src-getset/replication/rules",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(InternalBucketController.ReplicationRulesDTO.self)
                    #expect(dto.rules.isEmpty)
                })

            let (target, rule) = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-getset", endpoint: baseURL,
                destBucket: "repl-dst-getset")

            try await app.test(
                .GET, "/api/v1/buckets/repl-src-getset/replication/targets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(InternalBucketController.ReplicationTargetsDTO.self)
                    #expect(dto.targets.map(\.id) == [target.id])
                    #expect(dto.targets.first?.endpoint == baseURL)
                })

            try await app.test(
                .GET, "/api/v1/buckets/repl-src-getset/replication/rules",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(InternalBucketController.ReplicationRulesDTO.self)
                    #expect(dto.rules.map(\.id) == [rule.id])
                })
        }
    }

    @Test("more than 4 targets is rejected")
    func targetCountCapEnforced() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-targetcap")

            let targets = (0..<5).map { i in
                ReplicationTarget(
                    id: ReplicationTarget.zeroUUID, endpoint: baseURL, targetBucket: "dest-\(i)",
                    accessKeyId: accessKey, secretAccessKey: secretKey, region: region, enabled: true)
            }
            try await app.test(
                .PUT, "/api/v1/buckets/repl-src-targetcap/replication/targets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        InternalBucketController.ReplicationTargetsDTO(targets: targets))
                },
                afterResponse: { res async in #expect(res.status == .badRequest) })
        }
    }

    @Test("more than 4 rules is rejected")
    func ruleCountCapEnforced() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-rulecap")
            try await createBucket(app, bucketName: "repl-dst-rulecap")
            try await enableVersioning(app, bucketName: "repl-src-rulecap")

            let target = ReplicationTarget(
                id: ReplicationTarget.zeroUUID, endpoint: baseURL, targetBucket: "repl-dst-rulecap",
                accessKeyId: accessKey, secretAccessKey: secretKey, region: region, enabled: true)
            let savedTargets = try await saveTargets(
                app, token: token, bucketName: "repl-src-rulecap", targets: [target])
            let targetId = try #require(savedTargets.first).id

            let rules = (0..<5).map { _ in
                ReplicationRule(
                    id: ReplicationRule.zeroUUID, targetId: targetId, prefix: nil,
                    replicateDeletes: false, replicateExisting: false, enabled: true)
            }
            _ = try await saveRules(
                app, token: token, bucketName: "repl-src-rulecap", rules: rules,
                expectedStatus: .badRequest)
        }
    }

    @Test("setting targets validates required fields")
    func setTargetsValidatesRequiredFields() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-validate")

            // Empty destination bucket
            var badTarget = ReplicationTarget(
                id: ReplicationTarget.zeroUUID, endpoint: baseURL, targetBucket: "",
                accessKeyId: accessKey, secretAccessKey: secretKey, region: region, enabled: true)
            try await app.test(
                .PUT, "/api/v1/buckets/repl-src-validate/replication/targets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        InternalBucketController.ReplicationTargetsDTO(targets: [badTarget]))
                },
                afterResponse: { res async in #expect(res.status == .badRequest) })

            // Empty credentials
            badTarget = ReplicationTarget(
                id: ReplicationTarget.zeroUUID, endpoint: baseURL, targetBucket: "somewhere",
                accessKeyId: "", secretAccessKey: "", region: region, enabled: true)
            try await app.test(
                .PUT, "/api/v1/buckets/repl-src-validate/replication/targets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        InternalBucketController.ReplicationTargetsDTO(targets: [badTarget]))
                },
                afterResponse: { res async in #expect(res.status == .badRequest) })

            // Bad URL scheme
            badTarget = ReplicationTarget(
                id: ReplicationTarget.zeroUUID, endpoint: "ftp://bad", targetBucket: "somewhere",
                accessKeyId: accessKey, secretAccessKey: secretKey, region: region, enabled: true)
            try await app.test(
                .PUT, "/api/v1/buckets/repl-src-validate/replication/targets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        InternalBucketController.ReplicationTargetsDTO(targets: [badTarget]))
                },
                afterResponse: { res async in #expect(res.status == .badRequest) })
        }
    }

    @Test("resync is rejected for a disabled rule, a rule without replicateExisting, or an unknown rule id")
    func resyncValidatesRuleState() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-resyncval")
            try await createBucket(app, bucketName: "repl-dst-resyncval")
            try await enableVersioning(app, bucketName: "repl-src-resyncval")

            // replicateExisting left false
            let (_, ruleNoExisting) = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-resyncval", endpoint: baseURL,
                destBucket: "repl-dst-resyncval", replicateExisting: false)

            try await app.test(
                .POST,
                "/api/v1/buckets/repl-src-resyncval/replication/rules/\(ruleNoExisting.id.uuidString)/resync",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in #expect(res.status == .badRequest) })

            // Unknown rule id
            try await app.test(
                .POST,
                "/api/v1/buckets/repl-src-resyncval/replication/rules/\(UUID().uuidString)/resync",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in #expect(res.status == .notFound) })

            // Disable the rule - resync must be rejected even with replicateExisting true
            let disabledRule = ReplicationRule(
                id: ruleNoExisting.id, targetId: ruleNoExisting.targetId, prefix: nil,
                replicateDeletes: false, replicateExisting: true, enabled: false)
            _ = try await saveRules(
                app, token: token, bucketName: "repl-src-resyncval", rules: [disabledRule])

            try await app.test(
                .POST,
                "/api/v1/buckets/repl-src-resyncval/replication/rules/\(ruleNoExisting.id.uuidString)/resync",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in #expect(res.status == .badRequest) })
        }
    }

    // MARK: - Task health (list + retry via the actual API)

    @Test("listing tasks reports a failing task's attempt count and last error")
    func listTasksReportsFailureDetails() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-health")
            try await enableVersioning(app, bucketName: "repl-src-health")

            // Destination bucket never created - every attempt fails
            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-health", endpoint: baseURL,
                destBucket: "repl-dst-health-missing")

            _ = try await putObject(
                app, bucketName: "repl-src-health", key: "a.txt", data: Data("x".utf8))

            await ReplicationDispatcher.shared.drain()
            try await Task.sleep(nanoseconds: 300_000_000)

            try await app.test(
                .GET, "/api/v1/buckets/repl-src-health/replication/tasks",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(InternalBucketController.ReplicationTasksDTO.self)
                    let task = try #require(dto.tasks.first)
                    #expect(task.state == "pending")
                    #expect(task.key == "a.txt")
                    #expect(task.operation == "put")
                    #expect(task.attempts >= 1)
                    #expect(task.lastError != nil)
                })
        }
    }

    @Test("a dead-lettered task can be retried via the API and redelivers")
    func retryDeadLetteredTaskViaAPI() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-deadletter")
            try await enableVersioning(app, bucketName: "repl-src-deadletter")

            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-deadletter", endpoint: baseURL,
                destBucket: "repl-dst-deadletter")

            let content = Data("dead letter me".utf8)
            _ = try await putObject(
                app, bucketName: "repl-src-deadletter", key: "e.txt", data: content)

            // Force through every retry immediately (skip the real backoff) until dead-lettered
            var isFailed = false
            for _ in 0..<(ReplicationDispatcher.maxAttempts + 2) {
                await ReplicationDispatcher.shared.drain()
                if let row = try await ReplicationTask.query(on: app.db)
                    .filter(\.$bucketName == "repl-src-deadletter")
                    .first()
                {
                    if row.state == ReplicationTask.State.failed.rawValue {
                        isFailed = true
                        break
                    }
                    row.nextAttemptAt = Date().addingTimeInterval(-1)
                    try await row.save(on: app.db)
                }
                try await Task.sleep(nanoseconds: 100_000_000)
            }
            #expect(isFailed)

            let deadRow = try #require(
                try await ReplicationTask.query(on: app.db)
                    .filter(\.$bucketName == "repl-src-deadletter")
                    .first())

            // Fix the destination, then retry via the API
            try await createBucket(app, bucketName: "repl-dst-deadletter")
            try await enableVersioning(app, bucketName: "repl-dst-deadletter")

            try await app.test(
                .POST,
                "/api/v1/buckets/repl-src-deadletter/replication/tasks/\(deadRow.id!.uuidString)/retry",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(InternalBucketController.ReplicationTaskDTO.self)
                    #expect(dto.state == "pending")
                    #expect(dto.attempts == 0)
                })

            let replicated = try await waitUntil {
                readObject(bucketName: "repl-dst-deadletter", key: "e.txt") == content
            }
            #expect(replicated)
            #expect(
                try await ReplicationTask.query(on: app.db)
                    .filter(\.$bucketName == "repl-src-deadletter")
                    .count() == 0)
        }
    }

    @Test("retrying a task only affects tasks belonging to the requested bucket")
    func retryTaskScopedToOwnBucket() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-scope-a")
            try await createBucket(app, bucketName: "repl-src-scope-b")
            try await enableVersioning(app, bucketName: "repl-src-scope-a")
            try await enableVersioning(app, bucketName: "repl-src-scope-b")

            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-scope-b", endpoint: baseURL,
                destBucket: "repl-dst-scope-b-missing")

            _ = try await putObject(
                app, bucketName: "repl-src-scope-b", key: "a.txt", data: Data("x".utf8))
            await ReplicationDispatcher.shared.drain()

            let rowInB = try #require(
                try await ReplicationTask.query(on: app.db)
                    .filter(\.$bucketName == "repl-src-scope-b")
                    .first())

            // Attempting to retry bucket-b's task through bucket-a's path must 404
            try await app.test(
                .POST,
                "/api/v1/buckets/repl-src-scope-a/replication/tasks/\(rowInB.id!.uuidString)/retry",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in #expect(res.status == .notFound) })
        }
    }

    @Test("replication endpoints require ownership and auth")
    func replicationEndpointsRequireOwnershipAndAuth() async throws {
        try await withApp { app, _ in
            try await createBucket(app, bucketName: "repl-owned-bucket")

            // No auth at all
            try await app.test(
                .GET, "/api/v1/buckets/repl-owned-bucket/replication/targets",
                afterResponse: { res async in #expect(res.status == .unauthorized) })
            try await app.test(
                .GET, "/api/v1/buckets/repl-owned-bucket/replication/rules",
                afterResponse: { res async in #expect(res.status == .unauthorized) })
            try await app.test(
                .GET, "/api/v1/buckets/repl-owned-bucket/replication/tasks",
                afterResponse: { res async in #expect(res.status == .unauthorized) })

            // Authenticated, but a different (non-owning) user
            let otherToken = try await createUserAndLogin(
                app, username: "not-the-repl-owner@example.com")
            try await app.test(
                .GET, "/api/v1/buckets/repl-owned-bucket/replication/targets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: otherToken)
                },
                afterResponse: { res async in #expect(res.status == .notFound) })
            try await app.test(
                .GET, "/api/v1/buckets/repl-owned-bucket/replication/rules",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: otherToken)
                },
                afterResponse: { res async in #expect(res.status == .notFound) })
            try await app.test(
                .GET, "/api/v1/buckets/repl-owned-bucket/replication/tasks",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: otherToken)
                },
                afterResponse: { res async in #expect(res.status == .notFound) })
        }
    }

    // MARK: - Regression coverage for review findings

    @Test("replicated objects carry their tags to the target")
    func replicatedObjectsCarryTags() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-tags")
            try await createBucket(app, bucketName: "repl-dst-tags")
            try await enableVersioning(app, bucketName: "repl-src-tags")
            try await enableVersioning(app, bucketName: "repl-dst-tags")

            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-tags", endpoint: baseURL,
                destBucket: "repl-dst-tags")

            let content = Data("tagged object".utf8)
            let signed = signedHeaders(for: .PUT, path: "/repl-src-tags/tagged.txt", body: content)
            try await app.test(
                .PUT, "/repl-src-tags/tagged.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.headers.add(name: "x-amz-tagging", value: "project=alarik&env=test")
                    req.body = ByteBuffer(data: content)
                },
                afterResponse: { res in #expect(res.status == .ok) })

            let replicated = try await waitUntil {
                readObject(bucketName: "repl-dst-tags", key: "tagged.txt") == content
            }
            #expect(replicated)

            let (meta, _) = try ObjectFileHandler.readVersion(
                bucketName: "repl-dst-tags", key: "tagged.txt", versionId: nil, loadData: false)
            #expect(meta.tags?["project"] == "alarik")
            #expect(meta.tags?["env"] == "test")
        }
    }

    @Test("rapid same-key mutations replicate in order - the target settles on the final state")
    func sameKeyOperationsPreserveOrder() async throws {
        try await withApp { app, baseURL in
            let token = try await loginDefaultAdminUser(app)
            try await createBucket(app, bucketName: "repl-src-order")
            try await createBucket(app, bucketName: "repl-dst-order")
            try await enableVersioning(app, bucketName: "repl-src-order")
            try await enableVersioning(app, bucketName: "repl-dst-order")

            _ = try await configureReplication(
                app, token: token, sourceBucket: "repl-src-order", endpoint: baseURL,
                destBucket: "repl-dst-order", replicateDeletes: true)

            // Rapidly mutate the same key several times in a row - each PUT/DELETE enqueues
            // its own replication task, and several land "due" at once. Without per-key
            // exclusion in the dispatcher, a later task could finish delivering before an
            // earlier one, leaving the target on stale (or deleted) content.
            let finalContent = Data("final content wins".utf8)
            _ = try await putObject(
                app, bucketName: "repl-src-order", key: "race.txt", data: Data("v1".utf8))
            try await deleteObject(app, bucketName: "repl-src-order", key: "race.txt")
            _ = try await putObject(
                app, bucketName: "repl-src-order", key: "race.txt", data: Data("v2".utf8))
            try await deleteObject(app, bucketName: "repl-src-order", key: "race.txt")
            _ = try await putObject(
                app, bucketName: "repl-src-order", key: "race.txt", data: finalContent)

            let replicated = try await waitUntil {
                readObject(bucketName: "repl-dst-order", key: "race.txt") == finalContent
            }
            #expect(replicated)

            // Drain a few more times to give any wrongly-reordered delivery a chance to land,
            // then confirm the target really settled on the final content rather than getting
            // clobbered by an older task finishing later.
            for _ in 0..<5 {
                await ReplicationDispatcher.shared.drain()
                try await Task.sleep(nanoseconds: 200_000_000)
            }
            #expect(readObject(bucketName: "repl-dst-order", key: "race.txt") == finalContent)
            #expect(try await ReplicationTask.query(on: app.db).count() == 0)
        }
    }
}
