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
import PostgresKit
import Testing
import Vapor

@testable import Alarik

/// Tier 2 + Tier 3 (see Phase 1 plan): the parts of the cache-invalidation design that only a
/// real Postgres can exercise - actual `NOTIFY` delivery, `OIDCStateCache`'s DB-table mode, and
/// genuine cross-node propagation via two independent LISTEN connections. Local/manual only per
/// the plan's CI decision: the whole suite is skipped (not failed) unless `DATABASE_URL` is set
/// in the process environment, so the default `swift test` run (SQLite, no Postgres) is
/// unaffected. Point `DATABASE_URL` at a scratch Postgres database before running this suite -
/// every test migrates and reverts its own schema, but they all share that one database and run
/// `.serialized`, so nothing here is safe to run concurrently against the same database.
@Suite(
    "Postgres cache invalidation integration tests (Tier 2/3)",
    .serialized,
    .enabled(if: ProcessInfo.processInfo.environment["DATABASE_URL"] != nil)
)
struct PostgresCacheInvalidationTests {
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

    /// `configure(app)` only stashes this when `DATABASE_URL` was parsed as Postgres - since the
    /// whole suite is gated on `DATABASE_URL` being set, its absence here would mean `configure`
    /// itself didn't do what this suite assumes, which is worth failing loudly on rather than
    /// silently skipping.
    private func listenConfig(_ app: Application) throws -> PostgresConnection.Configuration {
        try #require(
            app.storage[PostgresListenConfigurationKey.self],
            "Expected configure(app) to have parsed DATABASE_URL as a Postgres configuration")
    }

    private func createUser(_ app: Application) async throws -> UUID {
        let user = User(
            name: "Postgres Cache Test User", username: UUID().uuidString,
            passwordHash: try Bcrypt.hash("TestPass123!"), isAdmin: false)
        try await user.save(on: app.db)
        return user.id!
    }

    private func createBucket(_ app: Application, userId: UUID, name: String) async throws {
        let bucket = Bucket(name: name, userId: userId)
        try await bucket.save(on: app.db)
    }

    private func createOIDCProvider(_ app: Application) async throws -> UUID {
        let provider = OIDCProvider(
            name: "Test SSO", issuerURL: "https://idp.example.com", clientId: "client",
            clientSecret: "secret")
        try await provider.save(on: app.db)
        return provider.id!
    }

    // MARK: - Tier 2: notify() sends a real pg_notify

    @Test("notify(on:) delivers a real pg_notify to an independently LISTEN-ing connection")
    func notifySendsRealNotify() async throws {
        try await withApp { app in
            let config = try listenConfig(app)
            let listener = try await PostgresConnection.connect(
                on: app.eventLoopGroup.next(), configuration: config, id: 1, logger: app.logger)

            let received = try await withThrowingTaskGroup(of: String?.self) { group in
                group.addTask {
                    try await listener.listen(on: CacheInvalidationChannel.name) { notifications in
                        for try await notification in notifications {
                            return notification.payload
                        }
                        return nil
                    }
                }
                group.addTask {
                    // Give the LISTEN a moment to actually take effect on the server before the
                    // NOTIFY fires - there's no synchronous "listening now" signal to wait on.
                    try await Task.sleep(for: .milliseconds(300))
                    CacheInvalidationService.notify(
                        on: app.db, cache: "bucketVersioning", op: .upsert,
                        key: "notify-test-bucket")
                    try await Task.sleep(for: .seconds(5))
                    return nil
                }

                defer { group.cancelAll() }
                for try await result in group where result != nil {
                    return result
                }
                return nil
            }

            try? await listener.close()

            let payload = try #require(received)
            let message = try JSONDecoder().decode(
                CacheInvalidationMessage.self, from: Data(payload.utf8))
            #expect(message.cache == "bucketVersioning")
            #expect(message.op == .upsert)
            #expect(message.key == "notify-test-bucket")
        }
    }

    // MARK: - Tier 2: OIDCStateCache Postgres mode, end-to-end

    @Test("OIDCStateCache Postgres mode: store, consume once, then nil on replay")
    func oidcStateCachePostgresStoreConsume() async throws {
        try await withApp { app in
            let providerId = try await createOIDCProvider(app)
            let state = UUID().uuidString

            try await OIDCStateCache.shared.store(
                on: app.db, state: state, providerId: providerId, nonce: "test-nonce",
                codeVerifier: "test-verifier")

            let entry = try await OIDCStateCache.shared.consume(on: app.db, state: state)
            #expect(entry?.providerId == providerId)
            #expect(entry?.nonce == "test-nonce")
            #expect(entry?.codeVerifier == "test-verifier")

            // Single-use: a second consume of the same state must find nothing - the row was
            // deleted by the first `consume`, not just marked used.
            let replay = try await OIDCStateCache.shared.consume(on: app.db, state: state)
            #expect(replay == nil)
        }
    }

    @Test("OIDCStateCache Postgres mode: removeExpired drops only stale rows")
    func oidcStateCachePostgresRemoveExpired() async throws {
        try await withApp { app in
            let providerId = try await createOIDCProvider(app)
            let staleState = UUID().uuidString
            let freshState = UUID().uuidString

            let sql = try #require(app.db as? any SQLDatabase)
            // Insert directly with a backdated created_at - `store` always writes `Date()`, so
            // there's no way to create an already-expired row through the public API.
            try await sql.insert(into: "oidc_states")
                .columns("id", "state", "provider_id", "nonce", "code_verifier", "created_at")
                .values(
                    UUID(), staleState, providerId, "n", "v",
                    Date().addingTimeInterval(-3600))
                .run()

            try await OIDCStateCache.shared.store(
                on: app.db, state: freshState, providerId: providerId, nonce: "n",
                codeVerifier: "v")

            try await OIDCStateCache.shared.removeExpired(on: app.db, olderThan: 600)

            #expect(try await OIDCStateCache.shared.consume(on: app.db, state: staleState) == nil)
            #expect(try await OIDCStateCache.shared.consume(on: app.db, state: freshState) != nil)
        }
    }

    @Test("OIDCStateCache Postgres mode: concurrent consume of the same state - exactly one wins")
    func oidcStateCachePostgresConcurrentConsumeRace() async throws {
        try await withApp { app in
            let providerId = try await createOIDCProvider(app)
            let state = UUID().uuidString

            try await OIDCStateCache.shared.store(
                on: app.db, state: state, providerId: providerId, nonce: "n", codeVerifier: "v")

            // Two racing consumers, same actor, same `state` - the actor itself already
            // serializes calls on a single node, so this is really exercising the DB-level
            // `DELETE ... RETURNING` atomicity that's the actual cross-node guarantee.
            let results = try await withThrowingTaskGroup(of: OIDCStateCache.Entry?.self) { group in
                for _ in 0..<2 {
                    group.addTask {
                        try await OIDCStateCache.shared.consume(on: app.db, state: state)
                    }
                }
                var collected: [OIDCStateCache.Entry?] = []
                for try await result in group {
                    collected.append(result)
                }
                return collected
            }

            let winners = results.compactMap { $0 }
            #expect(winners.count == 1)
            #expect(results.filter { $0 == nil }.count == 1)
        }
    }

    // MARK: - Tier 3: two independent, non-`.shared` cache instances, real cross-node propagation

    @Test("cross-node propagation: node A's write + notify reaches node B's independent cache")
    func crossNodePropagationBucketVersioning() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            try await createBucket(app, userId: userId, name: "cross-node-bucket")

            // "Node B": an independent cache instance (not `.shared` - a second real node would
            // have its own process-local singleton, so a second in-process instance is the
            // faithful stand-in) plus its own LISTEN connection, wired by hand the same way
            // `CacheInvalidationListener` wires the real one - `CacheReloadDispatch.apply` can't
            // be reused directly here since it always targets `.shared`.
            let nodeBCache = BucketVersioningCache()
            let config = try listenConfig(app)
            let nodeBListener = try await PostgresConnection.connect(
                on: app.eventLoopGroup.next(), configuration: config, id: 2, logger: app.logger)

            let propagated = try await withThrowingTaskGroup(of: Bool.self) { group in
                group.addTask {
                    try await nodeBListener.listen(on: CacheInvalidationChannel.name) {
                        notifications in
                        for try await notification in notifications {
                            guard
                                let data = notification.payload.data(using: .utf8),
                                let message = try? JSONDecoder().decode(
                                    CacheInvalidationMessage.self, from: data),
                                message.cache == "bucketVersioning", message.op == .upsert
                            else { continue }

                            if let bucket = try await Bucket.query(on: app.db)
                                .filter(\.$name == message.key).first()
                            {
                                let status =
                                    VersioningStatus(rawValue: bucket.versioningStatus) ?? .disabled
                                await nodeBCache.setStatus(for: message.key, status: status)
                            }
                            return true
                        }
                        return false
                    }
                }
                group.addTask {
                    // "Node A": mutate the DB and notify, exactly like a real write path does.
                    try await Task.sleep(for: .milliseconds(300))
                    guard
                        let bucket = try await Bucket.query(on: app.db)
                            .filter(\.$name == "cross-node-bucket").first()
                    else { return false }
                    bucket.versioningStatus = VersioningStatus.enabled.rawValue
                    try await bucket.save(on: app.db)
                    CacheInvalidationService.notify(
                        on: app.db, cache: "bucketVersioning", op: .upsert,
                        key: "cross-node-bucket")
                    try await Task.sleep(for: .seconds(5))
                    return false
                }

                defer { group.cancelAll() }
                for try await result in group where result {
                    return result
                }
                return false
            }

            try? await nodeBListener.close()

            #expect(propagated)
            #expect(await nodeBCache.getStatus(for: "cross-node-bucket") == .enabled)
            // "Node A" never touched its own `.shared` cache in this test (only the DB) -
            // confirms node B's state came from the notification round-trip, not from both
            // sides secretly sharing the same singleton.
            #expect(
                await BucketVersioningCache.shared.getStatus(for: "cross-node-bucket") == .disabled)
        }
    }
}
