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

import struct Foundation.UUID

/// Holds the per-attempt `providerId`/`nonce`/PKCE `code_verifier` for an in-flight OIDC login
/// between the `/login/:providerId` redirect and the shared `/callback` round-trip, keyed by the
/// `state` value round-tripped through the IdP. The callback route is a single fixed URL shared
/// by every provider (so admins only ever register one redirect URI with their IdP); `providerId`
/// is how the callback knows which provider an incoming `code` belongs to.
///
/// Single-node (SQLite): kept in memory - there's no server-side session otherwise (the server
/// is stateless JWT auth), and a login attempt is short-lived enough that this needs nothing
/// more durable. Multi-node (Postgres): this is the one cache in the app that a "keep the caches
/// in sync" mechanism can't fix, because the data doesn't exist anywhere but the originating
/// node's memory today - behind a load balancer, the login redirect can land on node A (which
/// would store the state in its own memory) while the IdP's callback redirect lands on node B,
/// which has never heard of it. So in Postgres mode this reads/writes a real table on every call
/// instead of an in-memory map - correct by construction, not a cache needing invalidation.
final actor OIDCStateCache {

    public static let shared = OIDCStateCache()

    struct Entry {
        let providerId: UUID
        let nonce: String
        let codeVerifier: String
        let createdAt: Date
    }

    private var map: [String: Entry] = [:]

    private func sqlDatabase(_ db: any Database) -> (any SQLDatabase)? {
        guard let sql = db as? any SQLDatabase, sql.dialect.name == "postgresql" else {
            return nil
        }
        return sql
    }

    func store(
        on db: any Database, state: String, providerId: UUID, nonce: String, codeVerifier: String
    ) async throws {
        guard let sql = sqlDatabase(db) else {
            map[state] = Entry(
                providerId: providerId, nonce: nonce, codeVerifier: codeVerifier,
                createdAt: Date())
            return
        }
        // `oidc_states.id` has no server-side default (Fluent's `.id()` schema helper doesn't
        // add one) - Model-based saves get their UUID generated client-side by Fluent before
        // the INSERT, but this raw SQLKit insert bypasses that, so it has to generate one itself.
        try await sql.insert(into: "oidc_states")
            .columns("id", "state", "provider_id", "nonce", "code_verifier", "created_at")
            .values(UUID(), state, providerId, nonce, codeVerifier, Date())
            .run()
    }

    /// Single-use: removes the entry on read so a `state` value can never be replayed. On
    /// Postgres this is one atomic `DELETE ... RETURNING` statement rather than a SELECT then a
    /// DELETE - the database serializes it for us, so two nodes racing to consume the same
    /// `state` can never both succeed.
    func consume(on db: any Database, state: String) async throws -> Entry? {
        guard let sql = sqlDatabase(db) else {
            return map.removeValue(forKey: state)
        }
        guard
            let row = try await sql.delete(from: "oidc_states")
                .where("state", .equal, state)
                .returning("provider_id", "nonce", "code_verifier", "created_at")
                .first()
        else {
            return nil
        }
        return Entry(
            providerId: try row.decode(column: "provider_id", as: UUID.self),
            nonce: try row.decode(column: "nonce", as: String.self),
            codeVerifier: try row.decode(column: "code_verifier", as: String.self),
            createdAt: try row.decode(column: "created_at", as: Date.self))
    }

    func removeExpired(on db: any Database, olderThan ttl: TimeInterval) async throws {
        let cutoff = Date().addingTimeInterval(-ttl)
        guard let sql = sqlDatabase(db) else {
            guard !map.isEmpty else { return }
            map = map.filter { $0.value.createdAt > cutoff }
            return
        }
        try await sql.delete(from: "oidc_states")
            .where("created_at", .lessThan, cutoff)
            .run()
    }
}
