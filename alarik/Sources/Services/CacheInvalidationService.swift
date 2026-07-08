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
import Vapor

/// The single Postgres channel every cache-invalidation signal is sent on. One channel, not
/// one per cache, keeps `CacheInvalidationListener` down to a single LISTEN connection.
enum CacheInvalidationChannel {
    static let name = "alarik_cache_invalidation"
}

/// A cache-invalidation signal. Deliberately carries no value - only which cache and which key
/// changed. The receiving node always re-reads its own DB (`CacheReloadDispatch`) rather than
/// trusting a value carried over the wire: one source of truth, no duplicate serialization
/// logic, and it's the only design that also works for `AccessKeyBucketMapCache` (a *derived*
/// cache - a cross-product via `user_id`, not a 1:1 table mirror - "the new value" isn't even a
/// well-defined thing to send for it).
struct CacheInvalidationMessage: Codable, Sendable {
    enum Op: String, Codable {
        /// Reload this one key's value from the DB.
        case upsert
        /// The DB row for this key is gone - just drop it locally, nothing to reload.
        case remove
        /// `accessKeyBucket` only: a bucket was deleted - strip it out of every access key's
        /// cached set. The only op where "reload from DB" doesn't apply (the bucket row is
        /// already gone by the time this arrives, so there'd be nothing to read).
        case removeBucket
    }

    let cache: String
    let op: Op
    let key: String
}

/// Sends cache-invalidation signals to every other node - a true no-op on SQLite, where there
/// is only ever one node and its own in-process cache mutation (already done by the caller
/// immediately before this) is already the whole story.
enum CacheInvalidationService {
    /// Fire-and-forget: sends a `NOTIFY` on `db` if and only if `db` is actually a Postgres
    /// connection. Never throws to the caller - a dropped or failed NOTIFY only costs the rest
    /// of the cluster staleness until the next reconnect-triggered full reload
    /// (`CacheInvalidationListener`), it must never fail the write path that triggered it.
    static func notify(on db: any Database, cache: String, op: CacheInvalidationMessage.Op, key: String) {
        guard let sql = db as? any SQLDatabase, sql.dialect.name == "postgresql" else { return }

        let message = CacheInvalidationMessage(cache: cache, op: op, key: key)
        Task {
            do {
                let payload = String(decoding: try JSONEncoder().encode(message), as: UTF8.self)
                // pg_notify(text, text) takes bind params, unlike a bare `NOTIFY chan, 'lit'` -
                // avoids hand-escaping arbitrary JSON into a SQL string literal.
                try await sql.raw(
                    "SELECT pg_notify(\(bind: CacheInvalidationChannel.name), \(bind: payload))"
                ).run()
            } catch {
                db.logger.error(
                    "Failed to send cache invalidation NOTIFY (cache=\(cache), op=\(op), key=\(key)): \(error)"
                )
            }
        }
    }
}
