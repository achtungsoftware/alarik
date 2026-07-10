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
import PostgresKit
import Vapor

/// Hands the parsed Postgres connection config from `configure.swift` to
/// `CacheInvalidationListener` without re-parsing `DATABASE_URL` a second time. The dedicated
/// LISTEN connection needs its own `PostgresConnection.Configuration` (session-scoped LISTEN
/// can't share Fluent's pooled connections), but it should still be built from the exact same
/// parse as the pooled connections Fluent itself uses.
struct PostgresListenConfigurationKey: StorageKey {
    typealias Value = PostgresConnection.Configuration
}

/// Runs the cluster-wide cache-invalidation LISTEN loop for the lifetime of the process - a
/// true no-op when running SQLite (single node, nothing to keep in sync). Registered as a
/// `LifecycleHandler` (matching `LoadCacheLifecycle`'s own shape) rather than being started
/// inline in `configure.swift`, specifically so Vapor's guarantee that `didBootAsync` handlers
/// run sequentially, in registration order, is what keeps this from racing
/// `LoadCacheLifecycle`'s boot-time bulk load - not two unrelated call sites hoping to stay in
/// the right sequence.
final actor CacheInvalidationListener: LifecycleHandler {
    static let shared = CacheInvalidationListener()

    private var task: Task<Void, Never>?

    func didBootAsync(_ app: Application) async throws {
        guard let config = app.storage[PostgresListenConfigurationKey.self] else {
            // SQLite: no cluster to keep in sync with.
            return
        }
        guard task == nil else { return }
        task = Task { [weak self] in
            await self?.runLoop(app: app, config: config)
        }
    }

    func shutdownAsync(_ app: Application) async {
        task?.cancel()
        task = nil
    }

    /// Connect, LISTEN, forward every notification to `CacheReloadDispatch` - reconnecting
    /// forever on any drop. Postgres does not redeliver missed NOTIFYs to a listener that was
    /// disconnected, so a full reload from the DB (reusing `LoadCacheLifecycle`'s exact
    /// boot-time logic) is the only sound way to close whatever gap the outage left, before
    /// resuming to listen again.
    private func runLoop(app: Application, config: PostgresConnection.Configuration) async {
        while !Task.isCancelled {
            do {
                let connection = try await PostgresConnection.connect(
                    on: app.eventLoopGroup.next(), configuration: config, id: 0,
                    logger: app.logger)
                do {
                    try await connection.listen(on: CacheInvalidationChannel.name) { notifications in
                        for try await notification in notifications {
                            await CacheReloadDispatch.apply(payload: notification.payload, app: app)
                        }
                    }
                } catch {
                    app.logger.error("Cache invalidation LISTEN dropped: \(error)")
                }
                try? await connection.close()
            } catch {
                app.logger.error("Cache invalidation LISTEN connection failed: \(error)")
            }

            guard !Task.isCancelled else { break }

            do {
                try await LoadCacheLifecycle.reloadAll(app: app)
            } catch {
                app.logger.error("Full cache reload after LISTEN reconnect failed: \(error)")
            }

            try? await Task.sleep(for: .seconds(5))
        }
    }
}
