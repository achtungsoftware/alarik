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
import Vapor

import struct Foundation.Date
import struct Foundation.UUID

/// Self-registers this node into `cluster_nodes` at boot and keeps it alive with a heartbeat -
/// a true no-op when `ClusterConfigurationKey` wasn't stashed in `app.storage` (cluster mode
/// off). Registered as a `LifecycleHandler` after `LoadCacheLifecycle`/`CacheInvalidationListener`
/// in `configure.swift`,  this must not announce the node until caches are loaded and the invalidation
/// LISTEN loop is live to receive/relay it.
///
/// Two independent periodic ticks, deliberately not one:
/// - **Heartbeat** (every `heartbeatInterval`): updates only this node's own `last_heartbeat_at`
///   directly in the DB, and mirrors it into the local cache immediately - no NOTIFY. Firing a
///   cluster-wide NOTIFY on every 10-second heartbeat from every node would itself be the kind
///   of stampede "automatic rebalancing" was designed to avoid (`CacheReloadDispatch`'s
///   `clusterNode` case triggers a rebalance walk on every NOTIFY it receives) - heartbeats must
///   never be able to trigger one.
/// - **Membership refresh** (every `membershipRefreshInterval`): a full re-read of every row,
///   reloaded into the local cache - the mechanism that actually propagates *peers'* updated
///   heartbeat timestamps to this node (since heartbeats don't NOTIFY), and a safety net against
///   a missed NOTIFY for status changes too.
final actor ClusterMembershipLifecycle: LifecycleHandler {
    static let shared = ClusterMembershipLifecycle()

    /// How often this node refreshes its own heartbeat.
    static let heartbeatInterval: Int64 = 10
    /// How often this node pulls the full membership table to refresh peer liveness/status.
    /// Deliberately longer than the heartbeat interval - this is a safety-net poll, not the
    /// primary propagation path (that's `CacheReloadDispatch`'s NOTIFY handling for genuine
    /// membership changes).
    static let membershipRefreshInterval: Int64 = 15

    private var heartbeatTask: Task<Void, Never>?
    private var refreshTask: Task<Void, Never>?

    func didBootAsync(_ app: Application) async throws {
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            return  // Cluster mode off - nothing to register.
        }
        guard heartbeatTask == nil, refreshTask == nil else { return }

        try await registerSelf(app: app, config: config)

        heartbeatTask = Task { [weak self] in
            await self?.heartbeatLoop(app: app, config: config)
        }
        refreshTask = Task { [weak self] in
            await self?.refreshLoop(app: app)
        }
    }

    func shutdownAsync(_ app: Application) async {
        heartbeatTask?.cancel()
        heartbeatTask = nil
        refreshTask?.cancel()
        refreshTask = nil
    }

    /// Upserts this node's own row (creating it on first-ever boot, refreshing address/heartbeat
    /// on every restart), updates the local cache immediately (not waiting on the NOTIFY
    /// round-trip - same "update local state first, then notify" order, then notifies the cluster. A restart always re-activates a
    /// previously-`draining` node - if an operator restarts the process, that's a clear signal
    /// they want it back in service, not a state the node should second-guess.
    private func registerSelf(app: Application, config: ClusterConfiguration) async throws {
        let now = Date()
        let node: ClusterNode
        if let existing = try await ClusterNode.find(config.nodeId, on: app.db) {
            node = existing
            node.address = config.address
            node.status = ClusterNode.Status.active.rawValue
            node.lastHeartbeatAt = now
        } else {
            node = ClusterNode(
                id: config.nodeId, address: config.address, status: .active, joinedAt: now,
                lastHeartbeatAt: now)
        }
        try await node.save(on: app.db)

        await ClusterNodeCache.shared.upsert(
            ClusterNodeInfo(
                id: config.nodeId, address: config.address, status: .active, lastHeartbeatAt: now)
        )
        CacheInvalidationService.notify(
            on: app.db, cache: "clusterNode", op: .upsert, key: config.nodeId.uuidString)
    }

    private func heartbeatLoop(app: Application, config: ClusterConfiguration) async {
        while !Task.isCancelled {
            try? await Task.sleep(for: .seconds(Self.heartbeatInterval))
            guard !Task.isCancelled else { return }

            let now = Date()
            do {
                try await ClusterNode.query(on: app.db)
                    .filter(\.$id == config.nodeId)
                    .set(\.$lastHeartbeatAt, to: now)
                    .update()
            } catch {
                app.logger.error("Cluster heartbeat update failed: \(error)")
                continue
            }

            if var current = await ClusterNodeCache.shared.get(id: config.nodeId) {
                current = ClusterNodeInfo(
                    id: current.id, address: current.address, status: current.status,
                    lastHeartbeatAt: now)
                await ClusterNodeCache.shared.upsert(current)
            }
        }
    }

    private func refreshLoop(app: Application) async {
        while !Task.isCancelled {
            try? await Task.sleep(for: .seconds(Self.membershipRefreshInterval))
            guard !Task.isCancelled else { return }

            do {
                let rows = try await ClusterNode.query(on: app.db).all()
                let snapshot = rows.compactMap { row -> ClusterNodeInfo? in
                    guard let id = row.id else { return nil }
                    // Fail closed on an unrecognized status rather than defaulting to the
                    // most-trusting `.active` state - a row this node can't interpret must never
                    // silently become eligible for placement/forwarding.
                    guard let status = ClusterNode.Status(rawValue: row.status) else {
                        app.logger.error(
                            "Cluster node \(id) has unrecognized status '\(row.status)' - excluding it from this refresh"
                        )
                        return nil
                    }
                    return ClusterNodeInfo(
                        id: id, address: row.address, status: status,
                        lastHeartbeatAt: row.lastHeartbeatAt)
                }
                await ClusterNodeCache.shared.load(initialData: snapshot)
            } catch {
                app.logger.error("Cluster membership refresh failed: \(error)")
            }
        }
    }
}
