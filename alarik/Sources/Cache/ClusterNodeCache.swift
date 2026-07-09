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

/// A plain, `Sendable` snapshot of a `ClusterNode` row - the cache never exposes the Fluent
/// model class itself (same reasoning as every other `{Thing}Cache`: callers get a value type,
/// never something that could accidentally be saved back to the DB from a cache read).
struct ClusterNodeInfo: Sendable, Equatable {
    let id: UUID
    let address: String
    let status: ClusterNode.Status
    let lastHeartbeatAt: Date
}

/// This is every node's in-memory view of cluster membership - `PlacementService` and
/// `ObjectRoutingService` read `activeNodes(...)` on every request rather than hitting the DB.
final actor ClusterNodeCache {
    public static let shared = ClusterNodeCache()

    /// How long a node's `lastHeartbeatAt` can go without an update before every other node
    /// treats it as unavailable. Must comfortably exceed `ClusterMembershipLifecycle`'s
    /// heartbeat interval (10s) so one or two missed ticks (a brief GC pause, a slow query)
    /// don't flap a healthy node in and out of the active set.
    static let heartbeatStaleness: TimeInterval = 30

    private var nodes: [UUID: ClusterNodeInfo] = [:]

    func load(initialData: [ClusterNodeInfo]) {
        nodes = Dictionary(uniqueKeysWithValues: initialData.map { ($0.id, $0) })
    }

    func upsert(_ node: ClusterNodeInfo) {
        nodes[node.id] = node
    }

    func remove(id: UUID) {
        nodes.removeValue(forKey: id)
    }

    func get(id: UUID) -> ClusterNodeInfo? {
        nodes[id]
    }

    func all() -> [ClusterNodeInfo] {
        Array(nodes.values)
    }

    /// `active`-status nodes whose heartbeat hasn't gone stale - the exact candidate set
    /// placement/routing/replication treat as "up". A node's own liveness (whether it's stale)
    /// is derived here at read time rather than tracked via a separate detector, since
    /// `lastHeartbeatAt` already carries everything needed to decide.
    func activeNodes(now: Date = Date(), staleness: TimeInterval = ClusterNodeCache.heartbeatStaleness)
        -> [ClusterNodeInfo]
    {
        nodes.values.filter {
            $0.status == .active && now.timeIntervalSince($0.lastHeartbeatAt) <= staleness
        }
    }
}
