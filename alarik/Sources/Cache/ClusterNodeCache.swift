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
    /// Self-reported disk capacity, mirrored from `ClusterNode.totalBytes`/`availableBytes`.
    /// `nil` until the node's first post-upgrade heartbeat - `ClusterCapacityPolicy` treats a
    /// `nil` as "unknown", never as "full". Defaulted so every pre-existing construction site
    /// that isn't about capacity (membership load/reconcile from a `ClusterNode` row that
    /// doesn't carry it, test fixtures, etc.) doesn't need to opt in explicitly.
    let totalBytes: Int64?
    let availableBytes: Int64?

    init(
        id: UUID, address: String, status: ClusterNode.Status, lastHeartbeatAt: Date,
        totalBytes: Int64? = nil, availableBytes: Int64? = nil
    ) {
        self.id = id
        self.address = address
        self.status = status
        self.lastHeartbeatAt = lastHeartbeatAt
        self.totalBytes = totalBytes
        self.availableBytes = availableBytes
    }
}

/// This is every node's in-memory view of cluster membership - `PlacementService` and
/// `ObjectRoutingService` read `activeNodes(...)` on every request rather than hitting the DB.
final actor ClusterNodeCache {
    public static let shared = ClusterNodeCache()

    /// How long a node's `lastHeartbeatAt` can go without an update before every other node
    /// treats it as unavailable. Set well above `ClusterMembershipLifecycle`'s heartbeat
    /// interval (10s) - six missed ticks - so a node that's briefly too busy to heartbeat (a GC
    /// pause, a slow query, an IO-heavy burst) isn't falsely marked down and flapped out of the
    /// active set. Membership stability matters more than fast failure detection here: a genuine
    /// crash is recovered by an operator draining the node (which excludes it immediately, no
    /// staleness wait), so a generous window costs nothing but avoids spurious placement churn.
    static let heartbeatStaleness: TimeInterval = 60

    private var nodes: [UUID: ClusterNodeInfo] = [:]

    func load(initialData: [ClusterNodeInfo]) {
        nodes = Dictionary(uniqueKeysWithValues: initialData.map { ($0.id, $0) })
    }

    /// Like `load`, but for the periodic membership refresh rather than the boot-time bulk load -
    /// and, critically, additive-only: a node whose record is simply absent from `snapshot` is
    /// left untouched, never dropped. `snapshot` comes from `ClusterNode.all`, a best-effort
    /// cluster-wide fan-out (`MetadataListingService`), so a peer that's merely slow to answer
    /// (or whose record sits on a node this one fan-out couldn't reach) is indistinguishable from
    /// a genuine departure. Staleness is handled independently by `activeNodes()`'s own
    /// heartbeat-age check, and genuine removal already has its own authoritative signal via the
    /// explicit `remove(id:)` call from `CacheReloadDispatch`'s `("clusterNode", .remove)` case -
    /// `reconcile` doesn't need to duplicate either of those by also treating
    /// "not in this particular fan-out" as removal.
    ///
    /// For nodes that *are* present in `snapshot`, still prefer the existing cached entry
    /// whenever it's strictly fresher - a newer heartbeat, or (on an equal heartbeat) a
    /// non-`active` status the snapshot was read just before it committed, so an in-flight drain
    /// is never resurrected as active by a stale snapshot row racing a concurrent
    /// `upsert`/`remove`.
    func reconcile(snapshot: [ClusterNodeInfo]) {
        for node in snapshot {
            if let existing = nodes[node.id], Self.prefersExisting(existing, over: node) {
                continue
            }
            nodes[node.id] = node
        }
    }

    private static func prefersExisting(_ existing: ClusterNodeInfo, over snapshot: ClusterNodeInfo)
        -> Bool
    {
        if existing.lastHeartbeatAt != snapshot.lastHeartbeatAt {
            return existing.lastHeartbeatAt > snapshot.lastHeartbeatAt
        }
        // Same heartbeat: prefer a cached non-active status (a drain/removal the snapshot missed)
        // over the snapshot's active one.
        return existing.status != .active && snapshot.status == .active
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
