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
/// Deliberately **not** a `StoreBackedCache`, unlike every other cache here.
///
/// The read-through those use resolves a miss by reading `MetadataStore` - but `MetadataStore`
/// works out *where* to read from using this cache. Repairing membership by consulting the store
/// would therefore need the very answer it is trying to produce. A node that knows only itself
/// would ask only itself, and stay wrong.
///
/// Membership is repaired along the one path that needs no prior knowledge: re-querying the
/// statically configured `CLUSTER_SEED_NODES` addresses - see
/// `ClusterMembershipLifecycle.refreshNow`, which callers invoke before failing an operation
/// because their view looks too small.
final actor ClusterNodeCache {
    public static let shared = ClusterNodeCache()

    /// How long `lastHeartbeatAt` may lag before a node counts as unreachable. Six missed ticks,
    /// so a briefly-busy node isn't flapped out. Affects liveness only, never ownership.
    static let heartbeatStaleness: TimeInterval = 60

    /// Ownership is deliberately decoupled from liveness.
    ///
    /// A key's replicas are ranked over every *registered* node, so a node going down does not
    /// hand its keys to someone else and hand them back on recovery. Reads tolerate it (any one
    /// replica answers), writes to it queue in the outbox and replay, and the record stays where
    /// it was written. Ownership changes only when an operator drains or removes a node.
    ///
    /// Deriving ownership from the live set instead makes every blip reshuffle part of the
    /// keyspace, so records stop being where the current placement says they are.

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

    /// Who *owns* a key: every registered, non-draining node, regardless of whether it is
    /// answering right now. Deliberately ignores heartbeat staleness - see the doc comment above.
    func placementNodes() -> [ClusterNodeInfo] {
        nodes.values.filter { $0.status == .active }
    }

    /// Who is *reachable* right now - for admission checks, broadcast targets and liveness
    /// reporting. Never for deciding where a key lives; use `placementNodes()` for that.
    func activeNodes(now: Date = Date(), staleness: TimeInterval = ClusterNodeCache.heartbeatStaleness)
        -> [ClusterNodeInfo]
    {
        nodes.values.filter {
            $0.status == .active && now.timeIntervalSince($0.lastHeartbeatAt) <= staleness
        }
    }
}
