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

/// Automatic rebalancing on cluster membership change - triggered by `CacheReloadDispatch`'s
/// `clusterNode` NOTIFY handling (node join, drain, removal) and by the manual admin resync
/// action. `ObjectFileHandler.listAllVersions` only ever sees what's physically on *this* node's
/// own disk, so this walk is inherently self-scoped: every node reacts to the same NOTIFY and
/// cleans up after itself independently, there's no central coordinator. Walks *every* version
/// and delete marker, not just the current one - a historical version left behind on a node
/// that's lost responsibility for its key is exactly as reclaimable as a current one, and a
/// delete marker not copied to a newly-responsible node would make that node incorrectly serve
/// an older, "deleted" version as current.
///
/// Two phases per (key, version) entry, both keyed off recomputing
/// `PlacementService.responsibleNodes` under the *current* membership - placement is per-key, so
/// every version of a key shares the same responsible set:
///
/// - **Copy**: if this node is still responsible, push a `reason: .rebalance` task to every
///   *other* responsible node that may not yet have a copy. Enqueueing is idempotent/harmless
///   when the target already has the object - `pushObject` always overwrites with the exact same
///   version - so this never needs to first check what a node already holds. A delete marker is
///   pushed exactly like any other version (`pushObject` doesn't special-case it).
/// - **Reclaim**: if this node is no longer responsible for a key it still holds a version of,
///   that version is only safe to delete once every currently-responsible node has actually
///   received a copy - deleting first and confirming later risks the only copy vanishing before
///   a replica exists. Since a completed `ClusterReplicationTask` row is deleted (the outbox's
///   "row gone means delivered" model), "no outstanding pending/failed task for this key" is
///   exactly that confirmation signal, checked with one batched query per page rather than per
///   version - this naturally batches every version of one key together once nothing for that
///   key is outstanding. When outstanding work remains, reclaim is simply skipped this pass -
///   self-healing, it's reconsidered on the next rebalance trigger.
enum ClusterRebalanceService {
    enum RebalanceReason: String {
        case membershipChange
        case manualResync
    }

    /// How long to wait before re-walking after a pass found reclaim candidates it couldn't
    /// clear yet (an outstanding copy task still in flight). Comfortably longer than the
    /// dispatcher normally needs to drain a just-enqueued copy task (it wakes immediately on
    /// enqueue), short enough that reclaimed disk space doesn't linger for long under normal
    /// conditions.
    static let gatedReclaimFollowUpDelay: Duration = .seconds(30)

    /// Fire-and-forget entry point - never blocks the caller (the NOTIFY dispatch path, or an
    /// admin API request) on a walk that could take a long time over a large cluster.
    static func scheduleRebalance(app: Application, reason: RebalanceReason) async {
        guard app.storage[ClusterConfigurationKey.self] != nil else { return }
        Task {
            do {
                try await rebalance(app: app, reason: reason)
            } catch {
                app.logger.error("Cluster rebalance walk failed (reason: \(reason)): \(error)")
            }
        }
    }

    /// `scheduleRebalance` only ever fires reactively - once per membership-change NOTIFY, or
    /// once per manual resync request. Reclaim needs a *second* pass after a first pass's copy
    /// tasks have actually cleared, which nothing else would otherwise ever trigger, so a walk
    /// that found reclaim candidates still gated on outstanding work schedules its own follow-up
    /// pass after `gatedReclaimFollowUpDelay` - self-perpetuating every interval until reclaim
    /// actually clears, then stopping on its own once nothing is gated anymore.
    static func rebalance(app: Application, reason: RebalanceReason) async throws {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return }
        let active = await ClusterNodeCache.shared.activeNodes()
        guard !active.isEmpty else { return }

        let buckets = try await Bucket.query(on: app.db).all()
        var hasGatedReclaims = false
        for bucket in buckets {
            let bucketHasGatedReclaims = try await rebalanceBucket(
                app: app, bucketName: bucket.name, activeNodes: active, selfNodeId: config.nodeId)
            hasGatedReclaims = hasGatedReclaims || bucketHasGatedReclaims
        }

        if hasGatedReclaims {
            Task {
                try? await Task.sleep(for: Self.gatedReclaimFollowUpDelay)
                await scheduleRebalance(app: app, reason: reason)
            }
        }
    }

    /// Batches inserts one page at a time (`Collection.create(on:)`, a single statement per page
    /// rather than one awaited insert per object/node pair), mirroring
    /// `ReplicationService.resync`'s exact pattern. Returns whether this bucket had reclaim
    /// candidates still gated on outstanding work, so the caller knows whether a follow-up pass
    /// is worth scheduling.
    @discardableResult
    private static func rebalanceBucket(
        app: Application, bucketName: String, activeNodes: [ClusterNodeInfo], selfNodeId: UUID
    ) async throws -> Bool {
        var keyMarker: String?
        var versionIdMarker: String?
        var hasGatedReclaims = false
        repeat {
            let (versions, deleteMarkers, _, isTruncated, nextKeyMarker, nextVersionIdMarker) =
                try ObjectFileHandler.listAllVersions(
                    bucketName: bucketName, prefix: "", delimiter: nil, keyMarker: keyMarker,
                    versionIdMarker: versionIdMarker, maxKeys: 1000)

            var copyTasks: [ClusterReplicationTask] = []
            var reclaimCandidates: [(key: String, versionId: String?)] = []

            for entry in versions + deleteMarkers {
                let responsible = PlacementService.responsibleNodes(
                    bucketName: bucketName, key: entry.key, activeNodes: activeNodes)
                if responsible.contains(where: { $0.id == selfNodeId }) {
                    // This node is still responsible - only *other* responsible nodes ever need
                    // a copy pushed to them.
                    for node in responsible where node.id != selfNodeId {
                        copyTasks.append(
                            ClusterReplicationTask(
                                bucketName: bucketName, key: entry.key,
                                versionId: entry.versionId, operation: .put,
                                targetNodeId: node.id, reason: .rebalance))
                    }
                } else {
                    reclaimCandidates.append((entry.key, entry.versionId))
                }
            }

            if !copyTasks.isEmpty {
                try await copyTasks.create(on: app.db)
            }

            var reclaimTasks: [ClusterReplicationTask] = []
            if !reclaimCandidates.isEmpty {
                let stillOutstanding = try await ClusterReplicationTask.query(on: app.db)
                    .filter(\.$bucketName == bucketName)
                    .filter(\.$key ~~ reclaimCandidates.map(\.key))
                    .filter(
                        \.$state
                            ~~ [
                                ClusterReplicationTask.State.pending.rawValue,
                                ClusterReplicationTask.State.failed.rawValue,
                            ]
                    )
                    .all()
                    .reduce(into: Set<String>()) { $0.insert($1.key) }

                reclaimTasks = reclaimCandidates
                    .filter { !stillOutstanding.contains($0.key) }
                    .map { candidate in
                        ClusterReplicationTask(
                            bucketName: bucketName, key: candidate.key,
                            versionId: candidate.versionId, operation: .delete,
                            targetNodeId: selfNodeId, reason: .reclaim)
                    }
                if !reclaimTasks.isEmpty {
                    try await reclaimTasks.create(on: app.db)
                }
                if reclaimTasks.count < reclaimCandidates.count {
                    hasGatedReclaims = true
                }
            }

            if !copyTasks.isEmpty || !reclaimTasks.isEmpty {
                ClusterReplicationDispatcher.shared.wake()
            }

            keyMarker = isTruncated ? nextKeyMarker : nil
            versionIdMarker = isTruncated ? nextVersionIdMarker : nil
        } while keyMarker != nil
        return hasGatedReclaims
    }
}
