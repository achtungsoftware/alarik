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
import Vapor

/// Drains the `erasure_coded_replication_tasks` outbox. Every `.put`-operation row (whatever its
/// reason - `.write` straggler catch-up, `.rebalance` re-placement, or `.reconstruct` of a lost
/// shard) is delivered via `ErasureCodedRebalanceService.reconstructAndPlaceShard`: there's no
/// cheaper "just re-push my local copy" path in EC the way whole-object replication has one, since
/// no single node ever holds more than one shard - reconstruction from `k` current survivors *is*
/// the delivery mechanism, uniformly. (Reclaiming a node's own stale shard needs no outbox row at
/// all - it's a local delete done inline once the object is confirmed reconstructable without it.)
enum ErasureCodedDispatcher {
    static let maxAttempts = 8

    static let shared = GenericOutboxDispatcher<ErasureCodedReplicationTask>(
        maxAttempts: maxAttempts,
        maxConcurrentDeliveries: 4,
        logContext: "EC shard replication",
        failedStateValue: ErasureCodedReplicationTask.State.failed.rawValue,
        fetchDue: { db, limit in
            try await ErasureCodedReplicationTask.query(on: db)
                .filter(\.$state == ErasureCodedReplicationTask.State.pending.rawValue)
                .filter(\.$nextAttemptAt <= Date())
                .sort(\.$nextAttemptAt, .ascending)
                .limit(limit)
                .all()
        },
        dedupKey: { row in
            "\(row.bucketName)\u{0}\(row.key)\u{0}\(row.versionId ?? "")\u{0}\(row.shardIndex)\u{0}\(row.targetNodeId)"
        },
        attemptDelivery: { row, app in
            // Target-only delivery: every node's dispatcher drains this same shared table, and
            // unlike legacy replication - where only a node physically holding the object can
            // deliver, so `.skip` naturally leaves each row to its one capable node - ANY node
            // can reconstruct a shard. Without this gate, several nodes would race to gather `k`
            // shards and RS-decode the same repair redundantly on every drain tick. The target
            // itself is the one node whose delivery is always both possible and useful: it
            // rebuilds its own missing shard from survivors, and a *down* target simply leaves
            // the row pending (correct - the shard can't land on a down node anyway) until the
            // target's own 2s tick drains it on return.
            guard let selfNodeId = app.storage[ClusterConfigurationKey.self]?.nodeId else {
                return .skip
            }
            if row.targetNodeId != selfNodeId {
                // A target that's left the membership entirely will never drain its own row -
                // burn an attempt so the row dead-letters and gets purged instead of sitting
                // pending forever (the rebalance sweep re-detects the gap under the new
                // membership with a fresh, correctly-targeted task).
                if await ClusterNodeCache.shared.get(id: row.targetNodeId) == nil {
                    return .failure(ErasureCodedDispatcherError.unknownTarget(row.targetNodeId))
                }
                return .skip
            }
            do {
                switch row.operation {
                case ErasureCodedReplicationTask.Operation.put.rawValue:
                    try await ErasureCodedRebalanceService.reconstructAndPlaceShard(
                        app: app, bucketName: row.bucketName, key: row.key, versionId: row.versionId,
                        shardIndex: row.shardIndex, targetNodeId: row.targetNodeId)
                case ErasureCodedReplicationTask.Operation.delete.rawValue:
                    // The gate above already confirmed row.targetNodeId == selfNodeId, so this is
                    // always this node's own shard directory - remove it directly rather than
                    // looping an HTTP DELETE through to itself. Whole-directory, not a specific
                    // index: `shardIndex` on a delete task is the `-1` sentinel (a node only ever
                    // holds the one shard it's currently responsible for, so there's nothing
                    // index-specific to target) - mirrors `handleDelete`'s exact behavior for the
                    // network-received case.
                    try? FileManager.default.removeItem(
                        atPath: ErasureCodedObjectHandler.shardBasePath(
                            bucketName: row.bucketName, key: row.key, versionId: row.versionId))
                default:
                    throw ErasureCodedDispatcherError.unknownOperation(row.operation)
                }
                return .success
            } catch {
                return .failure(error)
            }
        },
        describeFailure: { row in
            "\(row.key) shard \(row.shardIndex) to node \(row.targetNodeId) (bucket: \(row.bucketName), reason: \(row.reason))"
        },
        purgeExpired: { db in
            try await ErasureCodedReplicationTask.query(on: db)
                .filter(\.$state == ErasureCodedReplicationTask.State.failed.rawValue)
                .filter(\.$createdAt < Date().addingTimeInterval(-7 * 24 * 3600))
                .delete()
        }
    )

    static func purgeExpiredFailures(on db: any Database) async throws {
        try await shared.purgeExpiredFailures(on: db)
    }
}

enum ErasureCodedDispatcherError: Error, CustomStringConvertible {
    case unknownOperation(String)
    case unknownTarget(UUID)

    var description: String {
        switch self {
        case .unknownOperation(let operation):
            "Unknown EC replication task operation: \(operation)"
        case .unknownTarget(let id):
            "Target node \(id) is not in the active membership cache"
        }
    }
}
