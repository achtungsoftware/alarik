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
import Vapor

/// Drains this node's own `erasure-coded-replication-tasks` mailbox. `dueTasks` only ever
/// returns tasks this node itself owns - ownership is `targetNodeId` for this table, so every
/// row drained here already targets this exact node, no separate gate needed. Every
/// `.put`-operation row (`.write` straggler catch-up, `.rebalance` re-placement, or
/// `.reconstruct` of a lost shard) is delivered via
/// `ErasureCodedRebalanceService.reconstructAndPlaceShard`: no single node ever holds more than
/// one shard, so reconstruction from `k` current survivors *is* the delivery mechanism,
/// uniformly. Reclaiming a node's own stale shard needs no outbox row - it's a local delete done
/// inline once the object is confirmed reconstructable without it.
enum ErasureCodedDispatcher {
    static let maxAttempts = 8

    static let shared = GenericOutboxDispatcher<ErasureCodedReplicationTask>(
        maxAttempts: maxAttempts,
        maxConcurrentDeliveries: 4,
        logContext: "EC shard replication",
        failedStateValue: ErasureCodedReplicationTask.State.failed.rawValue,
        fetchDue: { app, limit in
            await OutboxMailbox.retryPendingEnqueues(
                ErasureCodedReplicationTask.self, app: app,
                collection: OutboxCollections.erasureCodedReplicationTasks)
            return OutboxMailbox.dueTasks(
                ErasureCodedReplicationTask.self, app: app,
                collection: OutboxCollections.erasureCodedReplicationTasks, limit: limit)
        },
        dedupKey: { row in
            "\(row.bucketName)\u{0}\(row.key)\u{0}\(row.versionId ?? "")\u{0}\(row.shardIndex)\u{0}\(row.targetNodeId)"
        },
        attemptDelivery: { row, app in
            do {
                switch row.operation {
                case ErasureCodedReplicationTask.Operation.put.rawValue:
                    try await ErasureCodedRebalanceService.reconstructAndPlaceShard(
                        app: app, bucketName: row.bucketName, key: row.key, versionId: row.versionId,
                        shardIndex: row.shardIndex, targetNodeId: row.targetNodeId)
                case ErasureCodedReplicationTask.Operation.delete.rawValue:
                    // Ownership already guarantees row.targetNodeId == this node, so this is
                    // always this node's own shard directory - remove it directly rather than
                    // looping an HTTP DELETE through to itself. Whole-directory, not a specific
                    // index: a node only ever holds the one shard it's currently responsible for.
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
        persist: { row, _ in
            try OutboxMailbox.update(row, collection: OutboxCollections.erasureCodedReplicationTasks)
        },
        remove: { row, _ in
            OutboxMailbox.remove(row, collection: OutboxCollections.erasureCodedReplicationTasks)
        },
        describeFailure: { row in
            "\(row.key) shard \(row.shardIndex) to node \(row.targetNodeId) (bucket: \(row.bucketName), reason: \(row.reason))"
        },
        purgeExpired: { app in
            OutboxMailbox.purgeExpiredFailures(
                ErasureCodedReplicationTask.self, app: app,
                collection: OutboxCollections.erasureCodedReplicationTasks,
                failedStateValue: ErasureCodedReplicationTask.State.failed.rawValue)
        }
    )

    static func purgeExpiredFailures(app: Application) async throws {
        try await shared.purgeExpiredFailures(app: app)
    }
}

enum ErasureCodedDispatcherError: Error, CustomStringConvertible {
    case unknownOperation(String)

    var description: String {
        switch self {
        case .unknownOperation(let operation):
            "Unknown EC replication task operation: \(operation)"
        }
    }
}
