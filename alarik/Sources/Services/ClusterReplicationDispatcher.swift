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

/// Drains this node's own `cluster-replication-tasks` mailbox: pushes/deletes each due row on
/// its target node via `ClusterReplicationClient`. Ownership is the *enqueuing* node (the one
/// with the local copy to push), not `targetNodeId` - see `ClusterReplicationTask`'s doc comment.
enum ClusterReplicationDispatcher {
    static let maxAttempts = 8

    static let shared = GenericOutboxDispatcher<ClusterReplicationTask>(
        maxAttempts: maxAttempts,
        maxConcurrentDeliveries: 4,
        logContext: "Cluster replication",
        failedStateValue: ClusterReplicationTask.State.failed.rawValue,
        fetchDue: { app, limit in
            await OutboxMailbox.retryPendingEnqueues(
                ClusterReplicationTask.self, app: app,
                collection: OutboxCollections.clusterReplicationTasks)
            return OutboxMailbox.dueTasks(
                ClusterReplicationTask.self, app: app,
                collection: OutboxCollections.clusterReplicationTasks, limit: limit)
        },
        dedupKey: { row in "\(row.bucketName)\u{0}\(row.key)\u{0}\(row.targetNodeId)" },
        attemptDelivery: { row, app in
            // The local copy this node had at enqueue time may since have been reclaimed (a
            // later rebalance pass, or this node losing responsibility for the key) - skip
            // rather than burning an attempt on a guaranteed failure; if the copy is genuinely
            // gone, nothing else will ever retry this exact row (ownership means only this node
            // ever sees it), so it will skip harmlessly forever, same as the old shared-table
            // design already tolerated for this exact scenario.
            if row.operation == ClusterReplicationTask.Operation.put.rawValue {
                let hasLocalCopy =
                    (try? await app.threadPool.runIfActive {
                        try ObjectFileHandler.resolvePath(
                            bucketName: row.bucketName, key: row.key, versionId: row.versionId)
                    }) != nil
                guard hasLocalCopy else { return .skip }
            }

            do {
                guard let node = await ClusterNodeCache.shared.get(id: row.targetNodeId) else {
                    throw ClusterReplicationDispatcherError.unknownTarget(row.targetNodeId)
                }
                switch row.operation {
                case ClusterReplicationTask.Operation.put.rawValue:
                    try await ClusterReplicationClient.pushObject(
                        app: app, to: node, bucketName: row.bucketName, key: row.key,
                        versionId: row.versionId)
                case ClusterReplicationTask.Operation.delete.rawValue:
                    try await ClusterReplicationClient.deleteObject(
                        app: app, to: node, bucketName: row.bucketName, key: row.key,
                        versionId: row.versionId)
                default:
                    throw ClusterReplicationDispatcherError.unknownOperation(row.operation)
                }
                return .success
            } catch {
                return .failure(error)
            }
        },
        persist: { row, _ in
            try OutboxMailbox.update(row, collection: OutboxCollections.clusterReplicationTasks)
        },
        remove: { row, _ in
            OutboxMailbox.remove(row, collection: OutboxCollections.clusterReplicationTasks)
        },
        describeFailure: { row in
            "\(row.key) to node \(row.targetNodeId) (bucket: \(row.bucketName), reason: \(row.reason))"
        },
        purgeExpired: { app in
            OutboxMailbox.purgeExpiredFailures(
                ClusterReplicationTask.self, app: app,
                collection: OutboxCollections.clusterReplicationTasks,
                failedStateValue: ClusterReplicationTask.State.failed.rawValue)
        }
    )

    static func purgeExpiredFailures(app: Application) async throws {
        try await shared.purgeExpiredFailures(app: app)
    }
}

private enum ClusterReplicationDispatcherError: Error, CustomStringConvertible {
    case unknownOperation(String)
    case unknownTarget(UUID)

    var description: String {
        switch self {
        case .unknownOperation(let operation):
            "Unknown cluster replication task operation: \(operation)"
        case .unknownTarget(let id):
            "Target node \(id) is not in the active membership cache"
        }
    }
}
