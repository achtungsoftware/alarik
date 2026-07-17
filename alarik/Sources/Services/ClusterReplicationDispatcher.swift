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

/// Drains the `cluster_replication_tasks` outbox: pushes/deletes each due row on its target
/// node via `ClusterReplicationClient`.
enum ClusterReplicationDispatcher {
    static let maxAttempts = 8

    static let shared = GenericOutboxDispatcher<ClusterReplicationTask>(
        maxAttempts: maxAttempts,
        maxConcurrentDeliveries: 4,
        logContext: "Cluster replication",
        failedStateValue: ClusterReplicationTask.State.failed.rawValue,
        fetchDue: { db, limit in
            try await ClusterReplicationTask.query(on: db)
                .filter(\.$state == ClusterReplicationTask.State.pending.rawValue)
                .filter(\.$nextAttemptAt <= Date())
                .sort(\.$nextAttemptAt, .ascending)
                .limit(limit)
                .all()
        },
        dedupKey: { row in "\(row.bucketName)\u{0}\(row.key)\u{0}\(row.targetNodeId)" },
        attemptDelivery: { row, app in
            // Any node's tick can pick up any pending row, so a `.put` this node doesn't
            // physically have would just be a guaranteed failure - skip instead of burning
            // the shared attempts budget; the node that actually has it retries on its own tick.
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
        describeFailure: { row in
            "\(row.key) to node \(row.targetNodeId) (bucket: \(row.bucketName), reason: \(row.reason))"
        },
        purgeExpired: { db in
            try await ClusterReplicationTask.query(on: db)
                .filter(\.$state == ClusterReplicationTask.State.failed.rawValue)
                .filter(\.$createdAt < Date().addingTimeInterval(-7 * 24 * 3600))
                .delete()
        }
    )

    static func purgeExpiredFailures(on db: any Database) async throws {
        try await shared.purgeExpiredFailures(on: db)
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
