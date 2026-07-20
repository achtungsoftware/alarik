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

/// Drains this node's own `replication-tasks` mailbox to external S3-compatible replication
/// targets. Lower `maxConcurrentDeliveries` than the other dispatchers - each task here is a
/// full object body transfer, not a small JSON POST.
enum ReplicationDispatcher {
    static let maxAttempts = 8

    static let shared = GenericOutboxDispatcher<ReplicationTask>(
        maxAttempts: maxAttempts,
        maxConcurrentDeliveries: 4,
        logContext: "Replication",
        failedStateValue: ReplicationTask.State.failed.rawValue,
        fetchDue: { app, limit in
            await OutboxMailbox.retryPendingEnqueues(
                ReplicationTask.self, app: app, collection: OutboxCollections.replicationTasks)
            return OutboxMailbox.dueTasks(
                ReplicationTask.self, app: app, collection: OutboxCollections.replicationTasks,
                limit: limit)
        },
        // Different targets for the same key are unrelated deliveries - dedup key includes
        // targetId so they're never held up by each other, only same-(key,target) is mutexed.
        dedupKey: { row in "\(row.bucketName)\u{0}\(row.key)\u{0}\(row.targetId)" },
        attemptDelivery: { row, app in
            do {
                switch row.operation {
                case ReplicationTask.Operation.put.rawValue:
                    try await ReplicationClient.replicatePut(
                        app: app, target: row, bucketName: row.bucketName, key: row.key,
                        versionId: row.versionId)
                case ReplicationTask.Operation.delete.rawValue:
                    try await ReplicationClient.replicateDelete(target: row, key: row.key)
                default:
                    throw ReplicationDispatcherError.unknownOperation(row.operation)
                }
                return .success
            } catch {
                return .failure(error)
            }
        },
        persist: { row, _ in try OutboxMailbox.update(row, collection: OutboxCollections.replicationTasks) },
        remove: { row, _ in OutboxMailbox.remove(row, collection: OutboxCollections.replicationTasks) },
        describeFailure: { row in "\(row.key) to \(row.endpoint) (bucket: \(row.bucketName))" },
        purgeExpired: { app in
            OutboxMailbox.purgeExpiredFailures(
                ReplicationTask.self, app: app, collection: OutboxCollections.replicationTasks,
                failedStateValue: ReplicationTask.State.failed.rawValue)
        }
    )

    static func purgeExpiredFailures(app: Application) async throws {
        try await shared.purgeExpiredFailures(app: app)
    }
}

private enum ReplicationDispatcherError: Error, CustomStringConvertible {
    case unknownOperation(String)

    var description: String {
        switch self {
        case .unknownOperation(let operation):
            "Unknown replication task operation: \(operation)"
        }
    }
}
