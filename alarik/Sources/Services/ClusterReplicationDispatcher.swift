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
/// node via `ClusterReplicationClient` and applies retry bookkeeping on failure. Structural clone of
/// `ReplicationDispatcher` - identical actor shape, re-entrancy coalescing, bounded-concurrency
/// drain loop, same-key mutex, and backoff formula - just re-targeted at an internal peer node
/// (`ClusterReplicationClient.pushObject`/`deleteObject`) instead of an external S3-compatible
/// endpoint (`ReplicationClient`).
final actor ClusterReplicationDispatcher {
    static let shared = ClusterReplicationDispatcher()

    static let maxAttempts = 8
    static let batchSize = 50
    static let maxConcurrentDeliveries = 4

    private var app: Application?
    private var isDraining = false
    private var pendingWake = false

    func configure(app: Application) {
        self.app = app
    }

    nonisolated func wake() {
        Task { await self.drain() }
    }

    func drain() async {
        guard let app else { return }
        if isDraining {
            pendingWake = true
            return
        }
        isDraining = true
        defer {
            isDraining = false
            if pendingWake {
                pendingWake = false
                wake()
            }
        }

        while true {
            let due: [ClusterReplicationTask]
            do {
                due = try await ClusterReplicationTask.query(on: app.db)
                    .filter(\.$state == ClusterReplicationTask.State.pending.rawValue)
                    .filter(\.$nextAttemptAt <= Date())
                    .sort(\.$nextAttemptAt, .ascending)
                    .limit(Self.batchSize)
                    .all()
            } catch {
                app.logger.error("Cluster replication dispatcher failed to query outbox: \(error)")
                return
            }

            guard !due.isEmpty else { return }

            var remaining = due
            var inFlightKeys: Set<String> = []

            func popNextEligible() -> ClusterReplicationTask? {
                guard let index = remaining.firstIndex(where: { !inFlightKeys.contains(Self.taskKey($0)) })
                else { return nil }
                let row = remaining.remove(at: index)
                inFlightKeys.insert(Self.taskKey(row))
                return row
            }

            await withTaskGroup(of: String.self) { group in
                var inFlight = 0
                while inFlight < Self.maxConcurrentDeliveries, let row = popNextEligible() {
                    let key = Self.taskKey(row)
                    group.addTask {
                        await Self.deliver(row, app: app)
                        return key
                    }
                    inFlight += 1
                }
                while let finishedKey = await group.next() {
                    inFlightKeys.remove(finishedKey)
                    guard let row = popNextEligible() else { continue }
                    let key = Self.taskKey(row)
                    group.addTask {
                        await Self.deliver(row, app: app)
                        return key
                    }
                }
            }

            if due.count < Self.batchSize && remaining.isEmpty {
                return
            }
        }
    }

    private static func taskKey(_ row: ClusterReplicationTask) -> String {
        "\(row.bucketName)\u{0}\(row.key)\u{0}\(row.targetNodeId)"
    }

    private static func deliver(_ row: ClusterReplicationTask, app: Application) async {
        var succeeded = false
        var failureReason: String?
        do {
            guard let node = await ClusterNodeCache.shared.get(id: row.targetNodeId) else {
                throw DispatcherError.unknownTarget(row.targetNodeId)
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
                throw DispatcherError.unknownOperation(row.operation)
            }
            succeeded = true
        } catch {
            succeeded = false
            failureReason = "\(error)"
        }

        do {
            if succeeded {
                try await row.delete(on: app.db)
            } else {
                row.attempts += 1
                row.lastError = failureReason
                if row.attempts >= maxAttempts {
                    row.state = ClusterReplicationTask.State.failed.rawValue
                    app.logger.warning(
                        "Cluster replication of \(row.key) to node \(row.targetNodeId) failed permanently after \(row.attempts) attempts (bucket: \(row.bucketName), reason: \(row.reason))"
                    )
                } else {
                    let backoff = min(30.0 * pow(2.0, Double(row.attempts)), 3600.0)
                    row.nextAttemptAt = Date().addingTimeInterval(backoff)
                }
                try await row.save(on: app.db)
            }
        } catch {
            app.logger.error("Cluster replication dispatcher failed to update outbox row: \(error)")
        }
    }

    static func purgeExpiredFailures(on db: any Database) async throws {
        try await ClusterReplicationTask.query(on: db)
            .filter(\.$state == ClusterReplicationTask.State.failed.rawValue)
            .filter(\.$createdAt < Date().addingTimeInterval(-7 * 24 * 3600))
            .delete()
    }
}

private enum DispatcherError: Error, CustomStringConvertible {
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
