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

/// Drains the `replication_tasks` outbox: replicates each due row to its target via
/// `ReplicationClient` and applies retry bookkeeping on failure. At-least-once delivery: a row
/// is only removed after a successful transfer, so restarts and target outages never lose
/// replication work (the flip side - the target may occasionally see a duplicate PUT of the
/// same version - is harmless since object bytes for a given `versionId` are immutable).
///
/// Structural clone of `NotificationDispatcher` - same actor shape, same re-entrancy
/// coalescing, same bounded-concurrency drain loop, same backoff formula. The one deliberate
/// difference is a lower `maxConcurrentDeliveries`: each task here is a full object body
/// transfer (potentially multipart, potentially gigabytes), not a small JSON POST, so it's
/// worth being more conservative on outbound bandwidth/memory by default.
final actor ReplicationDispatcher {
    static let shared = ReplicationDispatcher()

    static let maxAttempts = 8
    static let batchSize = 50
    static let maxConcurrentDeliveries = 4

    private var app: Application?
    private var isDraining = false
    private var pendingWake = false

    /// Must be called once at boot (configure.swift) before any events flow.
    func configure(app: Application) {
        self.app = app
    }

    /// Kicks off a drain pass without blocking the caller. Safe to call from anywhere.
    nonisolated func wake() {
        Task { await self.drain() }
    }

    /// Processes every due pending row, in batches, until none remain. Serialized: a second
    /// drain requested while one is running just marks a follow-up pass.
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
            let due: [ReplicationTask]
            do {
                due = try await ReplicationTask.query(on: app.db)
                    .filter(\.$state == ReplicationTask.State.pending.rawValue)
                    .filter(\.$nextAttemptAt <= Date())
                    .sort(\.$nextAttemptAt, .ascending)
                    .limit(Self.batchSize)
                    .all()
            } catch {
                app.logger.error("Replication dispatcher failed to query outbox: \(error)")
                return
            }

            guard !due.isEmpty else { return }

            // Bounded parallelism: at most maxConcurrentDeliveries transfers in flight - a
            // slow/unreachable target delays its own queue, not the whole outbox. Tasks for
            // the same (bucket, key, target) are never run concurrently: `due` is oldest-first,
            // so a (key, target) pair already in flight has its later, conflicting task skipped
            // this pass rather than raced against it - two versions of the same key are never
            // in doubt about which order they take effect on that target. Different targets for
            // the same key are unrelated deliveries with no ordering to protect, so they're
            // never held up by each other - `taskKey` includes `targetId` for exactly this
            // reason (see `ClusterReplicationDispatcher.taskKey`'s equivalent `targetNodeId`). A
            // skipped task is still `pending` and picked up on the very next pass (once the row
            // it conflicted with has been deleted or rescheduled), so nothing here is a queue -
            // it's a same-(key,target) mutex over this one drain call.
            var remaining = due
            var inFlightKeys: Set<String> = []

            func popNextEligible() -> ReplicationTask? {
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

            // `remaining` only has leftovers when same-key conflicts prevented them from
            // running this pass - loop again immediately (the conflicting row has since been
            // resolved) instead of the normal "fewer than a full batch -> nothing else due"
            // early exit.
            if due.count < Self.batchSize && remaining.isEmpty {
                return
            }
        }
    }

    private static func taskKey(_ row: ReplicationTask) -> String {
        "\(row.bucketName)\u{0}\(row.key)\u{0}\(row.targetId)"
    }

    /// Replicates one outbox row to its (snapshotted) target. Success deletes the row; any
    /// failure schedules a retry with exponential backoff, dead-lettering after `maxAttempts`.
    private static func deliver(_ row: ReplicationTask, app: Application) async {
        var succeeded = false
        var failureReason: String?
        do {
            switch row.operation {
            case ReplicationTask.Operation.put.rawValue:
                try await ReplicationClient.replicatePut(
                    target: row, bucketName: row.bucketName, key: row.key,
                    versionId: row.versionId)
            case ReplicationTask.Operation.delete.rawValue:
                try await ReplicationClient.replicateDelete(target: row, key: row.key)
            default:
                // Never silently treat an unrecognized operation as delivered - that would
                // delete the outbox row without ever replicating it. Fails loudly instead,
                // same retry/dead-letter path as a real transport error.
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
                    row.state = ReplicationTask.State.failed.rawValue
                    app.logger.warning(
                        "Replication of \(row.key) to \(row.endpoint) failed permanently after \(row.attempts) attempts (bucket: \(row.bucketName))"
                    )
                } else {
                    // 60s, 120s, 240s, ... capped at 1h
                    let backoff = min(30.0 * pow(2.0, Double(row.attempts)), 3600.0)
                    row.nextAttemptAt = Date().addingTimeInterval(backoff)
                }
                try await row.save(on: app.db)
            }
        } catch {
            // Bookkeeping failure: the row stays pending and will be retried on a later
            // tick - worst case is a duplicate transfer, never a lost one
            app.logger.error("Replication dispatcher failed to update outbox row: \(error)")
        }
    }

    /// Purges dead-lettered rows older than 7 days - called from the hourly cleanup task.
    static func purgeExpiredFailures(on db: any Database) async throws {
        try await ReplicationTask.query(on: db)
            .filter(\.$state == ReplicationTask.State.failed.rawValue)
            .filter(\.$createdAt < Date().addingTimeInterval(-7 * 24 * 3600))
            .delete()
    }
}

private enum DispatcherError: Error, CustomStringConvertible {
    case unknownOperation(String)

    var description: String {
        switch self {
        case .unknownOperation(let operation):
            "Unknown replication task operation: \(operation)"
        }
    }
}
