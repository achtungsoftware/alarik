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

/// Outcome of one delivery attempt. `.skip` means "leave the row untouched" - e.g. a `.put`
/// row this node doesn't physically have the object for; it stays pending and gets picked up
/// again, possibly by whichever node actually does.
enum OutboxDeliveryOutcome {
    case success
    case failure(any Error)
    case skip
}

/// Same-key-in-flight tracking for one drain pass. `@unchecked Sendable`: only ever touched
/// synchronously on the actor's executor, between `await` points, never from inside a
/// `group.addTask` child closure - same pattern this codebase already uses for Fluent models.
private final class DrainState<Row: OutboxRow>: @unchecked Sendable {
    var remaining: [Row]
    var inFlightKeys: Set<String> = []

    init(remaining: [Row]) {
        self.remaining = remaining
    }

    func popNextEligible(dedupKey: ((Row) -> String)?) -> Row? {
        var index = 0
        while index < remaining.count {
            let key = dedupKey?(remaining[index])
            if key == nil || !inFlightKeys.contains(key!) {
                let row = remaining.remove(at: index)
                if let key { inFlightKeys.insert(key) }
                return row
            }
            index += 1
        }
        return nil
    }
}

/// Shared drain/backoff/dedup/dead-letter engine behind `ReplicationDispatcher`,
/// `ClusterReplicationDispatcher`, and `NotificationDispatcher` - each is now a thin
/// instantiation of this actor with its own row type and closures.
final actor GenericOutboxDispatcher<Row: OutboxRow> {
    private let maxAttempts: Int
    private let batchSize: Int
    private let maxConcurrentDeliveries: Int
    private let logContext: String
    private let failedStateValue: String
    private let fetchDue: @Sendable (any Database, Int) async throws -> [Row]
    private let dedupKey: (@Sendable (Row) -> String)?
    private let attemptDelivery: @Sendable (Row, Application) async -> OutboxDeliveryOutcome
    private let describeFailure: @Sendable (Row) -> String
    private let purgeExpired: @Sendable (any Database) async throws -> Void

    private var app: Application?
    private var isDraining = false
    private var pendingWake = false

    init(
        maxAttempts: Int = 8,
        batchSize: Int = 50,
        maxConcurrentDeliveries: Int,
        logContext: String,
        failedStateValue: String,
        fetchDue: @escaping @Sendable (any Database, Int) async throws -> [Row],
        dedupKey: (@Sendable (Row) -> String)? = nil,
        attemptDelivery: @escaping @Sendable (Row, Application) async -> OutboxDeliveryOutcome,
        describeFailure: @escaping @Sendable (Row) -> String,
        purgeExpired: @escaping @Sendable (any Database) async throws -> Void
    ) {
        self.maxAttempts = maxAttempts
        self.batchSize = batchSize
        self.maxConcurrentDeliveries = maxConcurrentDeliveries
        self.logContext = logContext
        self.failedStateValue = failedStateValue
        self.fetchDue = fetchDue
        self.dedupKey = dedupKey
        self.attemptDelivery = attemptDelivery
        self.describeFailure = describeFailure
        self.purgeExpired = purgeExpired
    }

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

        let maxAttempts = self.maxAttempts
        let logContext = self.logContext
        let failedStateValue = self.failedStateValue
        let attemptDelivery = self.attemptDelivery
        let describeFailure = self.describeFailure
        let dedupKey = self.dedupKey

        while true {
            let due: [Row]
            do {
                due = try await fetchDue(app.db, batchSize)
            } catch {
                app.logger.error("\(logContext) dispatcher failed to query outbox: \(error)")
                return
            }

            guard !due.isEmpty else { return }

            let state = DrainState(remaining: due)

            // Tracks whether this batch changed anything - a `.skip` leaves its row completely
            // untouched (still due, same nextAttemptAt), so a batch that's ENTIRELY skips would
            // be fetched again byte-for-byte identical on the next `while true` iteration below.
            // Without this guard, a node whose outbox is dominated by rows it can't act on right
            // now (e.g. every EC dispatcher sees every shard-repair row cluster-wide, but only
            // ever delivers the ones targeting itself) would spin in a tight, sleepless
            // re-SELECT loop for as long as that domination holds - a real, synchronous CPU/DB
            // busy-loop, not just wasted work.
            let madeProgress = Progress()

            await withTaskGroup(of: (key: String?, skipped: Bool).self) { group in
                var inFlight = 0
                while inFlight < maxConcurrentDeliveries, let row = state.popNextEligible(dedupKey: dedupKey) {
                    let key = dedupKey?(row)
                    group.addTask {
                        let skipped = await Self.deliver(
                            row, app: app, maxAttempts: maxAttempts, logContext: logContext,
                            failedStateValue: failedStateValue, attemptDelivery: attemptDelivery,
                            describeFailure: describeFailure)
                        return (key, skipped)
                    }
                    inFlight += 1
                }
                while let finished = await group.next() {
                    if !finished.skipped { await madeProgress.markTrue() }
                    if let finishedKey = finished.key { state.inFlightKeys.remove(finishedKey) }
                    guard let row = state.popNextEligible(dedupKey: dedupKey) else { continue }
                    let key = dedupKey?(row)
                    group.addTask {
                        let skipped = await Self.deliver(
                            row, app: app, maxAttempts: maxAttempts, logContext: logContext,
                            failedStateValue: failedStateValue, attemptDelivery: attemptDelivery,
                            describeFailure: describeFailure)
                        return (key, skipped)
                    }
                }
            }

            if due.count < batchSize && state.remaining.isEmpty {
                return
            }
            guard await madeProgress.value else { return }
        }
    }

    /// A single `Bool` flipped from multiple `withTaskGroup` child tasks - `@unchecked Sendable`
    /// would race here (unlike `DrainState`, this IS mutated from inside `group.addTask`), so a
    /// tiny actor is used instead of a plain var.
    private actor Progress {
        private(set) var value = false
        func markTrue() { value = true }
    }

    /// Delivers one row, returning whether the outcome was `.skip` (untouched - the caller uses
    /// this to detect an all-skip batch, which must not be re-fetched with no delay).
    @discardableResult
    private static func deliver(
        _ row: Row, app: Application, maxAttempts: Int, logContext: String,
        failedStateValue: String,
        attemptDelivery: @Sendable (Row, Application) async -> OutboxDeliveryOutcome,
        describeFailure: @Sendable (Row) -> String
    ) async -> Bool {
        switch await attemptDelivery(row, app) {
        case .skip:
            return true
        case .success:
            do {
                try await row.delete(on: app.db)
            } catch {
                app.logger.error("\(logContext) dispatcher failed to delete outbox row: \(error)")
            }
        case .failure(let error):
            row.attempts += 1
            row.lastError = "\(error)"
            if row.attempts >= maxAttempts {
                row.state = failedStateValue
                app.logger.warning(
                    "\(logContext) failed permanently after \(row.attempts) attempts: \(describeFailure(row))"
                )
            } else {
                let backoff = min(30.0 * pow(2.0, Double(row.attempts)), 3600.0)
                row.nextAttemptAt = Date().addingTimeInterval(backoff)
            }
            do {
                try await row.save(on: app.db)
            } catch {
                app.logger.error("\(logContext) dispatcher failed to update outbox row: \(error)")
            }
        }
        return false
    }

    func purgeExpiredFailures(on db: any Database) async throws {
        try await purgeExpired(db)
    }
}
