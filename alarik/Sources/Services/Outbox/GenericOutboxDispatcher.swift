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

            await withTaskGroup(of: String?.self) { group in
                var inFlight = 0
                while inFlight < maxConcurrentDeliveries, let row = state.popNextEligible(dedupKey: dedupKey) {
                    let key = dedupKey?(row)
                    group.addTask {
                        await Self.deliver(
                            row, app: app, maxAttempts: maxAttempts, logContext: logContext,
                            failedStateValue: failedStateValue, attemptDelivery: attemptDelivery,
                            describeFailure: describeFailure)
                        return key
                    }
                    inFlight += 1
                }
                while let finishedKey = await group.next() {
                    if let finishedKey { state.inFlightKeys.remove(finishedKey) }
                    guard let row = state.popNextEligible(dedupKey: dedupKey) else { continue }
                    let key = dedupKey?(row)
                    group.addTask {
                        await Self.deliver(
                            row, app: app, maxAttempts: maxAttempts, logContext: logContext,
                            failedStateValue: failedStateValue, attemptDelivery: attemptDelivery,
                            describeFailure: describeFailure)
                        return key
                    }
                }
            }

            if due.count < batchSize && state.remaining.isEmpty {
                return
            }
        }
    }

    private static func deliver(
        _ row: Row, app: Application, maxAttempts: Int, logContext: String,
        failedStateValue: String,
        attemptDelivery: @Sendable (Row, Application) async -> OutboxDeliveryOutcome,
        describeFailure: @Sendable (Row) -> String
    ) async {
        switch await attemptDelivery(row, app) {
        case .skip:
            return
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
    }

    func purgeExpiredFailures(on db: any Database) async throws {
        try await purgeExpired(db)
    }
}
