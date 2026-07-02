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

/// Drains the `notification_deliveries` outbox: POSTs each due row to its webhook URL and
/// applies retry bookkeeping on failure. At-least-once delivery: a row is only removed after
/// a 2xx response, so restarts and receiver outages never lose events (the flip side -
/// receivers may occasionally see a duplicate - is standard webhook semantics).
///
/// Woken explicitly by `NotificationService.emit` for near-instant delivery of fresh events,
/// plus a 2-second background tick (configure.swift) that picks up retries whose backoff has
/// elapsed. Re-entrant wakes while a drain is running are coalesced into one follow-up pass.
final actor NotificationDispatcher {
    static let shared = NotificationDispatcher()

    static let maxAttempts = 8
    static let batchSize = 50
    static let maxConcurrentDeliveries = 8

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
            let due: [NotificationDelivery]
            do {
                due = try await NotificationDelivery.query(on: app.db)
                    .filter(\.$state == NotificationDelivery.State.pending.rawValue)
                    .filter(\.$nextAttemptAt <= Date())
                    .sort(\.$nextAttemptAt, .ascending)
                    .limit(Self.batchSize)
                    .all()
            } catch {
                app.logger.error("Webhook dispatcher failed to query outbox: \(error)")
                return
            }

            guard !due.isEmpty else { return }

            // Bounded parallelism: at most maxConcurrentDeliveries requests in flight - a
            // slow receiver delays its own queue, not the whole outbox
            await withTaskGroup(of: Void.self) { group in
                var iterator = due.makeIterator()
                var inFlight = 0
                while inFlight < Self.maxConcurrentDeliveries, let row = iterator.next() {
                    group.addTask { await Self.deliver(row, app: app) }
                    inFlight += 1
                }
                while await group.next() != nil {
                    if let row = iterator.next() {
                        group.addTask { await Self.deliver(row, app: app) }
                    }
                }
            }

            if due.count < Self.batchSize {
                return
            }
        }
    }

    /// POSTs one outbox row. 2xx deletes the row; anything else (including transport errors)
    /// schedules a retry with exponential backoff, dead-lettering after `maxAttempts`.
    private static func deliver(_ row: NotificationDelivery, app: Application) async {
        var succeeded = false
        var failureReason: String?
        do {
            let response = try await app.client.post(URI(string: row.url)) { clientReq in
                clientReq.headers.contentType = .json
                clientReq.headers.replaceOrAdd(name: .userAgent, value: "Alarik-Webhook")
                if let secret = row.secret, !secret.isEmpty {
                    clientReq.headers.replaceOrAdd(
                        name: "X-Alarik-Signature-256",
                        value: NotificationService.signature(payload: row.payload, secret: secret)
                    )
                }
                clientReq.body = ByteBuffer(string: row.payload)
            }.get()
            succeeded = (200..<300).contains(response.status.code)
            if !succeeded {
                failureReason = "HTTP \(response.status.code)"
            }
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
                    row.state = NotificationDelivery.State.failed.rawValue
                    app.logger.warning(
                        "Webhook delivery to \(row.url) failed permanently after \(row.attempts) attempts (bucket: \(row.bucketName))"
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
            // tick - worst case is a duplicate delivery, never a lost one
            app.logger.error("Webhook dispatcher failed to update outbox row: \(error)")
        }
    }

    /// Purges dead-lettered rows older than 7 days - called from the hourly cleanup task.
    static func purgeExpiredFailures(on db: any Database) async throws {
        try await NotificationDelivery.query(on: db)
            .filter(\.$state == NotificationDelivery.State.failed.rawValue)
            .filter(\.$createdAt < Date().addingTimeInterval(-7 * 24 * 3600))
            .delete()
    }
}
