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

/// Drains this node's own `notification-deliveries` mailbox: POSTs each due row to its webhook
/// URL. `fetchDue` only ever returns tasks this node itself owns (see `OutboxMailbox`) - no
/// target-gating needed, unlike the old shared-table design.
enum NotificationDispatcher {
    static let maxAttempts = 8

    static let shared = GenericOutboxDispatcher<NotificationDelivery>(
        maxAttempts: maxAttempts,
        maxConcurrentDeliveries: 8,
        logContext: "Webhook delivery",
        fetchDue: { app, limit in
            await OutboxMailbox.retryPendingEnqueues(
                NotificationDelivery.self, app: app, collection: OutboxCollections.notificationDeliveries)
            return OutboxMailbox.dueTasks(
                NotificationDelivery.self, app: app,
                collection: OutboxCollections.notificationDeliveries, limit: limit)
        },
        attemptDelivery: { row, app in
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
                guard (200..<300).contains(response.status.code) else {
                    return .failure(NotificationDispatcherError.httpStatus(response.status.code))
                }
                return .success
            } catch {
                return .failure(error)
            }
        },
        persist: { row, _ in try OutboxMailbox.update(row, collection: OutboxCollections.notificationDeliveries) },
        remove: { row, _ in OutboxMailbox.remove(row, collection: OutboxCollections.notificationDeliveries) },
        describeFailure: { row in "\(row.url) (bucket: \(row.bucketName))" },
        purgeExpired: { app in
            OutboxMailbox.purgeExpiredFailures(
                NotificationDelivery.self, app: app, collection: OutboxCollections.notificationDeliveries)
        }
    )

    static func purgeExpiredFailures(app: Application) async throws {
        try await shared.purgeExpiredFailures(app: app)
    }
}

private enum NotificationDispatcherError: Error, CustomStringConvertible {
    case httpStatus(UInt)

    var description: String {
        switch self {
        case .httpStatus(let code):
            "HTTP \(code)"
        }
    }
}
