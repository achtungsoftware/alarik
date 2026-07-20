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

/// One pending (or dead-lettered) webhook delivery - the persistent outbox row that makes
/// notifications survive restarts and receiver outages. The target `url`/`secret` are
/// snapshotted from the rule at emit time, so editing or deleting a rule never breaks
/// deliveries already in flight. Backed by `OutboxMailbox`, not Fluent: no independent ground
/// truth rediscovers "this webhook still needs firing", so `ownerNodeId` is a real stored field,
/// and lost owner copies need `OutboxMailbox`'s backup-mirroring/promotion to survive an outage.
final class NotificationDelivery: @unchecked Sendable, Codable {
    enum State: String {
        case pending
        /// Retries exhausted - kept for a few days so failures are inspectable, then purged
        /// by the hourly cleanup task.
        case failed
    }

    let id: UUID
    var bucketName: String
    var ruleId: UUID
    var url: String
    var secret: String?

    /// The exact JSON body to POST (already serialized - the signature must cover the
    /// precise bytes sent, so the body is frozen at emit time).
    var payload: String

    var attempts: Int
    var nextAttemptAt: Date
    var state: String

    /// The reason the most recent delivery attempt failed - an HTTP status ("HTTP 503") or a
    /// transport error description. Nil until the first failure; not cleared on success since
    /// a successful row is deleted outright, never left around with a stale error.
    var lastError: String?

    let createdAt: Date

    /// The node whose local mailbox directory this task's file lives in - see `OutboxMailboxRow`.
    var ownerNodeId: UUID

    init(
        bucketName: String,
        ruleId: UUID,
        url: String,
        secret: String?,
        payload: String,
        ownerNodeId: UUID
    ) {
        self.id = UUID()
        self.bucketName = bucketName
        self.ruleId = ruleId
        self.url = url
        self.secret = secret
        self.payload = payload
        self.attempts = 0
        self.nextAttemptAt = Date()
        self.state = State.pending.rawValue
        self.createdAt = Date()
        self.ownerNodeId = ownerNodeId
    }
}

extension NotificationDelivery: OutboxMailboxRow {}
