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
import Vapor

import struct Foundation.Date
import struct Foundation.UUID

/// One pending (or dead-lettered) webhook delivery - the persistent outbox row that makes
/// notifications survive restarts and receiver outages. The target `url`/`secret` are
/// snapshotted from the rule at emit time, so editing or deleting a rule never breaks
/// deliveries already in flight.
final class NotificationDelivery: Model, @unchecked Sendable {
    static let schema = "notification_deliveries"

    enum State: String {
        case pending
        /// Retries exhausted - kept for a few days so failures are inspectable, then purged
        /// by the hourly cleanup task.
        case failed
    }

    @ID(key: .id)
    var id: UUID?

    @Field(key: "bucket_name")
    var bucketName: String

    @Field(key: "rule_id")
    var ruleId: UUID

    @Field(key: "url")
    var url: String

    @Field(key: "secret")
    var secret: String?

    /// The exact JSON body to POST (already serialized - the signature must cover the
    /// precise bytes sent, so the body is frozen at emit time).
    @Field(key: "payload")
    var payload: String

    @Field(key: "attempts")
    var attempts: Int

    @Field(key: "next_attempt_at")
    var nextAttemptAt: Date

    @Field(key: "state")
    var state: String

    @Field(key: "created_at")
    var createdAt: Date

    init() {}

    init(
        bucketName: String,
        ruleId: UUID,
        url: String,
        secret: String?,
        payload: String
    ) {
        self.bucketName = bucketName
        self.ruleId = ruleId
        self.url = url
        self.secret = secret
        self.payload = payload
        self.attempts = 0
        self.nextAttemptAt = Date()
        self.state = State.pending.rawValue
        self.createdAt = Date()
    }
}
