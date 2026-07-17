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

/// One pending (or dead-lettered) replication task - the persistent outbox row that makes
/// replication survive restarts and remote-endpoint outages. The target's connection details
/// are snapshotted at enqueue time (not just referenced by `targetId`), so editing or deleting
/// a target never changes what an in-flight task does - same reasoning as
/// `NotificationDelivery` snapshotting `url`/`secret` rather than a rule reference.
///
/// Unlike a webhook delivery, this row never holds the object's bytes - `bucketName` + `key` +
/// `versionId` is enough to re-read the exact payload from `ObjectFileHandler` at delivery
/// time, since that version's bytes are immutable once written (replication requires
/// versioning to be enabled - see `InternalBucketController`).
final class ReplicationTask: Model, @unchecked Sendable {
    static let schema = "replication_tasks"

    enum State: String {
        case pending
        /// Retries exhausted - kept for a few days so failures are inspectable, then purged
        /// by the hourly cleanup task.
        case failed
    }

    enum Operation: String {
        case put
        case delete
    }

    @ID(key: .id)
    var id: UUID?

    @Field(key: "bucket_name")
    var bucketName: String

    /// Kept for display/traceability in the delivery-health UI only - never re-resolved. The
    /// fields actually used to perform the task (`endpoint`...`region` below) are snapshotted
    /// independently.
    @Field(key: "rule_id")
    var ruleId: UUID

    @Field(key: "target_id")
    var targetId: UUID

    @Field(key: "endpoint")
    var endpoint: String

    @Field(key: "target_bucket")
    var targetBucket: String

    @Field(key: "access_key_id")
    var accessKeyId: String

    @Field(key: "secret_access_key")
    var secretAccessKey: String

    @Field(key: "region")
    var region: String

    @Field(key: "key")
    var key: String

    @Field(key: "version_id")
    var versionId: String?

    @Field(key: "operation")
    var operation: String

    @Field(key: "attempts")
    var attempts: Int

    @Field(key: "next_attempt_at")
    var nextAttemptAt: Date

    @Field(key: "state")
    var state: String

    /// The reason the most recent attempt failed - an HTTP status or a transport error
    /// description. Nil until the first failure; not cleared on success since a successful row
    /// is deleted outright, never left around with a stale error.
    @Field(key: "last_error")
    var lastError: String?

    @Field(key: "created_at")
    var createdAt: Date

    init() {}

    init(
        bucketName: String,
        ruleId: UUID,
        target: ReplicationTarget,
        key: String,
        versionId: String?,
        operation: Operation
    ) {
        self.bucketName = bucketName
        self.ruleId = ruleId
        self.targetId = target.id
        self.endpoint = target.endpoint
        self.targetBucket = target.targetBucket
        self.accessKeyId = target.accessKeyId
        self.secretAccessKey = target.secretAccessKey
        self.region = target.region
        self.key = key
        self.versionId = versionId
        self.operation = operation.rawValue
        self.attempts = 0
        self.nextAttemptAt = Date()
        self.state = State.pending.rawValue
        self.createdAt = Date()
    }
}

extension ReplicationTask: OutboxRow {}
