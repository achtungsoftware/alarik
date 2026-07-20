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

/// One pending (or dead-lettered) replication task - the persistent outbox row that makes
/// replication survive restarts and remote-endpoint outages. The target's connection details
/// are snapshotted at enqueue time (not just referenced by `targetId`), so editing or deleting
/// a target never changes what an in-flight task does. Never holds the object's bytes - re-reads
/// the immutable versioned payload from `ObjectFileHandler` at delivery time. Backed by
/// `OutboxMailbox`, not Fluent - `ownerNodeId` is a real stored field (see `NotificationDelivery`).
final class ReplicationTask: @unchecked Sendable, Codable {
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

    let id: UUID
    var bucketName: String

    /// Kept for display/traceability in the delivery-health UI only - never re-resolved. The
    /// fields actually used to perform the task (`endpoint`...`region` below) are snapshotted
    /// independently.
    var ruleId: UUID
    var targetId: UUID
    var endpoint: String
    var targetBucket: String
    var accessKeyId: String
    var secretAccessKey: String
    var region: String
    var key: String
    var versionId: String?
    var operation: String
    var attempts: Int
    var nextAttemptAt: Date
    var state: String

    /// The reason the most recent attempt failed - an HTTP status or a transport error
    /// description. Nil until the first failure; not cleared on success since a successful row
    /// is deleted outright, never left around with a stale error.
    var lastError: String?

    let createdAt: Date

    /// The node whose local mailbox directory this task's file lives in - see `OutboxMailboxRow`.
    var ownerNodeId: UUID

    init(
        bucketName: String,
        ruleId: UUID,
        target: ReplicationTarget,
        key: String,
        versionId: String?,
        operation: Operation,
        ownerNodeId: UUID
    ) {
        self.id = UUID()
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
        self.ownerNodeId = ownerNodeId
    }
}

extension ReplicationTask: OutboxMailboxRow {}
