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

/// One pending (or dead-lettered) internal-cluster object push/delete - the persistent outbox
/// row that makes cluster replication survive restarts and peer-node outages. Never holds the
/// object's bytes, only enough to re-read the exact payload from `ObjectFileHandler` at delivery
/// time. Backed by `OutboxMailbox`, not Fluent. `ownerNodeId` is a REAL stored field, not a
/// `targetNodeId` alias: delivery pushes the object FROM the node holding the local copy TO
/// `targetNodeId`, so the mailbox owner is always the *enqueuing* node, never the target.
final class ClusterReplicationTask: @unchecked Sendable, Codable {
    enum State: String {
        case pending
        /// Retries exhausted - kept for a few days so failures are inspectable, then purged by
        /// the hourly cleanup task, same as `ReplicationTask`.
        case failed
    }

    enum Operation: String {
        case put
        case delete
    }

    /// Why this task exists - lets the console distinguish "catching up a replica that missed a
    /// synchronous write" from "moving data because membership changed" from "removing a copy
    /// this node is no longer responsible for" without a separate job-tracking table (see
    /// `ClusterRebalanceService`).
    enum Reason: String, CaseIterable {
        case write
        case rebalance
        case reclaim
    }

    let id: UUID
    var bucketName: String
    var key: String
    var versionId: String?
    var operation: String
    var targetNodeId: UUID
    var reason: String
    var attempts: Int
    var nextAttemptAt: Date
    var state: String
    var lastError: String?
    let createdAt: Date

    /// The node whose local mailbox directory this task's file lives in - the node that
    /// enqueued it (and holds the local copy to push), not `targetNodeId`. See the type's own
    /// doc comment for why.
    var ownerNodeId: UUID

    init(
        bucketName: String,
        key: String,
        versionId: String?,
        operation: Operation,
        targetNodeId: UUID,
        reason: Reason,
        ownerNodeId: UUID
    ) {
        self.id = UUID()
        self.bucketName = bucketName
        self.key = key
        self.versionId = versionId
        self.operation = operation.rawValue
        self.targetNodeId = targetNodeId
        self.reason = reason.rawValue
        self.attempts = 0
        self.nextAttemptAt = Date()
        self.state = State.pending.rawValue
        self.createdAt = Date()
        self.ownerNodeId = ownerNodeId
    }
}

extension ClusterReplicationTask: OutboxMailboxRow {}
