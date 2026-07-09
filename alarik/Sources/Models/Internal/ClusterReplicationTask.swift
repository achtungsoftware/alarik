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

/// One pending (or dead-lettered) internal-cluster object push/delete - the persistent outbox
/// row that makes cluster replication survive restarts and peer-node outages. Structurally
/// identical to `ReplicationTask`; never holds the object's bytes, only enough to re-read the
/// exact payload from `ObjectFileHandler` at delivery time (`bucketName`+`key`+`versionId`).
final class ClusterReplicationTask: Model, @unchecked Sendable {
    static let schema = "cluster_replication_tasks"

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
    enum Reason: String {
        case write
        case rebalance
        case reclaim
    }

    @ID(key: .id)
    var id: UUID?

    @Field(key: "bucket_name")
    var bucketName: String

    @Field(key: "key")
    var key: String

    @Field(key: "version_id")
    var versionId: String?

    @Field(key: "operation")
    var operation: String

    @Field(key: "target_node_id")
    var targetNodeId: UUID

    @Field(key: "reason")
    var reason: String

    @Field(key: "attempts")
    var attempts: Int

    @Field(key: "next_attempt_at")
    var nextAttemptAt: Date

    @Field(key: "state")
    var state: String

    @Field(key: "last_error")
    var lastError: String?

    @Field(key: "created_at")
    var createdAt: Date

    init() {}

    init(
        bucketName: String,
        key: String,
        versionId: String?,
        operation: Operation,
        targetNodeId: UUID,
        reason: Reason
    ) {
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
    }
}
