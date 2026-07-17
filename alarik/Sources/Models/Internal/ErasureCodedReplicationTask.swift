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

/// One pending (or dead-lettered) shard push/delete/reconstruction - the erasure-coding sibling
/// of `ClusterReplicationTask`, targeting one `(bucketName, key, versionId, shardIndex)` shard
/// instead of a whole object. Never holds shard bytes, only enough to re-derive them at delivery
/// time: `.write`/`.rebalance`/`.reclaim` re-read this node's own local shard file; `.reconstruct`
/// re-derives the shard from `k` healthy survivors via `ReedSolomonEngine`.
final class ErasureCodedReplicationTask: Model, @unchecked Sendable {
    static let schema = "erasure_coded_replication_tasks"

    enum State: String {
        case pending
        case failed
    }

    enum Operation: String {
        case put
        case delete
    }

    /// Why this task exists - `.reconstruct` is the one case `ClusterReplicationTask.Reason`
    /// never needs: rebuilding a permanently-lost shard from survivors, rather than copying an
    /// existing file to a new home.
    enum Reason: String, CaseIterable {
        case write
        case rebalance
        case reclaim
        case reconstruct
    }

    @ID(key: .id)
    var id: UUID?

    @Field(key: "bucket_name")
    var bucketName: String

    @Field(key: "key")
    var key: String

    @Field(key: "version_id")
    var versionId: String?

    @Field(key: "shard_index")
    var shardIndex: Int

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
        shardIndex: Int,
        operation: Operation,
        targetNodeId: UUID,
        reason: Reason
    ) {
        self.bucketName = bucketName
        self.key = key
        self.versionId = versionId
        self.shardIndex = shardIndex
        self.operation = operation.rawValue
        self.targetNodeId = targetNodeId
        self.reason = reason.rawValue
        self.attempts = 0
        self.nextAttemptAt = Date()
        self.state = State.pending.rawValue
        self.createdAt = Date()
    }
}

extension ErasureCodedReplicationTask: OutboxRow {}
