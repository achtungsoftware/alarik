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

/// One pending (or dead-lettered) shard push/delete/reconstruction - the erasure-coding sibling
/// of `ClusterReplicationTask`, targeting one `(bucketName, key, versionId, shardIndex)` shard
/// instead of a whole object. Never holds shard bytes: `.write`/`.rebalance`/`.reclaim` re-read
/// this node's own local shard file; `.reconstruct` re-derives it from `k` healthy survivors via
/// `ReedSolomonEngine`. Backed by `OutboxMailbox`, not Fluent - `ownerNodeId` is a computed alias
/// of `targetNodeId`, needing no backup mirroring since the rebalance/scrub walk self-heals gaps.
final class ErasureCodedReplicationTask: @unchecked Sendable, Codable {

    enum Operation: String, Codable, Sendable {
        case put
        case delete
    }

    /// Why this task exists - `.reconstruct` is the one case `ClusterReplicationTask.Reason`
    /// never needs: rebuilding a permanently-lost shard from survivors, rather than copying an
    /// existing file to a new home.
    enum Reason: String, Codable, Sendable, CaseIterable {
        case write
        case rebalance
        case reclaim
        case reconstruct
    }

    let id: UUID
    var bucketName: String
    var key: String
    var versionId: String?
    var shardIndex: Int
    var operation: Operation
    var targetNodeId: UUID
    var reason: Reason
    var attempts: Int
    var nextAttemptAt: Date
    var state: OutboxRowState
    var lastError: String?
    let createdAt: Date

    init(
        bucketName: String,
        key: String,
        versionId: String?,
        shardIndex: Int,
        operation: Operation,
        targetNodeId: UUID,
        reason: Reason
    ) {
        self.id = UUID()
        self.bucketName = bucketName
        self.key = key
        self.versionId = versionId
        self.shardIndex = shardIndex
        self.operation = operation
        self.targetNodeId = targetNodeId
        self.reason = reason
        self.attempts = 0
        self.nextAttemptAt = Date()
        self.state = .pending
        self.createdAt = Date()
    }
}

extension ErasureCodedReplicationTask: OutboxMailboxRow {
    var ownerNodeId: UUID {
        get { targetNodeId }
        set { targetNodeId = newValue }
    }
}
