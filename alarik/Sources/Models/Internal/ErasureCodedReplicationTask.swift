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

import Crypto
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
    /// For a `.delete` of a non-versioned object: the ETag of the generation being removed. A
    /// delayed delivery must not remove a key that was re-created in the meantime, so the
    /// receiver compares before deleting. Nil for versioned deletes (a version id is unique) and
    /// for rows written before this field existed.
    var expectedETag: String?

    init(
        bucketName: String,
        key: String,
        versionId: String?,
        shardIndex: Int,
        operation: Operation,
        targetNodeId: UUID,
        reason: Reason,
        expectedETag: String? = nil
    ) {
        // The id IS the task's identity, so the mailbox can tell a repeat from a new task by
        // path alone (`OutboxMailbox.store`): read-repair and the rank-0 gap sweep re-request the
        // same (version, index, target) many times over, and used to list every node's mailbox
        // per key to find out whether one was already queued.
        self.id = Self.identity(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex,
            operation: operation, targetNodeId: targetNodeId)
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
        self.expectedETag = expectedETag
    }

    static func identity(
        bucketName: String, key: String, versionId: String?, shardIndex: Int, operation: Operation,
        targetNodeId: UUID
    ) -> UUID {
        let material = [bucketName, key, versionId ?? "", String(shardIndex), operation.rawValue, targetNodeId.uuidString]
            .joined(separator: "\u{0}")
        let digest = Array(SHA256.hash(data: Data(material.utf8)).prefix(16))
        return UUID(uuid: (
            digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
            digest[8], digest[9], digest[10], digest[11], digest[12], digest[13], digest[14], digest[15]))
    }
}

extension ErasureCodedReplicationTask: OutboxMailboxRow {
    var ownerNodeId: UUID {
        get { targetNodeId }
        set { targetNodeId = newValue }
    }
}
