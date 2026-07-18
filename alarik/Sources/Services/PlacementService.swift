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

/// Deterministic, coordination-free object placement. Pure functions of `(bucketName, key,
/// activeNodes)` - no DB/actor access, so this is unit-testable without a running cluster, and
/// any node can compute the same answer independently without asking anyone else.
enum PlacementService {
    /// Cluster-wide replication factor - not a per-bucket setting.
    static let replicationFactor = 3

    /// Rendezvous (HRW - "highest random weight") hashing: for each candidate node, compute a
    /// pseudo-random weight from `(nodeId, bucketName, key)` and take the top `N` by weight.
    /// Deterministic (every node computes the identical answer from the same active-node list)
    /// and, critically for *automatic* rebalancing not being a stampede, minimally disruptive on
    /// membership change - adding or removing one node only reassigns the objects whose specific
    /// top-N set changes, unlike naive `hash % nodeCount` which reshuffles almost everything.
    ///
    /// Returns fewer than `replicationFactor` nodes when the cluster itself has fewer active
    /// nodes than that - never invents replicas that don't exist. Returns `[]` for an empty
    /// cluster (never called in practice: `ObjectRoutingService` short-circuits to `.local` when
    /// there are no peers at all).
    static func responsibleNodes(
        bucketName: String,
        key: String,
        activeNodes: [ClusterNodeInfo]
    ) -> [ClusterNodeInfo] {
        responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: activeNodes, count: replicationFactor)
    }

    /// Same ranking as the 3-arg overload, truncated to `count` instead of `replicationFactor` -
    /// used for erasure-coded placement across `k+m` nodes. Both overloads sort from the same
    /// `(bucketName, key, activeNodes)` input, so top-3 is always a prefix of top-`count`: rank-0
    /// is simultaneously "primary of the top-3" and "primary of the top-`count`" for every key.
    static func responsibleNodes(
        bucketName: String,
        key: String,
        activeNodes: [ClusterNodeInfo],
        count: Int
    ) -> [ClusterNodeInfo] {
        guard !activeNodes.isEmpty else { return [] }
        return Array(rankedNodes(bucketName: bucketName, key: key, activeNodes: activeNodes).prefix(min(count, activeNodes.count)))
    }

    private static func rankedNodes(
        bucketName: String, key: String, activeNodes: [ClusterNodeInfo]
    ) -> [ClusterNodeInfo] {
        activeNodes
            .map { node in (node: node, weight: weight(nodeId: node.id, bucketName: bucketName, key: key)) }
            .sorted { lhs, rhs in
                lhs.weight != rhs.weight ? lhs.weight > rhs.weight : lhs.node.id.uuidString > rhs.node.id.uuidString
            }
            .map(\.node)
    }

    /// Minimum number of acks (including the coordinating node's own local write) required
    /// before a write is told "success" - majority of the *intended* replica count
    /// (`replicationFactor`), capped at however many replicas actually exist. A cluster running
    /// with fewer than `replicationFactor` nodes can't be asked for more acks than it has
    /// replicas to give.
    static func quorumThreshold(replicaCount: Int) -> Int {
        guard replicaCount > 0 else { return 0 }
        let majority = (replicationFactor / 2) + 1
        return min(majority, replicaCount)
    }

    /// Minimum acked shards before an erasure-coded write is told "success": all `k` data
    /// shards, plus one parity shard of slack when `m >= 2` allows it (mirrors plain
    /// replication's quorum never acking early). Capped at `k+m` for degenerate inputs.
    static func ecQuorumThreshold(dataShards: Int, parityShards: Int) -> Int {
        guard dataShards > 0 else { return 0 }
        let threshold = parityShards >= 2 ? dataShards + 1 : dataShards
        return min(threshold, dataShards + parityShards)
    }

    /// A cluster that has shrunk below `k+m` active nodes has nowhere left to place every shard -
    /// throws rather than silently writing a degraded/incomplete stripe.
    static func ensureErasureCodingAdmission(
        activeNodeCount: Int, dataShards: Int, parityShards: Int
    ) throws {
        let required = dataShards + parityShards
        guard activeNodeCount >= required else {
            throw PlacementServiceError.insufficientNodesForErasureCoding(
                required: required, active: activeNodeCount)
        }
    }

    /// HRW weight: SHA256 of `nodeId|bucketName|key`, taken as the first 8 bytes interpreted as
    /// a big-endian `UInt64`. `Crypto` is already a dependency (webhook HMAC signing, content
    /// hashing) so this needs no new library, and SHA256's avalanche property gives a well
    /// distributed, stable-per-input weight without needing a dedicated hash function.
    private static func weight(nodeId: UUID, bucketName: String, key: String) -> UInt64 {
        let input = "\(nodeId.uuidString)|\(bucketName)|\(key)"
        let digest = SHA256.hash(data: Data(input.utf8))
        var value: UInt64 = 0
        for byte in digest.prefix(8) {
            value = (value << 8) | UInt64(byte)
        }
        return value
    }
}

enum PlacementServiceError: Error, CustomStringConvertible {
    case insufficientNodesForErasureCoding(required: Int, active: Int)

    var description: String {
        switch self {
        case .insufficientNodesForErasureCoding(let required, let active):
            "Erasure coding requires at least \(required) active cluster nodes, only \(active) are active"
        }
    }
}
