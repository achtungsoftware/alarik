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
import Testing

@testable import Alarik

/// `PlacementService` is a pure function of `(bucketName, key, activeNodes)` - no DB, no actor,
/// no cluster required, so this suite runs everywhere `swift test` runs, unlike the Postgres/
/// multi-node cluster tests.
@Suite("PlacementService tests")
struct PlacementServiceTests {
    private func node(_ label: String, status: ClusterNode.Status = .active, heartbeatAge: TimeInterval = 0)
        -> ClusterNodeInfo
    {
        // Deterministic per-label UUID (not random) so HRW ranking is reproducible across runs.
        let digest = Insecure.MD5.hash(data: Data(label.utf8))
        let bytes = Array(digest)
        let uuid = UUID(uuid: (
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
        ))
        return ClusterNodeInfo(
            id: uuid, address: "http://\(label):8080", status: status,
            lastHeartbeatAt: Date().addingTimeInterval(-heartbeatAge))
    }

    @Test("responsibleNodes returns empty for an empty cluster")
    func emptyClusterReturnsEmpty() {
        let result = PlacementService.responsibleNodes(
            bucketName: "my-bucket", key: "my-key", activeNodes: [])
        #expect(result.isEmpty)
    }

    @Test("responsibleNodes never returns more nodes than are active")
    func neverExceedsActiveNodeCount() {
        let single = [node("a")]
        let result = PlacementService.responsibleNodes(
            bucketName: "my-bucket", key: "my-key", activeNodes: single)
        #expect(result.count == 1)
        #expect(result == single)
    }

    @Test("responsibleNodes returns exactly replicationFactor nodes when enough are active")
    func returnsReplicationFactorNodes() {
        let nodes = (0..<10).map { node("node-\($0)") }
        let result = PlacementService.responsibleNodes(
            bucketName: "my-bucket", key: "my-key", activeNodes: nodes)
        #expect(result.count == PlacementService.replicationFactor)
        // No duplicates.
        #expect(Set(result.map(\.id)).count == result.count)
    }

    @Test("responsibleNodes is deterministic for the same inputs")
    func isDeterministic() {
        let nodes = (0..<8).map { node("node-\($0)") }
        let first = PlacementService.responsibleNodes(
            bucketName: "bucket-a", key: "key-1", activeNodes: nodes)
        let second = PlacementService.responsibleNodes(
            bucketName: "bucket-a", key: "key-1", activeNodes: nodes)
        #expect(first.map(\.id) == second.map(\.id))
    }

    @Test("responsibleNodes assigns different keys to different node sets (not always the same nodes)")
    func distributesAcrossNodes() {
        let nodes = (0..<10).map { node("node-\($0)") }
        var chosenAtLeastOnce: Set<UUID> = []
        for i in 0..<200 {
            let result = PlacementService.responsibleNodes(
                bucketName: "bucket", key: "key-\(i)", activeNodes: nodes)
            chosenAtLeastOnce.formUnion(result.map(\.id))
        }
        // Over 200 distinct keys and 10 nodes, every node should show up as responsible for at
        // least something - a real (if weak) distribution check, not just "it returns something".
        #expect(chosenAtLeastOnce.count == nodes.count)
    }

    @Test("adding one node only reassigns a minority of keys (HRW minimal-disruption property)")
    func addingANodeReassignsFewKeys() {
        let before = (0..<9).map { node("node-\($0)") }
        let after = before + [node("node-9")]

        var changed = 0
        let totalKeys = 500
        for i in 0..<totalKeys {
            let key = "key-\(i)"
            let beforeSet = Set(
                PlacementService.responsibleNodes(bucketName: "b", key: key, activeNodes: before)
                    .map(\.id))
            let afterSet = Set(
                PlacementService.responsibleNodes(bucketName: "b", key: key, activeNodes: after)
                    .map(\.id))
            if beforeSet != afterSet { changed += 1 }
        }

        // Naive `hash % nodeCount` would reshuffle the large majority of keys on any membership
        // change. HRW should only touch roughly replicationFactor/newNodeCount of them - assert
        // a generous upper bound (well under half) to catch a regression to naive hashing
        // without being a flaky exact-percentage assertion.
        #expect(changed < totalKeys / 2)
    }

    @Test("quorumThreshold is majority of replicationFactor, capped at replicaCount")
    func quorumThresholdMajority() {
        #expect(PlacementService.quorumThreshold(replicaCount: 0) == 0)
        #expect(PlacementService.quorumThreshold(replicaCount: 1) == 1)
        #expect(PlacementService.quorumThreshold(replicaCount: 2) == 2)
        // replicationFactor is 3 -> majority is 2, capped at whatever's actually available.
        #expect(PlacementService.quorumThreshold(replicaCount: 3) == 2)
        #expect(PlacementService.quorumThreshold(replicaCount: 10) == 2)
    }

    @Test("responsibleNodes(count:) never returns more nodes than are active or requested")
    func countOverloadRespectsBounds() {
        let nodes = (0..<10).map { node("node-\($0)") }
        #expect(
            PlacementService.responsibleNodes(
                bucketName: "b", key: "k", activeNodes: nodes, count: 6
            ).count == 6)
        // Fewer active nodes than requested - never invents nodes.
        let three = Array(nodes.prefix(3))
        #expect(
            PlacementService.responsibleNodes(
                bucketName: "b", key: "k", activeNodes: three, count: 6
            ).count == 3)
    }

    @Test("top-3 is always a prefix of top-(k+m) - same ranked list, different truncation")
    func top3IsPrefixOfTopKPlusM() {
        let nodes = (0..<12).map { node("node-\($0)") }
        for i in 0..<50 {
            let key = "key-\(i)"
            let top3 = PlacementService.responsibleNodes(
                bucketName: "b", key: key, activeNodes: nodes
            ).map(\.id)
            let topKM = PlacementService.responsibleNodes(
                bucketName: "b", key: key, activeNodes: nodes, count: 8
            ).map(\.id)
            #expect(Array(topKM.prefix(3)) == top3)
        }
    }

    @Test("ecQuorumThreshold requires all k data shards, plus one parity shard of slack when m >= 2")
    func ecQuorumThresholdMatchesDesign() {
        #expect(PlacementService.ecQuorumThreshold(dataShards: 0, parityShards: 2) == 0)
        #expect(PlacementService.ecQuorumThreshold(dataShards: 4, parityShards: 1) == 4)
        #expect(PlacementService.ecQuorumThreshold(dataShards: 4, parityShards: 2) == 5)
        // Capped at k+m even in degenerate cases.
        #expect(PlacementService.ecQuorumThreshold(dataShards: 4, parityShards: 2) <= 6)
    }

    @Test("ensureErasureCodingAdmission throws when the cluster has fewer than k+m active nodes")
    func admissionThrowsWhenClusterTooSmall() {
        #expect(throws: PlacementServiceError.self) {
            try PlacementService.ensureErasureCodingAdmission(
                activeNodeCount: 5, dataShards: 4, parityShards: 2)
        }
    }

    @Test("ensureErasureCodingAdmission succeeds when the cluster has at least k+m active nodes")
    func admissionSucceedsWhenClusterLargeEnough() throws {
        try PlacementService.ensureErasureCodingAdmission(
            activeNodeCount: 6, dataShards: 4, parityShards: 2)
        try PlacementService.ensureErasureCodingAdmission(
            activeNodeCount: 10, dataShards: 4, parityShards: 2)
    }
}
