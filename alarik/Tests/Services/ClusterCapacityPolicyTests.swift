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

/// `ClusterCapacityPolicy` is a pure function of `(selfNode, peers)` plus an optional threshold
/// override - no DB, no actor, no cluster required, mirroring `PlacementServiceTests`.
@Suite("ClusterCapacityPolicy tests", .serialized)
struct ClusterCapacityPolicyTests {
    private func node(
        _ label: String, totalBytes: Int64? = 1000, availableBytes: Int64? = 500
    ) -> ClusterNodeInfo {
        // Deterministic per-label UUID (not random) so tie-break assertions are reproducible.
        let digest = Insecure.MD5.hash(data: Data(label.utf8))
        let bytes = Array(digest)
        let uuid = UUID(
            uuid: (
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15]
            ))
        return ClusterNodeInfo(
            id: uuid, address: "http://\(label):8080", status: .active, lastHeartbeatAt: Date(),
            totalBytes: totalBytes, availableBytes: availableBytes)
    }

    @Test("isNearFull is false when capacity is unknown (nil) - fail open")
    func isNearFullFailsOpenOnUnknownCapacity() {
        #expect(!ClusterCapacityPolicy.isNearFull(totalBytes: nil, availableBytes: nil))
        #expect(!ClusterCapacityPolicy.isNearFull(totalBytes: 1000, availableBytes: nil))
        #expect(!ClusterCapacityPolicy.isNearFull(totalBytes: nil, availableBytes: 500))
        #expect(!ClusterCapacityPolicy.isNearFull(totalBytes: 0, availableBytes: 0))
    }

    @Test("isNearFull is true below the threshold, false at or above it")
    func isNearFullBoundary() {
        // 5% free, threshold 10% - near-full.
        #expect(
            ClusterCapacityPolicy.isNearFull(
                totalBytes: 1000, availableBytes: 50, thresholdPercent: 10))
        // Exactly at the threshold - not near-full (strictly less-than).
        #expect(
            !ClusterCapacityPolicy.isNearFull(
                totalBytes: 1000, availableBytes: 100, thresholdPercent: 10))
        // Comfortably above the threshold.
        #expect(
            !ClusterCapacityPolicy.isNearFull(
                totalBytes: 1000, availableBytes: 900, thresholdPercent: 10))
    }

    @Test("preferredCoordinator returns nil when self isn't near-full")
    func noRedirectWhenSelfHasRoom() {
        let selfNode = node("a", totalBytes: 1000, availableBytes: 900)
        let peer = node("b", totalBytes: 1000, availableBytes: 900)
        #expect(
            ClusterCapacityPolicy.preferredCoordinator(
                selfNode: selfNode, peers: [peer], thresholdPercent: 10) == nil)
    }

    @Test("preferredCoordinator returns nil when there are no peers to redirect to")
    func noRedirectWhenNoPeers() {
        let selfNode = node("a", totalBytes: 1000, availableBytes: 10)
        #expect(
            ClusterCapacityPolicy.preferredCoordinator(
                selfNode: selfNode, peers: [], thresholdPercent: 10) == nil)
    }

    @Test("preferredCoordinator returns nil when every peer is also near-full - never hard-refuses")
    func noRedirectWhenEveryPeerIsAlsoNearFull() {
        let selfNode = node("a", totalBytes: 1000, availableBytes: 10)
        let peer = node("b", totalBytes: 1000, availableBytes: 20)
        #expect(
            ClusterCapacityPolicy.preferredCoordinator(
                selfNode: selfNode, peers: [peer], thresholdPercent: 10) == nil)
    }

    @Test("preferredCoordinator picks the peer with the most free space, ignoring near-full peers")
    func picksRoomiestNonFullPeer() {
        let selfNode = node("a", totalBytes: 1000, availableBytes: 10)
        let nearFullPeer = node("b", totalBytes: 1000, availableBytes: 50)
        let roomyPeer = node("c", totalBytes: 1000, availableBytes: 800)
        let mediumPeer = node("d", totalBytes: 1000, availableBytes: 300)

        let result = ClusterCapacityPolicy.preferredCoordinator(
            selfNode: selfNode, peers: [nearFullPeer, mediumPeer, roomyPeer], thresholdPercent: 10)

        #expect(result?.id == roomyPeer.id)
    }

    @Test("preferredCoordinator tie-breaks deterministically by node id")
    func tieBreaksDeterministically() {
        let selfNode = node("a", totalBytes: 1000, availableBytes: 10)
        let peer1 = node("b", totalBytes: 1000, availableBytes: 800)
        let peer2 = node("c", totalBytes: 1000, availableBytes: 800)

        let first = ClusterCapacityPolicy.preferredCoordinator(
            selfNode: selfNode, peers: [peer1, peer2], thresholdPercent: 10)
        let second = ClusterCapacityPolicy.preferredCoordinator(
            selfNode: selfNode, peers: [peer2, peer1], thresholdPercent: 10)

        #expect(first?.id == second?.id)
    }

    @Test("minFreePercent resolves to 10.0 when CLUSTER_MIN_FREE_PERCENT is unset")
    func minFreePercentDefaultsWhenUnset() {
        let original = ProcessInfo.processInfo.environment["CLUSTER_MIN_FREE_PERCENT"]
        unsetenv("CLUSTER_MIN_FREE_PERCENT")
        defer {
            if let original {
                setenv("CLUSTER_MIN_FREE_PERCENT", original, 1)
            }
        }

        #expect(ClusterCapacityPolicy.minFreePercent() == 10.0)
    }

    @Test("minFreePercent resolves to the parsed value when CLUSTER_MIN_FREE_PERCENT is set")
    func minFreePercentResolvesConfiguredValue() {
        let original = ProcessInfo.processInfo.environment["CLUSTER_MIN_FREE_PERCENT"]
        setenv("CLUSTER_MIN_FREE_PERCENT", "25", 1)
        defer {
            if let original {
                setenv("CLUSTER_MIN_FREE_PERCENT", original, 1)
            } else {
                unsetenv("CLUSTER_MIN_FREE_PERCENT")
            }
        }

        #expect(ClusterCapacityPolicy.minFreePercent() == 25.0)
    }
}
