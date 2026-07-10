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

import Vapor

/// Soft, capacity-aware preference for which node coordinates a *new* write - deliberately never
/// consulted by `PlacementService.responsibleNodes`, `ClusterNodeCache.activeNodes`, or
/// `ClusterRebalanceService.rebalanceBucket`'s reclaim logic: capacity data flows in parallel to
/// "who is responsible for key K", never through it, so a near-full node never has its existing
/// data reclaimed and read-routing never changes. This is purely about nudging *new-write*
/// coordination away from a near-full node, among the exact same nodes HRW already picked as
/// responsible for that key - never a node outside that set.
enum ClusterCapacityPolicy {
    /// Percentage of a node's total disk that must remain free before new-write coordination
    /// stops preferring it away. Default 10% - overridable via `CLUSTER_MIN_FREE_PERCENT`. Read
    /// fresh each call (same pattern as `AlarikRegion.resolve()`) - it never changes at runtime,
    /// and a malformed value has zero safety implications (unlike `CLUSTER_NODE_ADDRESS`
    /// /`CLUSTER_SECRET`), so silently falling back to the default is correct here.
    static func minFreePercent() -> Double {
        Environment.sanitizedGet("CLUSTER_MIN_FREE_PERCENT").flatMap(Double.init) ?? 10.0
    }

    static func freePercent(totalBytes: Int64?, availableBytes: Int64?) -> Double? {
        guard let totalBytes, let availableBytes, totalBytes > 0 else { return nil }
        return (Double(availableBytes) / Double(totalBytes)) * 100
    }

    /// `nil` capacity (not yet heartbeated, or a lookup failure) always reads as "not near-full" -
    /// fail open, since treating unknown capacity as "full" would needlessly redirect writes away
    /// from a node that's actually fine.
    static func isNearFull(
        totalBytes: Int64?, availableBytes: Int64?, thresholdPercent: Double = minFreePercent()
    ) -> Bool {
        guard let pct = freePercent(totalBytes: totalBytes, availableBytes: availableBytes) else {
            return false
        }
        return pct < thresholdPercent
    }

    static func isNearFull(_ node: ClusterNodeInfo, thresholdPercent: Double = minFreePercent())
        -> Bool
    {
        isNearFull(
            totalBytes: node.totalBytes, availableBytes: node.availableBytes,
            thresholdPercent: thresholdPercent)
    }

    /// The one pure, testable decision function behind the write-coordination redirect. `peers`
    /// must already be the exact true-responsible-minus-self set the caller computed (never
    /// re-derived, never widened here) - this can only ever pick one of the *same* nodes HRW
    /// already selected for the key. Returns `nil` ("coordinate locally, no redirect") when self
    /// isn't near-full, `peers` is empty, or every peer is also near-full - a write is never
    /// hard-refused for capacity reasons.
    static func preferredCoordinator(
        selfNode: ClusterNodeInfo, peers: [ClusterNodeInfo],
        thresholdPercent: Double = minFreePercent()
    ) -> ClusterNodeInfo? {
        guard isNearFull(selfNode, thresholdPercent: thresholdPercent) else { return nil }
        let candidates = peers.filter { !isNearFull($0, thresholdPercent: thresholdPercent) }
        guard !candidates.isEmpty else { return nil }
        return candidates.max { lhs, rhs in
            let l = freePercent(totalBytes: lhs.totalBytes, availableBytes: lhs.availableBytes) ?? -1
            let r = freePercent(totalBytes: rhs.totalBytes, availableBytes: rhs.availableBytes) ?? -1
            return l != r ? l < r : lhs.id.uuidString < rhs.id.uuidString
        }
    }
}
