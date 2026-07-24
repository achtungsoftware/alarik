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
import Testing

@testable import Alarik

/// Pure in-memory actor tests - no DB, no app, no cluster required. Instantiates its own
/// (non-`.shared`) `ClusterNodeCache` instance, same pattern the other cache invalidation tests
/// use.
@Suite("ClusterNodeCache tests")
struct ClusterNodeCacheTests {
    @Test("upsert then get returns the stored node")
    func upsertThenGet() async {
        let cache = ClusterNodeCache()
        let id = UUID()
        await cache.upsert(
            ClusterNodeInfo(id: id, address: "http://a:8080", status: .active, lastHeartbeatAt: Date()))

        let fetched = await cache.get(id: id)
        #expect(fetched?.address == "http://a:8080")
        #expect(fetched?.status == .active)
    }

    @Test("remove drops the node")
    func removeDropsNode() async {
        let cache = ClusterNodeCache()
        let id = UUID()
        await cache.upsert(
            ClusterNodeInfo(id: id, address: "http://a:8080", status: .active, lastHeartbeatAt: Date()))
        await cache.remove(id: id)

        #expect(await cache.get(id: id) == nil)
    }

    @Test("load replaces the whole set")
    func loadReplacesSet() async {
        let cache = ClusterNodeCache()
        await cache.upsert(
            ClusterNodeInfo(id: UUID(), address: "http://stale:8080", status: .active, lastHeartbeatAt: Date()))

        let freshId = UUID()
        await cache.load(initialData: [
            ClusterNodeInfo(id: freshId, address: "http://fresh:8080", status: .active, lastHeartbeatAt: Date())
        ])

        let all = await cache.all()
        #expect(all.count == 1)
        #expect(all.first?.id == freshId)
    }

    @Test("placementNodes keeps a node with a stale heartbeat - ownership must not follow liveness")
    func placementNodesIgnoresStaleHeartbeat() async {
        let cache = ClusterNodeCache()
        let live = UUID()
        let stale = UUID()
        await cache.upsert(
            ClusterNodeInfo(
                id: live, address: "http://live:8080", status: .active, lastHeartbeatAt: Date()))
        await cache.upsert(
            ClusterNodeInfo(
                id: stale, address: "http://stale:8080", status: .active,
                lastHeartbeatAt: Date().addingTimeInterval(-3600)))

        // A node being briefly unreachable must not hand its keys to someone else, or records
        // stop being where the current placement says they are.
        let placement = await cache.placementNodes().map(\.id)
        #expect(placement.count == 2)
        #expect(placement.contains(stale))

        // Liveness still reports it as down.
        #expect(await cache.activeNodes().map(\.id) == [live])
    }

    @Test("placementNodes excludes a drained node - the one way ownership does change")
    func placementNodesExcludesDrained() async {
        let cache = ClusterNodeCache()
        let drained = UUID()
        await cache.upsert(
            ClusterNodeInfo(
                id: drained, address: "http://d:8080", status: .draining, lastHeartbeatAt: Date()))
        #expect(await cache.placementNodes().isEmpty)
    }

    @Test("activeNodes excludes draining/removed status")
    func activeNodesExcludesNonActiveStatus() async {
        let cache = ClusterNodeCache()
        let now = Date()
        await cache.load(initialData: [
            ClusterNodeInfo(id: UUID(), address: "http://a:8080", status: .active, lastHeartbeatAt: now),
            ClusterNodeInfo(id: UUID(), address: "http://b:8080", status: .draining, lastHeartbeatAt: now),
            ClusterNodeInfo(id: UUID(), address: "http://c:8080", status: .removed, lastHeartbeatAt: now),
        ])

        let active = await cache.activeNodes(now: now, staleness: 30)
        #expect(active.count == 1)
        #expect(active.first?.address == "http://a:8080")
    }

    @Test("activeNodes excludes nodes with a stale heartbeat")
    func activeNodesExcludesStaleHeartbeat() async {
        let cache = ClusterNodeCache()
        let now = Date()
        await cache.load(initialData: [
            ClusterNodeInfo(id: UUID(), address: "http://fresh:8080", status: .active, lastHeartbeatAt: now),
            ClusterNodeInfo(
                id: UUID(), address: "http://stale:8080", status: .active,
                lastHeartbeatAt: now.addingTimeInterval(-60)),
        ])

        let active = await cache.activeNodes(now: now, staleness: 30)
        #expect(active.count == 1)
        #expect(active.first?.address == "http://fresh:8080")
    }

    @Test("reachablePeers keeps active and draining, drops only removed")
    func reachablePeersDropsRemoved() async {
        let cache = ClusterNodeCache()
        let now = Date()
        await cache.load(initialData: [
            // A draining node is still live and may hold records mid-migration, so a listing must
            // still reach it - unlike a removed node, which is decommissioned and only times out.
            ClusterNodeInfo(id: UUID(), address: "http://active:8080", status: .active, lastHeartbeatAt: now),
            ClusterNodeInfo(id: UUID(), address: "http://draining:8080", status: .draining, lastHeartbeatAt: now),
            ClusterNodeInfo(id: UUID(), address: "http://removed:8080", status: .removed, lastHeartbeatAt: now),
        ])

        let reachable = Set(await cache.reachablePeers().map(\.address))
        #expect(reachable == ["http://active:8080", "http://draining:8080"])
    }

    @Test("a decommissioned node is not resurrected by a lagging same-heartbeat snapshot")
    func removedStaysRemovedAgainstStaleSnapshot() async {
        let cache = ClusterNodeCache()
        let id = UUID()
        let hb = Date()
        await cache.upsert(
            ClusterNodeInfo(id: id, address: "http://n:8080", status: .removed, lastHeartbeatAt: hb))

        // A peer that missed the decommission still gossips the node as draining, same heartbeat.
        // Reconciling that must NOT flip it back - that would bring the retired ghost back to life.
        await cache.reconcile(snapshot: [
            ClusterNodeInfo(id: id, address: "http://n:8080", status: .draining, lastHeartbeatAt: hb)
        ])
        #expect(await cache.get(id: id)?.status == .removed)

        // But a genuine restart - a strictly newer heartbeat, re-registering active - DOES revive
        // it, so decommissioning is never a permanent lock-out.
        await cache.reconcile(snapshot: [
            ClusterNodeInfo(
                id: id, address: "http://n:8080", status: .active,
                lastHeartbeatAt: hb.addingTimeInterval(1))
        ])
        #expect(await cache.get(id: id)?.status == .active)
    }
}
