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
}
