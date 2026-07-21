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

/// The node-local half of the distributed unique-name claim. The property that matters: one node
/// grants at most one live reservation per name, which is what stops two coordinators from both
/// reaching a majority and both creating the same bucket/user/access key.
@Suite("MetadataClaimRegistry tests")
struct MetadataClaimRegistryTests {
    @Test("a second claimant is refused while the first reservation is live")
    func secondClaimantRefused() async {
        let registry = MetadataClaimRegistry()
        let first = UUID()
        let second = UUID()

        #expect(await registry.reserve(collection: "buckets", id: "test", token: first))
        #expect(await registry.reserve(collection: "buckets", id: "test", token: second) == false)
    }

    @Test("the holder can re-reserve its own token, so a retry isn't locked out")
    func sameTokenIsIdempotent() async {
        let registry = MetadataClaimRegistry()
        let token = UUID()
        #expect(await registry.reserve(collection: "buckets", id: "test", token: token))
        #expect(await registry.reserve(collection: "buckets", id: "test", token: token))
    }

    @Test("different names don't block each other")
    func differentNamesAreIndependent() async {
        let registry = MetadataClaimRegistry()
        #expect(await registry.reserve(collection: "buckets", id: "a", token: UUID()))
        #expect(await registry.reserve(collection: "buckets", id: "b", token: UUID()))
        // Same id in a different collection is a different name too.
        #expect(await registry.reserve(collection: "users", id: "a", token: UUID()))
    }

    @Test("releasing frees the name for the next claimant")
    func releaseFreesTheName() async {
        let registry = MetadataClaimRegistry()
        let first = UUID()
        #expect(await registry.reserve(collection: "buckets", id: "test", token: first))
        await registry.release(collection: "buckets", id: "test", token: first)
        #expect(await registry.reserve(collection: "buckets", id: "test", token: UUID()))
    }

    @Test("a stale release from a previous claimant cannot free the current reservation")
    func staleReleaseIsIgnored() async {
        let registry = MetadataClaimRegistry()
        let first = UUID()
        let second = UUID()
        #expect(await registry.reserve(collection: "buckets", id: "test", token: first))
        await registry.release(collection: "buckets", id: "test", token: first)
        #expect(await registry.reserve(collection: "buckets", id: "test", token: second))

        // `first` is long gone; its late release must not hand `second`'s name away.
        await registry.release(collection: "buckets", id: "test", token: first)
        #expect(await registry.reserve(collection: "buckets", id: "test", token: UUID()) == false)
    }

    @Test("an expired reservation stops blocking, so a dead claimant can't hold a name forever")
    func expiredReservationIsReclaimable() async {
        let registry = MetadataClaimRegistry()
        let dead = UUID()
        let now = Date()
        #expect(
            await registry.reserve(
                collection: "buckets", id: "test", token: dead, ttl: 10, now: now))

        // Same instant: still held.
        #expect(
            await registry.reserve(
                collection: "buckets", id: "test", token: UUID(), ttl: 10, now: now) == false)

        // Past its TTL: reclaimable.
        #expect(
            await registry.reserve(
                collection: "buckets", id: "test", token: UUID(), ttl: 10,
                now: now.addingTimeInterval(11)))
    }

    @Test("purgeExpired drops only entries that have actually lapsed")
    func purgeKeepsLiveReservations() async {
        let registry = MetadataClaimRegistry()
        let now = Date()
        _ = await registry.reserve(
            collection: "buckets", id: "expired", token: UUID(), ttl: 1, now: now)
        _ = await registry.reserve(
            collection: "buckets", id: "live", token: UUID(), ttl: 60, now: now)

        await registry.purgeExpired(now: now.addingTimeInterval(5))
        #expect(await registry.liveReservationCount(now: now.addingTimeInterval(5)) == 1)
    }

    @Test("concurrent claimants for one name: exactly one wins")
    func concurrentClaimantsExactlyOneWins() async {
        let registry = MetadataClaimRegistry()
        let winners = await withTaskGroup(of: Bool.self, returning: Int.self) { group in
            for _ in 0..<32 {
                group.addTask {
                    await registry.reserve(collection: "buckets", id: "contended", token: UUID())
                }
            }
            var count = 0
            for await granted in group where granted { count += 1 }
            return count
        }
        #expect(winners == 1, "expected exactly one grant, got \(winners)")
    }
}
