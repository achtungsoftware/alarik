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

/// Issue #28: storage figures must be cached, coalesced and never computed more often than the
/// walk limit allows - these pin the contract of `StorageStatsService` and `AsyncSemaphore`.
@Suite("StorageStatsService tests")
struct StorageStatsServiceTests {
    private actor Counter {
        var value = 0
        var peak = 0
        var inFlight = 0
        func increment() { value += 1 }
        func enter() { inFlight += 1; peak = max(peak, inFlight) }
        func leave() { inFlight -= 1 }
    }

    private let key = StorageStatsService.Key(scope: .clusterTotal, bucketName: "b", prefix: "p/")

    @Test("a fresh figure is served from cache without recomputing")
    func cacheHit() async throws {
        let service = StorageStatsService(ttl: 60, maxConcurrentWalks: 1)
        let computations = Counter()
        for _ in 0..<3 {
            let stats = try await service.stats(for: key) {
                await computations.increment()
                return StorageStatsService.Computation(sizeBytes: 42, objectCount: 7)
            }
            #expect(stats.sizeBytes == 42 && stats.objectCount == 7)
        }
        #expect(await computations.value == 1)
    }

    @Test("an expired figure is recomputed, and different keys never share a cache slot")
    func expiryAndKeyIsolation() async throws {
        let service = StorageStatsService(ttl: 0, maxConcurrentWalks: 1)
        let computations = Counter()
        let compute: @Sendable () async throws -> StorageStatsService.Computation = {
            await computations.increment()
            return StorageStatsService.Computation(sizeBytes: 1, objectCount: 1)
        }
        _ = try await service.stats(for: key, compute: compute)
        _ = try await service.stats(for: key, compute: compute)
        let other = StorageStatsService.Key(scope: .ownedShare, bucketName: "b", prefix: "p/")
        _ = try await service.stats(for: other, compute: compute)
        #expect(await computations.value == 3)
    }

    @Test("concurrent requests for one key coalesce into a single computation")
    func coalescing() async throws {
        let service = StorageStatsService(ttl: 60, maxConcurrentWalks: 4)
        let computations = Counter()
        try await withThrowingTaskGroup(of: StorageStatsService.Stats.self) { group in
            for _ in 0..<8 {
                group.addTask {
                    try await service.stats(for: key) {
                        await computations.increment()
                        try await Task.sleep(for: .milliseconds(50))
                        return StorageStatsService.Computation(sizeBytes: 9, objectCount: 3)
                    }
                }
            }
            for try await stats in group { #expect(stats.objectCount == 3) }
        }
        #expect(await computations.value == 1)
    }

    @Test("a failed computation is not cached and the next request retries")
    func failureIsNotCached() async throws {
        struct WalkFailed: Error {}
        let service = StorageStatsService(ttl: 60, maxConcurrentWalks: 1)
        await #expect(throws: WalkFailed.self) {
            try await service.stats(for: key) { throw WalkFailed() }
        }
        let stats = try await service.stats(for: key) { StorageStatsService.Computation(sizeBytes: 5, objectCount: 1) }
        #expect(stats.sizeBytes == 5)
    }

    @Test("an incomplete figure (a peer did not answer) is returned but not cached")
    func incompleteIsNotCached() async throws {
        let service = StorageStatsService(ttl: 60, maxConcurrentWalks: 1)
        let computations = Counter()
        let partial = try await service.stats(for: key) {
            await computations.increment()
            return StorageStatsService.Computation(sizeBytes: 1, objectCount: 1, complete: false)
        }
        #expect(partial.objectCount == 1)
        let full = try await service.stats(for: key) {
            await computations.increment()
            return StorageStatsService.Computation(sizeBytes: 2, objectCount: 2)
        }
        #expect(full.objectCount == 2)
        #expect(await computations.value == 2)
    }

    @Test("the same bucket under a different membership is a different key")
    func membershipIsPartOfTheKey() {
        let a = ClusterNodeInfo(id: UUID(), address: "http://a", status: .active, lastHeartbeatAt: Date())
        let b = ClusterNodeInfo(id: UUID(), address: "http://b", status: .active, lastHeartbeatAt: Date())
        let one = StorageStatsService.Key(scope: .ownedShare, bucketName: "b", prefix: "", activeNodes: [a])
        let two = StorageStatsService.Key(scope: .ownedShare, bucketName: "b", prefix: "", activeNodes: [a, b])
        let twoReordered = StorageStatsService.Key(scope: .ownedShare, bucketName: "b", prefix: "", activeNodes: [b, a])
        #expect(one != two)
        #expect(two == twoReordered)
    }

    @Test("noteChange(bucketName:) from the storage layer drops that bucket's figures before the next read")
    func noteChangeInvalidates() async throws {
        let service = StorageStatsService(ttl: 60, maxConcurrentWalks: 1)
        let computations = Counter()
        let compute: @Sendable () async throws -> StorageStatsService.Computation = {
            await computations.increment()
            return StorageStatsService.Computation(sizeBytes: 1, objectCount: 1)
        }
        let allBuckets = StorageStatsService.Key(scope: .ownedShareAllBuckets, bucketName: "", prefix: "")
        let otherBucket = StorageStatsService.Key(scope: .clusterTotal, bucketName: "other", prefix: "")
        for k in [key, allBuckets, otherBucket] { _ = try await service.stats(for: k, compute: compute) }
        #expect(await computations.value == 3)

        service.noteChange(bucketName: "b")  // synchronous, as the storage layer calls it
        _ = try await service.stats(for: key, compute: compute)  // recomputed
        _ = try await service.stats(for: allBuckets, compute: compute)  // spans every bucket: recomputed
        _ = try await service.stats(for: otherBucket, compute: compute)  // untouched: cached
        #expect(await computations.value == 5)
    }

    @Test("a write that lands while a walk is running keeps that walk's result out of the cache")
    func writeDuringWalkIsNotCachedOver() async throws {
        let service = StorageStatsService(ttl: 60, maxConcurrentWalks: 1)
        let computations = Counter()
        let first = try await service.stats(for: key) {
            await computations.increment()
            // The write races the walk: whatever this figure says, it may be missing the write.
            service.noteChange(bucketName: "b")
            return StorageStatsService.Computation(sizeBytes: 1, objectCount: 1)
        }
        #expect(first.objectCount == 1)
        let second = try await service.stats(for: key) {
            await computations.increment()
            return StorageStatsService.Computation(sizeBytes: 2, objectCount: 2)
        }
        #expect(second.objectCount == 2)
        #expect(await computations.value == 2)
    }

    @Test("metadata pseudo-bucket writes never invalidate anything")
    func metadataWritesAreIgnored() async throws {
        let service = StorageStatsService(ttl: 60, maxConcurrentWalks: 1)
        let computations = Counter()
        let allBuckets = StorageStatsService.Key(scope: .ownedShareAllBuckets, bucketName: "", prefix: "")
        let compute: @Sendable () async throws -> StorageStatsService.Computation = {
            await computations.increment()
            return StorageStatsService.Computation(sizeBytes: 1, objectCount: 1)
        }
        _ = try await service.stats(for: allBuckets, compute: compute)
        service.noteChange(bucketName: MetadataNamespace.bucketName)
        _ = try await service.stats(for: allBuckets, compute: compute)
        #expect(await computations.value == 1)
    }

    @Test("coalesceOnly shares one in-flight computation but never stores the result")
    func coalesceOnlyPolicy() async throws {
        let service = StorageStatsService(ttl: 60, maxConcurrentWalks: 1)
        let computations = Counter()
        let compute: @Sendable () async throws -> StorageStatsService.Computation = {
            await computations.increment()
            return StorageStatsService.Computation(sizeBytes: 1, objectCount: 1)
        }
        _ = try await service.stats(for: key, policy: .coalesceOnly, compute: compute)
        _ = try await service.stats(for: key, policy: .coalesceOnly, compute: compute)
        #expect(await computations.value == 2)
    }

    @Test("invalidate(bucketName:) drops only that bucket's figures")
    func invalidation() async throws {
        let service = StorageStatsService(ttl: 60, maxConcurrentWalks: 1)
        let computations = Counter()
        let otherBucket = StorageStatsService.Key(scope: .clusterTotal, bucketName: "other", prefix: "")
        let compute: @Sendable () async throws -> StorageStatsService.Computation = {
            await computations.increment()
            return StorageStatsService.Computation(sizeBytes: 1, objectCount: 1)
        }
        _ = try await service.stats(for: key, compute: compute)
        _ = try await service.stats(for: otherBucket, compute: compute)
        await service.invalidate(bucketName: "b")
        _ = try await service.stats(for: key, compute: compute)  // recomputed
        _ = try await service.stats(for: otherBucket, compute: compute)  // still cached
        #expect(await computations.value == 3)
    }

    @Test("AsyncSemaphore never lets more than its permits run at once and releases on throw")
    func semaphoreBound() async throws {
        let semaphore = AsyncSemaphore(permits: 2)
        let gauge = Counter()
        struct Boom: Error {}
        await withTaskGroup(of: Void.self) { group in
            for i in 0..<10 {
                group.addTask {
                    _ = try? await semaphore.withPermit {
                        await gauge.enter()
                        try await Task.sleep(for: .milliseconds(20))
                        await gauge.leave()
                        if i % 3 == 0 { throw Boom() }
                    }
                }
            }
        }
        #expect(await gauge.peak <= 2)
        #expect(await gauge.inFlight == 0)
        // Every permit came back: two more can still run concurrently.
        try await semaphore.withPermit { try await semaphore.withPermit {} }
    }
}
