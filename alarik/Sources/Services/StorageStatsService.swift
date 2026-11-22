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
import Synchronization
import Vapor

/// Size/object-count figures for a bucket or folder are a recursive walk over every file below
/// it - the single most expensive read this server does. Left unguarded, a console page of
/// folder rows fires one walk per row at once, and the walk ran synchronously on the async
/// executor, so a bucket with many small files saturated every core and stalled unrelated
/// requests (issue #28). This service makes every such figure:
///
/// - **cached** for `ttl`, keyed by what was measured (`Key`), so repeated views and polling
///   dashboards re-read a number instead of re-walking a tree;
/// - **coalesced**: concurrent requests for the same key share one computation;
/// - **bounded**: the local walks themselves run through `localWalk`, at most
///   `maxConcurrentWalks` at a time, on the blocking-IO thread pool.
///
/// Freshness: the storage layer reports every object/shard write and delete through
/// `noteChange(bucketName:)`, which bumps that bucket's change generation. A cached figure is
/// only served while its generation is current, and a computation only enters the cache if
/// nothing changed while it ran - so a node's own numbers are exact right after a write, and
/// `ttl` only bounds staleness for changes a node never saw. Figures that aggregate *other*
/// nodes are never cached here (see `CachePolicy.coalesceOnly`): each node caches its own share
/// and invalidates it itself.
actor StorageStatsService {
    static let shared = StorageStatsService()

    /// Whether a computed figure may be stored, or only shared with concurrent identical
    /// requests. Aggregates over other nodes use `coalesceOnly`: this node cannot know when a
    /// peer's share changed, but the peer's own cache can.
    enum CachePolicy: Sendable {
        case cache
        case coalesceOnly
    }

    struct Key: Hashable, Sendable {
        /// Which figure was measured - the same bucket/prefix can have several:
        /// `clusterTotal` (every node summed), `ownedShare` (this node's primary-owner share),
        /// `ownedShareAllBuckets` (the same, across all buckets).
        enum Scope: Hashable, Sendable {
            case clusterTotal
            case ownedShare
            case ownedShareAllBuckets
        }
        let scope: Scope
        let bucketName: String
        let prefix: String
        /// The cluster membership the figure was computed under - a node's owned share and the
        /// cluster total both follow placement, so a figure from before a join/leave must not
        /// be served after it. Empty when not clustered.
        let membership: String

        init(scope: Scope, bucketName: String, prefix: String, activeNodes: [ClusterNodeInfo] = []) {
            self.scope = scope
            self.bucketName = bucketName
            self.prefix = prefix
            self.membership = activeNodes.map(\.id.uuidString).sorted().joined(separator: ",")
        }
    }

    /// What a computation hands back. `complete == false` (a peer could not be reached, so the
    /// figure is a lower bound) is returned to the caller but never cached - the next request
    /// gets to try again instead of serving an under-count for the whole TTL.
    struct Computation: Sendable {
        let sizeBytes: Int64
        let objectCount: Int
        var complete = true
    }

    struct Stats: Sendable, Codable, Equatable {
        let sizeBytes: Int64
        let objectCount: Int
        /// When the walk ran - lets a UI say "as of", and tests assert cache hits.
        let computedAt: Date

        var summary: (sizeBytes: Int64, objectCount: Int) { (sizeBytes, objectCount) }
    }

    private struct Entry {
        let stats: Stats
        /// `changeGeneration(for:)` of the key's bucket when the walk started.
        let generation: UInt64
    }

    private let ttl: TimeInterval
    private let walkLimiter: AsyncSemaphore
    private var cache: [Key: Entry] = [:]
    /// Per-bucket write/delete generations, plus one under `""` for anything at all (the
    /// all-buckets share). A plain mutex, not actor state, so the synchronous storage layer can
    /// report a change without hopping executors.
    private let generations = Mutex<[String: UInt64]>([:])
    private var inFlight: [Key: Task<(stats: Stats, complete: Bool), any Error>] = [:]

    init(
        ttl: TimeInterval = Constants.storageStatsCacheTTL,
        maxConcurrentWalks: Int = Constants.storageStatsMaxConcurrentWalks
    ) {
        self.ttl = ttl
        self.walkLimiter = AsyncSemaphore(permits: maxConcurrentWalks)
    }

    /// The cached figure for `key`, or the result of `compute` when there is none fresh enough -
    /// at most one `compute` runs per key at a time, late arrivals await the one in flight.
    func stats(
        for key: Key, policy: CachePolicy = .cache,
        compute: @escaping @Sendable () async throws -> Computation
    ) async throws -> Stats {
        let generationNow = changeGeneration(for: key)
        if policy == .cache, let entry = cache[key], entry.generation == generationNow,
            Date().timeIntervalSince(entry.stats.computedAt) < ttl
        {
            return entry.stats
        }
        if let running = inFlight[key] {
            return try await running.value.stats
        }
        let task = Task<(stats: Stats, complete: Bool), any Error> {
            let result = try await compute()
            let stats = Stats(
                sizeBytes: result.sizeBytes, objectCount: result.objectCount, computedAt: Date())
            return (stats, result.complete)
        }
        inFlight[key] = task
        defer { inFlight[key] = nil }
        let (stats, complete) = try await task.value
        // A write that landed mid-walk may or may not be in the figure - don't cache it.
        if complete, policy == .cache, changeGeneration(for: key) == generationNow {
            cache[key] = Entry(stats: stats, generation: generationNow)
        }
        return stats
    }

    /// Called by the storage layer after any object or shard in `bucketName` is written or
    /// removed. Cheap and synchronous. Control-plane writes to the metadata pseudo-bucket are
    /// continuous in a cluster and never part of a user-visible figure, so they are ignored.
    nonisolated func noteChange(bucketName: String) {
        guard bucketName != MetadataNamespace.bucketName else { return }
        generations.withLock {
            $0[bucketName, default: 0] += 1
            $0["", default: 0] += 1
        }
    }

    private nonisolated func changeGeneration(for key: Key) -> UInt64 {
        // The all-buckets share spans every bucket, so it follows the global generation.
        let bucket = key.scope == .ownedShareAllBuckets ? "" : key.bucketName
        return generations.withLock { $0[bucket] ?? 0 }
    }

    /// Runs one blocking directory walk on the IO thread pool, holding one of the limited walk
    /// permits for its duration. Everything that enumerates a bucket tree for a figure goes
    /// through here, including the per-node share a cluster peer computes on request.
    nonisolated func localWalk<T: Sendable>(
        app: Application, _ walk: @escaping @Sendable () throws -> T
    ) async throws -> T {
        try await walkLimiter.withPermit {
            try await app.threadPool.runIfActive(walk)
        }
    }

    /// Drops every cached figure for `bucketName`, whatever scope or prefix.
    func invalidate(bucketName: String) {
        cache = cache.filter { $0.key.bucketName != bucketName }
    }

    func invalidateAll() {
        cache.removeAll()
    }
}
