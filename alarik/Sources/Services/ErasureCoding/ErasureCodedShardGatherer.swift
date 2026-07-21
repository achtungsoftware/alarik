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
import Vapor

enum ErasureCodedGatherError: Error, CustomStringConvertible {
    /// No reachable responsible node holds any shard for this (key, version), and none were
    /// unreachable - the object is positively absent. Callers map this to a 404.
    case notFound
    /// Shards exist somewhere, but fewer than `needed` could be gathered right now (a holder was
    /// unreachable, or a fetch failed). The object exists but is currently degraded - callers map
    /// this to a 503, never a 404 (a 404 would tell clients/replication the object was deleted).
    case degraded(found: Int, needed: Int)
    /// The gathered shards disagree on object identity (etag) - a read raced a non-versioned
    /// overwrite and picked up a mix of generations. Decoding a mix would produce garbage, so the
    /// gatherer refuses; the read path retries a fresh gather (the overwrite is brief).
    case inconsistent

    var description: String {
        switch self {
        case .notFound:
            "No shard exists for this object on any reachable responsible node"
        case .degraded(let found, let needed):
            "Only \(found) of \(needed) required shards could be gathered (object is degraded)"
        case .inconsistent:
            "Gathered shards disagree on object identity (concurrent overwrite)"
        }
    }
}

/// One gathered set of shards, keyed by true shard index, plus the object metadata they all agree
/// on. `paths` are local file paths (this node's own shard, or a fetched temp copy); callers that
/// received temp copies (`isTemp`) must unlink them once done.
struct GatheredShards {
    var shards: [Int: (path: String, isTemp: Bool)]
    var meta: ObjectMeta
    /// Every shard index any *reachable* responsible node reported holding (from the discovery
    /// phase, across the whole responsible set - not just the shards actually fetched). Lets the
    /// read path spot indices missing cluster-wide and trigger read-repair, without a second probe.
    var heldIndices: Set<Int>
    /// The responsible *ranks* whose node answered discovery. A shard index is only safe to treat
    /// as "missing" (and reconstruct) when its rank-holder was reachable-but-not-holding it - if
    /// the holder was simply unreachable, its shard is very likely still there, so reconstructing
    /// would be wasted churn against a node that's merely briefly down.
    var reachableRanks: Set<Int>

    func cleanup() {
        for entry in shards.values where entry.isTemp {
            _ = POSIXFile.unlink(entry.path)
        }
    }
}

/// Location-independent shard gathering, shared by the read coordinator and the reconstruction/
/// rebalance path. The invariant that makes this necessary: a shard's index is fixed at encode
/// time (stored in its filename/header), but which node holds a given index drifts as HRW ranks
/// shift on every membership change - so gathering must *discover* what each node actually holds
/// (`/held`) and fetch shards by their true index, never assume "node at rank r holds shard r".
enum ErasureCodedShardGatherer {
    /// Discovers held indices across `responsible` (in parallel), plans a covering set of `needed`
    /// (plus one spare when a healthy extra exists) distinct indices - optionally excluding one
    /// index the caller is about to reconstruct - fetches them (local reads never hit the network),
    /// and verifies the gathered shards agree on identity before returning. Throws
    /// `ErasureCodedGatherError` distinguishing genuine absence (404) from degraded availability
    /// (503) from a concurrent-overwrite race (retryable).
    static func gather(
        app: Application, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo], selfNodeId: UUID, needed: Int, wantSpare: Bool,
        excludingIndex: Int?, requestId: String
    ) async throws -> GatheredShards {
        // Phase 1: discover which shard indices each responsible node actually holds, AND which
        // generation each of those shards belongs to - one round trip, no separate probe pass.
        // `nil` means unreachable (kept distinct from "reachable, holds none" so absence and
        // degradation don't get conflated).
        let discovered: [(node: ClusterNodeInfo, shards: [ClusterReplicationClient.HeldShard]?)] =
            await withTaskGroup(
                of: (node: ClusterNodeInfo, shards: [ClusterReplicationClient.HeldShard]?).self
            ) { group in
                for node in responsible {
                    group.addTask {
                        if node.id == selfNodeId {
                            return (
                                node,
                                await localHeldShards(
                                    app: app, bucketName: bucketName, key: key,
                                    versionId: versionId)
                            )
                        }
                        let held = await ClusterReplicationClient.heldShardsDetailed(
                            app: app, node: node, bucketName: bucketName, key: key,
                            versionId: versionId)
                        return (node, held)
                    }
                }
                var results:
                    [(node: ClusterNodeInfo, shards: [ClusterReplicationClient.HeldShard]?)] = []
                for await outcome in group { results.append(outcome) }
                return results
            }

        var anyUnreachable = false
        var reportedIndices: Set<Int> = []
        var reachableRanks: Set<Int> = []
        for entry in discovered {
            if let shards = entry.shards {
                reportedIndices.formUnion(shards.map(\.index))
                if let rank = responsible.firstIndex(where: { $0.id == entry.node.id }) {
                    reachableRanks.insert(rank)
                }
            } else {
                anyUnreachable = true
            }
        }
        let allHeldIndices = reportedIndices
        if let excludingIndex { reportedIndices.remove(excludingIndex) }

        // Nothing anywhere and everyone answered -> the object genuinely doesn't exist here.
        if reportedIndices.isEmpty {
            if anyUnreachable { throw ErasureCodedGatherError.degraded(found: 0, needed: needed) }
            throw ErasureCodedGatherError.notFound
        }

        // Newest generation wins. Copies of the same key can legitimately disagree: a
        // non-versioned overwrite, or a tombstone, that hasn't reached every holder - placement
        // drift and rank-0 coordinator failover both produce exactly this. Assembling "whichever
        // holder answered first" would nondeterministically serve the OLD generation, and for a
        // revoked credential's tombstone that is a resurrection. So the winner is decided by
        // (updatedAtMillis, etag) across every reported copy - the same newest-wins rule MinIO
        // applies across disagreeing drives and Cassandra applies across replica timestamps.
        //
        // Generation is tracked per (node, index), never per node: one node can hold shards from
        // two different generations at once (its own old copy plus a newly delivered one), so
        // judging a whole node by one sampled shard would both exclude good shards and admit
        // stale ones.
        var winner: Generation?
        for entry in discovered {
            for shard in entry.shards ?? [] where shard.index != excludingIndex {
                guard let generation = Generation(shard) else { continue }
                if winner == nil || generation.isNewer(than: winner!) { winner = generation }
            }
        }

        // Stale means POSITIVELY known to be older than the winner. A shard whose generation is
        // unknown (an older peer that answered with bare indices) is not stale - it stays usable
        // as a fallback, with the identity verification below still guarding the final assembly.
        func isStale(_ shard: ClusterReplicationClient.HeldShard) -> Bool {
            guard let winner, let generation = Generation(shard) else { return false }
            return generation != winner
        }

        // Phase 2: pick one holder per distinct index (two nodes can hold the same index, either
        // transiently mid-reindex or as divergent generations). Winner-generation copies are
        // preferred; unknown-generation copies are a fallback; stale copies are never planned in.
        var candidates: [(index: Int, node: ClusterNodeInfo, generationKnown: Bool)] = []
        for entry in discovered {
            for shard in entry.shards ?? [] where shard.index != excludingIndex {
                guard !isStale(shard) else { continue }
                candidates.append((shard.index, entry.node, Generation(shard) != nil))
            }
        }
        candidates.sort { $0.generationKnown && !$1.generationKnown }

        let target = needed + (wantSpare ? 1 : 0)
        var plan: [(index: Int, node: ClusterNodeInfo)] = []
        var claimed: Set<Int> = []
        if let excludingIndex { claimed.insert(excludingIndex) }
        for candidate in candidates where !claimed.contains(candidate.index) {
            claimed.insert(candidate.index)
            plan.append((candidate.index, candidate.node))
            if plan.count >= target { break }
        }

        // Phase 3: fetch the planned (index, node) pairs in parallel - local ranks read from disk,
        // remote ranks fetch to a temp file.
        var gathered: [Int: (path: String, isTemp: Bool)] = [:]
        let requestIdCopy = requestId
        let fetched: [(index: Int, result: (path: String, isTemp: Bool)?)] = await withTaskGroup(
            of: (index: Int, result: (path: String, isTemp: Bool)?).self
        ) { group in
            for item in plan {
                group.addTask {
                    if item.node.id == selfNodeId {
                        let localPath = ErasureCodedObjectHandler.shardPath(
                            bucketName: bucketName, key: key, versionId: versionId,
                            shardIndex: item.index)
                        guard FileManager.default.fileExists(atPath: localPath) else {
                            return (item.index, nil)
                        }
                        return (item.index, (localPath, false))
                    }
                    guard
                        let tempPath = try? await ClusterReplicationClient.fetchShard(
                            app: app, candidates: [item.node], bucketName: bucketName, key: key,
                            versionId: versionId, shardIndex: item.index, requestId: requestIdCopy)
                    else { return (item.index, nil) }
                    return (item.index, (tempPath, true))
                }
            }
            var results: [(index: Int, result: (path: String, isTemp: Bool)?)] = []
            for await outcome in group { results.append(outcome) }
            return results
        }
        for entry in fetched {
            if let result = entry.result { gathered[entry.index] = result }
        }

        guard gathered.count >= needed else {
            for entry in gathered.values where entry.isTemp { _ = POSIXFile.unlink(entry.path) }
            // Shards were reported to exist (reportedIndices non-empty) but too few of the WINNING
            // generation could be pulled together - degraded, never "not found", and never
            // silently backfilled with the older generation's shards.
            throw ErasureCodedGatherError.degraded(found: gathered.count, needed: needed)
        }

        // Phase 4: verify identity agreement. The generation filter above should already guarantee
        // this, but unknown-generation copies (older peers) can still slip a cross-generation mix
        // in, which would decode to garbage - so the assembled set is checked, not assumed.
        let paths = gathered.values.map(\.path)
        let headers = try? await S3Service.offloadBlockingIO(app) {
            try paths.map { path -> ErasureCodedShardHeader in
                let reader = try ErasureCodedShardReader(path: path)
                defer { reader.close() }
                return reader.header
            }
        }
        guard let headers, let reference = headers.first?.objectMeta else {
            for entry in gathered.values where entry.isTemp { _ = POSIXFile.unlink(entry.path) }
            throw ErasureCodedGatherError.degraded(found: 0, needed: needed)
        }
        let consistent = headers.allSatisfy {
            $0.objectMeta.etag == reference.etag && $0.objectMeta.size == reference.size
        }
        guard consistent else {
            for entry in gathered.values where entry.isTemp { _ = POSIXFile.unlink(entry.path) }
            throw ErasureCodedGatherError.inconsistent
        }
        // The assembly must BE the winning generation, not merely internally consistent - an
        // all-stale set agrees with itself perfectly and would silently serve the old record.
        if let winner, reference.etag != winner.etag {
            for entry in gathered.values where entry.isTemp { _ = POSIXFile.unlink(entry.path) }
            throw ErasureCodedGatherError.inconsistent
        }

        // Phase 5: read repair. Overwrite each positively-stale copy with the winning generation's
        // shard of the same index, so divergence heals on the read path instead of lingering until
        // some later walk notices - the repair Cassandra performs on a digest mismatch. Only runs
        // when divergence was actually detected, and only pushes indices already assembled here.
        let staleTargets: [(node: ClusterNodeInfo, index: Int)] = discovered.flatMap {
            entry -> [(ClusterNodeInfo, Int)] in
            guard entry.node.id != selfNodeId else { return [] }
            return (entry.shards ?? []).compactMap { shard in
                isStale(shard) && gathered[shard.index] != nil
                    ? (entry.node, shard.index) : nil
            }
        }
        if !staleTargets.isEmpty {
            await withTaskGroup(of: Void.self) { group in
                for target in staleTargets {
                    guard let source = gathered[target.index] else { continue }
                    group.addTask {
                        do {
                            try await ClusterReplicationClient.pushShard(
                                app: app, to: target.node, sourcePath: source.path,
                                bucketName: bucketName, key: key, versionId: versionId,
                                shardIndex: target.index)
                            app.logger.info(
                                "Read repair: replaced stale copy of shard \(target.index) for '\(key)' on node \(target.node.id)."
                            )
                        } catch {
                            app.logger.debug(
                                "Read repair push of shard \(target.index) for '\(key)' to \(target.node.id) failed (retried on a later read): \(error)"
                            )
                        }
                    }
                }
            }
        }

        return GatheredShards(
            shards: gathered, meta: reference, heldIndices: allHeldIndices,
            reachableRanks: reachableRanks)
    }

    /// This node's own held shards with their generations - the local counterpart of the peer
    /// `/held?detail=1` probe, reading each shard's header straight off disk.
    private static func localHeldShards(
        app: Application, bucketName: String, key: String, versionId: String?
    ) async -> [ClusterReplicationClient.HeldShard] {
        let indices = ErasureCodedObjectHandler.locallyHeldShardIndices(
            bucketName: bucketName, key: key, versionId: versionId)
        guard !indices.isEmpty else { return [] }
        let read = try? await S3Service.offloadBlockingIO(app) {
            indices.compactMap { index -> ClusterReplicationClient.HeldShard? in
                let path = ErasureCodedObjectHandler.shardPath(
                    bucketName: bucketName, key: key, versionId: versionId, shardIndex: index)
                guard let reader = try? ErasureCodedShardReader(path: path) else { return nil }
                defer { reader.close() }
                let meta = reader.header.objectMeta
                return ClusterReplicationClient.HeldShard(
                    index: index, etag: meta.etag,
                    updatedAtMillis: Int64(meta.updatedAt.timeIntervalSince1970 * 1000))
            }
        }
        // Header reads failing wholesale must not read as "holds nothing" - fall back to bare
        // indices (unknown generation), exactly how an older peer would answer.
        return read
            ?? indices.map {
                ClusterReplicationClient.HeldShard(index: $0, etag: "", updatedAtMillis: 0)
            }
    }

    /// Which write a shard belongs to. `nil` when the reporting peer didn't supply one (an older
    /// binary answering with bare indices).
    private struct Generation: Equatable {
        let etag: String
        let updatedAtMillis: Int64

        init?(_ shard: ClusterReplicationClient.HeldShard) {
            guard !shard.etag.isEmpty else { return nil }
            self.etag = shard.etag
            self.updatedAtMillis = shard.updatedAtMillis
        }

        /// Timestamp first, etag as a deterministic tiebreaker so every node independently picks
        /// the same winner for an exact tie.
        func isNewer(than other: Generation) -> Bool {
            updatedAtMillis != other.updatedAtMillis
                ? updatedAtMillis > other.updatedAtMillis : etag > other.etag
        }
    }
}
