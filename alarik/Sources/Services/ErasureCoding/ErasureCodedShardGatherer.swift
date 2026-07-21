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

        // Nobody reported holding anything. Whether that proves absence depends on how many
        // owners answered: a write only returns success once `quorum` of them have it, so a
        // reader that heard "nothing here" from more than `total - quorum` owners has necessarily
        // asked at least one node any successful write would have touched. Below that threshold
        // absence is unproven and this is a degraded read, not a 404.
        //
        // Requiring *every* owner to answer instead would mean no record could ever be proven
        // absent while any owner is down - and since absence is what `putIfAbsent` checks, that
        // makes creating a user, bucket or access key fail for as long as one node is offline.
        if reportedIndices.isEmpty {
            let reachableOwners = discovered.filter { $0.shards != nil }.count
            let quorum = PlacementService.ecQuorumThreshold(
                dataShards: needed, parityShards: Swift.max(0, responsible.count - needed))
            if reachableOwners >= responsible.count - quorum + 1 {
                throw ErasureCodedGatherError.notFound
            }
            throw ErasureCodedGatherError.degraded(found: 0, needed: needed)
        }

        // Newest generation wins. Copies of the same key can legitimately disagree: a
        // non-versioned overwrite, or a tombstone, that hasn't reached every holder - placement
        // drift and rank-0 coordinator failover both produce exactly this. Assembling "whichever
        // holder answered first" would nondeterministically serve the OLD generation, and for a
        // revoked credential's tombstone that is a resurrection. So generations are ordered by
        // (updatedAtMillis, etag)
        //
        // Generation is tracked per (node, index), never per node: one node can hold shards from
        // two different generations at once (its own old copy plus a newly delivered one), so
        // judging a whole node by one sampled shard would both exclude good shards and admit
        // stale ones.
        var holdersByGeneration: [Generation: [Int: [ClusterNodeInfo]]] = [:]
        var unknownGenerationHolders: [Int: [ClusterNodeInfo]] = [:]
        for entry in discovered {
            for shard in entry.shards ?? [] where shard.index != excludingIndex {
                if let generation = Generation(shard) {
                    holdersByGeneration[generation, default: [:]][shard.index, default: []]
                        .append(entry.node)
                } else {
                    unknownGenerationHolders[shard.index, default: []].append(entry.node)
                }
            }
        }
        let orderedGenerations = holdersByGeneration.keys.sorted { $0.isNewer(than: $1) }

        // Try generations newest-first, and only give up once none of them can be assembled.
        //
        // Falling back matters: a write in flight (or one whose coordinator died mid-push) leaves
        // a NEWER generation that does not yet have `needed` shards anywhere. Refusing the read
        // then - while a complete older generation sits right there - turns every overwrite into
        // a window of spurious 503s. Falling back is also safe for deletes: a tombstone only
        // becomes the newest generation after its write reached quorum, so if it is assemblable
        // it wins here, and if it is not then the delete itself failed and the client was told so.
        for (position, generation) in orderedGenerations.enumerated() {
            var holders = holdersByGeneration[generation] ?? [:]
            // Copies whose generation this peer couldn't report are usable filler for indices
            // this generation doesn't otherwise cover - the final identity check still guards
            // against actually mixing generations.
            for (index, nodes) in unknownGenerationHolders where holders[index] == nil {
                holders[index] = nodes
            }
            guard
                let assembled = await assemble(
                    app: app, bucketName: bucketName, key: key, versionId: versionId,
                    holdersByIndex: holders, selfNodeId: selfNodeId, needed: needed,
                    wantSpare: wantSpare, excludingIndex: excludingIndex, requestId: requestId,
                    allHeldIndices: allHeldIndices, reachableRanks: reachableRanks)
            else { continue }

            // Read repair, but ONLY when serving the newest generation. Repairing while serving a
            // fallback would push an older generation over a newer one - convergence running
            // backwards.
            if position == 0 {
                await repairStaleCopies(
                    app: app, bucketName: bucketName, key: key, versionId: versionId,
                    discovered: discovered, winner: generation, gathered: assembled.shards,
                    selfNodeId: selfNodeId)
            }
            return assembled
        }

        // No known generation could be assembled. If nothing reported a generation at all (every
        // peer is an older binary), fall back to a generation-blind assembly.
        if orderedGenerations.isEmpty,
            let assembled = await assemble(
                app: app, bucketName: bucketName, key: key, versionId: versionId,
                holdersByIndex: unknownGenerationHolders, selfNodeId: selfNodeId, needed: needed,
                wantSpare: wantSpare, excludingIndex: excludingIndex, requestId: requestId,
                allHeldIndices: allHeldIndices, reachableRanks: reachableRanks)
        {
            return assembled
        }

        // Shards were reported to exist but none of them could be pulled together - degraded,
        // never "not found", and never silently backfilled from a generation that isn't whole.
        throw ErasureCodedGatherError.degraded(found: 0, needed: needed)
    }

    /// Fetches and verifies one candidate assembly. `nil` means "this generation could not be
    /// assembled right now" - the caller decides whether to try an older one.
    private static func assemble(
        app: Application, bucketName: String, key: String, versionId: String?,
        holdersByIndex: [Int: [ClusterNodeInfo]], selfNodeId: UUID, needed: Int, wantSpare: Bool,
        excludingIndex: Int?, requestId: String, allHeldIndices: Set<Int>, reachableRanks: Set<Int>
    ) async -> GatheredShards? {
        let target = needed + (wantSpare ? 1 : 0)
        // Sorted for determinism: two nodes gathering the same object should make the same plan,
        // so behaviour doesn't change run to run.
        let indices = holdersByIndex.keys.sorted().filter { $0 != excludingIndex }
        guard !indices.isEmpty else { return nil }

        // Every holder of an index is a fallback for that index, tried in order. Without this a
        // single slow or briefly-unreachable node loses a shard outright even though another node
        // holds the very same one - which is exactly how an object that is fully intact reads as
        // degraded, intermittently, depending on which node happened to be busy.
        let fetched: [(index: Int, result: (path: String, isTemp: Bool)?)] = await withTaskGroup(
            of: (index: Int, result: (path: String, isTemp: Bool)?).self
        ) { group in
            for index in indices.prefix(target) {
                let holders = holdersByIndex[index] ?? []
                group.addTask {
                    for node in holders {
                        if node.id == selfNodeId {
                            let localPath = ErasureCodedObjectHandler.shardPath(
                                bucketName: bucketName, key: key, versionId: versionId,
                                shardIndex: index)
                            if FileManager.default.fileExists(atPath: localPath) {
                                return (index, (localPath, false))
                            }
                            continue
                        }
                        if let tempPath = try? await ClusterReplicationClient.fetchShard(
                            app: app, candidates: [node], bucketName: bucketName, key: key,
                            versionId: versionId, shardIndex: index, requestId: requestId)
                        {
                            return (index, (tempPath, true))
                        }
                    }
                    return (index, nil)
                }
            }
            var results: [(index: Int, result: (path: String, isTemp: Bool)?)] = []
            for await outcome in group { results.append(outcome) }
            return results
        }

        var gathered: [Int: (path: String, isTemp: Bool)] = [:]
        for entry in fetched {
            if let result = entry.result { gathered[entry.index] = result }
        }

        func discard() {
            for entry in gathered.values where entry.isTemp { _ = POSIXFile.unlink(entry.path) }
        }

        guard gathered.count >= needed else {
            discard()
            return nil
        }

        // Verify identity agreement. The generation grouping above should already guarantee it,
        // but unknown-generation copies can still slip a cross-generation mix in, which would
        // decode to garbage - so the assembled set is checked, never assumed.
        let paths = gathered.values.map(\.path)
        let headers = try? await S3Service.offloadBlockingIO(app) {
            try paths.map { path -> ErasureCodedShardHeader in
                let reader = try ErasureCodedShardReader(path: path)
                defer { reader.close() }
                return reader.header
            }
        }
        guard let headers, let reference = headers.first?.objectMeta,
            headers.allSatisfy({
                $0.objectMeta.etag == reference.etag && $0.objectMeta.size == reference.size
            })
        else {
            discard()
            return nil
        }

        return GatheredShards(
            shards: gathered, meta: reference, heldIndices: allHeldIndices,
            reachableRanks: reachableRanks)
    }

    /// Overwrites copies positively known to be older than the generation just served, so
    /// divergence heals on the read path instead of lingering until some later walk notices - the
    /// repair Cassandra performs on a digest mismatch. Best-effort and only for indices already
    /// assembled here.
    private static func repairStaleCopies(
        app: Application, bucketName: String, key: String, versionId: String?,
        discovered: [(node: ClusterNodeInfo, shards: [ClusterReplicationClient.HeldShard]?)],
        winner: Generation, gathered: [Int: (path: String, isTemp: Bool)], selfNodeId: UUID
    ) async {
        let staleTargets: [(node: ClusterNodeInfo, index: Int)] = discovered.flatMap {
            entry -> [(ClusterNodeInfo, Int)] in
            guard entry.node.id != selfNodeId else { return [] }
            return (entry.shards ?? []).compactMap { shard in
                guard let generation = Generation(shard), generation != winner,
                    gathered[shard.index] != nil
                else { return nil }
                return (entry.node, shard.index)
            }
        }
        guard !staleTargets.isEmpty else { return }

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


    /// This node's own held shards with their generations - the local counterpart of the peer
    /// `/held?detail=1` probe, reading each shard's header straight off disk.
    private static func localHeldShards(
        app: Application, bucketName: String, key: String, versionId: String?
    ) async -> [ClusterReplicationClient.HeldShard] {
        let indices = ErasureCodedObjectHandler.locallyHeldShardIndices(
            bucketName: bucketName, key: key, versionId: versionId)
        guard !indices.isEmpty else { return [] }
        // `map`, never `compactMap`: the generation is an enrichment, so a shard whose header
        // can't be read at this instant (it is being written right now, say) is still reported as
        // held, with its generation unknown. Dropping it would understate what this node has, and
        // enough missing indices turns an object that exists into a NoSuchKey.
        let read = try? await S3Service.offloadBlockingIO(app) {
            indices.map { index -> ClusterReplicationClient.HeldShard in
                let path = ErasureCodedObjectHandler.shardPath(
                    bucketName: bucketName, key: key, versionId: versionId, shardIndex: index)
                guard let reader = try? ErasureCodedShardReader(path: path) else {
                    return ClusterReplicationClient.HeldShard(
                        index: index, etag: "", updatedAtMillis: 0)
                }
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
    private struct Generation: Hashable {
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
