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

#if canImport(Darwin)
    import Darwin
#elseif canImport(Glibc)
    import Glibc
#elseif canImport(Musl)
    import Musl
#endif

import Foundation
import Vapor

enum ErasureCodedRebalanceError: Error, CustomStringConvertible {
    case unknownTarget(UUID)
    case notConfigured
    case insufficientResponsibleNodes(required: Int, found: Int)
    case tooFewSurvivors(needed: Int, available: Int)

    var description: String {
        switch self {
        case .unknownTarget(let id):
            "Target node \(id) is not in the active membership cache"
        case .notConfigured:
            "Erasure coding is not configured on this node"
        case .insufficientResponsibleNodes(let required, let found):
            "Need \(required) currently-active responsible nodes to reconstruct, only \(found) found"
        case .tooFewSurvivors(let needed, let available):
            "Need \(needed) healthy source shards to reconstruct, only \(available) available"
        }
    }
}

/// Self-healing for erasure-coded shards: reconstruction-based delivery for `ErasureCodedDispatcher`
/// (a shard missing from the original write is no different from one lost later, both are just
/// "rebuildable from any `k` survivors"), plus the membership-change walk that detects gaps and
/// stale local copies. Unlike `ClusterRebalanceService`, no EC node ever holds more than one
/// shard - "copying" only works via reconstruction, so only the current rank-0 for a key runs the
/// full gap-fill sweep, avoiding every responsible node redundantly probing all peers.
enum ErasureCodedRebalanceService {
    enum RebalanceReason: String {
        case membershipChange
        case manualResync
    }

    static let gatedReclaimFollowUpDelay: Duration = .seconds(30)
    /// Caps how many local shards `rebalance`'s walk processes concurrently. Was 16; brought down
    /// after evidence showed a freshly-rejoined node unable to promptly serve unrelated foreground
    /// requests for several seconds - genuine CPU/thread contention from too many concurrent
    /// Reed-Solomon operations, not just network queuing. 8 keeps most of the wall-clock gain over
    /// a fully-sequential walk while leaving headroom for foreground requests.
    static let maxConcurrentShardRebalances = 8

    /// Resolves the shard counts a specific `bucketName`'s shards were *actually* encoded with -
    /// the object-data config for a regular bucket, or the independently-configured (auto-capped)
    /// metadata config for `.alarik.sys`. Every placement/reconstruction computation here must go
    /// through this rather than trusting the object-data `ecConfig` directly, since metadata is
    /// often encoded with a different k/m and using the wrong one corrupts rank/gap-fill decisions.
    private static func shardCounts(
        forBucket bucketName: String, app: Application, ecConfig: ClusterErasureCodingConfig,
        activeNodeCount: Int
    ) -> (dataShards: Int, parityShards: Int) {
        guard MetadataNamespace.isReserved(bucketName) else {
            return (ecConfig.dataShards, ecConfig.parityShards)
        }
        let metadataConfig = app.storage[ClusterMetadataErasureCodingConfigKey.self] ?? .default
        return metadataConfig.effective(activeNodeCount: activeNodeCount)
    }

    /// Reconstructs the shard at `shardIndex` of (bucketName, key, versionId) from up to
    /// `dataShards + 1` survivors (one spare, so a single bad checksum doesn't fail the whole
    /// reconstruction) and pushes it to `targetNodeId`. A no-op if the target already has it.
    /// The single delivery mechanism `ErasureCodedDispatcher` uses for every `.put` outbox row.
    static func reconstructAndPlaceShard(
        app: Application, bucketName: String, key: String, versionId: String?, shardIndex: Int,
        targetNodeId: UUID
    ) async throws {
        guard let target = await ClusterNodeCache.shared.get(id: targetNodeId) else {
            throw ErasureCodedRebalanceError.unknownTarget(targetNodeId)
        }
        // Idempotency short-circuit, but ONLY for versioned objects (each version has its own
        // shard directory, so existence reliably means correctness). A non-versioned object
        // reuses the same path across generations, so an existing shard may be a stale copy a
        // down replica missed - always reconstruct and overwrite there instead.
        if versionId != nil,
            await ClusterReplicationClient.shardExists(
                app: app, node: target, bucketName: bucketName, key: key, versionId: versionId,
                shardIndex: shardIndex)
        {
            return
        }
        // Placement set, not the live one: reconstructing must target the same nodes the
        // write did, or a shard lands somewhere no reader will look for it.
        let active = await ClusterNodeCache.shared.placementNodes()
        let selfNodeId = app.storage[ClusterConfigurationKey.self]?.nodeId ?? target.id

        // A metadata shard this node already holds is pushed verbatim. The reconstruction
        // below rebuilds a shard from the OTHER shards, which cannot work for a k=1/m=0
        // record: there are no other shards, so the gather (which excludes this very index)
        // finds nothing, takes the record as deleted, and returns silently placing nothing.
        if MetadataNamespace.isReserved(bucketName) {
            let localPath = ErasureCodedObjectHandler.shardPath(
                bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex)
            if FileManager.default.fileExists(atPath: localPath) {
                if target.id != selfNodeId {
                    try await ClusterReplicationClient.pushShard(
                        app: app, to: target, sourcePath: localPath, bucketName: bucketName,
                        key: key, versionId: versionId, shardIndex: shardIndex)
                }
                return
            }
        }

        // For metadata, discover the TRUE as-written (dataShards, parityShards) rather than
        // trusting the live recompute - see `rebalanceOne`'s identical reasoning. Unlike
        // `rebalanceOne` (which already has this shard's own header in hand), the shard being
        // reconstructed here doesn't exist locally yet, so the ground truth has to be probed from
        // whoever still holds a copy - exactly `MetadataStore.discoverShardCounts`'s job, reused
        // directly rather than duplicated.
        let (dataShards, parityShards): (Int, Int)
        if MetadataNamespace.isReserved(bucketName) {
            if let discovered = await MetadataStore.discoverShardCounts(
                app: app, key: key, candidates: active, selfNodeId: selfNodeId)
            {
                (dataShards, parityShards) = (discovered.dataShards, discovered.totalShards - discovered.dataShards)
            } else {
                guard let ecConfig = app.storage[ClusterErasureCodingConfigKey.self] else {
                    throw ErasureCodedRebalanceError.notConfigured
                }
                (dataShards, parityShards) = shardCounts(
                    forBucket: bucketName, app: app, ecConfig: ecConfig,
                    activeNodeCount: active.count)
            }
        } else {
            guard let ecConfig = app.storage[ClusterErasureCodingConfigKey.self] else {
                throw ErasureCodedRebalanceError.notConfigured
            }
            (dataShards, parityShards) = shardCounts(
                forBucket: bucketName, app: app, ecConfig: ecConfig, activeNodeCount: active.count)
        }
        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: active,
            count: dataShards + parityShards)
        // Deliberately not `responsible.count == dataShards + parityShards`: the cluster commonly
        // runs below full k+m strength (a node drained or down), and that's exactly when
        // self-healing matters most - requiring full strength would permanently block reindexing
        // survivors until the missing node returns. Only a rank for `shardIndex` and enough
        // discovered survivors to reconstruct are actually needed.
        guard shardIndex < responsible.count else {
            throw ErasureCodedRebalanceError.insufficientResponsibleNodes(
                required: shardIndex + 1, found: responsible.count)
        }

        // Gather candidates: `responsible` for regular object data (k/m never drifts from the
        // live recompute); the full active set for metadata, since a metadata record's true
        // holder(s) can fall outside the current `responsible` window entirely. `gather` queries
        // each candidate for what it actually holds, so widening here is as safe as
        // `MetadataStore.localGet`'s own widen-to-all-active fallback.
        let gatherCandidates = MetadataNamespace.isReserved(bucketName) ? active : responsible
        let gathered: GatheredShards
        do {
            gathered = try await ErasureCodedShardGatherer.gather(
                app: app, bucketName: bucketName, key: key, versionId: versionId,
                responsible: gatherCandidates, selfNodeId: selfNodeId,
                needed: dataShards, wantSpare: true, excludingIndex: shardIndex,
                requestId: UUID().uuidString)
        } catch ErasureCodedGatherError.notFound {
            // The object no longer exists anywhere (deleted out from under an in-flight rebalance
            // task) - nothing to reconstruct; treat as done so the outbox row is cleared.
            return
        } catch ErasureCodedGatherError.degraded(_, let needed) {
            throw ErasureCodedRebalanceError.tooFewSurvivors(needed: needed, available: 0)
        }
        defer { gathered.cleanup() }

        let scratchPath = Constants.erasureCodingScratchDirectory + UUID().uuidString + ".ecshard"
        defer { _ = POSIXFile.unlink(scratchPath) }
        let availablePaths = gathered.shards.mapValues(\.path)
        try await S3Service.offloadBlockingIO(app) {
            try reconstructShardFile(
                availablePaths: availablePaths, missingIndex: shardIndex,
                dataShards: dataShards, parityShards: parityShards,
                outputPath: scratchPath)
        }

        if target.id == selfNodeId {
            // The dispatcher's target-only delivery gate (`ErasureCodedDispatcher`) means this is
            // now the common case: the node reconstructing the shard IS its destination. Commit
            // directly instead of looping the bytes through an HTTP push to itself - skips a
            // redundant network round trip and a second full spool-then-copy of the same file.
            let finalPath = ErasureCodedObjectHandler.shardPath(
                bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex)
            try await S3Service.offloadBlockingIO(app) {
                try ErasureCodedObjectHandler.commitShardFile(
                    sourcePath: scratchPath, finalPath: finalPath, bucketName: bucketName, key: key)
            }
        } else {
            // Reachable via a manual resync or a future non-dispatcher caller, where the
            // reconstructing node genuinely differs from the target - falls back to the network
            // push exactly as before.
            try await ClusterReplicationClient.pushShard(
                app: app, to: target, sourcePath: scratchPath, bucketName: bucketName, key: key,
                versionId: versionId, shardIndex: shardIndex)
        }
    }

    /// Reads every gathered survivor stripe by stripe, reconstructing just `missingIndex` at
    /// each step and streaming it into a fresh `.ecshard` file - never buffers the whole object,
    /// same streaming shape as `StripeEncoder`/`StripeDecoder`. Tolerates a per-stripe checksum
    /// failure among the survivors as long as enough of the *other* gathered sources are still
    /// healthy for that specific stripe (mirrors `StripeDecoder`'s per-stripe resilience).
    private static func reconstructShardFile(
        availablePaths: [Int: String], missingIndex: Int, dataShards: Int, parityShards: Int,
        outputPath: String
    ) throws {
        var readers: [Int: ErasureCodedShardReader] = [:]
        defer { for reader in readers.values { reader.close() } }
        for (index, path) in availablePaths {
            readers[index] = try ErasureCodedShardReader(path: path)
        }
        guard let reference = readers.values.first?.header else {
            throw ErasureCodedRebalanceError.tooFewSurvivors(needed: dataShards, available: 0)
        }

        let header = ErasureCodedShardHeader(
            shardIndex: missingIndex, dataShards: dataShards, parityShards: parityShards,
            stripeUnitSize: reference.stripeUnitSize, stripeCount: reference.stripeCount,
            objectMeta: reference.objectMeta)
        var writer = try ErasureCodedShardWriter(path: outputPath, header: header)
        do {
            for stripeIndex in 0..<reference.stripeCount {
                var available: [Int: Data] = [:]
                for (index, reader) in readers {
                    if let chunk = try? reader.readStripe(stripeIndex) {
                        available[index] = chunk
                    }
                }
                guard available.count >= dataShards else {
                    throw ErasureCodedRebalanceError.tooFewSurvivors(
                        needed: dataShards, available: available.count)
                }
                let recovered = try ReedSolomonEngine.reconstruct(
                    availableShards: available, missingIndices: [missingIndex],
                    dataCount: dataShards, parityCount: parityShards)
                try writer.appendStripe(recovered[missingIndex]!)
            }
            try writer.finish()
        } catch {
            writer.abort()
            throw error
        }
    }

    // MARK: - Membership-change walk

    /// Coalesces walk requests: `clusterNode` cache NOTIFYs fire on every heartbeat refresh (not
    /// just genuine membership changes - see `CacheReloadDispatch`), and each walk is a full local
    /// `.ecshard` disk scan plus `k+m` `shardExists` probes per rank-0 key, so running one per
    /// heartbeat would be a periodic probe storm on a large store. The debouncer guarantees at
    /// most one walk in flight, folds a burst of requests-during-a-walk into a single trailing
    /// walk, and spaces walk starts by at least `minWalkInterval` under sustained churn.
    private static let debouncer = RebalanceDebouncer()
    private static let minWalkInterval: Duration = .seconds(15)

    private actor RebalanceDebouncer {
        private var running = false
        private var pending = false

        /// True when the caller should start a walk now; false when one is already in flight (the
        /// request is folded into a single trailing walk instead).
        func requestStart() -> Bool {
            if running {
                pending = true
                return false
            }
            running = true
            return true
        }

        /// After a walk finishes, whether a coalesced request arrived during it and a trailing
        /// walk should run.
        func finish() -> Bool {
            if pending {
                pending = false
                return true
            }
            running = false
            return false
        }
    }

    static func scheduleRebalance(app: Application, reason: RebalanceReason) async {
        guard app.storage[ClusterConfigurationKey.self] != nil,
            app.storage[ClusterErasureCodingConfigKey.self] != nil
        else { return }
        guard await debouncer.requestStart() else { return }
        Task {
            var runAgain = true
            while runAgain {
                do {
                    try await rebalance(app: app, reason: reason)
                } catch {
                    app.logger.error("EC rebalance walk failed (reason: \(reason)): \(error)")
                }
                runAgain = await debouncer.finish()
                if runAgain { try? await Task.sleep(for: minWalkInterval) }
            }
        }
    }

    static func rebalance(app: Application, reason: RebalanceReason) async throws {
        guard let config = app.storage[ClusterConfigurationKey.self],
            let ecConfig = app.storage[ClusterErasureCodingConfigKey.self]
        else { return }
        // Responsibility (and therefore reclaim) must be judged against registered nodes.
        // Against the live set, a peer being briefly down would reassign its keys here and this
        // walk would start moving - and reclaiming - shards that were never misplaced.
        let active = await ClusterNodeCache.shared.placementNodes()
        guard !active.isEmpty else { return }

        let localShards = try await S3Service.offloadBlockingIO(app) {
            ErasureCodedObjectHandler.listAllLocalShards()
        }
        guard !localShards.isEmpty else { return }

        // Bounded concurrency, not one shard at a time: each shard's gap-check/reclaim-safety
        // work is independent, so a sequential loop only adds latency on a node holding many
        // shards. A hard cap (not fully unbounded) keeps a single walk from flooding the
        // connection pool when local state is large.
        var hasGatedReclaims = false
        try await withThrowingTaskGroup(of: Bool.self) { group in
            var iterator = localShards.makeIterator()
            func submitNext() {
                guard let (path, header) = iterator.next() else { return }
                group.addTask {
                    try await rebalanceOne(
                        app: app, path: path, header: header, activeNodes: active,
                        selfNodeId: config.nodeId, ecConfig: ecConfig)
                }
            }
            for _ in 0..<min(maxConcurrentShardRebalances, localShards.count) {
                submitNext()
            }
            while let gated = try await group.next() {
                hasGatedReclaims = hasGatedReclaims || gated
                submitNext()
            }
        }

        if hasGatedReclaims {
            Task {
                try? await Task.sleep(for: gatedReclaimFollowUpDelay)
                await scheduleRebalance(app: app, reason: reason)
            }
        }
    }

    /// One local shard file's worth of rebalance logic. Returns whether it's a reclaim candidate
    /// still gated on outstanding work (see `ClusterRebalanceService`'s identical reasoning).
    private static func rebalanceOne(
        app: Application, path: String, header: ErasureCodedShardHeader,
        activeNodes: [ClusterNodeInfo], selfNodeId: UUID, ecConfig: ClusterErasureCodingConfig
    ) async throws -> Bool {
        let bucketName = header.objectMeta.bucketName
        let key = header.objectMeta.key
        let versionId = header.objectMeta.versionId == "null" ? nil : header.objectMeta.versionId

        // For metadata (`.alarik.sys`), the live-recomputed `shardCounts` can legitimately differ
        // from what THIS shard was actually encoded with, since auto-capping only shrinks as
        // membership grows and nothing re-encodes an existing record. Trust this shard's own
        // `header` (fixed at write time), not the live recompute, or a lone copy could be
        // reclaimed before any replacement exists. Object-data buckets never auto-cap, so the two
        // always agree there.
        let (dataShards, parityShards): (Int, Int)
        if MetadataNamespace.isReserved(bucketName) {
            (dataShards, parityShards) = (header.dataShards, header.parityShards)
        } else {
            (dataShards, parityShards) = shardCounts(
                forBucket: bucketName, app: app, ecConfig: ecConfig,
                activeNodeCount: activeNodes.count)
        }
        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: activeNodes,
            count: dataShards + parityShards)
        let selfRank = responsible.firstIndex(where: { $0.id == selfNodeId })

        // Control-plane metadata gets its own, deliberately conservative treatment. Three rules,
        // each learned from a real failure:
        //
        // 1. NEVER reclaimed. The reclaim paths below infer staleness from rank arithmetic that
        //    assumes the operator-fixed object-data k/m; metadata uses a different, auto-capping
        //    k/m, so a live record can look stale purely because the widths differ - and deleting
        //    it destroys a user or credential, not a redundant copy. An orphaned metadata shard
        //    costs a few KB; deleting a live one is unrecoverable.
        //
        // 2. Self-managed collections are skipped outright. `cluster-nodes` records are pinned to
        //    k=1/m=0 by `resolveRouting` and rewritten by their owner's heartbeat every few
        //    seconds - "widening" them can never converge (every walk re-fires forever), and
        //    gap-filling them fights their self-coordinated placement with doomed repair tasks
        //    that flood the outbox and starve real object repairs (observed as read-repair and
        //    scrub timeouts). `oidc-states` are single-use and TTL-swept. Both are exactly the
        //    tombstone-exempt set.
        //
        // 3. Durability for a narrow record is added by COPYING, never by re-encoding. A k=1/m=0
        //    record (the founding node's boot seed: admin user, its access key) IS the payload,
        //    decodable from any single copy, so replicating shard 0 onto the current responsible
        //    nodes is purely additive - a failed push leaves the record exactly as durable as
        //    before. Re-encoding in place to a wider k/m was tried and is catastrophic on
        //    mid-write failure: the surviving header demands more shards than exist while the one
        //    readable copy is already gone (this wiped the seeded admin user and locked every
        //    node out of the cluster).
        if MetadataNamespace.isReserved(bucketName) {
            if let (collection, _) = MetadataNamespace.splitKey(key),
                MetadataCollections.tombstoneExempt.contains(collection)
            {
                return false
            }

            if dataShards == 1, parityShards == 0 {
                let metadataConfig =
                    app.storage[ClusterMetadataErasureCodingConfigKey.self] ?? .default
                let width = metadataConfig.effective(activeNodeCount: activeNodes.count)
                let targets = PlacementService.responsibleNodes(
                    bucketName: bucketName, key: key, activeNodes: activeNodes,
                    count: width.dataShards + width.parityShards)
                for target in targets where target.id != selfNodeId {
                    // Skip targets that already hold a copy, so repeat walks converge to
                    // silence instead of re-pushing forever.
                    if await ClusterReplicationClient.shardExists(
                        app: app, node: target, bucketName: bucketName, key: key,
                        versionId: versionId, shardIndex: header.shardIndex)
                    {
                        continue
                    }
                    do {
                        // Push the walked shard file verbatim - NOT `reconstructAndPlaceShard`,
                        // which gathers the OTHER shards to rebuild this one; a k=1/m=0 record
                        // has no other shards, so that gather finds nothing and silently
                        // succeeds without placing anything.
                        try await ClusterReplicationClient.pushShard(
                            app: app, to: target, sourcePath: path, bucketName: bucketName,
                            key: key, versionId: versionId, shardIndex: header.shardIndex)
                        app.logger.info(
                            "Replicated single-copy metadata record \(key) to node \(target.id).")
                    } catch {
                        app.logger.debug(
                            "Could not place a copy of single-copy metadata record \(key) on \(target.id) (retried on the next walk): \(error)"
                        )
                    }
                }
            } else if selfRank == 0 {
                // Properly-width records still get the normal gap-fill sweep.
                try await sweepGaps(
                    app: app, bucketName: bucketName, key: key, versionId: versionId,
                    responsible: responsible)
            }
            return false
        }

        guard let selfRank else {
            // No longer responsible at all - reclaim, gated on the node now ranked at this
            // shard's own index already holding a confirmed copy.
            return try await reclaimIfSafe(
                app: app, bucketName: bucketName, key: key, versionId: versionId,
                staleShardIndex: header.shardIndex, responsible: responsible,
                requiredDataShards: dataShards)
        }

        guard selfRank == 0 else {
            // Not rank-0 for this key - if this local file's index no longer matches this
            // node's current rank, it's stale (rank-0's sweep below will deliver the correct
            // index here); safe to reclaim once that's confirmed delivered.
            if header.shardIndex != selfRank {
                return try await reclaimIfSafe(
                    app: app, bucketName: bucketName, key: key, versionId: versionId,
                    staleShardIndex: header.shardIndex, responsible: responsible,
                    requiredDataShards: dataShards)
            }
            return false
        }

        // Rank-0: sweep every current rank (including self) and fill any gap. Idempotent -
        // `reconstructAndPlaceShard` no-ops once a target already holds its shard, and enqueuing
        // a `.pending` task that's already outstanding is deduplicated the same way
        // `ClusterRebalanceService.rebalanceBucket` dedupes copy tasks.
        try await sweepGaps(
            app: app, bucketName: bucketName, key: key, versionId: versionId,
            responsible: responsible)
        return false
    }

    private static func sweepGaps(
        app: Application, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo]
    ) async throws {
        // Parallel, not sequential: each rank's `shardExists` check is independent, so awaiting
        // one at a time only adds latency on a node holding many shards - this runs once per
        // local rank-0 shard on every walk, and that cost compounds as local state grows.
        let candidates = await withTaskGroup(
            of: (shardIndex: Int, targetNodeId: UUID)?.self,
            returning: [(shardIndex: Int, targetNodeId: UUID)].self
        ) { group in
            for (rank, node) in responsible.enumerated() {
                group.addTask {
                    let exists = await ClusterReplicationClient.shardExists(
                        app: app, node: node, bucketName: bucketName, key: key,
                        versionId: versionId, shardIndex: rank)
                    return exists ? nil : (rank, node.id)
                }
            }
            var results: [(shardIndex: Int, targetNodeId: UUID)] = []
            for await candidate in group {
                if let candidate { results.append(candidate) }
            }
            return results
        }
        try await enqueueReconstructTasks(
            app: app, bucketName: bucketName, key: key, versionId: versionId,
            candidates: candidates, reason: .rebalance)
    }

    /// Rebuilds shards that are *genuinely missing* - held by no responsible node - onto their
    /// current rank-holders, reconstructing from `k` survivors. Drives read-repair and the
    /// scrubber's post-delete rebuild. Deliberately does NOT act on shards a read's decode
    /// reports "corrupt" - that can be transit damage rather than on-disk rot; corruption is
    /// instead confirmed and healed by the node that actually holds the copy
    /// (`ErasureCodedScrubber.verifyAndHealObjectShards`).
    static func healObject(
        app: Application, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo], missingIndices: Set<Int>
    ) async {
        let candidates: [(shardIndex: Int, targetNodeId: UUID)] =
            missingIndices
            .filter { $0 < responsible.count }
            .sorted()
            .map { ($0, responsible[$0].id) }

        do {
            try await enqueueReconstructTasks(
                app: app, bucketName: bucketName, key: key, versionId: versionId,
                candidates: candidates, reason: .reconstruct)
        } catch {
            app.logger.warning(
                "EC read-repair for '\(key)' could not enqueue reconstruction: \(error)")
        }
    }

    /// Enqueues one `.put`/reconstruct outbox row per `(shardIndex, targetNodeId)` candidate,
    /// deduped against rows already pending for this key (across all versions - `versionId` is part
    /// of the identity), and clearing any dead-lettered rows so a repair can be retried. Shared by
    /// the rank-0 gap sweep and read-repair.
    private static func enqueueReconstructTasks(
        app: Application, bucketName: String, key: String, versionId: String?,
        candidates: [(shardIndex: Int, targetNodeId: UUID)],
        reason: ErasureCodedReplicationTask.Reason
    ) async throws {
        guard !candidates.isEmpty else { return }

        // A candidate's target is whichever node ranks that shard index - could be any node in
        // the cluster, not just this one, so "already outstanding" needs the cluster-wide fan-out
        // (`OutboxMailbox` mailboxes are per-owner-node; a target's own pending/failed row for
        // this repair lives on *that* node's disk, not necessarily this one's).
        let existingTasks = await OutboxMailbox.listAllAcrossCluster(
            ErasureCodedReplicationTask.self, app: app,
            collection: OutboxCollections.erasureCodedReplicationTasks
        ).filter {
            $0.bucketName == bucketName && $0.key == key
                && $0.operation == .put
        }

        // Dedup key includes versionId: without it, a pending task for one version of this key
        // would suppress enqueueing the same (shardIndex, target) pair for a *different* version,
        // silently leaving that other version's gap unfilled. `existingTasks` fetches every
        // version's tasks for the key, so the version must be part of the identity.
        func pairKey(_ versionId: String?, _ shardIndex: Int, _ targetNodeId: UUID) -> String {
            "\(versionId ?? "")\u{0}\(shardIndex)\u{0}\(targetNodeId)"
        }
        var coveredPairs: Set<String> = []
        for task in existingTasks {
            let pair = pairKey(task.versionId, task.shardIndex, task.targetNodeId)
            if task.state == .pending {
                coveredPairs.insert(pair)
            } else if task.state == .failed {
                // Dead-lettered - reset it in place on its owning node via the cross-node retry
                // RPC (rather than deleting and recreating, which would require reaching directly
                // into a peer's mailbox directory) so a stalled repair actually gets retried.
                let retried = await OutboxMailbox.retryAcrossCluster(
                    ErasureCodedReplicationTask.self, app: app,
                    collection: OutboxCollections.erasureCodedReplicationTasks, taskId: task.id)
                if retried { coveredPairs.insert(pair) }
            }
        }

        let newTasks = candidates
            .filter { !coveredPairs.contains(pairKey(versionId, $0.shardIndex, $0.targetNodeId)) }
            .map { candidate in
                ErasureCodedReplicationTask(
                    bucketName: bucketName, key: key, versionId: versionId,
                    shardIndex: candidate.shardIndex, operation: .put,
                    targetNodeId: candidate.targetNodeId, reason: reason)
            }
        guard !newTasks.isEmpty else { return }
        for task in newTasks {
            await OutboxMailbox.enqueue(
                app: app, collection: OutboxCollections.erasureCodedReplicationTasks, row: task)
        }
        ErasureCodedDispatcher.shared.wake()
    }

    /// Deletes this node's stale/orphaned local shard once positively confirmed reconstructable
    /// without it. Unlike `ClusterRebalanceService`'s "no outstanding task means delivered" gate,
    /// an EC shard's redistribution task is created by a *different* node (rank-0), so "zero
    /// outstanding tasks" is also trivially true before that node's sweep has even run - a real
    /// race that could delete this node's only copy before a replacement exists anywhere.
    /// Positively counting how many of `responsible` already hold their own correct-index shard
    /// closes that gap.
    private static func reclaimIfSafe(
        app: Application, bucketName: String, key: String, versionId: String?,
        staleShardIndex: Int, responsible: [ClusterNodeInfo], requiredDataShards: Int
    ) async throws -> Bool {
        // Scoped to this version: an in-flight repair of a *different* version of the same key
        // mustn't hold this version's stale shard hostage. Cluster-wide fan-out for the same
        // reason `enqueueReconstructTasks` needs one - an outstanding repair's target (and thus
        // its mailbox owner) could be any node, not just this one.
        let keyTasks = await OutboxMailbox.listAllAcrossCluster(
            ErasureCodedReplicationTask.self, app: app,
            collection: OutboxCollections.erasureCodedReplicationTasks
        ).filter {
            $0.bucketName == bucketName && $0.key == key
                && ($0.state == .pending
                    || $0.state == .failed)
        }
        let outstanding = keyTasks.filter { $0.versionId == versionId }.count
        guard outstanding == 0 else { return true }

        // Positive safety gate: only drop this stale shard once at least `dataShards` of the
        // currently-responsible nodes independently hold their own correct-index shard.
        // Parallel, not sequential - see `sweepGaps`'s identical reasoning.
        let healthyCount = await withTaskGroup(of: Bool.self, returning: Int.self) { group in
            for (rank, node) in responsible.enumerated() {
                group.addTask {
                    await ClusterReplicationClient.shardExists(
                        app: app, node: node, bucketName: bucketName, key: key,
                        versionId: versionId, shardIndex: rank)
                }
            }
            var count = 0
            for await healthy in group where healthy { count += 1 }
            return count
        }
        guard healthyCount >= requiredDataShards else { return true }

        // Surgical: remove only the one stale shard file, never the whole `.ecshards` directory -
        // mid-reindex this node can hold both its freshly delivered new-rank shard and this stale
        // old-rank one in the same directory, and taking the live shard with it would destroy data
        // and re-trigger reconstruction in a churn loop.
        ErasureCodedObjectHandler.removeLocalShard(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: staleShardIndex)
        return false
    }
}
