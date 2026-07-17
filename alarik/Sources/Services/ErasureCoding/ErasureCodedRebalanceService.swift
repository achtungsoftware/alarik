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

import Fluent
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

/// Self-healing for erasure-coded shards: reconstruction-based delivery (used by
/// `ErasureCodedDispatcher` for every outbox reason - a shard that failed to land during the
/// original write is no different from one lost to a later node departure, both are just
/// "currently missing, rebuildable from any `k` survivors") plus the membership-change-triggered
/// walk that detects gaps and stale local copies in the first place.
///
/// Unlike `ClusterRebalanceService` (where the node holding a full copy can push it to every
/// other responsible node directly), no single EC node ever holds more than one shard - "copying"
/// a shard to a node that's missing it is only possible via reconstruction from survivors. To
/// avoid every one of the `k+m` responsible nodes redundantly probing all `k+m` peers on every
/// membership change, only the *current* rank-0 for a key runs its full gap-fill sweep - the same
/// single-coordinator reasoning `ObjectRoutingService.erasureCodedRoutingDecision` already applies
/// to writes.
enum ErasureCodedRebalanceService {
    enum RebalanceReason: String {
        case membershipChange
        case manualResync
    }

    static let gatedReclaimFollowUpDelay: Duration = .seconds(30)

    /// Reconstructs the shard at `shardIndex` of (bucketName, key, versionId) from up to
    /// `dataShards + 1` currently-available survivors (one spare, so a single bad stripe
    /// checksum among the gathered sources doesn't fail the whole reconstruction) and pushes the
    /// result to `targetNodeId`. A no-op if the target already has it (checked first, so a
    /// straggler that actually landed after the write coordinator stopped listening, or a repeat
    /// dispatcher attempt, never does redundant work). This is the single delivery mechanism
    /// `ErasureCodedDispatcher` uses for every `.put`-operation outbox row, regardless of reason.
    static func reconstructAndPlaceShard(
        app: Application, bucketName: String, key: String, versionId: String?, shardIndex: Int,
        targetNodeId: UUID
    ) async throws {
        guard let target = await ClusterNodeCache.shared.get(id: targetNodeId) else {
            throw ErasureCodedRebalanceError.unknownTarget(targetNodeId)
        }
        // Idempotency short-circuit - but ONLY for versioned objects, where each version has its
        // own shard directory, so "a shard already exists at this path" reliably means "the correct
        // shard is already there". For a NON-versioned object every generation reuses the same path,
        // so an existing shard may be a STALE copy a down replica missed while an overwrite landed
        // elsewhere - exactly the async catch-up case. Skipping on mere existence there would leave
        // the stale shard forever (and, once reads reject cross-generation mixes, make the object
        // unreadable from that node). Non-versioned therefore always reconstructs and overwrites,
        // matching how plain replication's catch-up unconditionally re-pushes the current object.
        if versionId != nil,
            await ClusterReplicationClient.shardExists(
                app: app, node: target, bucketName: bucketName, key: key, versionId: versionId,
                shardIndex: shardIndex)
        {
            return
        }
        guard let ecConfig = app.storage[ClusterErasureCodingConfigKey.self] else {
            throw ErasureCodedRebalanceError.notConfigured
        }

        let active = await ClusterNodeCache.shared.activeNodes()
        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: active, count: ecConfig.totalShards)
        // Deliberately not `responsible.count == ecConfig.totalShards`: the cluster commonly
        // runs below full k+m strength (one node drained or down), and that's exactly when
        // self-healing/redistribution matters most, not a condition to refuse to run under -
        // requiring full strength here meant a single missing node permanently blocked every
        // reindex of the *surviving* shards, since this error never clears until the missing
        // node comes back. All that's actually needed: a rank for `shardIndex` must currently
        // exist to place onto, and enough survivors (discovered by what each node actually holds,
        // not by a rank==index assumption that's false after any membership change) to reconstruct.
        guard shardIndex < responsible.count else {
            throw ErasureCodedRebalanceError.insufficientResponsibleNodes(
                required: shardIndex + 1, found: responsible.count)
        }

        let gathered: GatheredShards
        do {
            gathered = try await ErasureCodedShardGatherer.gather(
                app: app, bucketName: bucketName, key: key, versionId: versionId,
                responsible: responsible, selfNodeId: (app.storage[ClusterConfigurationKey.self]?.nodeId ?? target.id),
                needed: ecConfig.dataShards, wantSpare: true, excludingIndex: shardIndex,
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
                dataShards: ecConfig.dataShards, parityShards: ecConfig.parityShards,
                outputPath: scratchPath)
        }

        try await ClusterReplicationClient.pushShard(
            app: app, to: target, sourcePath: scratchPath, bucketName: bucketName, key: key,
            versionId: versionId, shardIndex: shardIndex)
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
        let active = await ClusterNodeCache.shared.activeNodes()
        guard !active.isEmpty else { return }

        let localShards = try await S3Service.offloadBlockingIO(app) {
            ErasureCodedObjectHandler.listAllLocalShards()
        }
        guard !localShards.isEmpty else { return }

        var hasGatedReclaims = false
        for (path, header) in localShards {
            let bucketHasGatedReclaims = try await rebalanceOne(
                app: app, path: path, header: header, activeNodes: active, selfNodeId: config.nodeId,
                ecConfig: ecConfig)
            hasGatedReclaims = hasGatedReclaims || bucketHasGatedReclaims
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

        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: activeNodes, count: ecConfig.totalShards)
        let selfRank = responsible.firstIndex(where: { $0.id == selfNodeId })

        guard let selfRank else {
            // No longer responsible at all - reclaim, gated on the node now ranked at this
            // shard's own index already holding a confirmed copy.
            return try await reclaimIfSafe(
                app: app, bucketName: bucketName, key: key, versionId: versionId,
                staleShardIndex: header.shardIndex, responsible: responsible, ecConfig: ecConfig)
        }

        guard selfRank == 0 else {
            // Not rank-0 for this key - if this local file's index no longer matches this
            // node's current rank, it's stale (rank-0's sweep below will deliver the correct
            // index here); safe to reclaim once that's confirmed delivered.
            if header.shardIndex != selfRank {
                return try await reclaimIfSafe(
                    app: app, bucketName: bucketName, key: key, versionId: versionId,
                    staleShardIndex: header.shardIndex, responsible: responsible, ecConfig: ecConfig)
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
        var candidates: [(shardIndex: Int, targetNodeId: UUID)] = []
        for (rank, node) in responsible.enumerated() {
            let exists = await ClusterReplicationClient.shardExists(
                app: app, node: node, bucketName: bucketName, key: key, versionId: versionId,
                shardIndex: rank)
            if !exists {
                candidates.append((rank, node.id))
            }
        }
        try await enqueueReconstructTasks(
            app: app, bucketName: bucketName, key: key, versionId: versionId,
            candidates: candidates, reason: .rebalance)
    }

    /// Rebuilds shards that are *genuinely missing* - held by no responsible node - onto their
    /// current rank-holders, reconstructing from `k` survivors. Drives read-repair of missing
    /// shards and the scrubber's post-delete rebuild. Deliberately does NOT act on "corrupt"
    /// shards reported by a read's decode: a checksum failure seen while decoding a *fetched* copy
    /// can be transit damage rather than on-disk rot, so deleting the source would risk destroying
    /// a healthy shard. Corruption is instead confirmed and healed by the node that actually holds
    /// the copy (`ErasureCodedScrubber.verifyAndHealObjectShards`, via the scrubber or a read-
    /// triggered verify-heal request) - authoritative, with no transit ambiguity. Best-effort and
    /// off the response path; enqueues via the same deduped outbox `sweepGaps` uses.
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

        let existingTasks = try await ErasureCodedReplicationTask.query(on: app.db)
            .filter(\.$bucketName == bucketName)
            .filter(\.$key == key)
            .filter(\.$operation == ErasureCodedReplicationTask.Operation.put.rawValue)
            .filter(
                \.$state
                    ~~ [
                        ErasureCodedReplicationTask.State.pending.rawValue,
                        ErasureCodedReplicationTask.State.failed.rawValue,
                    ]
            )
            .all()

        // Dedup key includes versionId: without it, a pending task for one version of this key
        // would suppress enqueueing the same (shardIndex, target) pair for a *different* version,
        // silently leaving that other version's gap unfilled. The `existingTasks` query above
        // fetches every version's tasks for the key, so the version must be part of the identity.
        func pairKey(_ versionId: String?, _ shardIndex: Int, _ targetNodeId: UUID) -> String {
            "\(versionId ?? "")\u{0}\(shardIndex)\u{0}\(targetNodeId)"
        }
        var pendingPairs: Set<String> = []
        for task in existingTasks where task.state == ErasureCodedReplicationTask.State.pending.rawValue {
            pendingPairs.insert(pairKey(task.versionId, task.shardIndex, task.targetNodeId))
        }
        for task in existingTasks where task.state == ErasureCodedReplicationTask.State.failed.rawValue {
            try await task.delete(on: app.db)
        }

        let newTasks = candidates
            .filter { !pendingPairs.contains(pairKey(versionId, $0.shardIndex, $0.targetNodeId)) }
            .map { candidate in
                ErasureCodedReplicationTask(
                    bucketName: bucketName, key: key, versionId: versionId,
                    shardIndex: candidate.shardIndex, operation: .put,
                    targetNodeId: candidate.targetNodeId, reason: reason)
            }
        guard !newTasks.isEmpty else { return }
        try await newTasks.create(on: app.db)
        ErasureCodedDispatcher.shared.wake()
    }

    /// Deletes this node's stale/orphaned local shard once the object is positively confirmed
    /// reconstructable from the *current* responsible set without it - mirrors
    /// `ClusterRebalanceService`'s "no outstanding task means delivered" reclaim gate, but adds
    /// a second, positive check `ClusterRebalanceService` doesn't need: unlike whole-object
    /// replication (where "no pending copy task" reliably means "already delivered", since the
    /// source node created that task itself before this check ever runs), an EC shard's
    /// redistribution task is created by a *different* node (whichever currently ranks 0 for
    /// this key) via an independent, unsynchronized rebalance pass. "Zero outstanding tasks" is
    /// also trivially true *before* that node's sweep has run at all - a real race that would
    /// otherwise let this node delete its only copy of a shard before anyone has reconstructed
    /// it elsewhere. Positively counting how many of `responsible`'s nodes already hold their
    /// own correct-index shard closes that gap: only reclaim once there are independently enough
    /// (>= `ecConfig.dataShards`) to reconstruct without this one.
    private static func reclaimIfSafe(
        app: Application, bucketName: String, key: String, versionId: String?,
        staleShardIndex: Int, responsible: [ClusterNodeInfo], ecConfig: ClusterErasureCodingConfig
    ) async throws -> Bool {
        // Scoped to this version: an in-flight repair of a *different* version of the same key
        // mustn't hold this version's stale shard hostage. (Optional-field equality is handled
        // in-memory to sidestep Fluent's `nil` vs `.null` filter ambiguity for the plain,
        // versionless path.)
        let keyTasks = try await ErasureCodedReplicationTask.query(on: app.db)
            .filter(\.$bucketName == bucketName)
            .filter(\.$key == key)
            .filter(
                \.$state
                    ~~ [
                        ErasureCodedReplicationTask.State.pending.rawValue,
                        ErasureCodedReplicationTask.State.failed.rawValue,
                    ]
            )
            .all()
        let outstanding = keyTasks.filter { $0.versionId == versionId }.count
        guard outstanding == 0 else { return true }

        // Positive safety gate: only drop this stale shard once at least `dataShards` of the
        // currently-responsible nodes independently hold their own correct-index shard, so the
        // object stays reconstructable without this copy. Discovery-based, so it's correct even
        // while indices are still drifting toward their new ranks.
        var healthyCount = 0
        for (rank, node) in responsible.enumerated() {
            if await ClusterReplicationClient.shardExists(
                app: app, node: node, bucketName: bucketName, key: key, versionId: versionId,
                shardIndex: rank)
            {
                healthyCount += 1
            }
        }
        guard healthyCount >= ecConfig.dataShards else { return true }

        // Surgical: remove only the one stale shard file, never the whole `.ecshards` directory -
        // mid-reindex this node can hold both its freshly delivered new-rank shard and this stale
        // old-rank one in the same directory, and taking the live shard with it would destroy data
        // and re-trigger reconstruction in a churn loop.
        ErasureCodedObjectHandler.removeLocalShard(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: staleShardIndex)
        return false
    }
}
