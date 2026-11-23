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

enum ErasureCodedCoordinatorError: Error, CustomStringConvertible {
    case peerCountMismatch(expected: Int, actual: Int)
    case quorumNotReached(required: Int, achieved: Int)

    var description: String {
        switch self {
        case .peerCountMismatch(let expected, let actual):
            "Expected \(expected) peers for erasure-coded placement, got \(actual)"
        case .quorumNotReached(let required, let achieved):
            "Erasure-coded write quorum not reached: needed \(required) shards acked, only \(achieved) succeeded"
        }
    }
}

/// Coordinates one erasure-coded write end to end: encode locally to scratch, place this node's
/// own shard (rank-0 = shard index 0) directly, fan the rest out to `peers` (the other `k+m-1`
/// responsible nodes, already in HRW rank order - `peers[i]` always owns shard index `i+1`), and
/// wait for `PlacementService.ecQuorumThreshold` acks before returning. Only the *coordinator*
/// (rank-0, pinned by `ObjectRoutingService.erasureCodedRoutingDecision`) ever calls this.
enum ErasureCodedWriteCoordinator {
    /// Throws `.quorumNotReached` (never partial success) when fewer than `ecQuorumThreshold`
    /// shards land synchronously, rolling back every shard that DID land first - an EC object
    /// with too few shards isn't reconstructable at all, so quorum is a hard gate here, unlike
    /// plain replication's soft catch-up. `priorLatestVersionId` lets rollback restore `.latest`
    /// to the pre-write version. `ecConfig` is a plain shard-count pair (not the concrete config
    /// type) so other callers can drive the same write path with their own shard counts.
    static func write(
        app: Application,
        bucketName: String,
        key: String,
        objectMeta: ObjectMeta,
        payloadSources: [(path: String, offset: Int, size: Int)],
        peers: [ClusterNodeInfo],
        ecConfig: (dataShards: Int, parityShards: Int),
        priorLatestVersionId: String? = nil,
        stripeUnitSize: Int = Constants.erasureCodingStripeUnitSize
    ) async throws {
        let totalShards = ecConfig.dataShards + ecConfig.parityShards
        guard peers.count == totalShards - 1 else {
            throw ErasureCodedCoordinatorError.peerCountMismatch(
                expected: totalShards - 1, actual: peers.count)
        }

        let scratchDir = Constants.erasureCodingScratchDirectory + UUID().uuidString + "/"
        defer { try? FileManager.default.removeItem(atPath: scratchDir) }

        let versionId = objectMeta.versionId
        let localShardPath0 = ErasureCodedObjectHandler.shardPath(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: 0)

        let scratchPaths = try await S3Service.offloadBlockingIO(app) {
            try StripeEncoder.encode(
                objectMeta: objectMeta, payloadSources: payloadSources,
                dataShards: ecConfig.dataShards, parityShards: ecConfig.parityShards,
                stripeUnitSize: stripeUnitSize,
                shardPath: { "\(scratchDir)\($0).ecshard" })
        }

        // A versioned write lands in a fresh per-version directory, so nothing pre-existing is
        // at risk and shards can go straight into place. A non-versioned write reuses the one
        // directory the previous generation lives in: its shards are staged everywhere (locally
        // the scratch files already are the stage) and only moved into place after quorum -
        // see `ErasureCodedObjectHandler.stagingDirectory`.
        let stagingToken: String? = versionId == nil ? UUID().uuidString : nil
        let scratchPath0 = scratchPaths[0]
        if stagingToken == nil {
            try await S3Service.offloadBlockingIO(app) {
                try copyIntoPlace(from: scratchPath0, to: localShardPath0)
                StorageStatsService.shared.noteChange(bucketName: bucketName)
            }
        }

        var delivered: Set<Int> = [0]
        let quorum = PlacementService.ecQuorumThreshold(
            dataShards: ecConfig.dataShards, parityShards: ecConfig.parityShards)

        // `!peers.isEmpty`, NOT `delivered.count < quorum`: a narrow layout can meet quorum from
        // the local shard alone (metadata on a 2-node cluster auto-caps to k=1/m=1, whose
        // threshold is 1), and skipping the fan-out there acked the write as durable while a
        // perfectly reachable second node sat idle and the only other copy went to the outbox.
        // Losing that node before the outbox drained lost the record. Attempting delivery costs
        // nothing extra - undelivered shards fall through to the outbox exactly as before.
        if !peers.isEmpty {
            await withTaskGroup(of: (shardIndex: Int, ok: Bool).self) { group in
                for (offset, peer) in peers.enumerated() {
                    let shardIndex = offset + 1
                    let sourcePath = scratchPaths[shardIndex]
                    group.addTask {
                        let ok = await attemptImmediateShardPush(
                            app: app, node: peer, sourcePath: sourcePath, bucketName: bucketName,
                            key: key, versionId: versionId, shardIndex: shardIndex,
                            stagingToken: stagingToken)
                        return (shardIndex, ok)
                    }
                }
                for await outcome in group {
                    if outcome.ok { delivered.insert(outcome.shardIndex) }
                    // A staged write must hear every outcome: a push that lands after quorum
                    // still has to be committed, or that peer keeps the previous generation.
                    // The group awaits the stragglers either way, so this costs no latency.
                    if stagingToken == nil, delivered.count >= quorum { break }
                }
                // In-place writes: stragglers keep running in the background (structured
                // concurrency awaits them even though this loop stopped listening) - same
                // reasoning as ClusterReplicationService.replicateWrite; their eventual success
                // is harmless (the receiver just re-writes the identical shard) even after
                // rollback below.
            }
        }

        guard delivered.count >= quorum else {
            await rollback(
                app: app, bucketName: bucketName, key: key, versionId: versionId, peers: peers,
                delivered: delivered, priorLatestVersionId: priorLatestVersionId,
                stagingToken: stagingToken)
            throw ErasureCodedCoordinatorError.quorumNotReached(
                required: quorum, achieved: delivered.count)
        }

        var committed = delivered
        if let stagingToken {
            do {
                try await S3Service.offloadBlockingIO(app) {
                    try copyIntoPlace(from: scratchPath0, to: localShardPath0)
                    StorageStatsService.shared.noteChange(bucketName: bucketName)
                }
            } catch {
                // Nothing has replaced the old generation anywhere yet - drop the stages.
                await rollback(
                    app: app, bucketName: bucketName, key: key, versionId: versionId, peers: peers,
                    delivered: delivered, priorLatestVersionId: priorLatestVersionId,
                    stagingToken: stagingToken)
                throw error
            }
            // A peer whose commit fails is treated as undelivered: the outbox reconstructs its
            // shard from the committed majority, exactly as for a push that never arrived.
            committed = await commitStaged(
                app: app, token: stagingToken, bucketName: bucketName, key: key, peers: peers,
                delivered: delivered)
        }

        let undeliveredShardIndices = (1..<totalShards).filter { !committed.contains($0) }
        if !undeliveredShardIndices.isEmpty {
            await enqueueOutbox(
                app: app, bucketName: bucketName, key: key, versionId: versionId,
                shardIndices: undeliveredShardIndices, peers: peers)
        }
    }

    /// Best-effort: undoes every shard that landed before quorum failed, then repairs the
    /// `.latest` pointer. Failures here are logged, not thrown - an orphaned shard is reclaimed by
    /// `ErasureCodedRebalanceService`'s health sweep anyway. The pointer repair is load-bearing:
    /// peers that received the shard already repointed `.latest` to this now-deleted version, so
    /// leaving it unrepaired would 404 a previously-readable object. Broadcast to all peers
    /// (idempotent) and applied locally; a nil `versionId` skips it (non-versioned write).
    private static func rollback(
        app: Application, bucketName: String, key: String, versionId: String?,
        peers: [ClusterNodeInfo], delivered: Set<Int>, priorLatestVersionId: String?,
        stagingToken: String? = nil
    ) async {
        if let stagingToken {
            // Staged write: nothing is in place anywhere, so the previous generation is intact.
            // Stages on peers that acked are dropped synchronously; a peer that timed out may
            // hold one too, but is probably down, so that discard is detached (see commitStaged).
            await withTaskGroup(of: Void.self) { group in
                for (offset, peer) in peers.enumerated() {
                    if delivered.contains(offset + 1) {
                        group.addTask {
                            await ClusterReplicationClient.discardStagedShards(
                                app: app, to: peer, token: stagingToken)
                        }
                    } else {
                        Task { await ClusterReplicationClient.discardStagedShards(app: app, to: peer, token: stagingToken) }
                    }
                }
            }
            return
        }
        if delivered.contains(0) {
            try? FileManager.default.removeItem(
                atPath: ErasureCodedObjectHandler.shardBasePath(
                    bucketName: bucketName, key: key, versionId: versionId))
        }
        await withTaskGroup(of: Void.self) { group in
            for (offset, peer) in peers.enumerated() where delivered.contains(offset + 1) {
                group.addTask {
                    try? await ClusterReplicationClient.deleteShard(
                        app: app, to: peer, bucketName: bucketName, key: key, versionId: versionId)
                }
            }
        }

        guard versionId != nil else { return }
        ErasureCodedObjectHandler.restoreLatest(
            bucketName: bucketName, key: key, priorVersionId: priorLatestVersionId)
        await withTaskGroup(of: Void.self) { group in
            for peer in peers {
                group.addTask {
                    await ClusterReplicationClient.restoreShardLatest(
                        app: app, to: peer, bucketName: bucketName, key: key,
                        priorVersionId: priorLatestVersionId)
                }
            }
        }
    }

    private static func copyIntoPlace(from scratchPath: String, to finalPath: String) throws {
        // Cross-directory rename isn't guaranteed atomic across filesystems - copy via
        // AtomicObjectWriter (temp file next to finalPath, then same-filesystem rename), same
        // approach the receiving side of a network shard push uses.
        var writer = try AtomicObjectWriter(finalPath: finalPath)
        do {
            var reader = WindowedFileReader()
            try reader.readFile(at: scratchPath) { try writer.writeRaw($0) }
            try writer.finish()
        } catch {
            writer.abort()
            throw error
        }
    }

    /// Commits the stages on every peer that received its shard, in parallel. Returns the
    /// shard indices now in place (local shard 0 included); a failed commit also drops that
    /// peer's stage so it can't be mistaken for live data later.
    private static func commitStaged(
        app: Application, token: String, bucketName: String, key: String,
        peers: [ClusterNodeInfo], delivered: Set<Int>
    ) async -> Set<Int> {
        // A push that timed out on our side may still have landed: drop that stage so the outbox
        // repair is the only thing that ever updates this peer. Detached, never awaited - the
        // peer is most likely down, and a request to a down host waits out the HTTP client's
        // connection backoff (seconds), which must not be added to the write's latency.
        for (offset, peer) in peers.enumerated() where !delivered.contains(offset + 1) {
            Task { await ClusterReplicationClient.discardStagedShards(app: app, to: peer, token: token) }
        }

        var committed: Set<Int> = [0]
        await withTaskGroup(of: (shardIndex: Int, ok: Bool).self) { group in
            for (offset, peer) in peers.enumerated() where delivered.contains(offset + 1) {
                group.addTask {
                    do {
                        try await ClusterReplicationService.withTimeout(
                            ClusterReplicationService.synchronousTimeout
                        ) {
                            try await ClusterReplicationClient.commitStagedShards(
                                app: app, to: peer, token: token, bucketName: bucketName, key: key)
                        }
                        return (offset + 1, true)
                    } catch {
                        app.logger.warning(
                            "Committing staged EC shard \(offset + 1) of '\(key)' on node \(peer.id) failed - falling back to async repair: \(error)"
                        )
                        // Detached for the same reason as above: the peer may have just died.
                        Task { await ClusterReplicationClient.discardStagedShards(app: app, to: peer, token: token) }
                        return (offset + 1, false)
                    }
                }
            }
            for await outcome in group where outcome.ok {
                committed.insert(outcome.shardIndex)
            }
        }
        return committed
    }

    private static func attemptImmediateShardPush(
        app: Application, node: ClusterNodeInfo, sourcePath: String, bucketName: String,
        key: String, versionId: String?, shardIndex: Int, stagingToken: String?
    ) async -> Bool {
        do {
            try await ClusterReplicationService.withTimeout(ClusterReplicationService.synchronousTimeout) {
                try await ClusterReplicationClient.pushShard(
                    app: app, to: node, sourcePath: sourcePath, bucketName: bucketName, key: key,
                    versionId: versionId, shardIndex: shardIndex, stagingToken: stagingToken)
            }
            return true
        } catch {
            app.logger.warning(
                "Synchronous EC shard push of '\(key)' shard \(shardIndex) to node \(node.id) failed or timed out - falling back to async retry: \(error)"
            )
            return false
        }
    }

    private static func enqueueOutbox(
        app: Application, bucketName: String, key: String, versionId: String?,
        shardIndices: [Int], peers: [ClusterNodeInfo]
    ) async {
        for shardIndex in shardIndices {
            let peer = peers[shardIndex - 1]
            let task = ErasureCodedReplicationTask(
                bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex,
                operation: .put, targetNodeId: peer.id, reason: .write)
            await OutboxMailbox.enqueue(
                app: app, collection: OutboxCollections.erasureCodedReplicationTasks, row: task)
        }
        ErasureCodedDispatcher.shared.wake()
    }

    /// Rewrites an EC object's metadata (tags, custom `x-amz-meta-*`, content-type) in place -
    /// the EC counterpart of `ObjectFileHandler.rewriteMetadata`. Never touches payload bytes;
    /// each of the `k+m` nodes rewrites just its own shard's header. Best-effort fan-out to
    /// peers, matching `ClusterReplicationService.replicateWrite`'s handling of the equivalent
    /// plain-object metadata push - an in-place edit has no outbox task backing it, so a peer
    /// that misses this update just stays stale until the next full write to that key.
    static func rewriteMetadata(
        app: Application, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo], selfNodeId: UUID,
        transform: @escaping @Sendable (inout ObjectMeta) -> Void
    ) async throws -> ObjectMeta {
        // Whatever indices this node actually holds - not the one its rank implies, which is a
        // different thing entirely once placement has drifted.
        let localIndices = ErasureCodedObjectHandler.locallyHeldShardIndices(
            bucketName: bucketName, key: key, versionId: versionId)
        guard let firstIndex = localIndices.first else {
            throw ErasureCodedRebalanceError.insufficientResponsibleNodes(
                required: 1, found: 0)
        }

        let updatedHeader = try await S3Service.offloadBlockingIO(app) {
            var header: ErasureCodedShardHeader?
            for index in localIndices {
                let path = ErasureCodedObjectHandler.shardPath(
                    bucketName: bucketName, key: key, versionId: versionId, shardIndex: index)
                let rewritten = try ErasureCodedObjectHandler.rewriteShardMetadata(
                    at: path, transform: transform)
                if index == firstIndex { header = rewritten }
            }
            return header!
        }
        let updatedMeta = updatedHeader.objectMeta

        // The index sent is a hint only; each peer patches whatever it holds.
        await withTaskGroup(of: Void.self) { group in
            for peer in responsible where peer.id != selfNodeId {
                group.addTask {
                    try? await ClusterReplicationClient.patchShardMetadata(
                        app: app, to: peer, bucketName: bucketName, key: key, versionId: versionId,
                        shardIndex: 0, meta: updatedMeta)
                }
            }
        }
        return updatedMeta
    }
}
