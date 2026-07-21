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

        let scratchPath0 = scratchPaths[0]
        try await S3Service.offloadBlockingIO(app) {
            try copyIntoPlace(from: scratchPath0, to: localShardPath0)
        }

        var delivered: Set<Int> = [0]
        let quorum = PlacementService.ecQuorumThreshold(
            dataShards: ecConfig.dataShards, parityShards: ecConfig.parityShards)

        if delivered.count < quorum, !peers.isEmpty {
            await withTaskGroup(of: (shardIndex: Int, ok: Bool).self) { group in
                for (offset, peer) in peers.enumerated() {
                    let shardIndex = offset + 1
                    let sourcePath = scratchPaths[shardIndex]
                    group.addTask {
                        let ok = await attemptImmediateShardPush(
                            app: app, node: peer, sourcePath: sourcePath, bucketName: bucketName,
                            key: key, versionId: versionId, shardIndex: shardIndex)
                        return (shardIndex, ok)
                    }
                }
                for await outcome in group {
                    if outcome.ok { delivered.insert(outcome.shardIndex) }
                    if delivered.count >= quorum { break }
                }
                // Stragglers keep running in the background (structured concurrency awaits them
                // even though this loop stopped listening) - same reasoning as
                // ClusterReplicationService.replicateWrite; their eventual success is harmless
                // (the receiver just re-writes the identical shard) even after rollback below.
            }
        }

        guard delivered.count >= quorum else {
            await rollback(
                app: app, bucketName: bucketName, key: key, versionId: versionId, peers: peers,
                delivered: delivered, priorLatestVersionId: priorLatestVersionId)
            throw ErasureCodedCoordinatorError.quorumNotReached(
                required: quorum, achieved: delivered.count)
        }

        let undeliveredShardIndices = (1..<totalShards).filter { !delivered.contains($0) }
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
        peers: [ClusterNodeInfo], delivered: Set<Int>, priorLatestVersionId: String?
    ) async {
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
            let sourceFd = POSIXFile.open(scratchPath, O_RDONLY)
            guard sourceFd >= 0 else { throw ClusterProxyError.objectNotFound }
            defer { _ = POSIXFile.close(sourceFd) }

            var statInfo = stat()
            guard POSIXFile.fstat(sourceFd, &statInfo) == 0 else {
                throw ClusterProxyError.objectNotFound
            }
            var remaining = Int(statInfo.st_size)
            let windowSize = Constants.fileCopyWindowSize
            var window = [UInt8](repeating: 0, count: windowSize)
            while remaining > 0 {
                let toRead = Swift.min(windowSize, remaining)
                let bytesRead = POSIXFile.read(sourceFd, &window, toRead)
                guard bytesRead > 0 else { throw ClusterProxyError.objectNotFound }
                try window.withUnsafeBytes { raw in
                    try writer.writeRaw(UnsafeRawBufferPointer(rebasing: raw.prefix(bytesRead)))
                }
                remaining -= bytesRead
            }
            try writer.finish()
        } catch {
            writer.abort()
            throw error
        }
    }

    private static func attemptImmediateShardPush(
        app: Application, node: ClusterNodeInfo, sourcePath: String, bucketName: String,
        key: String, versionId: String?, shardIndex: Int
    ) async -> Bool {
        do {
            try await ClusterReplicationService.withTimeout(ClusterReplicationService.synchronousTimeout) {
                try await ClusterReplicationClient.pushShard(
                    app: app, to: node, sourcePath: sourcePath, bucketName: bucketName, key: key,
                    versionId: versionId, shardIndex: shardIndex)
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
