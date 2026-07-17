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
    /// Throws `.quorumNotReached` (never returns partial success) when fewer than
    /// `ecQuorumThreshold` shards land synchronously - and rolls back every shard that DID land
    /// (this node's own, plus any peer that already acked) before throwing, so a failed PUT never
    /// leaves a partially-shard-covered, half-written object discoverable by a later GET. This is
    /// the one place EC's quorum is a hard gate, not a soft preference: unlike plain replication
    /// (which always acks the client after its own local write, letting peers catch up
    /// asynchronously regardless of quorum), an EC object with too few shards placed isn't
    /// reconstructable at all - there's nothing safe to leave half-visible.
    /// `priorLatestVersionId` is the version `.latest` pointed at before this write began (nil for
    /// a first-ever write, or a non-versioned write where the pointer isn't used). It's needed
    /// only by the rollback path: if quorum fails, peers that already received the shard will have
    /// repointed their `.latest` to this now-deleted version, so rollback restores them (and this
    /// node) to the prior version - otherwise a failed PUT would leave `.latest` dangling at a
    /// shardless version and make the previously-readable object 404.
    static func write(
        app: Application,
        bucketName: String,
        key: String,
        objectMeta: ObjectMeta,
        payloadSources: [(path: String, offset: Int, size: Int)],
        peers: [ClusterNodeInfo],
        ecConfig: ClusterErasureCodingConfig,
        priorLatestVersionId: String? = nil
    ) async throws {
        let totalShards = ecConfig.totalShards
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

    /// Best-effort: undoes every shard that landed before quorum failed, then repairs the `.latest`
    /// pointer. Failures here are logged, not thrown - the client already gets `.quorumNotReached`
    /// regardless, and a shard this couldn't clean up is exactly what
    /// `ErasureCodedRebalanceService`'s health sweep would eventually reclaim as an orphan anyway.
    ///
    /// The latest-pointer repair is the load-bearing part: every peer that received the shard
    /// demoted its prior version and repointed `.latest` to this (now-deleted) version on receipt.
    /// Left unrepaired, a subsequent GET would resolve `.latest` to a version with no shards and
    /// 404 an object that was perfectly readable a moment ago. Restore is broadcast to *all* peers
    /// (idempotent - a peer that never received the shard just re-affirms its existing pointer),
    /// and applied locally too, since this node's own prior version was demoted before the write.
    /// Only versioned writes use the pointer, so a nil `versionId` skips the restore entirely.
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
            do {
                try await task.save(on: app.db)
            } catch {
                app.logger.error(
                    "Failed to enqueue EC shard replication task for '\(key)' shard \(shardIndex) -> \(peer.id): \(error)"
                )
            }
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
        guard let selfRank = responsible.firstIndex(where: { $0.id == selfNodeId }) else {
            throw ErasureCodedRebalanceError.insufficientResponsibleNodes(
                required: 1, found: 0)
        }
        let localPath = ErasureCodedObjectHandler.shardPath(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: selfRank)

        let updatedHeader = try await S3Service.offloadBlockingIO(app) {
            try ErasureCodedObjectHandler.rewriteShardMetadata(at: localPath, transform: transform)
        }
        let updatedMeta = updatedHeader.objectMeta

        let peers = responsible.enumerated().filter { $0.offset != selfRank }
        await withTaskGroup(of: Void.self) { group in
            for (rank, peer) in peers {
                group.addTask {
                    try? await ClusterReplicationClient.patchShardMetadata(
                        app: app, to: peer, bucketName: bucketName, key: key, versionId: versionId,
                        shardIndex: rank, meta: updatedMeta)
                }
            }
        }
        return updatedMeta
    }
}
