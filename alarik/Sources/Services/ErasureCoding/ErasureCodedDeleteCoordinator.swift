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

/// EC-aware delete primitives, layered on top of `ClusterReplicationService.coordinateDelete`
/// exactly the way `ErasureCodedWriteCoordinator`/`ErasureCodedReadCoordinator` layer onto plain
/// PUT/GET: checked first, falls through to the existing plain-replication delete path unchanged
/// when the target isn't (or can't be) erasure-coded.
enum ErasureCodedDeleteCoordinator {
    /// Resolves whether this node is within the current `k+m` responsible set for a key - `nil`
    /// when not clustered, EC isn't configured, or membership hasn't populated (all "not
    /// applicable, use the plain path" cases).
    static func ecPlacement(
        app: Application, bucketName: String, key: String
    ) async -> (selfNodeId: UUID, responsible: [ClusterNodeInfo], ecConfig: ClusterErasureCodingConfig)? {
        guard let config = app.storage[ClusterConfigurationKey.self],
            let ecConfig = app.storage[ClusterErasureCodingConfigKey.self]
        else { return nil }
        let active = await ClusterNodeCache.shared.activeNodes()
        guard !active.isEmpty else { return nil }
        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: active, count: ecConfig.totalShards)
        guard responsible.contains(where: { $0.id == config.nodeId }) else { return nil }
        return (config.nodeId, responsible, ecConfig)
    }

    static func localShardExists(
        bucketName: String, key: String, versionId: String?, selfRank: Int
    ) -> Bool {
        FileManager.default.fileExists(
            atPath: ErasureCodedObjectHandler.shardPath(
                bucketName: bucketName, key: key, versionId: versionId, shardIndex: selfRank))
    }

    /// Whether (bucketName, key, versionId) is erasure-coded anywhere in `responsible` - this node
    /// holds a shard (index-agnostic, so a drifted-index shard still counts), or a peer does.
    /// Index-agnostic and peer-aware so a byte-removal delete correctly recognizes an EC target
    /// even when this coordinating node's own shard has moved (or hasn't arrived yet) - the
    /// rank==index assumption it replaces would silently miss such a shard and fall through to the
    /// plain path, leaking every EC shard of the version.
    static func targetIsErasureCoded(
        app: Application, responsible: [ClusterNodeInfo], selfNodeId: UUID,
        bucketName: String, key: String, versionId: String?
    ) async -> Bool {
        if ErasureCodedObjectHandler.holdsAnyLocalShard(
            bucketName: bucketName, key: key, versionId: versionId)
        {
            return true
        }
        return await withTaskGroup(of: Bool.self) { group in
            for node in responsible where node.id != selfNodeId {
                group.addTask {
                    let held = await ClusterReplicationClient.heldShards(
                        app: app, node: node, bucketName: bucketName, key: key, versionId: versionId)
                    return !(held ?? []).isEmpty
                }
            }
            for await holds in group where holds {
                group.cancelAll()
                return true
            }
            return false
        }
    }

    /// Creates a fresh delete marker as an erasure-coded (trivially small, zero-payload) object -
    /// a marker is a *write*, so it's coded like any other write when EC is configured, rather
    /// than inspecting what format the version it supersedes happened to use.
    static func createDeleteMarker(
        app: Application, bucketName: String, key: String, peers: [ClusterNodeInfo],
        ecConfig: ClusterErasureCodingConfig
    ) async throws -> ObjectMeta {
        // Deliberately not `ObjectFileHandler.prepareVersionedWrite`: its returned meta always
        // has `isDeleteMarker` forced to `false` (it's built for regular writes), which would
        // silently turn this marker into an ordinary zero-byte version - the same reason the
        // plain path's `ObjectFileHandler.createDeleteMarker` builds its `ObjectMeta` by hand
        // too, rather than routing through it.
        let versionId = ObjectMeta.generateVersionId()
        let versionedMeta = ObjectMeta(
            bucketName: bucketName, key: key, size: 0, contentType: "", etag: "",
            updatedAt: Date(), versionId: versionId, isLatest: true, isDeleteMarker: true)

        // Capture the prior latest before demoting, so a failed-quorum rollback restores it.
        let priorLatestVersionId = try? ObjectFileHandler.getLatestVersionId(
            bucketName: bucketName, key: key)
        try ObjectFileHandler.markAllVersionsNotLatest(bucketName: bucketName, key: key)
        // markAllVersionsNotLatest only ever looks at `.obj` files - this node's own prior local
        // EC shards for this key need the EC-aware equivalent too.
        ErasureCodedObjectHandler.markAllLocalShardsNotLatest(bucketName: bucketName, key: key)

        // No payload - StripeEncoder already handles a zero-byte, zero-source object with no
        // special-casing (see its own design doc comment).
        try await ErasureCodedWriteCoordinator.write(
            app: app, bucketName: bucketName, key: key, objectMeta: versionedMeta,
            payloadSources: [], peers: peers,
            ecConfig: (ecConfig.dataShards, ecConfig.parityShards),
            priorLatestVersionId: priorLatestVersionId)

        try await S3Service.offloadBlockingIO(app) {
            try ObjectFileHandler.updateLatestPointer(
                bucketName: bucketName, key: key, versionId: versionId)
        }
        return versionedMeta
    }

    /// Removes this node's local shard directory for (bucketName, key, versionId) and fans the
    /// shard-delete out to the other `k+m-1` responsible nodes. Only called once the caller has
    /// confirmed the target is erasure-coded somewhere in the responsible set
    /// (`targetIsErasureCoded`); the local removal is a no-op when this node happens to hold no
    /// shard (a peer does), and never touches a plain `.obj`.
    static func removeVersion(
        app: Application, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo], selfNodeId: UUID
    ) async throws {
        try? FileManager.default.removeItem(
            atPath: ErasureCodedObjectHandler.shardBasePath(
                bucketName: bucketName, key: key, versionId: versionId))

        let peers = responsible.filter { $0.id != selfNodeId }
        guard !peers.isEmpty else { return }

        var delivered: Set<UUID> = []
        await withTaskGroup(of: (id: UUID, ok: Bool).self) { group in
            for peer in peers {
                group.addTask {
                    let ok =
                        (try? await ClusterReplicationClient.deleteShard(
                            app: app, to: peer, bucketName: bucketName, key: key,
                            versionId: versionId)) != nil
                    return (peer.id, ok)
                }
            }
            for await outcome in group where outcome.ok { delivered.insert(outcome.id) }
        }

        // A stray shard left behind after a failed peer delete is a storage leak, not a
        // correctness hazard (unreachable once no version references it) - still worth a
        // durable retry via the same outbox every other shard delivery uses. `shardIndex: -1`:
        // deletes don't target a specific index (a node only ever holds the one it's currently
        // responsible for), so there's nothing meaningful to record there.
        let undelivered = peers.filter { !delivered.contains($0.id) }
        guard !undelivered.isEmpty else { return }
        for peer in undelivered {
            let task = ErasureCodedReplicationTask(
                bucketName: bucketName, key: key, versionId: versionId, shardIndex: -1,
                operation: .delete, targetNodeId: peer.id, reason: .write)
            await OutboxMailbox.enqueue(
                app: app, collection: OutboxCollections.erasureCodedReplicationTasks, row: task)
        }
        ErasureCodedDispatcher.shared.wake()
    }
}
