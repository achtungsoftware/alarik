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

import AsyncHTTPClient
import Crypto
import Foundation
import NIOCore
import Vapor

enum MetadataStoreError: Error, CustomStringConvertible {
    case corruptRecord(collection: String, id: String)
    case coordinatorUnreachable(collection: String, id: String)

    var description: String {
        switch self {
        case .corruptRecord(let collection, let id):
            "Metadata record \(collection)/\(id) failed decode/checksum verification"
        case .coordinatorUnreachable(let collection, let id):
            "Could not reach the coordinating node for metadata record \(collection)/\(id)"
        }
    }
}

/// Byte-oriented K/V store for Alarik's own control-plane metadata (users, buckets, access
/// keys, cluster membership, outbox tasks, ...), backed by the same erasure-coding engine as
/// regular object data, under the reserved `.alarik.sys` namespace. Records are non-versioned.
///
/// Placement resolves fresh per call, never cached, so a membership change is picked up on the
/// very next call. Reads run locally from any node (gather-and-decode is idempotent); writes/CAS
/// forward to rank-0 so two nodes can never race to assign conflicting shard placements.
enum MetadataStore {
    // MARK: - Codable convenience

    private static let decoder = JSONDecoder()
    private static let encoder = JSONEncoder()

    static func get<T: Decodable>(
        _ type: T.Type, app: Application, collection: String, id: String
    ) async throws -> T? {
        guard let data = try await get(app: app, collection: collection, id: id) else {
            return nil
        }
        return try decoder.decode(T.self, from: data)
    }

    static func put<T: Encodable>(
        app: Application, collection: String, id: String, value: T
    ) async throws {
        try await put(app: app, collection: collection, id: id, value: encoder.encode(value))
    }

    static func putIfAbsent<T: Encodable>(
        app: Application, collection: String, id: String, value: T
    ) async throws -> Bool {
        try await putIfAbsent(
            app: app, collection: collection, id: id, value: encoder.encode(value))
    }

    static func consumeIfPresent<T: Decodable>(
        _ type: T.Type, app: Application, collection: String, id: String
    ) async throws -> T? {
        guard let data = try await consumeIfPresent(app: app, collection: collection, id: id)
        else { return nil }
        return try decoder.decode(T.self, from: data)
    }

    // MARK: - Byte-oriented core
    //
    // Records are stored wrapped in a `MetadataEnvelope` (tombstone flag, last-write timestamp,
    // schema version). The wrapping lives here, at the boundary between the public API and
    // routing, which is what keeps it invisible to everything else: the Codable overloads above
    // keep their exact semantics, and the rank-0 forwarding path in
    // `InternalClusterMetadataController` relays already-wrapped bytes opaquely, so it - and all
    // of EC replication/rebalance/scrub - needs no envelope awareness at all. A tombstone is just
    // another record write and propagates through that same machinery.
    //
    // The `executeLocal*` entry points below deliberately do NOT wrap: they receive bytes that
    // were already wrapped by the originating node.

    /// The record's own bytes, or `nil` when absent *or* tombstoned. Applies any outstanding
    /// schema migration in memory (see `MetadataMigrations`).
    static func get(app: Application, collection: String, id: String) async throws -> Data? {
        guard let envelope = try await getEnvelope(app: app, collection: collection, id: id),
            !envelope.isTombstone, let payload = envelope.payload
        else { return nil }
        return MetadataMigrations.upgrade(
            payload: payload, collection: collection, storedVersion: envelope.schemaVersion,
            logger: app.logger)
    }

    /// The stored envelope exactly as written - tombstones included, no migration applied.
    /// Needed wherever a record must be moved or compared rather than consumed: the rebalance
    /// widen path, listing merges, and the tombstone GC sweep.
    static func getEnvelope(
        app: Application, collection: String, id: String
    ) async throws -> MetadataEnvelope? {
        let key = MetadataNamespace.key(collection: collection, id: id)
        let routing = await resolveRouting(app: app, key: key)
        guard let stored = try await localGet(app: app, key: key, routing: routing) else {
            return nil
        }
        return MetadataEnvelope.decode(stored)
    }

    static func put(app: Application, collection: String, id: String, value: Data) async throws {
        let envelope = MetadataEnvelope.live(
            payload: value, schemaVersion: MetadataMigrations.currentVersion(for: collection))
        try await putEnvelope(app: app, collection: collection, id: id, envelope: envelope)
    }

    /// Stores `envelope` verbatim - no re-stamping of `updatedAtMillis`, no tombstone filtering.
    /// The rebalance widen path depends on this: re-wrapping there would bump every record's
    /// timestamp on a purely physical move, and would skip tombstones entirely (since `get`
    /// reports them as absent), leaving them stuck at their original narrow placement.
    static func putEnvelope(
        app: Application, collection: String, id: String, envelope: MetadataEnvelope
    ) async throws {
        let key = MetadataNamespace.key(collection: collection, id: id)
        let routing = await resolveRouting(app: app, key: key)
        let value = try envelope.encoded()
        if routing.isLocalCoordinator {
            try await executeLocalPut(app: app, key: key, value: value, routing: routing)
        } else {
            do {
                try await forwardPut(
                    app: app, node: routing.primary, collection: collection, id: id, value: value)
            } catch {
                // Rank-0 pinning exists to serialize writes, but a plain put/tombstone is safe
                // under last-writer-wins even with two coordinators (`MetadataEnvelope.supersedes`
                // picks the same winner everywhere) - so when rank-0 is unreachable, coordinate
                // locally rather than fail. The alternative is far worse: `activeNodes` tolerates
                // a stale heartbeat for up to a minute, so a crashed rank-0 makes every write to
                // its keys - including *revoking a credential* - error out for that whole window.
                // CAS operations (`putIfAbsent`/`consumeIfPresent`) deliberately keep the strict
                // single-coordinator requirement; uniqueness cannot be LWW-merged.
                app.logger.warning(
                    "Rank-0 coordinator unreachable for '\(collection)/\(id)' - coordinating this write locally so it isn't blocked by a down peer: \(error)"
                )
                try await executeLocalPut(
                    app: app, key: key, value: value,
                    routing: routing.assumingLocalCoordination(app: app))
            }
        }
    }

    /// Creates the record only if absent, returning whether this call created it. Mutual
    /// exclusion comes from two layered guarantees: rank-0 pinning ensures only one node ever
    /// coordinates writes for this key, and `MetadataKeyLock` closes the remaining intra-process
    /// race window (two concurrent requests on the coordinator itself). See `MetadataKeyLock`'s
    /// doc comment for the full reasoning.
    static func putIfAbsent(
        app: Application, collection: String, id: String, value: Data
    ) async throws -> Bool {
        let key = MetadataNamespace.key(collection: collection, id: id)
        let routing = await resolveRouting(app: app, key: key)
        let envelope = MetadataEnvelope.live(
            payload: value, schemaVersion: MetadataMigrations.currentVersion(for: collection))
        let wrapped = try envelope.encoded()
        guard routing.isLocalCoordinator else {
            return try await forwardPutIfAbsent(
                app: app, node: routing.primary, collection: collection, id: id, value: wrapped)
        }
        return try await executeLocalPutIfAbsent(
            app: app, collection: collection, id: id, value: wrapped)
    }

    /// Marks the record deleted. For most collections this *writes a tombstone* rather than
    /// removing bytes: a replica that was unreachable during the delete would otherwise come back
    /// still holding the record and resurrect it (a revoked access key going live again). The
    /// tombstone is an ordinary record write, so it replicates, reconstructs, and self-heals
    /// through the same paths as any other write, and beats the stale copy on `updatedAtMillis`.
    ///
    /// `MetadataCollections.tombstoneExempt` collections remove the bytes outright - see that
    /// declaration for why neither of them needs the protection.
    static func delete(app: Application, collection: String, id: String) async throws {
        guard !MetadataCollections.tombstoneExempt.contains(collection) else {
            try await purge(app: app, collection: collection, id: id)
            return
        }
        let tombstone = MetadataEnvelope.tombstone(
            schemaVersion: MetadataMigrations.currentVersion(for: collection))
        try await putEnvelope(app: app, collection: collection, id: id, envelope: tombstone)
    }

    /// Physically removes the record's bytes cluster-wide, leaving nothing behind. This is what
    /// `delete` used to do for every collection; it now backs tombstone GC
    /// (`MetadataTombstoneSweep`) and the tombstone-exempt collections.
    ///
    /// Deliberately NO rank-0-unreachable failover here, unlike `putEnvelope`: a purge is
    /// physical removal, which is not LWW-mergeable - two uncoordinated purges racing a
    /// concurrent recreate could remove the new record. A purge blocked by a down rank-0 simply
    /// happens on a later GC/TTL sweep; nothing is ever wrong in the meantime, just unreclaimed.
    static func purge(app: Application, collection: String, id: String) async throws {
        let key = MetadataNamespace.key(collection: collection, id: id)
        let routing = await resolveRouting(app: app, key: key)
        if routing.isLocalCoordinator {
            try await executeLocalDelete(app: app, key: key, routing: routing)
        } else {
            try await forwardDelete(
                app: app, node: routing.primary, collection: collection, id: id)
        }
    }

    /// Atomic read-then-delete, single-use by construction: the record is gone the instant this
    /// returns non-nil, cluster-wide, and can never be consumed twice.
    ///
    /// Honest scope of that guarantee: it holds as long as every consumer resolves the SAME
    /// rank-0 for the key. Under membership drift two nodes can transiently disagree on rank-0
    /// and each consume "the" record once. The only current caller is `oidc-states`, where the
    /// worst case of that window is a replayed-but-still-TTL-checked login state - accepted
    /// deliberately rather than paying for consensus here. Do NOT build anything on this method
    /// where double-consumption would be a security or billing event.
    static func consumeIfPresent(
        app: Application, collection: String, id: String
    ) async throws -> Data? {
        let key = MetadataNamespace.key(collection: collection, id: id)
        let routing = await resolveRouting(app: app, key: key)
        let stored: Data?
        if routing.isLocalCoordinator {
            stored = try await executeLocalConsumeIfPresent(
                app: app, collection: collection, id: id)
        } else {
            stored = try await forwardConsumeIfPresent(
                app: app, node: routing.primary, collection: collection, id: id)
        }
        guard let stored else { return nil }
        let envelope = MetadataEnvelope.decode(stored)
        guard !envelope.isTombstone, let payload = envelope.payload else { return nil }
        return MetadataMigrations.upgrade(
            payload: payload, collection: collection, storedVersion: envelope.schemaVersion,
            logger: app.logger)
    }

    // MARK: - Routing

    /// `responsible` is empty whenever there's nowhere to place shards over the network (not
    /// clustered, or cluster mode is on but membership hasn't populated yet) - callers treat
    /// that uniformly as "coordinate locally, zero peers," never as an error.
    struct Routing {
        let responsible: [ClusterNodeInfo]
        let selfNodeId: UUID?
        let dataShards: Int
        let parityShards: Int

        var peers: [ClusterNodeInfo] {
            guard let selfNodeId else { return [] }
            return responsible.filter { $0.id != selfNodeId }
        }

        var primary: ClusterNodeInfo? { responsible.first }

        /// True when this node should coordinate the write itself - either it IS rank-0, or
        /// there's no meaningful "rank-0" to speak of (nothing responsible, nowhere else to go).
        var isLocalCoordinator: Bool {
            guard let selfNodeId, let primary else { return true }
            return primary.id == selfNodeId
        }

        /// Routing for the rank-0-unreachable failover: this node takes over shard 0 (written
        /// locally, off-placement but discoverable - metadata reads probe any-index holders and
        /// widen to all known nodes), while the dead rank-0 is dropped from the target list.
        /// Without this, `ErasureCodedWriteCoordinator`'s peers-must-be-`k+m-1` contract breaks
        /// whenever the failover coordinator isn't in the responsible set at all.
        func assumingLocalCoordination(app: Application) -> Routing {
            guard let selfNodeId, let config = app.storage[ClusterConfigurationKey.self],
                let primary, primary.id != selfNodeId
            else { return self }
            var reordered = responsible
            if let selfIndex = reordered.firstIndex(where: { $0.id == selfNodeId }) {
                // Self already owns a slot: swap it into rank-0. The unreachable ex-rank-0
                // stays as a target (its shard push just fails; quorum is met without it).
                reordered.swapAt(0, selfIndex)
            } else {
                // Self takes the unreachable rank-0's slot outright - list length must stay
                // exactly k+m or the write coordinator rejects the placement.
                reordered[0] = ClusterNodeInfo(
                    id: selfNodeId, address: config.address, status: .active,
                    lastHeartbeatAt: Date())
            }
            return Routing(
                responsible: reordered, selfNodeId: selfNodeId,
                dataShards: dataShards, parityShards: parityShards)
        }
    }

    /// Resolves fresh placement for `key` every call - deliberately not cached, so a membership
    /// change is picked up on the very next call. Shard counts come from the metadata-specific
    /// config (graceful auto-cap), never the object-data config (which hard-refuses an undersized
    /// cluster) - metadata availability must never depend on object-data k+m fitting the cluster.
    /// `cluster-nodes/*` records are the exception, pinned to k=1/m=0: they're rewritten on every
    /// heartbeat, and a single-shard record can never disagree with itself under that churn.
    static func resolveRouting(app: Application, key: String) async -> Routing {
        let metadataConfig = app.storage[ClusterMetadataErasureCodingConfigKey.self] ?? .default
        let isClusterNode = key.hasPrefix("\(MetadataCollections.clusterNodes)/")
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            let (dataShards, parityShards) =
                isClusterNode ? (1, 0) : metadataConfig.effective(activeNodeCount: 1)
            return Routing(
                responsible: [], selfNodeId: nil, dataShards: dataShards,
                parityShards: parityShards)
        }

        // A `cluster-nodes/<X>` record is always coordinated by node X itself, bypassing HRW
        // ranking entirely, regardless of who initiates the write (e.g. an admin drain relayed
        // through another node). HRW ranking shifts with membership, so routing through the
        // computed rank-0 could land on a node that's down for unrelated reasons, leaving the
        // write stuck until it recovers. Routing both reads and writes through X itself ensures
        // the record for X lives in exactly one place, so every write and read always agree.
        if isClusterNode {
            let targetIdString = String(key.dropFirst("\(MetadataCollections.clusterNodes)/".count))
            if let targetId = UUID(uuidString: targetIdString) {
                let targetInfo: ClusterNodeInfo?
                if targetId == config.nodeId {
                    // Self - always resolvable from local config, even before any peer has ever
                    // heard of this node (first-ever boot, nothing in `ClusterNodeCache` yet).
                    targetInfo = ClusterNodeInfo(
                        id: config.nodeId, address: config.address, status: .active,
                        lastHeartbeatAt: Date())
                } else {
                    targetInfo = await ClusterNodeCache.shared.get(id: targetId)
                }
                if let targetInfo {
                    return Routing(
                        responsible: [targetInfo], selfNodeId: config.nodeId, dataShards: 1,
                        parityShards: 0)
                }
                // Target genuinely unknown to this node (a peer this node hasn't discovered at
                // all yet) - nothing to route to directly; fall through to the best-effort
                // HRW-based computation below, the same fallback every other collection uses.
                // Loud, because a write taking this path lands somewhere the owner-pinned READ
                // path will never look - it stays wrong until the owner's next heartbeat
                // self-write supersedes it, and that dependency should be visible in logs.
                app.logger.warning(
                    "cluster-nodes record for unknown node \(targetId) routed via HRW fallback - unreadable by the owner-pinned read path until that node's own heartbeat rewrites it."
                )
            }
        }

        let active = await ClusterNodeCache.shared.activeNodes()
        guard !active.isEmpty else {
            let (dataShards, parityShards) =
                isClusterNode ? (1, 0) : metadataConfig.effective(activeNodeCount: 1)
            return Routing(
                responsible: [], selfNodeId: config.nodeId, dataShards: dataShards,
                parityShards: parityShards)
        }
        let (dataShards, parityShards) =
            isClusterNode ? (1, 0) : metadataConfig.effective(activeNodeCount: active.count)
        let responsible = PlacementService.responsibleNodes(
            bucketName: MetadataNamespace.bucketName, key: key, activeNodes: active,
            count: dataShards + parityShards)
        return Routing(
            responsible: responsible, selfNodeId: config.nodeId, dataShards: dataShards,
            parityShards: parityShards)
    }

    // MARK: - Local execution (assumes THIS node is already the confirmed coordinator)

    /// Executes a put as though this node is already the confirmed coordinator - shared by
    /// `put`'s own local-coordinator branch and `InternalClusterMetadataController.handlePut`
    /// when handling a forwarded write from a peer that resolved *this* node as rank-0. The
    /// receiving side of a forward never re-derives "am I really rank-0" (same trust model
    /// `ObjectRoutingService`'s `isTrustedForward` already uses cluster-wide) - it re-resolves
    /// routing only to get its OWN correct `peers`/shard-count values for the local write.
    static func executeLocalPut(
        app: Application, collection: String, id: String, value: Data
    ) async throws {
        let key = MetadataNamespace.key(collection: collection, id: id)
        let routing = await resolveRouting(app: app, key: key)
        try await executeLocalPut(app: app, key: key, value: value, routing: routing)
    }

    static func executeLocalPutIfAbsent(
        app: Application, collection: String, id: String, value: Data
    ) async throws -> Bool {
        let key = MetadataNamespace.key(collection: collection, id: id)
        let routing = await resolveRouting(app: app, key: key)
        return try await MetadataKeyLock.shared.withLock(collection: collection, key: id) {
            // A full gather (with its widen-to-all-known fallback), never a local shard-0 file
            // check: this node being rank-0 TODAY says nothing about where the record's shards
            // were placed when it was written - a local-only check under placement drift reads
            // "absent" for a live record and double-claims a unique id (an access key value, a
            // username, a bucket name), silently orphaning one of the two claims via LWW later.
            //
            // Error policy: a tombstoned id is genuinely free to claim again (a released
            // username/bucket name must be reusable). Anything else - present, or
            // present-but-unreadable-right-now - is treated as taken, so a transient read
            // failure can never clobber a live record.
            let existing: Data?
            do {
                existing = try await localGet(app: app, key: key, routing: routing)
            } catch {
                app.logger.warning(
                    "putIfAbsent for '\(collection)/\(id)' could not verify absence (treating the id as taken): \(error)"
                )
                return false
            }
            if let existing, !MetadataEnvelope.decode(existing).isTombstone {
                return false
            }
            try await localWrite(app: app, key: key, value: value, routing: routing)
            return true
        }
    }

    static func executeLocalDelete(app: Application, collection: String, id: String) async throws {
        let key = MetadataNamespace.key(collection: collection, id: id)
        let routing = await resolveRouting(app: app, key: key)
        try await executeLocalDelete(app: app, key: key, routing: routing)
    }

    static func executeLocalConsumeIfPresent(
        app: Application, collection: String, id: String
    ) async throws -> Data? {
        let key = MetadataNamespace.key(collection: collection, id: id)
        let routing = await resolveRouting(app: app, key: key)
        return try await MetadataKeyLock.shared.withLock(collection: collection, key: id) {
            guard let stored = try await localGet(app: app, key: key, routing: routing),
                !MetadataEnvelope.decode(stored).isTombstone
            else { return nil }

            // Returns the stored envelope verbatim; the caller (`consumeIfPresent`) unwraps once,
            // so this stays correct whether it ran locally or was relayed from a peer.
            if MetadataCollections.tombstoneExempt.contains(collection) {
                try await executeLocalDelete(app: app, key: key, routing: routing)
            } else {
                let tombstone = MetadataEnvelope.tombstone(
                    schemaVersion: MetadataMigrations.currentVersion(for: collection))
                try await localWrite(
                    app: app, key: key, value: try tombstone.encoded(), routing: routing)
            }
            return stored
        }
    }

    private static func executeLocalPut(
        app: Application, key: String, value: Data, routing: Routing
    ) async throws {
        try await localWrite(app: app, key: key, value: value, routing: routing)
    }

    private static func executeLocalDelete(
        app: Application, key: String, routing: Routing
    ) async throws {
        try await ErasureCodedDeleteCoordinator.removeVersion(
            app: app, bucketName: MetadataNamespace.bucketName, key: key, versionId: nil,
            responsible: routing.peers, selfNodeId: routing.selfNodeId ?? UUID())
    }

    private static func localShard0Exists(key: String) -> Bool {
        FileManager.default.fileExists(
            atPath: ErasureCodedObjectHandler.shardPath(
                bucketName: MetadataNamespace.bucketName, key: key, versionId: nil,
                shardIndex: 0))
    }

    /// `resolveRouting`'s `responsible` set is recomputed from the CURRENT active-node count on
    /// every call, so a record written under a SMALLER membership view (e.g. by a cluster's
    /// founding node before any peer joined) can end up excluded once membership grows and HRW
    /// ranks shift. Control-plane records need to be reliably readable immediately, not after a
    /// rebalance cadence catches up - so a read that comes up empty is retried once against every
    /// currently active node before being treated as genuinely absent.
    private static func localGet(app: Application, key: String, routing: Routing) async throws
        -> Data?
    {
        guard !routing.responsible.isEmpty, let selfNodeId = routing.selfNodeId else {
            return try await directLocalRead(app: app, key: key)
        }

        // Every KNOWN node, not `activeNodes()` - a `.draining` node (notably this node itself,
        // mid-drain) is excluded from `activeNodes()` by design, since that set drives NEW
        // placement decisions. But reusing that exclusion here would mean a draining node can no
        // longer read data it still physically holds locally. Draining only means "don't place
        // new writes here", never "can't serve reads of what's already here" - so widen to every
        // known node rather than reuse the placement-only active set.
        let allKnown = await ClusterNodeCache.shared.all()
        let hasWiderFallback = allKnown.count > routing.responsible.count

        do {
            return try await attemptGather(
                app: app, key: key, candidates: routing.responsible, routing: routing,
                selfNodeId: selfNodeId)
        } catch let error as S3Error where error.code == "NoSuchKey" {
            // Genuinely not found among `routing.responsible` - still worth widening before
            // concluding absence, for the same placement-drift reason as below.
            guard hasWiderFallback else { return nil }
        } catch let error as S3Error where error.code == "ServiceUnavailable" && hasWiderFallback {
            // Found but under-gathered from THIS candidate set specifically - swallowed here
            // only because a strictly wider candidate set exists to retry against next. If the
            // wider attempt below ALSO comes back degraded, that failure is a genuine "not
            // enough healthy shards anywhere" and must propagate as ServiceUnavailable, not be
            // silently reinterpreted as "not found" (a caller like login/auth must never treat
            // a temporarily-unreconstructable record as if it never existed).
        }

        // `routing.responsible` can exclude the node(s) that actually hold this record if it was
        // written under a different membership view (see `allKnown` above). Retry against every
        // known node, the widest possible candidate set: `NoSuchKey` here is a genuine "not
        // found" (returned as `nil`), while `ServiceUnavailable` means "found but can't currently
        // reconstruct" and must propagate as a real error, not be silently treated as absence.
        do {
            return try await attemptGather(
                app: app, key: key, candidates: allKnown, routing: routing, selfNodeId: selfNodeId)
        } catch let error as S3Error where error.code == "NoSuchKey" {
            return nil
        }
    }

    private static func attemptGather(
        app: Application, key: String, candidates: [ClusterNodeInfo], routing: Routing,
        selfNodeId: UUID
    ) async throws -> Data? {
        let shardCounts =
            await discoverShardCounts(
                app: app, key: key, candidates: candidates, selfNodeId: selfNodeId)
            ?? (routing.dataShards, routing.dataShards + routing.parityShards)
        let (_, stream) = try await ErasureCodedReadCoordinator.read(
            app: app, bucketName: MetadataNamespace.bucketName, key: key, versionId: nil,
            responsible: candidates, selfNodeId: selfNodeId,
            requestId: UUID().uuidString,
            shardCounts: shardCounts)
        var data = Data()
        for try await buffer in stream {
            data.append(contentsOf: buffer.readableBytesView)
        }
        return data
    }

    /// Discovers the `(dataShards, totalShards)` a metadata record was *actually* encoded with,
    /// rather than trusting `routing`'s freshly-recomputed values - those derive from the CURRENT
    /// active-node count, which is unstable, while the k/m baked into shards on disk at write time
    /// is fixed forever. Every `.ecshard` header records its true encoding, so this probes ground
    /// truth directly: local copy first, then each candidate in parallel. `nil` only when no
    /// candidate holds anything. Not `private`: `ErasureCodedRebalanceService` reuses it too.
    static func discoverShardCounts(
        app: Application, key: String, candidates: [ClusterNodeInfo], selfNodeId: UUID
    ) async -> (dataShards: Int, totalShards: Int)? {
        if let header = localShardHeader(key: key) {
            return (header.dataShards, header.dataShards + header.parityShards)
        }
        let peers = candidates.filter { $0.id != selfNodeId }
        guard !peers.isEmpty else { return nil }
        return await withTaskGroup(
            of: (dataShards: Int, parityShards: Int)?.self,
            returning: (dataShards: Int, totalShards: Int)?.self
        ) { group in
            for peer in peers {
                group.addTask {
                    await ClusterReplicationClient.fetchShardEncoding(
                        app: app, node: peer, bucketName: MetadataNamespace.bucketName, key: key,
                        versionId: nil)
                }
            }
            for await result in group {
                if let result {
                    group.cancelAll()
                    return (result.dataShards, result.dataShards + result.parityShards)
                }
            }
            return nil
        }
    }

    private static func localShardHeader(key: String) -> ErasureCodedShardHeader? {
        let held = ErasureCodedObjectHandler.locallyHeldShardIndices(
            bucketName: MetadataNamespace.bucketName, key: key, versionId: nil)
        guard let index = held.first else { return nil }
        let path = ErasureCodedObjectHandler.shardPath(
            bucketName: MetadataNamespace.bucketName, key: key, versionId: nil, shardIndex: index)
        guard let reader = try? ErasureCodedShardReader(path: path) else { return nil }
        defer { reader.close() }
        return reader.header
    }

    private static func directLocalRead(app: Application, key: String) async throws -> Data? {
        let path = ErasureCodedObjectHandler.shardPath(
            bucketName: MetadataNamespace.bucketName, key: key, versionId: nil, shardIndex: 0)
        guard FileManager.default.fileExists(atPath: path) else { return nil }
        return try await S3Service.offloadBlockingIO(app) {
            var collected = Data()
            do {
                try StripeDecoder.decode(shardPaths: [0: path], range: nil) { chunk in
                    collected.append(chunk)
                }
            } catch {
                throw MetadataStoreError.corruptRecord(collection: "", id: key)
            }
            return collected
        }
    }

    private static func localWrite(
        app: Application, key: String, value: Data, routing: Routing
    ) async throws {
        try await S3Service.offloadBlockingIO(app) {
            try FileManager.default.createDirectory(
                atPath: Constants.spoolDirectory, withIntermediateDirectories: true)
        }
        let scratchPath = Constants.spoolDirectory + "metadata-write-" + UUID().uuidString
        try await S3Service.offloadBlockingIO(app) {
            try value.write(to: URL(fileURLWithPath: scratchPath))
        }
        defer { _ = POSIXFile.unlink(scratchPath) }

        let objectMeta = ObjectMeta(
            bucketName: MetadataNamespace.bucketName, key: key, size: value.count,
            contentType: "application/json", etag: S3Service.computeETag(value), updatedAt: Date())
        let stripeUnitSize = MetadataStripeSizing.chooseStripeUnitSize(
            payloadSize: value.count, dataShards: routing.dataShards)

        try await ErasureCodedWriteCoordinator.write(
            app: app, bucketName: MetadataNamespace.bucketName, key: key, objectMeta: objectMeta,
            payloadSources: value.isEmpty ? [] : [(scratchPath, 0, value.count)],
            peers: routing.peers, ecConfig: (routing.dataShards, routing.parityShards),
            stripeUnitSize: stripeUnitSize)
    }

    // MARK: - Forwarding to rank-0 (writes/CAS only - reads never forward)

    private static func forwardPut(
        app: Application, node: ClusterNodeInfo?, collection: String, id: String, value: Data
    ) async throws {
        guard let node, let config = app.storage[ClusterConfigurationKey.self] else {
            throw MetadataStoreError.coordinatorUnreachable(collection: collection, id: id)
        }
        var outbound = HTTPClientRequest(
            url: node.address + "/internal/cluster/metadata/put"
                + querySuffix(collection: collection, id: id))
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        outbound.headers.replaceOrAdd(name: .contentType, value: "application/octet-stream")
        outbound.body = .bytes(ByteBuffer(data: value))
        let response = try await LightweightClusterControlClient.shared.execute(
            outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
        guard response.status == .ok else {
            throw MetadataStoreError.coordinatorUnreachable(collection: collection, id: id)
        }
    }

    private static func forwardPutIfAbsent(
        app: Application, node: ClusterNodeInfo?, collection: String, id: String, value: Data
    ) async throws -> Bool {
        guard let node, let config = app.storage[ClusterConfigurationKey.self] else {
            throw MetadataStoreError.coordinatorUnreachable(collection: collection, id: id)
        }
        var outbound = HTTPClientRequest(
            url: node.address + "/internal/cluster/metadata/put-if-absent"
                + querySuffix(collection: collection, id: id))
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        outbound.headers.replaceOrAdd(name: .contentType, value: "application/octet-stream")
        outbound.body = .bytes(ByteBuffer(data: value))
        let response = try await LightweightClusterControlClient.shared.execute(
            outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
        guard response.status == .ok else {
            throw MetadataStoreError.coordinatorUnreachable(collection: collection, id: id)
        }
        return response.headers.first(name: "X-Alarik-Created") == "true"
    }

    private static func forwardDelete(
        app: Application, node: ClusterNodeInfo?, collection: String, id: String
    ) async throws {
        guard let node, let config = app.storage[ClusterConfigurationKey.self] else {
            throw MetadataStoreError.coordinatorUnreachable(collection: collection, id: id)
        }
        var outbound = HTTPClientRequest(
            url: node.address + "/internal/cluster/metadata/delete"
                + querySuffix(collection: collection, id: id))
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        let response = try await LightweightClusterControlClient.shared.execute(
            outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
        guard response.status == .ok else {
            throw MetadataStoreError.coordinatorUnreachable(collection: collection, id: id)
        }
    }

    private static func forwardConsumeIfPresent(
        app: Application, node: ClusterNodeInfo?, collection: String, id: String
    ) async throws -> Data? {
        guard let node, let config = app.storage[ClusterConfigurationKey.self] else {
            throw MetadataStoreError.coordinatorUnreachable(collection: collection, id: id)
        }
        var outbound = HTTPClientRequest(
            url: node.address + "/internal/cluster/metadata/consume-if-present"
                + querySuffix(collection: collection, id: id))
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        let response = try await LightweightClusterControlClient.shared.execute(
            outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
        if response.status == .notFound { return nil }
        guard response.status == .ok else {
            throw MetadataStoreError.coordinatorUnreachable(collection: collection, id: id)
        }
        let body = try await response.body.collect(upTo: 64 * 1024 * 1024)
        return Data(buffer: body)
    }

    private static func querySuffix(collection: String, id: String) -> String {
        var components = URLComponents()
        components.queryItems = [
            URLQueryItem(name: "collection", value: collection),
            URLQueryItem(name: "id", value: id),
        ]
        return "?" + (components.percentEncodedQuery ?? "")
    }
}
