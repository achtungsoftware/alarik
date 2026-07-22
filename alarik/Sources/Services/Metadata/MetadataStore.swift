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
    /// This node holds a shard of the record but the record spans more shards than it has, and it
    /// currently knows of no peers to gather the rest from (its membership view is momentarily
    /// empty - most often the first moments after a restart). Deliberately distinct from
    /// `corruptRecord`: the data is fine, this node just cannot assemble it *yet*.
    case insufficientLocalShards(id: String, dataShards: Int)
    /// The uniqueness claim for a create could not be acquired on a majority of the record's
    /// owners - contended, or too many owners unreachable. Retryable, and crucially NOT the
    /// same as "the name is taken": returning `false` here would surface as a false
    /// `BucketAlreadyExists`/`usernameTaken` for a name that may not exist at all.
    case claimNotAcquired(collection: String, id: String)

    var description: String {
        switch self {
        case .corruptRecord(let collection, let id):
            "Metadata record \(collection)/\(id) failed decode/checksum verification"
        case .coordinatorUnreachable(let collection, let id):
            "Could not reach the coordinating node for metadata record \(collection)/\(id)"
        case .insufficientLocalShards(let id, let dataShards):
            "Metadata record \(id) is encoded across \(dataShards) shards and this node knows of no peers to gather them from yet"
        case .claimNotAcquired(let collection, let id):
            "Could not acquire the uniqueness claim for \(collection)/\(id) on a majority of its owners - please retry"
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
                try await withCoordinator(
                    app: app, routing: routing, collection: collection, id: id,
                    local: {
                        try await executeLocalPut(
                            app: app, key: key, value: value, routing: routing)
                    },
                    forward: { node in
                        try await forwardPut(
                            app: app, node: node, collection: collection, id: id, value: value)
                    })
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
        return try await withCoordinator(
            app: app, routing: routing, collection: collection, id: id,
            local: {
                try await executeLocalPutIfAbsent(
                    app: app, collection: collection, id: id, value: wrapped)
            },
            forward: { node in
                try await forwardPutIfAbsent(
                    app: app, node: node, collection: collection, id: id, value: wrapped)
            })
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
                app: app, node: routing.coordinator, collection: collection, id: id)
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
        let stored: Data? = try await withCoordinator(
            app: app, routing: routing, collection: collection, id: id,
            local: { try await executeLocalConsumeIfPresent(app: app, collection: collection, id: id) },
            forward: { node in
                try await forwardConsumeIfPresent(
                    app: app, node: node, collection: collection, id: id)
            })
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

        /// Where the record lives - fixed by placement, unaffected by who is up.
        var primary: ClusterNodeInfo? { responsible.first }

        /// Reachable owners, in placement order. Empty when liveness is unknown.
        var reachableResponsible: [ClusterNodeInfo] = []

        /// Who serializes writes for this key: the first *reachable* owner.
        ///
        /// Placement is deliberately stable, so a dead node keeps owning its keys - but it must
        /// not keep *coordinating* them, or every create hashing to it fails until an operator
        /// intervenes. Every node ranks owners identically and picks the first reachable one, so
        /// they agree on the coordinator without talking to each other.
        var coordinator: ClusterNodeInfo? { reachableResponsible.first ?? primary }

        /// True when this node should coordinate the write itself.
        var isLocalCoordinator: Bool {
            guard let selfNodeId, let coordinator else { return true }
            return coordinator.id == selfNodeId
        }

        /// Coordinators to try, in placement order: apparently-reachable owners first, the rest
        /// as a last resort. Liveness is only a hint - `activeNodes` tolerates a stale heartbeat
        /// for a minute, so a just-killed node still looks fine here. Callers therefore walk this
        /// list and advance only when a forward actually fails, rather than trusting the guess.
        var coordinatorCandidates: [ClusterNodeInfo] {
            let reachableIds = Set(reachableResponsible.map(\.id))
            return reachableResponsible + responsible.filter { !reachableIds.contains($0.id) }
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

        // Registered nodes, not live ones: a record must stay where it was written even
        // while a replica is down. See `ClusterNodeCache.placementNodes`.
        let active = await ClusterNodeCache.shared.placementNodes()
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
        let liveIds = Set(await ClusterNodeCache.shared.activeNodes().map(\.id))
        return Routing(
            responsible: responsible, selfNodeId: config.nodeId, dataShards: dataShards,
            parityShards: parityShards,
            reachableResponsible: responsible.filter {
                liveIds.contains($0.id) || $0.id == config.nodeId
            })
    }

    /// Runs an operation on the key's coordinator, falling forward through the remaining owners
    /// when one can't be reached.
    ///
    /// Placement is stable, so a dead node keeps owning its keys - it must not also keep
    /// coordinating them, or every create hashing to it fails until an operator intervenes. Every
    /// node walks the same order and only advances on an observed failure, so they keep agreeing
    /// on one coordinator per key.
    private static func withCoordinator<T>(
        app: Application, routing: Routing, collection: String, id: String,
        local: () async throws -> T,
        forward: (ClusterNodeInfo) async throws -> T
    ) async throws -> T {
        var lastError: (any Error)?
        // Self first when it's an owner (no network at all), then the other owners in placement
        // order. Every node computes the same order, so they agree on the coordinator.
        var candidates = routing.coordinatorCandidates
        if let selfNodeId = routing.selfNodeId,
            let selfIndex = candidates.firstIndex(where: { $0.id == selfNodeId })
        {
            candidates.swapAt(0, selfIndex)
        }
        for candidate in candidates {
            if candidate.id == routing.selfNodeId { return try await local() }
            do {
                return try await forward(candidate)
            } catch {
                lastError = error
                app.logger.warning(
                    "Coordinator \(candidate.id) unreachable for '\(collection)/\(id)' - trying the next owner: \(error)"
                )
            }
        }
        // Nothing reachable - or nobody else exists at all, which is the normal standalone
        // (non-clustered) case where `responsible` is empty and this node is the whole cluster.
        // Coordinating here beats failing outright: the alternative is that one unreachable node
        // blocks writes to its keys indefinitely.
        if let lastError {
            app.logger.warning(
                "No reachable coordinator for '\(collection)/\(id)' - coordinating locally: \(lastError)"
            )
        }
        return try await local()
    }

    // MARK: - Distributed claim (unique-name safety across coordinators)

    /// Runs `body` holding a majority reservation on `(collection, id)` across the record's
    /// owners, or returns `nil` if a majority couldn't be reached.
    ///
    /// Coordination is per-node and liveness-dependent, so two nodes can briefly disagree about
    /// who coordinates a key and both run a check-then-write. A single node grants at most one
    /// live reservation per name, so only one of them can hold a majority - the other backs off
    /// instead of writing a duplicate. Standalone (no peers) is trivially a majority of one.
    private static func withClaimQuorum<T>(
        app: Application, routing: Routing, collection: String, id: String,
        body: () async throws -> T
    ) async throws -> T? {
        let owners = routing.responsible
        let token = UUID()
        // Owners other than this node; the local grant is taken directly.
        let peers = owners.filter { $0.id != routing.selfNodeId }
        let required = owners.isEmpty ? 1 : owners.count / 2 + 1

        await MetadataClaimRegistry.shared.purgeExpired()
        var granted = await MetadataClaimRegistry.shared.reserve(
            collection: collection, id: id, token: token) ? 1 : 0
        var grantedPeers: [ClusterNodeInfo] = []

        if granted > 0, !peers.isEmpty {
            let outcomes = await withTaskGroup(of: (ClusterNodeInfo, Bool).self) { group in
                for peer in peers {
                    group.addTask {
                        (
                            peer,
                            await requestClaim(
                                app: app, node: peer, collection: collection, id: id, token: token)
                        )
                    }
                }
                var results: [(ClusterNodeInfo, Bool)] = []
                for await outcome in group { results.append(outcome) }
                return results
            }
            for (peer, ok) in outcomes where ok {
                granted += 1
                grantedPeers.append(peer)
            }
        }

        func releaseAll() async {
            await MetadataClaimRegistry.shared.release(
                collection: collection, id: id, token: token)
            await withTaskGroup(of: Void.self) { group in
                for peer in grantedPeers {
                    group.addTask {
                        await releaseClaim(
                            app: app, node: peer, collection: collection, id: id, token: token)
                    }
                }
            }
        }

        guard granted >= required else {
            await releaseAll()
            return nil
        }
        do {
            let result = try await body()
            await releaseAll()
            return result
        } catch {
            await releaseAll()
            throw error
        }
    }

    private static func requestClaim(
        app: Application, node: ClusterNodeInfo, collection: String, id: String, token: UUID
    ) async -> Bool {
        await claimCall(
            app: app, node: node, path: "claim", collection: collection, id: id, token: token)
    }

    private static func releaseClaim(
        app: Application, node: ClusterNodeInfo, collection: String, id: String, token: UUID
    ) async {
        _ = await claimCall(
            app: app, node: node, path: "claim-release", collection: collection, id: id,
            token: token)
    }

    private static func claimCall(
        app: Application, node: ClusterNodeInfo, path: String, collection: String, id: String,
        token: UUID
    ) async -> Bool {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return false }
        var outbound = HTTPClientRequest(
            url: node.address + "/internal/cluster/metadata/\(path)"
                + querySuffix(collection: collection, id: id) + "&token=\(token.uuidString)")
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        do {
            let response = try await LightweightClusterControlClient.shared.execute(
                outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
            return response.status == .ok
        } catch {
            // Unreachable owner: not a grant. A majority of the rest can still be reached.
            return false
        }
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
        // Key lock OUTSIDE the claim, never inside. Two concurrent creates of the same name on
        // THIS node must serialize on the lock and let the loser see the winner's record (a clean
        // "already exists"); if they raced for the claim instead, the loser would fail its own
        // node's reservation and surface a retryable error for a name that is simply taken. It
        // also means only one request per key per node ever reaches the quorum RPC.
        let claimed = try await MetadataKeyLock.shared.withLock(collection: collection, key: id) {
            try await withClaimQuorum(
                app: app, routing: routing, collection: collection, id: id
            ) {
                // A full gather, never a local shard-0 file check: being rank-0 today says nothing
                // about where this record's shards were placed when it was written.
                //
                // A tombstoned id is free to claim again (a released username or bucket name must
                // be reusable). A read failure PROPAGATES rather than becoming `false`, which
                // callers read as "already taken" and would turn a legitimate create into a
                // silent no-op that still looks like success.
                let existing = try await localGet(app: app, key: key, routing: routing)
                if let existing, !MetadataEnvelope.decode(existing).isTombstone {
                    return false
                }
                try await localWrite(app: app, key: key, value: value, routing: routing)
                return true
            }
        }
        guard let claimed else {
            throw MetadataStoreError.claimNotAcquired(collection: collection, id: id)
        }
        return claimed
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
        // Same majority claim as `putIfAbsent`, and the same lock-outside-claim ordering: local
        // consumers serialize on the lock, so only one of them ever contends for the quorum.
        let consumed = try await MetadataKeyLock.shared.withLock(collection: collection, key: id) {
            try await withClaimQuorum(
                app: app, routing: routing, collection: collection, id: id
            ) {
                guard let stored = try await localGet(app: app, key: key, routing: routing),
                    !MetadataEnvelope.decode(stored).isTombstone
                else { return Data?.none }

                // Returns the stored envelope verbatim; the caller unwraps once, so this stays
                // correct whether it ran locally or was relayed from a peer.
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
        guard let consumed else {
            throw MetadataStoreError.claimNotAcquired(collection: collection, id: id)
        }
        return consumed
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
        var routing = routing
        if routing.responsible.isEmpty, app.storage[ClusterConfigurationKey.self] != nil {
            // An empty membership view in cluster mode is a transient startup state, not a fact
            // about the cluster: this node has restarted and hasn't re-seeded yet. Falling
            // straight through to the single-shard local read here is what made a restarting node
            // declare every multi-shard record unreadable. Re-query the statically configured
            // seeds first (debounced inside `refreshNow`) and re-resolve - the same repair
            // `ObjectRoutingService` performs before refusing a write for "too few nodes".
            await ClusterMembershipLifecycle.shared.refreshNow(app: app)
            routing = await resolveRouting(app: app, key: key)
        }

        guard !routing.responsible.isEmpty, let selfNodeId = routing.selfNodeId else {
            return try await directLocalRead(app: app, key: key)
        }

        // Do NOT "optimize" this into a local-only read when this node already holds a whole copy
        // (metadata is k=1, so it does). The gather is what resolves disagreement between replicas
        // by newest generation - skipping it would happily serve a copy this node holds but that a
        // newer write never reached, which is exactly how a deleted record (a revoked access key)
        // comes back to life. The extra probes are the price of that correctness, and they are
        // paid only on a cache miss, never per request.

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

    /// This node's own copy of a record, decoded without contacting anybody.
    ///
    /// Possible whenever the record is REPLICATED (`dataShards == 1`, the default metadata
    /// layout): the local copy is the whole record, so there is nothing to gather. `nil` when
    /// this node holds nothing for the key, or holds part of an older striped (`k > 1`) record -
    /// the caller must fall back to a cluster gather for those.
    ///
    /// This is what makes a cluster-wide listing cheap: without it, reporting the N records a
    /// node already holds costs N gathers (each probing every responsible node), which routinely
    /// overran the caller's 5s budget and made the node contribute *nothing* to the listing.
    /// Reporting exactly what this node holds is also what the merge in
    /// `MetadataListingService.list` expects - it resolves disagreement between nodes itself,
    /// newest-wins.
    static func localEnvelopeIfWholeCopy(app: Application, collection: String, id: String) async
        -> MetadataEnvelope?
    {
        let key = MetadataNamespace.key(collection: collection, id: id)
        guard let header = localShardHeader(key: key), header.dataShards == 1 else { return nil }
        guard let stored = try? await directLocalRead(app: app, key: key) else { return nil }
        return MetadataEnvelope.decode(stored)
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

    /// Last-resort read with no cluster to gather from: decode this node's own shard alone.
    ///
    /// Only ever valid for a k=1 record, where that single shard IS the whole payload. A wider
    /// record cannot be reconstructed from one shard by definition, and treating that as a decode
    /// failure was actively harmful: a node whose membership view was momentarily empty (the first
    /// moments after a restart) reported its ENTIRE control plane as corrupt, which cascaded into
    /// `AccessDenied` on valid credentials, because an unreadable access key is indistinguishable
    /// from a missing one to every caller above this line.
    private static func directLocalRead(app: Application, key: String) async throws -> Data? {
        // The header of whatever index this node actually holds - not a hardcoded shard 0, which
        // a node holding only shard 1 of a wider record would read as "no such record".
        guard let header = localShardHeader(key: key) else { return nil }
        guard header.dataShards == 1 else {
            throw MetadataStoreError.insufficientLocalShards(
                id: key, dataShards: header.dataShards)
        }
        let path = ErasureCodedObjectHandler.shardPath(
            bucketName: MetadataNamespace.bucketName, key: key, versionId: nil,
            shardIndex: header.shardIndex)
        return try await S3Service.offloadBlockingIO(app) {
            var collected = Data()
            do {
                // Keyed by the shard's TRUE index. This node may hold a parity copy rather than
                // index 0, and with k=1 a parity shard is a transform of the data, not a byte
                // copy of it - handing it over as index 0 decodes to garbage.
                try StripeDecoder.decode(shardPaths: [header.shardIndex: path], range: nil) {
                    chunk in
                    collected.append(chunk)
                }
            } catch {
                // A k=1 record that won't decode from its only shard genuinely is damaged.
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
