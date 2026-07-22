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

import Vapor

/// Admin-only console API for cluster visibility/control - node list + health, object placement
/// (computed on the fly, never persisted, since `PlacementService` is a cheap pure function),
/// rebalance progress, and drain/resync actions. Mounted under `/api/v1/admin/cluster/*`,
/// gated by `InternalAuthenticator` + `requireAdmin()` exactly like every other
/// `InternalAdminController` handler.
struct InternalClusterController: RouteCollection {
    struct ClusterNodeDTO: Content {
        let id: UUID
        let address: String
        let status: String
        let joinedAt: Date
        let lastHeartbeatAt: Date
        /// `status == active` AND the heartbeat hasn't gone stale - the same check
        /// `ClusterNodeCache.activeNodes` uses to decide placement eligibility, surfaced here so
        /// the console doesn't need to reimplement the staleness math.
        let isHealthy: Bool
        /// Self-reported disk capacity - `nil` until this node's first post-upgrade heartbeat.
        let totalBytes: Int64?
        let availableBytes: Int64?
        /// Same check `ClusterCapacityPolicy.isNearFull` uses to decide whether new-write
        /// coordination should prefer another node - surfaced here so the console doesn't need
        /// to reimplement the threshold math.
        let isNearFull: Bool
    }

    struct RebalanceStatusDTO: Content {
        let pendingCount: Int
        let failedCount: Int
        let pendingByReason: [String: Int]
        /// The cluster-wide constant `PlacementService.replicationFactor` - surfaced so the
        /// console can show it without hardcoding a value that would silently drift if the
        /// constant ever changes.
        let replicationFactor: Int
    }

    struct PlacementEntryDTO: Content {
        let key: String
        let nodeIds: [UUID]
        let size: Int
    }

    struct NodeStorageDTO: Content {
        let nodeId: UUID
        let sizeBytes: Int64
        let objectCount: Int
    }

    /// One outbox row's full detail - the drill-down behind the "Pending Replication by Reason"
    /// summary, for the exact question that summary can't answer: *which* node is a stuck task
    /// targeting, and why does it keep failing.
    struct ReplicationTaskDetailDTO: Content {
        let id: UUID
        let bucketName: String
        let key: String
        let operation: String
        let targetNodeId: UUID
        let reason: String
        let attempts: Int
        let nextAttemptAt: Date
        let state: String
        let lastError: String?
    }

    /// Deployment-wide erasure-coding health. `enabled` is false when this cluster runs plain
    /// 3x replication (no `CLUSTER_EC_*` config) - the console shows the k/m parameters and shard-
    /// repair backlog only then. `pendingReconstructCount` is broken out from the general pending
    /// tally because a `.reconstruct` task means a shard was *permanently lost* and is being
    /// rebuilt from survivors (a genuine durability event), distinct from a `.write`/`.rebalance`
    /// task that just moves or catches up an existing shard.
    struct ErasureCodingStatusDTO: Content {
        let enabled: Bool
        let dataShards: Int
        let parityShards: Int
        let totalShards: Int
        let quorumThreshold: Int
        let pendingCount: Int
        let failedCount: Int
        let pendingReconstructCount: Int
        let pendingByReason: [String: Int]
    }

    /// One EC shard-repair outbox row's full detail - the shard-level counterpart of
    /// `ReplicationTaskDetailDTO`, adding the `shardIndex`/`versionId` a shard task carries that a
    /// whole-object replication task doesn't.
    struct ErasureCodedTaskDetailDTO: Content {
        let id: UUID
        let bucketName: String
        let key: String
        let versionId: String?
        let shardIndex: Int
        let operation: String
        let targetNodeId: UUID
        let reason: String
        let attempts: Int
        let nextAttemptAt: Date
        let state: String
        let lastError: String?
    }

    func boot(routes: any RoutesBuilder) throws {
        let cluster = routes.grouped("admin").grouped("cluster")
        cluster.grouped("nodes").get(use: listNodes)
        cluster.grouped("nodes", ":nodeId", "drain").post(use: drainNode)
        cluster.grouped("resync").post(use: resync)
        cluster.grouped("rebalance", "status").get(use: rebalanceStatus)
        cluster.grouped("rebalance", "tasks").get(use: rebalanceTasks)
        cluster.grouped("erasure-coding", "status").get(use: erasureCodingStatus)
        cluster.grouped("erasure-coding", "tasks").get(use: erasureCodingTasks)
        cluster.grouped("erasure-coding", "scrub").post(use: erasureCodingScrub)
        cluster.grouped("placement").get(use: placement)
        cluster.grouped("storage").get(use: storage)
    }

    @Sendable
    func listNodes(req: Request) async throws -> [ClusterNodeDTO] {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let nodes = await ClusterNode.all(app: req.application)
            .sorted { $0.joinedAt < $1.joinedAt }
        let now = Date()
        return nodes.map { node -> ClusterNodeDTO in
            let isHealthy =
                node.status == .active
                && now.timeIntervalSince(node.lastHeartbeatAt) <= ClusterNodeCache.heartbeatStaleness
            return ClusterNodeDTO(
                id: node.id, address: node.address, status: node.status.rawValue, joinedAt: node.joinedAt,
                lastHeartbeatAt: node.lastHeartbeatAt, isHealthy: isHealthy,
                totalBytes: node.totalBytes, availableBytes: node.availableBytes,
                isNearFull: ClusterCapacityPolicy.isNearFull(
                    totalBytes: node.totalBytes, availableBytes: node.availableBytes))
        }
    }

    /// Excludes the node from new placement immediately (its next membership-refresh pull picks
    /// up the status change everywhere) and kicks off a rebalance walk to migrate its data off -
    /// the operator is expected to wait for `rebalanceStatus` to show no more pending/failed
    /// tasks before actually stopping the node's process.
    @Sendable
    func drainNode(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let nodeId = req.parameters.get("nodeId", as: UUID.self),
            let node = try await ClusterNode.find(app: req.application, id: nodeId)
        else {
            throw Abort(.notFound, reason: "Cluster node not found")
        }

        node.status = .draining
        try await node.save(app: req.application)

        // Any outstanding task that exists to keep this node in sync as a *responsible* replica
        // is now pointless - draining excludes it from placement. Deliberately NOT touching
        // `reclaim` tasks: those are the drained node cleaning up its own now-unowned copies,
        // which still runs. Cluster-wide broadcast, not a local delete: `ClusterReplicationTask`'s
        // mailbox owner is the *sender*, not `targetNodeId`, so stale tasks can live anywhere.
        await OutboxMailbox.purgeByTargetNodeAcrossCluster(
            ClusterReplicationTask.self, app: req.application,
            collection: OutboxCollections.clusterReplicationTasks, targetNodeId: nodeId
        ) { $0.targetNodeId == nodeId && $0.reason != .reclaim }

        // Same cleanup for the EC shard-repair outbox - a stale row aimed at this node's now-
        // obsolete rank is equally pointless. Unlike legacy replication, EC has no `.reclaim`-
        // reason rows to preserve (reclaiming a stale local shard is done inline, never via the
        // outbox), so every row targeting this node is cleared.
        await OutboxMailbox.purgeByTargetNodeAcrossCluster(
            ErasureCodedReplicationTask.self, app: req.application,
            collection: OutboxCollections.erasureCodedReplicationTasks, targetNodeId: nodeId
        ) { $0.targetNodeId == nodeId }

        // Webhook/external-replication tasks have no independent ground truth to regenerate them
        // (unlike the two collections above) - if this node owns any, they must be reassigned to
        // a still-active peer *before* it's allowed to leave, or they'd simply never be delivered.
        await OutboxMailbox.reassignOwnedTasks(
            NotificationDelivery.self, app: req.application,
            collection: OutboxCollections.notificationDeliveries, departingNodeId: nodeId)
        await OutboxMailbox.reassignOwnedTasks(
            ReplicationTask.self, app: req.application, collection: OutboxCollections.replicationTasks,
            departingNodeId: nodeId)

        await ClusterNodeCache.shared.upsert(
            ClusterNodeInfo(
                id: nodeId, address: node.address, status: .draining,
                lastHeartbeatAt: node.lastHeartbeatAt,
                totalBytes: node.totalBytes, availableBytes: node.availableBytes))
        CacheInvalidationService.notify(
            app: req.application, cache: "clusterNode", op: .upsert, key: nodeId.uuidString,
            nodeInfo: InternalClusterMetadataController.ClusterMemberWire(
                id: nodeId, address: node.address, status: .draining,
                lastHeartbeatAt: node.lastHeartbeatAt, totalBytes: node.totalBytes,
                availableBytes: node.availableBytes))
        // `CacheReloadDispatch`'s `("clusterNode", .upsert)` case triggers a rebalance walk on
        // every node that RECEIVES the broadcast above - but `notify` deliberately excludes the
        // caller itself (see `resync`'s identical doc comment), so without this explicit call,
        // whichever admin node happens to field this drain request would never run its own walk,
        // even if it physically holds shards that now need to move off the just-drained node.
        await ClusterRebalanceService.scheduleRebalance(
            app: req.application, reason: .membershipChange)
        await ErasureCodedRebalanceService.scheduleRebalance(
            app: req.application, reason: .membershipChange)

        return .ok
    }

    /// Manually triggers a rebalance walk on **every** node - the recovery path after a node has
    /// crashed or recovered past the outbox's dead-letter point. A rebalance walk only sees the
    /// node's own local disk, so re-replicating an under-replicated object requires the node that
    /// still holds it to run its own walk - hence this broadcasts a `clusterRebalance` NOTIFY.
    @Sendable
    func resync(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        // `CacheInvalidationService.notify`'s broadcast deliberately excludes the caller itself
        // (every other invalidation type re-reads/re-derives its OWN state locally before
        // broadcasting, so self was never a target to begin with) - but a resync has no such
        // local step of its own, so without this explicit call, whichever node happens to field
        // this admin request would broadcast the walk to every OTHER node while never running its
        // own. Both calls are required for genuine "every node" coverage.
        await ClusterRebalanceService.scheduleRebalance(app: req.application, reason: .manualResync)
        await ErasureCodedRebalanceService.scheduleRebalance(
            app: req.application, reason: .manualResync)
        CacheInvalidationService.notify(
            app: req.application, cache: "clusterRebalance", op: .upsert, key: "resync")
        return .ok
    }

    @Sendable
    func rebalanceStatus(req: Request) async throws -> RebalanceStatusDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        // Cluster-wide fan-out (`OutboxMailbox`'s mailboxes are per-owner-node, not a shared
        // table) then tallied in Swift - matches the "full-scan-then-filter" idiom already used
        // for every other cluster-wide outbox view in this migration.
        let tasks = await OutboxMailbox.listAllAcrossCluster(
            ClusterReplicationTask.self, app: req.application,
            collection: OutboxCollections.clusterReplicationTasks)

        var byReason: [String: Int] = [:]
        var pendingCount = 0
        for task in tasks where task.state == .pending {
            pendingCount += 1
            byReason[task.reason.rawValue, default: 0] += 1
        }
        let failedCount = tasks.filter { $0.state == .failed }.count

        return RebalanceStatusDTO(
            pendingCount: pendingCount, failedCount: failedCount, pendingByReason: byReason,
            replicationFactor: PlacementService.replicationFactor)
    }

    /// Full detail for outstanding (pending or failed) outbox rows - the "Pending Replication by
    /// Reason" card only ever answers "how many," which is exactly the gap operators keep hitting
    /// ("write: 8 stuck - is this normal?"): without knowing *which node* those 8 are aimed at and
    /// *why* they keep failing (`lastError`), there's no way to tell "waiting out a slow peer's
    /// backoff" from "targeting a node that's actually gone." Sorted by attempts descending so the
    /// most-stuck rows surface first; capped at 200 - a drill-down view, not a paginated browser.
    @Sendable
    func rebalanceTasks(req: Request) async throws -> [ReplicationTaskDetailDTO] {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let tasks = await OutboxMailbox.listAllAcrossCluster(
            ClusterReplicationTask.self, app: req.application,
            collection: OutboxCollections.clusterReplicationTasks
        ).filter {
            $0.state == .pending
                || $0.state == .failed
        }
        .sorted { $0.attempts > $1.attempts }
        .prefix(200)

        return tasks.map { task in
            ReplicationTaskDetailDTO(
                id: task.id, bucketName: task.bucketName, key: task.key, operation: task.operation.rawValue,
                targetNodeId: task.targetNodeId, reason: task.reason.rawValue, attempts: task.attempts,
                nextAttemptAt: task.nextAttemptAt, state: task.state.rawValue, lastError: task.lastError)
        }
    }

    /// Deployment-wide erasure-coding health: the configured k/m parameters plus the shard-repair
    /// backlog (the EC counterpart of `rebalanceStatus`). The counts come from the
    /// `erasure_coded_replication_tasks` outbox, counted-not-loaded exactly like `rebalanceStatus`,
    /// so a cluster mid-reconstruction with a huge backlog doesn't pull every row into memory.
    @Sendable
    func erasureCodingStatus(req: Request) async throws -> ErasureCodingStatusDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let ecConfig = req.application.storage[ClusterErasureCodingConfigKey.self] else {
            return ErasureCodingStatusDTO(
                enabled: false, dataShards: 0, parityShards: 0, totalShards: 0, quorumThreshold: 0,
                pendingCount: 0, failedCount: 0, pendingReconstructCount: 0, pendingByReason: [:])
        }

        let tasks = await OutboxMailbox.listAllAcrossCluster(
            ErasureCodedReplicationTask.self, app: req.application,
            collection: OutboxCollections.erasureCodedReplicationTasks)

        var byReason: [String: Int] = [:]
        var pendingCount = 0
        for task in tasks where task.state == .pending {
            pendingCount += 1
            byReason[task.reason.rawValue, default: 0] += 1
        }
        let failedCount = tasks.filter { $0.state == .failed }
            .count

        return ErasureCodingStatusDTO(
            enabled: true,
            dataShards: ecConfig.dataShards,
            parityShards: ecConfig.parityShards,
            totalShards: ecConfig.totalShards,
            quorumThreshold: PlacementService.ecQuorumThreshold(
                dataShards: ecConfig.dataShards, parityShards: ecConfig.parityShards),
            pendingCount: pendingCount,
            failedCount: failedCount,
            pendingReconstructCount: byReason[ErasureCodedReplicationTask.Reason.reconstruct.rawValue] ?? 0,
            pendingByReason: byReason)
    }

    /// Full detail for outstanding EC shard-repair rows - the drill-down behind
    /// `erasureCodingStatus`, shard-scoped. Sorted most-stuck-first, capped at 200.
    ///
    /// Triggers an immediate bit-rot scrub on **every** node - the EC counterpart of `resync`,
    /// needed since a scrub only ever sees a node's own local shards. Returns immediately; the
    /// scrub runs in the background and reports through the erasure-coding status endpoint.
    @Sendable
    func erasureCodingScrub(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        // See `resync`'s identical doc comment: `CacheInvalidationService.notify`'s broadcast
        // deliberately excludes the caller itself, so without this explicit local call, whichever
        // node happens to field this admin request would trigger every OTHER node's scrub while
        // never scrubbing its own local shards - the exact gap that let a corrupted shard on the
        // fielding node itself go undetected no matter how many times this endpoint was called.
        Task { await ErasureCodedScrubber.scrub(app: req.application) }
        CacheInvalidationService.notify(
            app: req.application, cache: "clusterScrub", op: .upsert, key: "scrub")
        return .ok
    }

    @Sendable
    func erasureCodingTasks(req: Request) async throws -> [ErasureCodedTaskDetailDTO] {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let tasks = await OutboxMailbox.listAllAcrossCluster(
            ErasureCodedReplicationTask.self, app: req.application,
            collection: OutboxCollections.erasureCodedReplicationTasks
        ).filter {
            $0.state == .pending
                || $0.state == .failed
        }
        .sorted { $0.attempts > $1.attempts }
        .prefix(200)

        return tasks.map { task in
            ErasureCodedTaskDetailDTO(
                id: task.id, bucketName: task.bucketName, key: task.key, versionId: task.versionId,
                shardIndex: task.shardIndex, operation: task.operation.rawValue,
                targetNodeId: task.targetNodeId, reason: task.reason.rawValue, attempts: task.attempts,
                nextAttemptAt: task.nextAttemptAt, state: task.state.rawValue, lastError: task.lastError)
        }
    }

    /// Paginated placement for a bucket (optionally prefix-scoped) - computed on the fly via
    /// `PlacementService`, never persisted. Not backed by a Fluent query, so pagination is done
    /// by hand over the in-memory listing rather than `.paginate(for:)`.
    @Sendable
    func placement(req: Request) async throws -> Page<PlacementEntryDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing bucket query parameter")
        }
        // Unlike every other query param this route reads, `bucket` flows straight into
        // filesystem path construction (via ClusterListingService -> ObjectFileHandler ->
        // BucketHandler.bucketURL) - confirming it names a real, previously-validated bucket
        // (CreateBucket already enforces safe S3 bucket-naming rules) rules out an admin-authed
        // caller passing an arbitrary string through to disk I/O.
        guard try await Bucket.find(app: req.application, name: bucketName) != nil else {
            throw Abort(.notFound, reason: "Bucket not found")
        }
        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let nodeFilter = req.query[UUID.self, at: "nodeId"]
        let page = req.query[Int.self, at: "page"] ?? 1
        let per = req.query[Int.self, at: "per"] ?? 25

        let (objects, _, _, _) = try await ClusterListingService.listObjects(
            req: req, bucketName: bucketName, prefix: prefix, delimiter: nil, maxKeys: 1000,
            marker: nil)
        // Reports where objects actually live, so it must use the same set placement does.
        let active = await ClusterNodeCache.shared.placementNodes()

        func entry(for object: ObjectMeta) -> PlacementEntryDTO {
            let responsible = PlacementService.responsibleNodes(
                bucketName: bucketName, key: object.key, activeNodes: active)
            return PlacementEntryDTO(
                key: object.key, nodeIds: responsible.map(\.id), size: object.size)
        }

        if let nodeFilter {
            // "Which keys does this node hold" needs every fetched object's placement resolved
            // before paging (a non-matching object can sort anywhere in the listing), unlike the
            // unfiltered path below - bounded by the same up-to-1000-object listing cap every
            // other placement view already has.
            let matching = objects.map(entry(for:)).filter { $0.nodeIds.contains(nodeFilter) }
            let start = max(0, (page - 1) * per)
            let end = min(matching.count, start + per)
            return Page(
                items: start < end ? Array(matching[start..<end]) : [],
                metadata: PageMetadata(page: page, per: per, total: matching.count))
        }

        // No node filter: placement hashing (SHA256-based HRW, one call per object) only ever
        // runs over this page's objects, not the full up-to-1000-object listing - cost scales
        // with `per`, not with how much of the bucket was fetched.
        let start = max(0, (page - 1) * per)
        let end = min(objects.count, start + per)
        let pageItems = start < end ? Array(objects[start..<end]).map(entry(for:)) : []

        return Page(items: pageItems, metadata: PageMetadata(page: page, per: per, total: objects.count))
    }

    /// Per-node storage breakdown across the whole cluster - expensive (a full multi-bucket disk
    /// walk on every active node), so the console fetches this only on manual refresh, never on
    /// its regular node/rebalance-status poll.
    @Sendable
    func storage(req: Request) async throws -> [NodeStorageDTO] {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let breakdown = try await ClusterListingService.nodeStorageBreakdown(req: req)
        return breakdown.map {
            NodeStorageDTO(nodeId: $0.nodeId, sizeBytes: $0.sizeBytes, objectCount: $0.objectCount)
        }
    }
}
