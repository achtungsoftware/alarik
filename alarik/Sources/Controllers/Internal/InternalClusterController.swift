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

import Fluent
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

    func boot(routes: any RoutesBuilder) throws {
        let cluster = routes.grouped("admin").grouped("cluster")
        cluster.grouped("nodes").get(use: listNodes)
        cluster.grouped("nodes", ":nodeId", "drain").post(use: drainNode)
        cluster.grouped("resync").post(use: resync)
        cluster.grouped("rebalance", "status").get(use: rebalanceStatus)
        cluster.grouped("placement").get(use: placement)
        cluster.grouped("storage").get(use: storage)
    }

    @Sendable
    func listNodes(req: Request) async throws -> [ClusterNodeDTO] {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let nodes = try await ClusterNode.query(on: req.db).sort(\.$joinedAt, .ascending).all()
        let now = Date()
        return nodes.compactMap { node -> ClusterNodeDTO? in
            guard let id = node.id else { return nil }
            let isHealthy =
                node.status == ClusterNode.Status.active.rawValue
                && now.timeIntervalSince(node.lastHeartbeatAt) <= ClusterNodeCache.heartbeatStaleness
            return ClusterNodeDTO(
                id: id, address: node.address, status: node.status, joinedAt: node.joinedAt,
                lastHeartbeatAt: node.lastHeartbeatAt, isHealthy: isHealthy)
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
            let node = try await ClusterNode.find(nodeId, on: req.db)
        else {
            throw Abort(.notFound, reason: "Cluster node not found")
        }

        node.status = ClusterNode.Status.draining.rawValue
        try await node.save(on: req.db)

        await ClusterNodeCache.shared.upsert(
            ClusterNodeInfo(
                id: nodeId, address: node.address, status: .draining,
                lastHeartbeatAt: node.lastHeartbeatAt))
        CacheInvalidationService.notify(
            on: req.db, cache: "clusterNode", op: .upsert, key: nodeId.uuidString)

        return .ok
    }

    /// Manually triggers a rebalance walk on **every** node - the recovery path after a node has
    /// crashed (a silent failure emits no membership NOTIFY, so nothing re-replicates its data
    /// automatically) or recovered past the outbox's dead-letter point. A rebalance walk only
    /// ever sees the node's own local disk, so re-replicating an under-replicated object requires
    /// the node that still holds it to run its own walk - hence this broadcasts a `clusterRebalance`
    /// NOTIFY that fans the walk out to all nodes, rather than rebalancing only whichever node
    /// happened to field this request.
    @Sendable
    func resync(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        CacheInvalidationService.notify(
            on: req.db, cache: "clusterRebalance", op: .upsert, key: "resync")
        return .ok
    }

    @Sendable
    func rebalanceStatus(req: Request) async throws -> RebalanceStatusDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let pending = try await ClusterReplicationTask.query(on: req.db)
            .filter(\.$state == ClusterReplicationTask.State.pending.rawValue)
            .all()
        let failedCount = try await ClusterReplicationTask.query(on: req.db)
            .filter(\.$state == ClusterReplicationTask.State.failed.rawValue)
            .count()

        var byReason: [String: Int] = [:]
        for task in pending {
            byReason[task.reason, default: 0] += 1
        }

        return RebalanceStatusDTO(
            pendingCount: pending.count, failedCount: failedCount, pendingByReason: byReason,
            replicationFactor: PlacementService.replicationFactor)
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
        guard try await Bucket.query(on: req.db).filter(\.$name == bucketName).first() != nil
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }
        let prefix = req.query[String.self, at: "prefix"] ?? ""

        let (objects, _, _, _) = try await ClusterListingService.listObjects(
            req: req, bucketName: bucketName, prefix: prefix, delimiter: nil, maxKeys: 1000,
            marker: nil)

        let page = req.query[Int.self, at: "page"] ?? 1
        let per = req.query[Int.self, at: "per"] ?? 25
        let start = max(0, (page - 1) * per)
        let end = min(objects.count, start + per)
        let pageObjects = start < end ? Array(objects[start..<end]) : []

        // Placement hashing (SHA256-based HRW, one call per object) only ever runs over this
        // page's objects, not the full up-to-1000-object listing - cost scales with `per`, not
        // with how much of the bucket was fetched.
        let active = await ClusterNodeCache.shared.activeNodes()
        let pageItems = pageObjects.map { object -> PlacementEntryDTO in
            let responsible = PlacementService.responsibleNodes(
                bucketName: bucketName, key: object.key, activeNodes: active)
            return PlacementEntryDTO(
                key: object.key, nodeIds: responsible.map(\.id), size: object.size)
        }

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
