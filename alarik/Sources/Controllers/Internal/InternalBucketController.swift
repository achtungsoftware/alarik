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
import XMLCoder
import ZIPFoundation

struct InternalBucketController: RouteCollection {
    struct VersioningStatusDTO: Content {
        let status: String
    }

    struct PolicyDTO: Content {
        let policy: String?
    }

    struct TagsDTO: Content {
        let tags: [String: String]
    }

    struct ObjectMetadataDTO: Content {
        let contentType: String
        let metadata: [String: String]
    }

    struct StatsDTO: Content {
        let sizeBytes: Int64
        let objectCount: Int
    }

    struct NotificationConfigDTO: Content {
        let rules: [NotificationRule]
    }

    struct DeliveryDTO: Content {
        let id: UUID
        let ruleId: UUID
        let url: String
        let state: String
        let attempts: Int
        let nextAttemptAt: Date
        let lastError: String?
        let createdAt: Date

        init(from delivery: NotificationDelivery) {
            self.id = delivery.id
            self.ruleId = delivery.ruleId
            self.url = delivery.url
            self.state = delivery.state
            self.attempts = delivery.attempts
            self.nextAttemptAt = delivery.nextAttemptAt
            self.lastError = delivery.lastError
            self.createdAt = delivery.createdAt
        }
    }

    struct DeliveriesDTO: Content {
        let deliveries: [DeliveryDTO]
    }

    struct ReplicationTargetsDTO: Content {
        let targets: [ReplicationTarget]
    }

    /// Fixed placeholder every already-configured webhook `secret`/replication
    /// `secretAccessKey` is replaced with before being sent to the client - GET and PUT
    /// responses never echo the real value back over the wire, matching the write-only pattern
    /// `AccessKey.ResponseDTO` already uses for access-key secrets. Unlike a full omission,
    /// this stays a non-empty, truthy string so the console can still show "a secret is
    /// configured" (e.g. the webhooks table's "Signed (HMAC)" badge) without ever revealing it,
    /// and it doubles as the "unchanged" sentinel on the way back in: `setBucketNotifications`/
    /// `setReplicationTargets` substitute the real stored secret whenever an existing rule's/
    /// target's incoming value is exactly this placeholder, so the console's existing
    /// edit-prefills-then-echoes-back flow round-trips correctly without ever needing to know
    /// the real secret.
    private static let secretMaskPlaceholder = "••••••••"

    private static func maskedNotificationRules(_ rules: [NotificationRule]) -> [NotificationRule] {
        rules.map { rule in
            var masked = rule
            if let secret = rule.secret, !secret.isEmpty {
                masked.secret = secretMaskPlaceholder
            }
            return masked
        }
    }

    private static func maskedReplicationTargets(_ targets: [ReplicationTarget]) -> [ReplicationTarget] {
        targets.map { target in
            var masked = target
            if !target.secretAccessKey.isEmpty {
                masked.secretAccessKey = secretMaskPlaceholder
            }
            return masked
        }
    }

    struct ReplicationRulesDTO: Content {
        let rules: [ReplicationRule]
    }

    struct ReplicationTaskDTO: Content {
        let id: UUID
        let ruleId: UUID
        let targetId: UUID
        let endpoint: String
        let key: String
        let versionId: String?
        let operation: String
        let state: String
        let attempts: Int
        let nextAttemptAt: Date
        let lastError: String?
        let createdAt: Date

        init(from task: ReplicationTask) {
            self.id = task.id
            self.ruleId = task.ruleId
            self.targetId = task.targetId
            self.endpoint = task.endpoint
            self.key = task.key
            self.versionId = task.versionId
            self.operation = task.operation
            self.state = task.state
            self.attempts = task.attempts
            self.nextAttemptAt = task.nextAttemptAt
            self.lastError = task.lastError
            self.createdAt = task.createdAt
        }
    }

    struct ReplicationTasksDTO: Content {
        let tasks: [ReplicationTaskDTO]
    }

    struct ShareRequestDTO: Content {
        let bucket: String
        let key: String
        /// Omit (or send null) for a link that never expires - it then works until explicitly
        /// revoked.
        let expiresInSeconds: Int?
    }

    struct ShareResponseDTO: Content {
        let url: String
        /// Nil for a link that never expires.
        let expiresAt: Date?
    }

    /// The global `app.routes.defaultMaxBodySize` (`configure.swift`) is set to 5TB - it has to
    /// be, to fit an actual S3 object upload. Every one of these routes is a small JSON
    /// subresource body (a bucket policy, a handful of tags, a few webhook rules) with no
    /// business ever buffering anywhere close to that: registered with the default `body:
    /// .collect` behavior (no explicit `maxSize`), Vapor would happily buffer up to 5TB in
    /// memory before the handler even runs and gets a chance to reject anything. Every route
    /// below that decodes a JSON body (not a file upload) is registered with this override
    /// instead of the `defaultMaxBodySize`-inheriting `.put(use:)`/`.post(use:)` convenience
    /// methods, to actually bound that worst case.
    private static let subresourceBodySizeLimit: ByteCount = "1mb"

    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("buckets").get(use: self.listBuckets)
        routes.grouped("buckets").post(use: self.createBucket)
        routes.grouped("buckets").grouped(":bucketName").delete(use: self.deleteBucket)
        routes.grouped("buckets").grouped(":bucketName").grouped("versioning").get(
            use: self.getVersioning)
        routes.grouped("buckets").grouped(":bucketName").grouped("versioning")
            .on(.PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit), use: self.setVersioning)
        routes.grouped("buckets").grouped(":bucketName").grouped("policy").get(
            use: self.getPolicy)
        routes.grouped("buckets").grouped(":bucketName").grouped("policy")
            .on(.PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit), use: self.setPolicy)
        routes.grouped("buckets").grouped(":bucketName").grouped("policy").delete(
            use: self.deletePolicy)
        routes.grouped("buckets").grouped(":bucketName").grouped("tags").get(
            use: self.getBucketTags)
        routes.grouped("buckets").grouped(":bucketName").grouped("tags")
            .on(.PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit), use: self.setBucketTags)
        routes.grouped("buckets").grouped(":bucketName").grouped("tags").delete(
            use: self.deleteBucketTags)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications").get(
            use: self.getBucketNotifications)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications")
            .on(
                .PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.setBucketNotifications)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications")
            .grouped(":ruleId").grouped("test").post(use: self.testBucketNotification)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications")
            .grouped("deliveries").get(use: self.listBucketNotificationDeliveries)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications")
            .grouped("deliveries").grouped(":deliveryId").grouped("retry")
            .post(use: self.retryBucketNotificationDelivery)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("targets").get(use: self.getReplicationTargets)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("targets")
            .on(
                .PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.setReplicationTargets)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("rules").get(use: self.getReplicationRules)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("rules")
            .on(
                .PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.setReplicationRules)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("rules").grouped(":ruleId").grouped("resync")
            .post(use: self.resyncReplicationRule)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("tasks").get(use: self.listReplicationTasks)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("tasks").grouped(":taskId").grouped("retry")
            .post(use: self.retryReplicationTask)
        routes.grouped("objects").get(use: self.listObjects)
        routes.grouped("objects", "stats").get(use: self.getObjectStats)
        routes.grouped("objects").on(.POST, body: .stream, use: self.uploadObject)
        routes.grouped("objects").delete(use: self.deleteObject)
        routes.grouped("objects", "download").post(use: self.downloadObjects)
        routes.grouped("objects", "versions").get(use: self.listObjectVersions)
        routes.grouped("objects", "version").delete(use: self.deleteObjectVersion)
        routes.grouped("objects", "tags").get(use: self.getObjectTags)
        routes.grouped("objects", "tags")
            .on(.PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit), use: self.setObjectTags)
        routes.grouped("objects", "tags").delete(use: self.deleteObjectTags)
        routes.grouped("objects", "metadata").get(use: self.getObjectMetadata)
        routes.grouped("objects", "metadata")
            .on(
                .PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.setObjectMetadata)
        routes.grouped("objects", "share").post(use: self.shareObject)
        routes.grouped("objects", "share").get(use: self.listSharedLinks)
        routes.grouped("objects", "share").grouped(":sharedLinkId").delete(
            use: self.deleteSharedLink)
    }

    /// Fetches a bucket owned by `userId` or throws the standard "Bucket not found" 404 - the
    /// same self-service ownership check needed by nearly every endpoint in this controller.
    private func requireOwnedBucket(req: Request, bucketName: String, userId: UUID) async throws
        -> Bucket
    {
        guard let bucket = try await Bucket.find(app: req.application, name: bucketName),
            bucket.userId == userId
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }
        return bucket
    }

    /// Same ownership check as `requireOwnedBucket`, for endpoints that only need to confirm
    /// the bucket exists and is owned by `userId`, without using the bucket object itself.
    private func requireOwnedBucketExists(req: Request, bucketName: String, userId: UUID)
        async throws
    {
        _ = try await requireOwnedBucket(req: req, bucketName: bucketName, userId: userId)
    }

    /// Lists the caller's own buckets. An optional `?search=` narrows to names containing that
    /// substring (case-insensitive, server-side) - needed so UI like a bucket picker can search
    /// as the user types instead of ever having to load every bucket up front.
    @Sendable
    func listBuckets(req: Request) async throws -> Page<Bucket.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)
        let search = req.query[String.self, at: "search"]?.trimmingCharacters(in: .whitespaces)

        var buckets = try await Bucket.all(app: req.application).filter { $0.userId == auth.userId }
        buckets.sort { ($0.creationDate ?? .distantPast) > ($1.creationDate ?? .distantPast) }

        if let search, !search.isEmpty {
            buckets = buckets.filter { $0.name.localizedCaseInsensitiveContains(search) }
        }

        // Never the raw model - a bucket the caller doesn't own but that happens to match
        // `search` would never reach here (already scoped to `auth.userId` above), but the raw
        // `Bucket` model doesn't carry anything sensitive by itself either way; this is purely
        // about not silently starting to leak the moment someone adds an eager-loaded relation
        // (like `.with(\.$user)`) to this query later, the same mistake the admin bucket list
        // endpoint made.
        let page = try buckets.paginated(for: req)
        return page.map { $0.toResponseDTO() }
    }

    @Sendable
    func createBucket(req: Request) async throws -> Bucket.ResponseDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        try Bucket.Create.validate(content: req)

        let create: Bucket.Create = try req.content.decode(Bucket.Create.self)

        if try await Bucket.find(app: req.application, name: create.name) != nil {
            throw Abort(.conflict, reason: "The requested bucket name is not available.")
        }

        try await BucketService.create(
            app: req.application, bucketName: create.name, userId: auth.userId,
            versioningEnabled: create.versioningEnabled)

        // Fetch the created bucket from the database to get the ID
        guard let bucket = try await Bucket.find(app: req.application, name: create.name),
            bucket.userId == auth.userId
        else {
            throw Abort(.internalServerError, reason: "Failed to retrieve created bucket")
        }

        return bucket.toResponseDTO()
    }

    @Sendable
    func deleteBucket(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        // Verify bucket exists and belongs to user
        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        try await BucketService.delete(
            req: req, bucketName: bucketName, userId: auth.userId, force: true)

        return .noContent
    }

    @Sendable
    func listObjects(req: Request) async throws -> Page<ObjectMeta.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        // Verify bucket exists and belongs to user
        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let search = req.query[String.self, at: "search"]?.trimmingCharacters(in: .whitespaces)

        // A search term means "find this anywhere under the current folder", so listing goes
        // recursive (no delimiter, hence no folder/commonPrefix grouping) instead of the normal
        // single-level folder view.
        let isSearching = search?.isEmpty == false
        let delimiter = isSearching ? "" : (req.query[String.self, at: "delimiter"] ?? "/")

        let (objects, commonPrefixes, _, _) = try await ClusterListingService.listObjects(
            req: req,
            bucketName: bucketName,
            prefix: prefix,
            delimiter: delimiter,
            maxKeys: 10000,
            marker: nil
        )

        // Convert objects to DTOs
        var items: [ObjectMeta.ResponseDTO] = []

        // Add folders (common prefixes) - none when searching, since listing is recursive
        for commonPrefix in commonPrefixes {
            items.append(ObjectMeta.ResponseDTO(folderKey: commonPrefix))
        }

        // Add files
        for object in objects {
            items.append(ObjectMeta.ResponseDTO(from: object))
        }

        if let search, isSearching {
            items = items.filter { $0.key.localizedCaseInsensitiveContains(search) }
        }

        // Sort: folders first, then by name
        items.sort { a, b in
            if a.isFolder != b.isFolder {
                return a.isFolder
            }
            return a.key < b.key
        }

        // Simple pagination for now
        let page = req.query[Int.self, at: "page"] ?? 1
        let per = req.query[Int.self, at: "per"] ?? 100
        let startIndex = (page - 1) * per
        let endIndex = min(startIndex + per, items.count)

        let paginatedItems = startIndex < items.count ? Array(items[startIndex..<endIndex]) : []

        return Page(
            items: paginatedItems,
            metadata: PageMetadata(
                page: page,
                per: per,
                total: items.count
            )
        )
    }

    /// Disk usage and object count for a bucket you own, optionally scoped to a folder
    /// (`prefix`) rather than the whole bucket - same underlying walk `listObjects` and the
    /// admin bucket list use (`BucketHandler.calculateStats`), just ownership-gated instead of
    /// admin-gated. Deliberately its own on-demand endpoint rather than a field embedded in
    /// `listObjects` - a recursive directory walk per row on every page load would make listing
    /// slow to scale; the console fetches this lazily, one call per bucket/folder row already
    /// on screen.
    @Sendable
    func getObjectStats(req: Request) async throws -> StatsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }
        let prefix = req.query[String.self, at: "prefix"] ?? ""

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let (sizeBytes, objectCount) = try await ClusterListingService.calculateStats(
            req: req, bucketName: bucketName, prefix: prefix)
        return StatsDTO(sizeBytes: sizeBytes, objectCount: objectCount)
    }

    /// Cap on how long a shared link can stay valid. Kept at the same 7 days as a S3
    /// presigned URL would allow, for familiarity - but this is an Alarik-specific limit, not
    /// a SigV4 constraint, since shared links don't use SigV4 at all.
    static let maxShareExpirySeconds = 604_800

    /// Creates a public link to an object you own - time-limited when `expiresInSeconds` is
    /// given, or non-expiring when it's omitted (working until explicitly revoked). Nothing
    /// about your account or credentials is exposed - the link is just an opaque, unguessable
    /// token (the new row's own id) that `SharedLinkController` looks up. Revoking access just
    /// means deleting the row, which happens automatically for expired links (see the cleanup
    /// task in configure.swift); non-expiring links are never auto-deleted.
    @Sendable
    func shareObject(req: Request) async throws -> ShareResponseDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        let input = try req.content.decode(ShareRequestDTO.self)

        try await requireOwnedBucketExists(
            req: req, bucketName: input.bucket, userId: auth.userId)

        // The object may live on any node, but this handler must NOT forward - the share URL it
        // returns is built from this node's own base address, so it has to be the one that
        // answers. Check locally first (either format), then (only if not local) confirm
        // existence on a responsible peer with a header-only probe rather than pulling the bytes.
        let localResult = try? ObjectFileHandler.readCurrentObject(
            bucketName: input.bucket, key: input.key, loadData: false)
        var existsLocally = (localResult ?? nil) != nil
        let (isLocal, _, responsible) = await ObjectRoutingService.erasureCodedReadPlacement(
            req: req, bucketName: input.bucket, key: input.key)
        if !existsLocally, isLocal, let config = req.application.storage[ClusterConfigurationKey.self],
            let selfRank = responsible.firstIndex(where: { $0.id == config.nodeId })
        {
            existsLocally = ErasureCodedDeleteCoordinator.localShardExists(
                bucketName: input.bucket, key: input.key, versionId: nil, selfRank: selfRank)
        }
        if !existsLocally {
            var existsRemotely = false
            // Deliberately not gated on `!isLocal`: that only means self is within the *wider*
            // top-(k+m) set, not that a plain (non-EC) object actually replicated here - once
            // k+m > 3 those diverge, and `existsLocally` above already checked disk directly
            // (both formats), so it's the accurate signal for whether a remote probe is needed.
            if !responsible.isEmpty {
                existsRemotely = await ClusterReplicationClient.objectExists(
                    app: req.application, candidates: responsible, bucketName: input.bucket,
                    key: input.key, versionId: nil)
                if !existsRemotely {
                    for (rank, node) in responsible.enumerated() {
                        if await ClusterReplicationClient.shardExists(
                            app: req.application, node: node, bucketName: input.bucket,
                            key: input.key, versionId: nil, shardIndex: rank)
                        {
                            existsRemotely = true
                            break
                        }
                    }
                }
            }
            guard existsRemotely else {
                throw Abort(.notFound, reason: "Object not found")
            }
        }

        let expiresAt: Date?
        if let expiresInSeconds = input.expiresInSeconds {
            guard expiresInSeconds > 0, expiresInSeconds <= Self.maxShareExpirySeconds else {
                throw Abort(
                    .badRequest,
                    reason:
                        "expiresInSeconds must be between 1 and \(Self.maxShareExpirySeconds) (7 days), or omitted for a link that never expires"
                )
            }
            expiresAt = Date().addingTimeInterval(TimeInterval(expiresInSeconds))
        } else {
            expiresAt = nil
        }

        let link = SharedLink(
            userId: auth.userId, bucketName: input.bucket, key: input.key, expiresAt: expiresAt)
        try await link.save(app: req.application)

        let url = "\(apiBaseURL)/api/v1/shared/\(link.id.uuidString)"
        return ShareResponseDTO(url: url, expiresAt: expiresAt)
    }

    /// Lists shared links created by the authenticated user, across all of their buckets.
    @Sendable
    func listSharedLinks(req: Request) async throws -> Page<SharedLink.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)

        let links = try await SharedLink.all(app: req.application)
            .filter { $0.userId == auth.userId }
            .sorted { $0.createdAt > $1.createdAt }

        return try links.paginated(for: req).map { $0.toResponseDTO() }
    }

    /// Revokes a shared link early, before it would otherwise expire.
    @Sendable
    func deleteSharedLink(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let sharedLinkId = req.parameters.get("sharedLinkId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid shared link ID.")
        }

        guard
            let link = try await SharedLink.find(app: req.application, id: sharedLinkId),
            link.userId == auth.userId
        else {
            throw Abort(.notFound, reason: "Shared link not found.")
        }

        try await link.delete(app: req.application)

        return .noContent
    }

    @Sendable
    func uploadObject(req: Request) async throws -> ObjectMeta.ResponseDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        let prefix = req.query[String.self, at: "prefix"] ?? ""

        // Verify bucket exists and belongs to user
        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        // Stream the multipart body to memory-or-spool-file with bounded memory, rather than
        // buffering the whole file via `Content.decode` - see `AdminUploadSpooler`.
        let spooled = try await AdminUploadSpooler.spool(req: req)
        defer { spooled.cleanup() }

        // Construct the full key path (prefix + filename)
        let keyPath = prefix.isEmpty ? spooled.filename : "\(prefix)\(spooled.filename)"

        let etag = spooled.md5Hex

        // Create object metadata
        var meta = ObjectMeta(
            bucketName: bucketName,
            key: keyPath,
            size: spooled.size,
            contentType: spooled.contentType ?? "application/octet-stream",
            etag: etag,
            updatedAt: Date()
        )

        // Get bucket versioning status from cache
        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        // Write object with versioning support - real blocking file IO, offloaded to the
        // blocking-IO thread pool rather than tying up the async executor. Small uploads arrive
        // in memory and take the direct write; large ones were spooled to disk and are copied
        // into the final .obj in fixed windows (mirrors S3Controller.handleObjectPut).
        let initialMeta = meta
        let storage = spooled.storage
        let writtenVersionId: String? = try await S3Service.offloadBlockingIO(req) {
            switch storage {
            case .memory(let data):
                if versioningStatus != .disabled {
                    return try ObjectFileHandler.writeVersioned(
                        metadata: initialMeta,
                        data: data,
                        bucketName: bucketName,
                        key: keyPath,
                        versioningStatus: versioningStatus
                    )
                } else {
                    let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
                    try ObjectFileHandler.write(metadata: initialMeta, data: data, to: path)
                    return nil
                }
            case .file(let spoolPath):
                let spoolSource = [(path: spoolPath, offset: 0, size: spooled.size)]
                if versioningStatus != .disabled {
                    return try ObjectFileHandler.writeVersionedStreamed(
                        metadata: initialMeta,
                        payloadSources: spoolSource,
                        bucketName: bucketName,
                        key: keyPath,
                        versioningStatus: versioningStatus
                    )
                } else {
                    let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
                    try ObjectFileHandler.writeStreamed(
                        metadata: initialMeta, payloadSources: spoolSource, to: path)
                    return nil
                }
            }
        }
        if let writtenVersionId {
            meta.versionId = writtenVersionId
            meta.isLatest = true
        }

        // The destination key is only known after decoding the multipart body above, so unlike
        // every other cluster-routed write this can't forward the *original* request (the body
        // stream is already consumed) - instead it always writes locally (as above) and, if this
        // node isn't actually one of the key's responsible nodes, pushes the freshly written
        // version out to whichever nodes are, then reclaims its own now-redundant local copy.
        let (isLocal, peers, responsible) = await ObjectRoutingService.coordinationTarget(
            req: req, bucketName: bucketName, key: keyPath)
        if isLocal {
            await ClusterReplicationService.replicateWrite(
                app: req.application, bucketName: bucketName, key: keyPath,
                versionId: meta.versionId, operation: .put, peers: peers)
        } else if !responsible.isEmpty {
            let reachedQuorum = await ClusterReplicationService.pushToResponsibleNodes(
                app: req.application, bucketName: bucketName, key: keyPath,
                versionId: meta.versionId, responsible: responsible)
            // This node isn't one of the object's replicas - once a quorum of the nodes that
            // actually are hold the exact version durably, the local copy written above is a
            // stray that violates the placement invariant and leaks disk, so drop it eagerly
            // (a local-only delete - the responsible nodes keep theirs). If quorum wasn't
            // reached synchronously it's kept as the sole durability backstop until the outbox
            // catches the responsible nodes up; a later rebalance walk reclaims it then.
            if reachedQuorum {
                try? await S3Service.offloadBlockingIO(req) {
                    _ = try? S3Service.deleteObject(
                        bucketName: bucketName, key: keyPath, versionId: nil,
                        versioningStatus: .disabled)
                }
            }
        }

        await NotificationService.emit(
            event: .objectCreatedPut, bucketName: bucketName, key: keyPath,
            size: meta.size, etag: etag, versionId: meta.versionId,
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)
        await ReplicationService.enqueuePut(
            app: req.application, bucketName: bucketName, key: keyPath,
            versionId: meta.versionId)

        return ObjectMeta.ResponseDTO(from: meta)
    }

    @Sendable
    func deleteObject(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        guard let key = req.query[String.self, at: "key"] else {
            throw Abort(.badRequest, reason: "Missing 'key' query parameter")
        }

        // Verify bucket exists and belongs to user
        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        // Check if this is a folder (prefix) deletion
        if key.hasSuffix("/") {
            // Keys under this prefix can live on any node, not just this one, so (unlike the
            // local-disk-only ObjectFileHandler.deletePrefix this used to call directly) the key
            // set has to come from a cluster-wide listing, with each key then routed and deleted -
            // locally or delegated to whichever node(s) actually hold it - via the same per-key
            // `deleteObjectClusterWide` logic Multi-Object-Delete uses. `versioningStatus:
            // .disabled` is passed unconditionally, regardless of the bucket's real status,
            // to preserve deletePrefix's original behavior of hard-deleting every version rather
            // than creating delete markers - folder delete has always been a permanent prune, not
            // a versioning-aware operation.

            // Same prefix sanitization/validation ObjectFileHandler.deletePrefix used to perform
            // before touching disk - a prefix that's entirely ".." components (e.g. "../../")
            // sanitizes down to empty and must be rejected outright, not silently listed as
            // "zero matching keys, nothing to do".
            var sanitizedPrefix = key
            if sanitizedPrefix.contains("..") {
                let components = sanitizedPrefix.components(separatedBy: "/")
                sanitizedPrefix = components.map { $0.replacingOccurrences(of: "..", with: "") }
                    .filter { !$0.isEmpty }
                    .joined(separator: "/")
                if key.hasSuffix("/") && !sanitizedPrefix.hasSuffix("/") {
                    sanitizedPrefix += "/"
                }
            }
            guard !sanitizedPrefix.isEmpty && sanitizedPrefix != "/" else {
                throw Abort(.internalServerError, reason: "Invalid prefix for deletion")
            }

            var marker: String? = nil
            var failedKeys = 0
            repeat {
                let (objects, _, isTruncated, nextMarker) = try await ClusterListingService.listObjects(
                    req: req, bucketName: bucketName, prefix: sanitizedPrefix, delimiter: nil,
                    maxKeys: 1000, marker: marker)
                for object in objects {
                    // A per-key delete can fail (a responsible peer being unreachable) - unlike
                    // the local-disk-only deletePrefix this replaced, whose only failure mode was
                    // a local IO error. Rather than silently reporting the whole folder deleted
                    // when some objects survived, count the failures and surface them below.
                    let outcome: S3Service.ObjectDeleteOutcome
                    do {
                        outcome = try await ClusterReplicationService.deleteObjectClusterWide(
                            req: req, bucketName: bucketName, key: object.key, versionId: nil,
                            versioningStatus: .disabled)
                    } catch {
                        failedKeys += 1
                        req.logger.warning(
                            "Folder delete: failed to delete '\(object.key)' under '\(sanitizedPrefix)': \(error)"
                        )
                        continue
                    }
                    await NotificationService.emit(
                        event: .objectRemovedDelete, bucketName: bucketName, key: object.key,
                        size: nil, etag: nil, versionId: outcome.versionId,
                        requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)
                    await ReplicationService.enqueueDelete(
                        app: req.application, bucketName: bucketName, key: object.key,
                        versionId: nil)
                }
                marker = isTruncated ? nextMarker : nil
            } while marker != nil

            if failedKeys > 0 {
                throw Abort(
                    .internalServerError,
                    reason:
                        "Failed to delete \(failedKeys) object(s) under the prefix; some may remain. Please retry."
                )
            }
        } else {
            let outcome = try await ClusterReplicationService.deleteObjectClusterWide(
                req: req, bucketName: bucketName, key: key, versionId: nil,
                versioningStatus: versioningStatus)
            await NotificationService.emit(
                event: outcome.isDeleteMarker
                    ? .objectRemovedDeleteMarkerCreated : .objectRemovedDelete,
                bucketName: bucketName, key: key, size: nil, etag: nil,
                versionId: outcome.versionId,
                requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)
            await ReplicationService.enqueueDelete(
                app: req.application, bucketName: bucketName, key: key,
                versionId: outcome.versionId)
        }

        return .noContent
    }

    @Sendable
    func downloadObjects(req: Request) async throws -> Response {
        let auth = try req.auth.require(AuthenticatedUser.self)
        let input = try req.content.decode(DownloadRequestDTO.self)

        guard !input.keys.isEmpty else {
            throw Abort(.badRequest, reason: "No keys provided for download")
        }

        // Verify bucket exists and belongs to user
        try await requireOwnedBucketExists(
            req: req, bucketName: input.bucket, userId: auth.userId)

        // If single file, download directly
        if input.keys.count == 1 && !input.keys[0].hasSuffix("/") {
            return try await downloadSingleFile(
                req: req, bucketName: input.bucket, key: input.keys[0], versionId: input.versionId)
        }

        // Multiple files or folders - create ZIP
        return try await downloadAsZip(req: req, bucketName: input.bucket, keys: input.keys)
    }

    private func downloadSingleFile(
        req: Request, bucketName: String, key: String, versionId: String? = nil
    ) async throws
        -> Response
    {
        let (isLocal, candidates, responsible) =
            await ObjectRoutingService.erasureCodedReadPlacement(
                req: req, bucketName: bucketName, key: key)

        if isLocal {
            // EC-aware: check this node's own local shard first, falling through to the plain
            // `.obj` path unchanged when the target isn't erasure-coded.
            if let config = req.application.storage[ClusterConfigurationKey.self],
                req.application.storage[ClusterErasureCodingConfigKey.self] != nil,
                let selfRank = responsible.firstIndex(where: { $0.id == config.nodeId }),
                ErasureCodedDeleteCoordinator.localShardExists(
                    bucketName: bucketName, key: key, versionId: versionId, selfRank: selfRank)
            {
                let (meta, body) = try await ErasureCodedReadCoordinator.read(
                    app: req.application, bucketName: bucketName, key: key,
                    versionId: versionId, responsible: responsible, selfNodeId: config.nodeId,
                    requestId: req.id)
                guard !meta.isDeleteMarker else {
                    throw Abort(.notFound, reason: "Object not found")
                }
                let tempPath = try await S3Controller.drainToTempFile(
                    stream: body, app: req.application)
                return streamedResponse(req: req, rawTempFile: tempPath, meta: meta, key: key)
            }

            // Resolve to the on-disk file so large objects can stream straight from it - a
            // console download of a multi-GB object must not buffer it in memory (same pattern
            // as SharedLinkController and the S3 GET handler).
            var path = try? ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: versionId)
            if path == nil, versionId == "null" {
                // The "null" version of a never-versioned key lives at the plain path
                let plainPath = ObjectFileHandler.storagePath(for: bucketName, key: key)
                if ObjectFileHandler.keyExists(for: bucketName, key: key, path: plainPath) {
                    path = plainPath
                }
            }

            if let path, let location = try? ObjectFileHandler.payloadLocation(path: path),
                !location.meta.isDeleteMarker
            {
                let meta = location.meta
                let response: Response
                if location.payloadSize > Constants.streamingThreshold {
                    response = S3Service.buildStreamingObjectResponse(
                        req: req, meta: meta, path: path, payloadOffset: location.payloadOffset)
                } else {
                    // Headers and body from the same read - one consistent snapshot of the file
                    guard
                        let (freshMeta, data) = try? ObjectFileHandler.read(from: path, loadData: true),
                        let data
                    else {
                        throw Abort(.internalServerError, reason: "Failed to read object data")
                    }
                    response = S3Service.buildVersionedObjectMetadataResponse(
                        meta: freshMeta, includeBody: true, data: data)
                }
                return Self.attachDownloadHeaders(to: response, key: key)
            }
        }

        // Not physically on this node - the object may still exist cluster-wide (any
        // responsible node could have served the console's list/browse request while a
        // *different* node happens to hold the actual bytes). Fetch it from a responsible peer
        // into a local temp file the same way CopyObject's cross-node source resolution and
        // downloadAsZip's per-file fallback already do, then stream that temp file straight to
        // the client (windowed, never buffering the whole object in memory - a console download
        // of a multi-GB object that lands on the wrong node must not OOM this node).
        //
        // `candidates` is empty whenever `isLocal` was true (wider top-(k+m) membership) - but
        // once k+m > 3 that doesn't guarantee legacy top-3 membership too, so a plain object can
        // fall through both local branches above and still need a cross-node fetch. Fall back to
        // the legacy top-3 in that case.
        let fetchCandidates =
            candidates.isEmpty ? Array(responsible.prefix(PlacementService.replicationFactor)) : candidates
        guard !fetchCandidates.isEmpty else {
            throw Abort(.notFound, reason: "Object not found")
        }
        let (tempPath, meta) = try await ClusterReplicationClient.fetchObjectToTempFile(
            app: req.application, candidates: fetchCandidates, bucketName: bucketName, key: key,
            versionId: versionId, requestId: req.id)
        guard !meta.isDeleteMarker else {
            _ = POSIXFile.unlink(tempPath)
            throw Abort(.notFound, reason: "Object not found")
        }
        return streamedResponse(req: req, rawTempFile: tempPath, meta: meta, key: key)
    }

    /// Streams a raw (header-less) temp file - the payload `ClusterReplicationClient
    /// .fetchObjectToTempFile` spooled from a peer - straight to the client in fixed windows,
    /// unlinking it once the stream drains (or fails). Distinct from
    /// `S3Service.buildStreamingObjectResponse`, which streams an on-disk *Alarik object file*
    /// (metadata header + payload); this file is bare bytes with `meta` supplied separately.
    private func streamedResponse(
        req: Request, rawTempFile tempPath: String, meta: ObjectMeta, key: String
    ) -> Response {
        let response = S3Service.buildVersionedObjectMetadataResponse(meta: meta, includeBody: false)
        Self.streamRawFile(req: req, path: tempPath, count: meta.size, into: response)
        return Self.attachDownloadHeaders(to: response, key: key)
    }

    /// Attaches a windowed-read streaming body over a raw on-disk file to `response`, deleting
    /// the file once the stream drains or fails - so a large payload (a peer-fetched object, a
    /// built ZIP) is never buffered whole in memory just to send it, and the throwaway temp file
    /// never leaks. `count` must be the exact byte length (drives Content-Length).
    private static func streamRawFile(req: Request, path: String, count: Int, into response: Response) {
        let threadPool = req.application.threadPool
        response.body = Response.Body(
            managedAsyncStream: { writer in
                let fd = try await threadPool.runIfActive { POSIXFile.open(path, O_RDONLY) }
                guard fd >= 0 else {
                    _ = POSIXFile.unlink(path)
                    throw Abort(.internalServerError, reason: "Failed to open file for streaming")
                }
                do {
                    try await StreamingIOLoops.readWindowed(
                        threadPool: threadPool, fd: fd, offset: 0, length: count,
                        chunkSize: Constants.streamingReadChunkSize
                    ) { chunk in
                        try await writer.writeBuffer(chunk)
                    }
                } catch {
                    _ = POSIXFile.close(fd)
                    _ = POSIXFile.unlink(path)
                    throw error
                }
                _ = POSIXFile.close(fd)
                _ = POSIXFile.unlink(path)
            }, count: count)
    }

    private static func attachDownloadHeaders(to response: Response, key: String) -> Response {
        let fileName = String(key.split(separator: "/").last ?? "download")
        response.headers.replaceOrAdd(
            name: "Content-Disposition",
            value: "attachment; filename=\"\(fileName.contentDispositionFilenameEscaped)\""
        )
        return response
    }

    private func downloadAsZip(req: Request, bucketName: String, keys: [String]) async throws
        -> Response
    {
        let tempDir = FileManager.default.temporaryDirectory
        let zipFileName = "download-\(UUID().uuidString).zip"
        let zipURL = tempDir.appendingPathComponent(zipFileName)

        // Every early exit below (a thrown error, or the "nothing to download" guard) must not
        // leak the temp ZIP file on disk - the previous version only cleaned it up on two of
        // several possible exit paths (e.g. `Archive(url:accessMode:)` or `archive.addEntry`
        // throwing left it behind forever). `streamRawFile` takes over ownership of `zipURL` on
        // the success path and unlinks it itself once sent, so this only fires when that never
        // happens.
        var handedOffToStreaming = false
        defer {
            if !handedOffToStreaming {
                try? FileManager.default.removeItem(at: zipURL)
            }
        }

        let archive: Archive
        do {
            archive = try Archive(url: zipURL, accessMode: .create)
        } catch {
            throw Abort(.internalServerError, reason: "Failed to create ZIP archive: \(error)")
        }

        var addedFiles = 0
        // Keys that couldn't be included (not found locally or on any peer, or failed to add to
        // the archive) - the previous version silently produced a ZIP missing some of the
        // requested files with no way for the caller to know. Surfaced via a response header
        // rather than failing the whole request, since a bucket-wide download partially
        // succeeding is still more useful than an all-or-nothing failure.
        var skippedKeys: [String] = []

        // Resolves `key` to a local file path holding its current bytes - either the object's
        // own on-disk path, or (if this node isn't responsible for it) a temp file fetched from
        // whichever node is (the same ObjectRoutingService.isResponsible + ClusterReplicationClient
        // .fetchObjectToTempFile primitive CopyObject's cross-node source fetch already uses).
        // `isTemp` tells the caller whether that path needs unlinking afterward.
        func resolveObjectFile(key: String) async -> (path: String, isTemp: Bool)? {
            if let path = try? ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: nil)
            {
                return (path, false)
            }

            let (isLocal, candidates) = await ObjectRoutingService.isResponsible(
                req: req, bucketName: bucketName, key: key)
            guard !isLocal, !candidates.isEmpty else { return nil }

            guard
                let (tempPath, _) = try? await ClusterReplicationClient.fetchObjectToTempFile(
                    app: req.application, candidates: candidates, bucketName: bucketName,
                    key: key, versionId: nil, requestId: req.id)
            else { return nil }

            return (tempPath, true)
        }

        // Streams straight from `key`'s resolved file into the ZIP entry
        // (`Archive.addEntry(with:fileURL:)` reads it in bounded chunks itself) rather than
        // loading the whole object into memory first, like the previous `Data`-buffering
        // version did for every single file regardless of size.
        func addObject(key: String, entryPath: String) async {
            guard let (filePath, isTemp) = await resolveObjectFile(key: key) else {
                skippedKeys.append(key)
                return
            }
            defer {
                if isTemp { _ = POSIXFile.unlink(filePath) }
            }
            do {
                try archive.addEntry(with: entryPath, fileURL: URL(fileURLWithPath: filePath))
                addedFiles += 1
            } catch {
                skippedKeys.append(key)
            }
        }

        for key in keys {
            if key.hasSuffix("/") {
                // It's a folder - add all files with this prefix
                let (objects, _, _, _) = try await ClusterListingService.listObjects(
                    req: req,
                    bucketName: bucketName,
                    prefix: key,
                    delimiter: nil,  // No delimiter to get all nested files
                    maxKeys: 10000,
                    marker: nil
                )

                for object in objects {
                    // Skip delete markers
                    if object.isDeleteMarker { continue }

                    // Use relative path from the folder prefix
                    let relativePath = String(object.key.dropFirst(key.count))
                    let zipEntryPath = relativePath.isEmpty ? object.key : relativePath
                    await addObject(key: object.key, entryPath: zipEntryPath)
                }
            } else {
                // Single file - use just the filename without path
                let filename = key.split(separator: "/").last.map(String.init) ?? key
                await addObject(key: key, entryPath: filename)
            }
        }

        guard addedFiles > 0 else {
            throw Abort(.notFound, reason: "No files found to download")
        }

        // Stream the finished archive straight off disk (unlinked once sent) rather than reading
        // the whole thing into memory - a folder download can produce an arbitrarily large ZIP.
        // `.int64Value` (not `.intValue`, which is a 32-bit `Int32` and would silently truncate
        // any archive over ~2GB) matches `streamRawFile`'s `count: Int` on this 64-bit platform.
        guard
            let attributes = try? FileManager.default.attributesOfItem(atPath: zipURL.path),
            let zipSizeNumber = attributes[.size] as? NSNumber
        else {
            throw Abort(.internalServerError, reason: "Failed to stat the generated ZIP archive")
        }
        let zipSize = Int(zipSizeNumber.int64Value)

        let response = Response(status: .ok)
        response.headers.contentType = .zip
        response.headers.replaceOrAdd(
            name: "Content-Disposition",
            value: "attachment; filename=\"\(bucketName)-download.zip\""
        )
        if !skippedKeys.isEmpty {
            let encodedSkipped = skippedKeys.map {
                $0.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? $0
            }.joined(separator: ",")
            response.headers.replaceOrAdd(name: "X-Alarik-Skipped-Keys", value: encodedSkipped)
        }
        handedOffToStreaming = true
        Self.streamRawFile(req: req, path: zipURL.path, count: zipSize, into: response)
        return response
    }

    @Sendable
    func getVersioning(req: Request) async throws -> VersioningStatusDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        // Verify bucket exists and belongs to user
        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return VersioningStatusDTO(status: bucket.versioningStatus)
    }

    /// S3 never lets a client set `Disabled` via PutBucketVersioning - only `Enabled` or
    /// `Suspended` are valid request values (verified against the API reference); `Disabled`
    /// only ever describes a bucket that has never had versioning touched at all. Once
    /// versioning has been enabled, the only way "off" is `Suspended`.
    @Sendable
    func setVersioning(req: Request) async throws -> VersioningStatusDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(VersioningStatusDTO.self)

        guard let newStatus = VersioningStatus(rawValue: input.status),
            newStatus != .disabled
        else {
            throw Abort(
                .badRequest,
                reason: "Invalid versioning status. Use 'Enabled' or 'Suspended'.")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        bucket.versioningStatus = newStatus.rawValue
        try await bucket.save(app: req.application)

        await BucketVersioningCache.shared.setStatus(for: bucketName, status: newStatus)
        CacheInvalidationService.notify(app: req.application, cache: "bucketVersioning", op: .upsert, key: bucketName)

        return VersioningStatusDTO(status: newStatus.rawValue)
    }

    @Sendable
    func getPolicy(req: Request) async throws -> PolicyDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return Self.policyResponse(for: bucket)
    }

    @Sendable
    func setPolicy(req: Request) async throws -> PolicyDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let rawJSON = try Self.requirePolicyBody(req: req)

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return try await Self.setPolicy(
            req: req, bucket: bucket, bucketName: bucketName, rawJSON: rawJSON)
    }

    @Sendable
    func deletePolicy(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        try await Self.deletePolicy(req: req, bucket: bucket, bucketName: bucketName)

        return .noContent
    }

    // MARK: - Shared policy logic (also used by InternalAdminController, which resolves the
    // bucket differently - any bucket, not just owned ones - but shares everything else)

    static func policyResponse(for bucket: Bucket) -> PolicyDTO {
        PolicyDTO(policy: bucket.policy)
    }

    static func requirePolicyBody(req: Request) throws -> String {
        let input = try req.content.decode(PolicyDTO.self)
        guard let rawJSON = input.policy else {
            throw Abort(.badRequest, reason: "Missing policy")
        }
        return rawJSON
    }

    static func setPolicy(req: Request, bucket: Bucket, bucketName: String, rawJSON: String)
        async throws -> PolicyDTO
    {
        if await BucketPolicyCache.shared.publicAccessBlock(for: bucketName)?.blockPublicPolicy
            == true
        {
            throw Abort(
                .forbidden,
                reason:
                    "Bucket policies cannot be set while BlockPublicPolicy is enabled in this bucket's Public Access Block configuration."
            )
        }

        let policy: BucketPolicy
        do {
            policy = try BucketPolicy.parseAndValidate(
                rawJSON: rawJSON, bucketName: bucketName, requestId: req.id)
        } catch let error as S3Error {
            throw Abort(.badRequest, reason: error.message)
        }

        bucket.policy = rawJSON
        try await bucket.save(app: req.application)

        await BucketPolicyCache.shared.setPolicy(for: bucketName, policy: policy)
        CacheInvalidationService.notify(app: req.application, cache: "bucketPolicy", op: .upsert, key: bucketName)

        return PolicyDTO(policy: rawJSON)
    }

    static func deletePolicy(req: Request, bucket: Bucket, bucketName: String) async throws {
        bucket.policy = nil
        try await bucket.save(app: req.application)

        await BucketPolicyCache.shared.removePolicy(for: bucketName)
        CacheInvalidationService.notify(app: req.application, cache: "bucketPolicy", op: .remove, key: bucketName)
    }

    @Sendable
    func getBucketTags(req: Request) async throws -> TagsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        guard let rawTags = bucket.tags else {
            return TagsDTO(tags: [:])
        }
        return TagsDTO(tags: Tagging.fromJSON(rawTags).tags)
    }

    /// Sets the bucket's tags, overwriting any existing tags entirely - matches the
    /// S3-protocol PutBucketTagging semantics (no merge).
    @Sendable
    func setBucketTags(req: Request) async throws -> TagsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(TagsDTO.self)

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        let tagging = Tagging(tags: input.tags)
        bucket.tags = tagging.toJSON()
        try await bucket.save(app: req.application)

        return TagsDTO(tags: tagging.tags)
    }

    @Sendable
    func deleteBucketTags(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        bucket.tags = nil
        try await bucket.save(app: req.application)

        return .noContent
    }

    // MARK: - Bucket notifications (webhooks)

    @Sendable
    func getBucketNotifications(req: Request) async throws -> NotificationConfigDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        guard let raw = bucket.notificationConfig else {
            return NotificationConfigDTO(rules: [])
        }
        return NotificationConfigDTO(
            rules: Self.maskedNotificationRules(NotificationConfiguration.fromJSON(raw).rules))
    }

    /// Replaces the bucket's webhook rules wholesale. Validates every rule, assigns ids to
    /// new rules, and gates private-address targets to admins (SSRF).
    @Sendable
    func setBucketNotifications(req: Request) async throws -> NotificationConfigDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(NotificationConfigDTO.self)

        guard input.rules.count <= NotificationConfiguration.maxRuleCount else {
            throw Abort(
                .badRequest,
                reason:
                    "A bucket may have at most \(NotificationConfiguration.maxRuleCount) notification rules."
            )
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)
        let existingRulesById = Dictionary(
            uniqueKeysWithValues:
                (bucket.notificationConfig.map { NotificationConfiguration.fromJSON($0).rules } ?? [])
                .map { ($0.id, $0) })

        // Validate + normalize each rule (fresh id for new rules so ids are always server-owned)
        let normalizedRules = try input.rules.map { rule -> NotificationRule in
            try WebhookURLValidator.validateStructure(rule.url)
            if WebhookURLValidator.isInternalHost(rule.url) && !auth.isAdmin {
                throw Abort(
                    .forbidden,
                    reason:
                        "Only administrators can configure webhooks pointing at private or loopback addresses."
                )
            }

            let unknownEvents = rule.events.filter { !NotificationRule.supportedEvents.contains($0) }
            guard unknownEvents.isEmpty else {
                throw Abort(
                    .badRequest, reason: "Unsupported event type(s): \(unknownEvents.joined(separator: ", "))"
                )
            }
            guard !rule.events.isEmpty else {
                throw Abort(.badRequest, reason: "Each notification rule must subscribe to at least one event.")
            }

            var normalized = rule
            if normalized.id == NotificationRule.zeroUUID {
                normalized.id = UUID()
            } else if normalized.secret == Self.secretMaskPlaceholder {
                // The client never sees the real secret (see `secretMaskPlaceholder`) - an
                // untouched edit echoes the placeholder straight back, which means "keep
                // whatever's already stored," not "literally set the secret to this placeholder."
                normalized.secret = existingRulesById[normalized.id]?.secret
            }
            return normalized
        }

        let config = NotificationConfiguration(rules: normalizedRules)
        bucket.notificationConfig = normalizedRules.isEmpty ? nil : config.toJSON()
        try await bucket.save(app: req.application)

        await NotificationConfigCache.shared.setConfig(for: bucketName, config: config)
        CacheInvalidationService.notify(app: req.application, cache: "notificationConfig", op: .upsert, key: bucketName)

        // Stop delivering already-queued events for rules that were just removed - if a rule
        // is deleted because its endpoint is wrong or compromised, in-flight deliveries to the
        // old URL must not keep firing until they exhaust their retries.
        let keptIds = Set(normalizedRules.map(\.id))
        await OutboxMailbox.purgeBucketAcrossCluster(
            NotificationDelivery.self, app: req.application,
            collection: OutboxCollections.notificationDeliveries, bucketName: bucketName
        ) { $0.bucketName == bucketName && !keptIds.contains($0.ruleId) }

        return NotificationConfigDTO(rules: Self.maskedNotificationRules(normalizedRules))
    }

    /// Enqueues an `s3:TestEvent` for one rule, proving end-to-end delivery (incl. signature)
    /// without needing a real object operation.
    @Sendable
    func testBucketNotification(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }
        guard let ruleId = req.parameters.get("ruleId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid rule id")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        guard let raw = bucket.notificationConfig,
            let rule = NotificationConfiguration.fromJSON(raw).rules.first(where: { $0.id == ruleId })
        else {
            throw Abort(.notFound, reason: "Notification rule not found")
        }

        try await NotificationService.emitTestEvent(
            rule: rule, bucketName: bucketName, requestId: req.id, app: req.application)

        return .accepted
    }

    /// Lists the bucket's most recent outbox rows (pending and dead-lettered), most-recent
    /// first - lets an owner see whether their webhooks are actually being delivered, and why
    /// a delivery is stuck or has failed.
    @Sendable
    func listBucketNotificationDeliveries(req: Request) async throws -> DeliveriesDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let deliveries = await OutboxMailbox.listAllAcrossCluster(
            NotificationDelivery.self, app: req.application,
            collection: OutboxCollections.notificationDeliveries
        )
        .filter { $0.bucketName == bucketName }
        .sorted { $0.createdAt > $1.createdAt }
        .prefix(100)

        return DeliveriesDTO(deliveries: deliveries.map(DeliveryDTO.init))
    }

    /// Requeues a dead-lettered (or still-pending) delivery for immediate redelivery - resets
    /// the retry backoff and attempt count so it gets a fresh run of `maxAttempts`, and wakes
    /// the dispatcher so it doesn't wait for the next background tick.
    @Sendable
    func retryBucketNotificationDelivery(req: Request) async throws -> DeliveryDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }
        guard let deliveryId = req.parameters.get("deliveryId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid delivery id")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        // Scope the lookup to this bucket too, not just the id - a delivery id alone doesn't
        // prove the caller owns the bucket it belongs to.
        let deliveries = await OutboxMailbox.listAllAcrossCluster(
            NotificationDelivery.self, app: req.application,
            collection: OutboxCollections.notificationDeliveries)
        guard let delivery = deliveries.first(where: { $0.id == deliveryId && $0.bucketName == bucketName })
        else {
            throw Abort(.notFound, reason: "Delivery not found")
        }

        let retried = await OutboxMailbox.retryAcrossCluster(
            NotificationDelivery.self, app: req.application,
            collection: OutboxCollections.notificationDeliveries, taskId: deliveryId,
            failedStateValue: NotificationDelivery.State.failed.rawValue)
        guard retried else {
            throw Abort(.notFound, reason: "Delivery not found")
        }

        // Mirrors exactly what `OutboxMailbox.retryOwned` just reset on whichever node actually
        // owns this delivery - avoids a second cluster-wide fetch just to read back what's
        // already known.
        delivery.state = NotificationDelivery.State.pending.rawValue
        delivery.attempts = 0
        delivery.nextAttemptAt = Date()
        delivery.lastError = nil

        NotificationDispatcher.shared.wake()

        return DeliveryDTO(from: delivery)
    }

    // MARK: - Bucket replication

    @Sendable
    func getReplicationTargets(req: Request) async throws -> ReplicationTargetsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        guard let raw = bucket.replicationConfig else {
            return ReplicationTargetsDTO(targets: [])
        }
        return ReplicationTargetsDTO(
            targets: Self.maskedReplicationTargets(ReplicationConfiguration.fromJSON(raw).targets))
    }

    /// Replaces the bucket's replication targets wholesale. Validates every target's endpoint
    /// and gates private-address targets to admins (SSRF), same as webhook URLs. Removing a
    /// target that's still referenced by a rule auto-disables that rule rather than leaving it
    /// pointing at a target that no longer exists.
    @Sendable
    func setReplicationTargets(req: Request) async throws -> ReplicationTargetsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(ReplicationTargetsDTO.self)

        guard input.targets.count <= ReplicationConfiguration.maxTargetCount else {
            throw Abort(
                .badRequest,
                reason:
                    "A bucket may have at most \(ReplicationConfiguration.maxTargetCount) replication targets."
            )
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)
        let existingConfig =
            bucket.replicationConfig.map { ReplicationConfiguration.fromJSON($0) } ?? .empty
        let existingTargetsById = Dictionary(
            uniqueKeysWithValues: existingConfig.targets.map { ($0.id, $0) })

        let normalizedTargets = try input.targets.map { target -> ReplicationTarget in
            try WebhookURLValidator.validateStructure(target.endpoint)
            if WebhookURLValidator.isInternalHost(target.endpoint) && !auth.isAdmin {
                throw Abort(
                    .forbidden,
                    reason:
                        "Only administrators can configure replication targets pointing at private or loopback addresses."
                )
            }
            guard !target.targetBucket.isEmpty else {
                throw Abort(
                    .badRequest, reason: "Each replication target requires a destination bucket.")
            }

            var normalized = target
            if normalized.id == ReplicationTarget.zeroUUID {
                normalized.id = UUID()
            } else if normalized.secretAccessKey == Self.secretMaskPlaceholder {
                // The client never sees the real secret (see `secretMaskPlaceholder`) - an
                // untouched edit echoes the placeholder straight back, which means "keep
                // whatever's already stored," not "literally set the secret to this placeholder."
                normalized.secretAccessKey = existingTargetsById[normalized.id]?.secretAccessKey ?? ""
            }

            guard !normalized.accessKeyId.isEmpty, !normalized.secretAccessKey.isEmpty else {
                throw Abort(.badRequest, reason: "Each replication target requires credentials.")
            }
            return normalized
        }

        let existingRules = existingConfig.rules
        let keptTargetIds = Set(normalizedTargets.map(\.id))
        // A rule referencing a target that no longer exists is disabled, never left dangling -
        // same "clean up, don't leave a dangling reference" precedent used when a webhook rule
        // is removed.
        let adjustedRules = existingRules.map { rule -> ReplicationRule in
            guard keptTargetIds.contains(rule.targetId) else {
                var disabled = rule
                disabled.enabled = false
                return disabled
            }
            return rule
        }

        let config = ReplicationConfiguration(targets: normalizedTargets, rules: adjustedRules)
        bucket.replicationConfig =
            (normalizedTargets.isEmpty && adjustedRules.isEmpty) ? nil : config.toJSON()
        try await bucket.save(app: req.application)

        await ReplicationConfigCache.shared.setConfig(for: bucketName, config: config)
        CacheInvalidationService.notify(app: req.application, cache: "replicationConfig", op: .upsert, key: bucketName)

        return ReplicationTargetsDTO(targets: Self.maskedReplicationTargets(normalizedTargets))
    }

    @Sendable
    func getReplicationRules(req: Request) async throws -> ReplicationRulesDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        guard let raw = bucket.replicationConfig else {
            return ReplicationRulesDTO(rules: [])
        }
        return ReplicationRulesDTO(rules: ReplicationConfiguration.fromJSON(raw).rules)
    }

    /// Replaces the bucket's replication rules wholesale. A non-empty rule set requires the
    /// bucket's versioning to be `Enabled` (without it, `versionId` can't unambiguously
    /// identify what to replicate), and every rule's `targetId` must resolve to a target
    /// already configured on this bucket.
    @Sendable
    func setReplicationRules(req: Request) async throws -> ReplicationRulesDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(ReplicationRulesDTO.self)

        guard input.rules.count <= ReplicationConfiguration.maxRuleCount else {
            throw Abort(
                .badRequest,
                reason:
                    "A bucket may have at most \(ReplicationConfiguration.maxRuleCount) replication rules."
            )
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        guard input.rules.isEmpty || bucket.isVersioningEnabled else {
            throw Abort(
                .badRequest,
                reason: "Replication rules require the bucket's versioning to be Enabled.")
        }

        let existingTargets =
            bucket.replicationConfig.map { ReplicationConfiguration.fromJSON($0).targets } ?? []
        let targetIds = Set(existingTargets.map(\.id))

        let normalizedRules = try input.rules.map { rule -> ReplicationRule in
            guard targetIds.contains(rule.targetId) else {
                throw Abort(.badRequest, reason: "Rule references an unknown replication target.")
            }

            var normalized = rule
            if normalized.id == ReplicationRule.zeroUUID {
                normalized.id = UUID()
            }
            return normalized
        }

        let config = ReplicationConfiguration(targets: existingTargets, rules: normalizedRules)
        bucket.replicationConfig =
            (existingTargets.isEmpty && normalizedRules.isEmpty) ? nil : config.toJSON()
        try await bucket.save(app: req.application)

        await ReplicationConfigCache.shared.setConfig(for: bucketName, config: config)
        CacheInvalidationService.notify(app: req.application, cache: "replicationConfig", op: .upsert, key: bucketName)

        // Stop delivering already-queued tasks for rules that were just removed - same
        // reasoning as the equivalent webhook-delivery purge above.
        let keptIds = Set(normalizedRules.map(\.id))
        await OutboxMailbox.purgeBucketAcrossCluster(
            ReplicationTask.self, app: req.application, collection: OutboxCollections.replicationTasks,
            bucketName: bucketName
        ) { $0.bucketName == bucketName && !keptIds.contains($0.ruleId) }

        return ReplicationRulesDTO(rules: normalizedRules)
    }

    /// Triggers a walk of the bucket's current objects under `rule.prefix`, enqueueing a `put`
    /// replication task for each - the explicit "do it now" trigger for a rule that already
    /// opted into `replicateExisting`, a manual action, never automatic.
    ///
    /// The walk itself runs in the background (`ReplicationService.resync`), not inline in this
    /// request: a bucket with hundreds of thousands of objects could otherwise hold the HTTP
    /// request open for minutes, well past any reasonable client/proxy timeout. This returns
    /// `202 Accepted` as soon as the rule/target have been validated - progress is only
    /// observable via the replication tasks list, same as any other asynchronously-delivered
    /// replication work.
    @Sendable
    func resyncReplicationRule(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }
        guard let ruleId = req.parameters.get("ruleId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid rule id")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        guard let raw = bucket.replicationConfig else {
            throw Abort(.notFound, reason: "Replication rule not found")
        }
        let config = ReplicationConfiguration.fromJSON(raw)
        guard let rule = config.rules.first(where: { $0.id == ruleId }) else {
            throw Abort(.notFound, reason: "Replication rule not found")
        }
        guard rule.enabled else {
            throw Abort(.badRequest, reason: "Cannot resync a disabled replication rule.")
        }
        guard rule.replicateExisting else {
            throw Abort(
                .badRequest,
                reason:
                    "This rule has not opted into existing-object replication (replicateExisting)."
            )
        }
        guard let target = config.target(for: rule.targetId), target.enabled else {
            throw Abort(
                .badRequest, reason: "This rule's replication target is missing or disabled.")
        }

        // Scoped to the Application, not the Request - this must keep running after the
        // response below has been sent and `req` may have gone away.
        let app = req.application
        let logger = req.application.logger
        Task {
            do {
                let enqueued = try await ReplicationService.resync(
                    app: app, bucketName: bucketName, rule: rule, target: target)
                logger.info(
                    "Replication resync for bucket '\(bucketName)' rule '\(rule.id)' enqueued \(enqueued) object(s)"
                )
            } catch {
                logger.error(
                    "Replication resync failed for bucket '\(bucketName)' rule '\(rule.id)': \(error)"
                )
            }
        }

        return .accepted
    }

    /// Lists the bucket's most recent replication outbox rows (pending and dead-lettered),
    /// most-recent first - the replication equivalent of `listBucketNotificationDeliveries`.
    @Sendable
    func listReplicationTasks(req: Request) async throws -> ReplicationTasksDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let tasks = await OutboxMailbox.listAllAcrossCluster(
            ReplicationTask.self, app: req.application, collection: OutboxCollections.replicationTasks
        )
        .filter { $0.bucketName == bucketName }
        .sorted { $0.createdAt > $1.createdAt }
        .prefix(100)

        return ReplicationTasksDTO(tasks: tasks.map(ReplicationTaskDTO.init))
    }

    /// Requeues a dead-lettered (or still-pending) replication task for immediate retry -
    /// resets the retry backoff and attempt count, then wakes the dispatcher.
    @Sendable
    func retryReplicationTask(req: Request) async throws -> ReplicationTaskDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }
        guard let taskId = req.parameters.get("taskId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid task id")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        // Scope the lookup to this bucket too, not just the id - a task id alone doesn't prove
        // the caller owns the bucket it belongs to.
        let allTasks = await OutboxMailbox.listAllAcrossCluster(
            ReplicationTask.self, app: req.application, collection: OutboxCollections.replicationTasks)
        guard let task = allTasks.first(where: { $0.id == taskId && $0.bucketName == bucketName }) else {
            throw Abort(.notFound, reason: "Replication task not found")
        }

        let retried = await OutboxMailbox.retryAcrossCluster(
            ReplicationTask.self, app: req.application, collection: OutboxCollections.replicationTasks,
            taskId: taskId, failedStateValue: ReplicationTask.State.failed.rawValue)
        guard retried else {
            throw Abort(.notFound, reason: "Replication task not found")
        }

        // Mirrors exactly what `OutboxMailbox.retryOwned` just reset on whichever node actually
        // owns this task - avoids a second cluster-wide fetch just to read back what's already
        // known.
        task.state = ReplicationTask.State.pending.rawValue
        task.attempts = 0
        task.nextAttemptAt = Date()
        task.lastError = nil

        ReplicationDispatcher.shared.wake()

        return ReplicationTaskDTO(from: task)
    }

    /// Returns the tags of the *current* version of an object. Internal API doesn't expose
    /// per-version tag management (the S3-protocol endpoints already do, via `versionId`).
    @Sendable
    func getObjectTags(req: Request) async throws -> Response {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing 'bucket' or 'key' query parameter")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        // Tags live in the object's own metadata, so this must run on a node that physically
        // holds the key - forward to a responsible node when this one doesn't (mirrors
        // getObjectMetadata, which this was inconsistent with before).
        let (isLocal, candidates, responsible) =
            await ObjectRoutingService.erasureCodedReadPlacement(
                req: req, bucketName: bucketName, key: key)
        if !isLocal {
            return try await ClusterForwardingClient.forward(req: req, candidates: candidates)
        }

        // isLocal only proves membership in the wider top-(k+m) set - once k+m > 3 a plain
        // (non-EC) object's legacy top-3 placement can exclude this node entirely, so a missing
        // local EC shard needs the legacy top-3 forward before falling into a local plain read.
        if !(await S3Controller.hasLocalECShard(
            req: req, bucketName: bucketName, key: key, versionId: nil, responsible: responsible)),
            let config = req.application.storage[ClusterConfigurationKey.self],
            !ObjectRoutingService.isLegacyReplica(responsible: responsible, selfNodeId: config.nodeId),
            !responsible.isEmpty
        {
            return try await ClusterForwardingClient.forward(
                req: req, candidates: Array(responsible.prefix(PlacementService.replicationFactor)))
        }

        let meta: ObjectMeta
        do {
            meta = try await S3Controller.resolveObjectMetaEitherFormat(
                req: req, bucketName: bucketName, key: key, versionId: nil,
                responsible: responsible)
        } catch is S3Error {
            throw Abort(.notFound, reason: "Object not found")
        }
        guard !meta.isDeleteMarker else {
            throw Abort(.notFound, reason: "Object not found")
        }

        return try await TagsDTO(tags: meta.tags ?? [:]).encodeResponse(for: req)
    }

    /// Sets the tags of the *current* version of an object, overwriting any existing tags
    /// entirely. Does not create a new version - modifies the existing version's metadata in
    /// place, matching the S3-protocol PutObjectTagging semantics.
    @Sendable
    func setObjectTags(req: Request) async throws -> Response {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing 'bucket' or 'key' query parameter")
        }

        let input = try req.content.decode(TagsDTO.self)
        guard input.tags.count <= Tagging.maxTagCount else {
            throw Abort(
                .badRequest,
                reason: "Object tags cannot be greater than \(Tagging.maxTagCount).")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let peers: [ClusterNodeInfo]
        switch try await ObjectRoutingService.routeForWrite(req: req, bucketName: bucketName, key: key)
        {
        case .local(let localPeers):
            peers = localPeers
        case .forwarded(let response):
            return response
        }

        do {
            _ = try await S3Controller.rewriteObjectMetadata(
                req: req, bucketName: bucketName, key: key, versionId: nil, peers: peers
            ) { $0.tags = input.tags }
        } catch let error as S3Error {
            throw Abort(error.status, reason: error.message)
        }

        return try await TagsDTO(tags: input.tags).encodeResponse(for: req)
    }

    @Sendable
    func getObjectMetadata(req: Request) async throws -> Response {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing 'bucket' or 'key' query parameter")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let (isLocal, candidates, responsible) =
            await ObjectRoutingService.erasureCodedReadPlacement(
                req: req, bucketName: bucketName, key: key)
        if !isLocal {
            return try await ClusterForwardingClient.forward(req: req, candidates: candidates)
        }

        // See getObjectTags's identical fallthrough.
        if !(await S3Controller.hasLocalECShard(
            req: req, bucketName: bucketName, key: key, versionId: nil, responsible: responsible)),
            let config = req.application.storage[ClusterConfigurationKey.self],
            !ObjectRoutingService.isLegacyReplica(responsible: responsible, selfNodeId: config.nodeId),
            !responsible.isEmpty
        {
            return try await ClusterForwardingClient.forward(
                req: req, candidates: Array(responsible.prefix(PlacementService.replicationFactor)))
        }

        let meta: ObjectMeta
        do {
            meta = try await S3Controller.resolveObjectMetaEitherFormat(
                req: req, bucketName: bucketName, key: key, versionId: nil,
                responsible: responsible)
        } catch is S3Error {
            throw Abort(.notFound, reason: "Object not found")
        }
        guard !meta.isDeleteMarker else {
            throw Abort(.notFound, reason: "Object not found")
        }

        return try await ObjectMetadataDTO(contentType: meta.contentType, metadata: meta.metadata)
            .encodeResponse(for: req)
    }

    /// Updates the Content-Type and custom (`x-amz-meta-*`) metadata of the *current* version
    /// of an object, in place - same shape as `setObjectTags`: no new version, no re-upload of
    /// the body, and (also matching S3's PutObjectTagging/metadata-only semantics) no
    /// webhook notification, since the object's data hasn't changed.
    @Sendable
    func setObjectMetadata(req: Request) async throws -> Response {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing 'bucket' or 'key' query parameter")
        }

        let input = try req.content.decode(ObjectMetadataDTO.self)
        guard !input.contentType.trimmingCharacters(in: .whitespaces).isEmpty else {
            throw Abort(.badRequest, reason: "Content-Type cannot be empty.")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let peers: [ClusterNodeInfo]
        switch try await ObjectRoutingService.routeForWrite(req: req, bucketName: bucketName, key: key)
        {
        case .local(let localPeers):
            peers = localPeers
        case .forwarded(let response):
            return response
        }

        // Lowercase keys, same normalization handleObjectPut applies to x-amz-meta-* headers
        let normalizedMetadata = Dictionary(
            uniqueKeysWithValues: input.metadata.map { ($0.key.lowercased(), $0.value) })

        do {
            _ = try await S3Controller.rewriteObjectMetadata(
                req: req, bucketName: bucketName, key: key, versionId: nil, peers: peers
            ) {
                $0.contentType = input.contentType
                $0.metadata = normalizedMetadata
            }
        } catch let error as S3Error {
            // This admin endpoint speaks Abort/JSON, not S3Error/XML - translate rather than
            // letting an S3-shaped error leak through and lose its status code to a generic 500.
            throw Abort(error.status, reason: error.message)
        }

        return try await ObjectMetadataDTO(
            contentType: input.contentType, metadata: normalizedMetadata
        ).encodeResponse(for: req)
    }

    @Sendable
    func deleteObjectTags(req: Request) async throws -> Response {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing 'bucket' or 'key' query parameter")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let peers: [ClusterNodeInfo]
        switch try await ObjectRoutingService.routeForWrite(req: req, bucketName: bucketName, key: key)
        {
        case .local(let localPeers):
            peers = localPeers
        case .forwarded(let response):
            return response
        }

        guard
            let path = try ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: nil)
        else {
            throw Abort(.notFound, reason: "Object not found")
        }

        let updatedMeta = try await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.rewriteMetadata(at: path) { $0.tags = nil }
        }

        // See S3Controller.handleObjectTaggingPut - an in-place metadata edit has no outbox
        // task backing it, so cluster peers must be pushed the change directly here.
        await ClusterReplicationService.replicateWrite(
            app: req.application, bucketName: bucketName, key: key,
            versionId: updatedMeta.versionId, operation: .put, peers: peers)

        return Response(status: .noContent)
    }

    /// Per-key (not bucket-wide), so unlike `listObjects`/`getObjectStats` this doesn't need the
    /// fan-out/merge machinery - it just needs to land on whichever node actually holds `key`,
    /// the same single-key `ObjectRoutingService` forwarding `ListParts` already uses.
    @Sendable
    func listObjectVersions(req: Request) async throws -> Response {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        guard let key = req.query[String.self, at: "key"] else {
            throw Abort(.badRequest, reason: "Missing 'key' query parameter")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        if case .forward(let candidates) = await ObjectRoutingService.routingDecision(
            req: req, bucketName: bucketName, key: key)
        {
            return try await ClusterForwardingClient.forward(req: req, candidates: candidates)
        }

        let versions = try ObjectFileHandler.listVersions(bucketName: bucketName, key: key)
        let dtos = versions.map { ObjectMeta.ResponseDTO(from: $0) }
        // Uses Vapor's own Content encoding (not a hand-rolled JSONEncoder) so the response is
        // byte-identical to what this route returned before routing was added - the console
        // frontend depends on the app-wide date format Content encoding applies.
        return try await dtos.encodeResponse(for: req)
    }

    @Sendable
    func deleteObjectVersion(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        guard let key = req.query[String.self, at: "key"] else {
            throw Abort(.badRequest, reason: "Missing 'key' query parameter")
        }

        guard let versionId = req.query[String.self, at: "versionId"] else {
            throw Abort(.badRequest, reason: "Missing 'versionId' query parameter")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)
        // Cluster peers physically hold the exact same version files as this node - unlike
        // ReplicationClient's external replication target, which has no per-version equivalent
        // to prune (see ReplicationClient.replicateDelete) - so this needs to route to (or
        // delegate to) whichever node(s) actually hold the version and replicate the deletion,
        // the same as S3Controller.handleObjectDelete's versionId path.
        let outcome = try await ClusterReplicationService.deleteObjectClusterWide(
            req: req, bucketName: bucketName, key: key, versionId: versionId,
            versioningStatus: versioningStatus)

        await NotificationService.emit(
            event: .objectRemovedDelete, bucketName: bucketName, key: key,
            size: nil, etag: nil, versionId: outcome.versionId,
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)

        return .noContent
    }
}
