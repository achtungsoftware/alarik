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
import ZIPFoundation

/// The console's object browser endpoints: listing, upload (plain and chunked), download,
/// delete, copy/move, per-object tags and metadata, and version history.
struct InternalObjectController: RouteCollection, ConsoleBucketRoutes {
    struct StatsDTO: Content {
        let sizeBytes: Int64
        let objectCount: Int
        /// When the underlying walk ran - figures are served from a short cache (issue #28).
        let computedAt: Date

        init(_ stats: StorageStatsService.Stats) {
            self.sizeBytes = stats.sizeBytes
            self.objectCount = stats.objectCount
            self.computedAt = stats.computedAt
        }
    }

    struct ObjectMetadataDTO: Content {
        let contentType: String
        let metadata: [String: String]
    }

    /// Same shape as a bucket's tag set.
    typealias TagsDTO = InternalBucketController.TagsDTO

    struct DownloadTicketRequestDTO: Content {
        let bucket: String
        let keys: [String]
        let versionId: String?
        /// Ask for display rather than a download; honoured only for types safe to render.
        let inline: Bool?
        let downloadName: String?
    }

    struct DownloadTicketDTO: Content {
        let ticket: String
        let expiresAt: Date
    }

    struct ObjectCopyRequestDTO: Content {
        let sourceBucket: String
        let sourceKey: String
        /// A specific version; with the same destination key this restores it as current.
        let sourceVersionId: String?
        let destinationBucket: String
        let destinationKey: String
        /// Remove the source once the copy is durable: move and rename.
        let deleteSource: Bool?
    }

    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("objects").get(use: self.listObjects)
        routes.grouped("objects", "stats").get(use: self.getObjectStats)
        routes.grouped("objects").on(.POST, body: .stream, use: self.uploadObject)
        // Only the part route streams a file body; the others take the small subresource cap.
        routes.grouped("objects", "multipart")
            .on(
                .POST, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.createMultipartUpload)
        routes.grouped("objects", "multipart", "part")
            .on(.PUT, body: .stream, use: self.uploadMultipartPart)
        routes.grouped("objects", "multipart", "complete")
            .on(
                .POST, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.completeMultipartUpload)
        routes.grouped("objects", "multipart")
            .on(
                .DELETE, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.abortMultipartUpload)
        routes.grouped("objects").delete(use: self.deleteObject)
        routes.grouped("objects", "copy")
            .on(.POST, body: .collect(maxSize: Self.subresourceBodySizeLimit), use: self.copyObject)
        routes.grouped("objects", "download").post(use: self.downloadObjects)
        routes.grouped("objects", "download-ticket")
            .on(
                .POST, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.issueDownloadTicket)
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
    }

    /// The object browser's listing: one cursor page of the folder `prefix` (or of search
    /// matches anywhere under it). `marker` continues from a previous page's `nextMarker`;
    /// `per` is clamped to `ConsoleObjectListing.maxPageSize`. See `ConsoleObjectListing` for
    /// why this is a cursor and not an offset (issue #27).
    @Sendable
    func listObjects(req: Request) async throws -> CursorPage<ObjectMeta.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }
        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let per = ConsoleObjectListing.clampedPageSize(req.query[Int.self, at: "per"])
        let marker = req.query[String.self, at: "marker"].flatMap { $0.isEmpty ? nil : $0 }
        let search = req.query[String.self, at: "search"]?.trimmingCharacters(in: .whitespaces) ?? ""

        if search.isEmpty {
            // An explicitly empty delimiter asks for a flat, recursive listing.
            let delimiter = req.query[String.self, at: "delimiter"].map { $0.isEmpty ? nil : $0 } ?? "/"
            return try await ConsoleObjectListing.page(
                req: req, bucketName: bucketName, prefix: prefix, delimiter: delimiter, per: per,
                marker: marker)
        }
        return try await ConsoleObjectListing.searchPage(
            req: req, bucketName: bucketName, prefix: prefix, term: search, per: per, marker: marker)
    }

    /// Disk usage and object count for a bucket you own, optionally scoped to a folder
    /// (`prefix`) rather than the whole bucket - same underlying walk `listObjects` and the
    /// admin bucket list use (`BucketHandler.calculateStats`), just ownership-gated instead of
    /// admin-gated. Deliberately its own on-demand endpoint rather than a field embedded in
    /// `listObjects` - a recursive directory walk per row on every page load would make listing
    /// slow to scale; the console fetches this lazily, a few rows at a time, and the figure is
    /// cached and rate-limited server-side by `StorageStatsService` (issue #28).
    @Sendable
    func getObjectStats(req: Request) async throws -> StatsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }
        let prefix = req.query[String.self, at: "prefix"] ?? ""

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        return StatsDTO(
            try await ClusterListingService.calculateStats(
                req: req, bucketName: bucketName, prefix: prefix))
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
        try ObjectKeyValidation.validate(keyPath)

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
        let versioningStatus = await BucketVersioningCache.shared.resolvedStatus(app: req.application, bucket: bucketName)

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

    // MARK: - Console multipart upload (issue #21)
    //
    // The single-request upload above has to pass the whole file through one HTTP request, which
    // reverse proxies routinely cap (Cloudflare: 100 MB). These four endpoints expose the same
    // multipart lifecycle the S3 API offers, authenticated with the console's JWT instead of
    // SigV4, so the browser can send a large file as independent parts. Unlike `uploadObject`,
    // the key is known up front, so every call can be routed to the upload's rank-0 node
    // *before* the body is touched - the same pinning the S3 handlers rely on, and the reason a
    // create/part/complete sequence spread across different nodes still converges.

    struct MultipartCreateDTO: Content {
        let uploadId: String
        let key: String
    }

    struct MultipartPartDTO: Content {
        let partNumber: Int
        let etag: String
    }

    struct MultipartCompleteDTO: Content {
        struct Part: Content {
            let partNumber: Int
            let etag: String
        }
        let parts: [Part]
    }

    private struct MultipartTarget {
        let bucketName: String
        let key: String
    }

    /// Auth + ownership + rank-0 routing shared by the four multipart handlers. Returns the peers
    /// to replicate to, or the forwarded response when another node owns this upload.
    private func routeMultipart(req: Request) async throws
        -> (target: MultipartTarget, routing: ObjectRoutingService.WriteRouting)
    {
        let auth = try req.auth.require(AuthenticatedUser.self)
        guard let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"], !key.isEmpty, !key.hasSuffix("/")
        else {
            throw Abort(.badRequest, reason: "Missing 'bucket' or 'key' query parameter")
        }
        try ObjectKeyValidation.validate(key)
        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)
        let routing = try await ObjectRoutingService.routeForWrite(
            req: req, bucketName: bucketName, key: key, requirePrimary: true)
        return (MultipartTarget(bucketName: bucketName, key: key), routing)
    }

    /// The upload must exist here AND have been created for this exact key - an uploadId is only
    /// meaningful together with the key it was opened for.
    private func requireUpload(req: Request, target: MultipartTarget) async throws -> (
        uploadId: String, meta: MultipartUploadMeta
    ) {
        guard let uploadId = req.query[String.self, at: "uploadId"] else {
            throw Abort(.badRequest, reason: "Missing 'uploadId' query parameter")
        }
        let bucketName = target.bucketName
        let meta = try? await S3Service.offloadBlockingIO(req) {
            try MultipartFileHandler.getUploadMeta(bucketName: bucketName, uploadId: uploadId)
        }
        guard let meta, meta.key == target.key else {
            throw Abort(.notFound, reason: "The specified upload does not exist.")
        }
        return (uploadId, meta)
    }

    /// `MultipartFileHandler` reports part/order problems and a vanished upload as
    /// `MultipartError`; everything else is a genuine server fault and must not masquerade as
    /// the client's mistake (the console only retries 5xx).
    private func abort(forCompletionError error: any Error) -> Abort {
        abort(for: error, serverFault: "Completing the upload failed.")
    }

    /// `S3Error`/`MultipartError` carry a client-safe status and message. Anything else is a
    /// server fault whose detail (paths, peers) goes to the log, never to the client.
    private func abort(for error: any Error, serverFault: String) -> Abort {
        if let error = error as? S3Error {
            return Abort(error.status, reason: error.message)
        }
        if let error = error as? MultipartError {
            return Abort(error.status, reason: error.message)
        }
        return Abort(.internalServerError, reason: serverFault)
    }

    @Sendable
    func createMultipartUpload(req: Request) async throws -> Response {
        let (target, routing) = try await routeMultipart(req: req)
        if case .forwarded(let response) = routing { return response }

        let contentType = req.query[String.self, at: "contentType"] ?? "application/octet-stream"
        let uploadId = try await S3Service.offloadBlockingIO(req) {
            try MultipartFileHandler.createUpload(
                bucketName: target.bucketName, key: target.key, contentType: contentType)
        }
        let response = Response(status: .ok)
        try response.content.encode(MultipartCreateDTO(uploadId: uploadId, key: target.key))
        return response
    }

    @Sendable
    func uploadMultipartPart(req: Request) async throws -> Response {
        // Every early throw below fires before the body is read - see `withGuaranteedBodyDrain`.
        try await StreamingBodySpooler.withGuaranteedBodyDrain(req: req) {
            let (target, routing) = try await routeMultipart(req: req)
            if case .forwarded(let response) = routing { return response }
            let (uploadId, _) = try await requireUpload(req: req, target: target)
            guard let partNumber = req.query[Int.self, at: "partNumber"], (1...10000).contains(partNumber)
            else {
                throw Abort(.badRequest, reason: "Part number must be between 1 and 10000.")
            }

            // Raw body, no SigV4 chunking: bounded memory, MD5 computed while spooling.
            let spooled = try await StreamingBodySpooler.spool(req: req, authInfo: nil)
            defer { spooled.cleanup() }
            guard spooled.size > 0 else {
                throw Abort(.badRequest, reason: "Request body is empty.")
            }

            let storage = spooled.storage
            let etag: String = try await S3Service.offloadBlockingIO(req) {
                switch storage {
                case .memory(let data):
                    return try MultipartFileHandler.writePart(
                        bucketName: target.bucketName, uploadId: uploadId,
                        partNumber: partNumber, data: data)
                case .file(let spoolPath):
                    return try MultipartFileHandler.writePartStreamed(
                        bucketName: target.bucketName, uploadId: uploadId,
                        partNumber: partNumber, spoolPath: spoolPath, etag: spooled.md5Hex,
                        size: spooled.size)
                }
            }
            let response = Response(status: .ok)
            try response.content.encode(MultipartPartDTO(partNumber: partNumber, etag: etag))
            return response
        }
    }

    @Sendable
    func completeMultipartUpload(req: Request) async throws -> Response {
        let (target, routing) = try await routeMultipart(req: req)
        let peers: [ClusterNodeInfo]
        switch routing {
        case .forwarded(let response): return response
        case .local(let localPeers): peers = localPeers
        }
        let (uploadId, uploadMeta) = try await requireUpload(req: req, target: target)
        let body = try req.content.decode(MultipartCompleteDTO.self)
        guard !body.parts.isEmpty else {
            throw Abort(.badRequest, reason: "At least one part is required.")
        }

        let completion: S3Controller.MultipartCompletion
        do {
            completion = try await S3Controller.completeMultipartUpload(
                req: req, bucketName: target.bucketName, key: target.key, uploadId: uploadId,
                parts: body.parts.map { (partNumber: $0.partNumber, etag: $0.etag) }, peers: peers)
        } catch {
            req.logger.error("Console multipart completion failed: \(error)")
            throw abort(forCompletionError: error)
        }

        var meta = ObjectMeta(
            bucketName: target.bucketName, key: target.key, size: completion.size,
            contentType: uploadMeta.contentType, etag: completion.etag, updatedAt: Date())
        if let versionId = completion.versionId {
            meta.versionId = versionId
            meta.isLatest = true
        }
        let response = Response(status: .ok)
        try response.content.encode(ObjectMeta.ResponseDTO(from: meta))
        return response
    }

    @Sendable
    func abortMultipartUpload(req: Request) async throws -> Response {
        let (target, routing) = try await routeMultipart(req: req)
        if case .forwarded(let response) = routing { return response }
        let (uploadId, _) = try await requireUpload(req: req, target: target)
        try await S3Service.offloadBlockingIO(req) {
            try MultipartFileHandler.abortUpload(bucketName: target.bucketName, uploadId: uploadId)
        }
        return Response(status: .noContent)
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

        let versioningStatus = await BucketVersioningCache.shared.resolvedStatus(app: req.application, bucket: bucketName)

        // Check if this is a folder (prefix) deletion
        if key.hasSuffix("/") {
            // Keys under this prefix can live on any node, not just this one, so the key set has
            // to come from a cluster-wide listing (never a walk of local disk alone), with each
            // key then routed and deleted -
            // locally or delegated to whichever node(s) actually hold it - via the same per-key
            // `deleteObjectClusterWide` logic Multi-Object-Delete uses. `versioningStatus:
            // .disabled` is passed unconditionally, regardless of the bucket's real status,
            // to preserve deletePrefix's original behavior of hard-deleting every version rather
            // than creating delete markers - folder delete has always been a permanent prune, not
            // a versioning-aware operation.

            // Sanitize before touching disk - a prefix that's entirely ".." components (e.g.
            // "../../") sanitizes down to empty and must be rejected outright, not silently listed
            // as "zero matching keys, nothing to do".
            let sanitizedPrefix = PathSafety.sanitizedPrefix(key)
            guard !sanitizedPrefix.isEmpty && sanitizedPrefix != "/" else {
                throw Abort(.internalServerError, reason: "Invalid prefix for deletion")
            }

            // Enumerate every VERSION and delete marker under the prefix, cluster-wide - NOT just
            // the current objects. A folder delete is a permanent prune, so on a versioning-enabled
            // bucket each stored version must be removed by its real versionId. The previous code
            // listed only current objects (a ListObjectsV2-style listing) and then deleted them at
            // the non-versioned path (versionId nil) - but a versioned object lives at a
            // `.versions/<id>` path, so that delete found nothing, removed nothing, and still
            // reported success. That is exactly the "folder delete says Successful but nothing is
            // gone" symptom on a versioned bucket. `listAllVersions` reports a non-versioned object
            // (matching S3) as versionId "null", which `deleteVersion` maps back to the plain path,
            // so this single loop prunes versioned and non-versioned buckets alike.
            var keyMarker: String? = nil
            var versionIdMarker: String? = nil
            var failedKeys = 0
            repeat {
                let (versions, deleteMarkers, _, isTruncated, nextKeyMarker, nextVersionIdMarker) =
                    try await ClusterListingService.listAllVersions(
                        req: req, bucketName: bucketName, prefix: sanitizedPrefix, delimiter: nil,
                        keyMarker: keyMarker, versionIdMarker: versionIdMarker, maxKeys: 1000)
                for object in versions + deleteMarkers {
                    // Delete by the object's own versionId, exactly as the proven single-object
                    // console delete does: nil for a truly non-versioned object (a plain-path
                    // delete), the literal "null" for a suspended null version, or a real id for a
                    // stored version. Coercing nil -> "null" here broke the cluster replica-delete
                    // fan-out (a non-versioned object's replicas survived), while the single-object
                    // path - which passes nil - worked; matching it fixes that.
                    let targetVersionId = object.versionId
                    // A per-key delete can fail (a responsible peer being unreachable) - unlike
                    // the local-disk-only deletePrefix this replaced, whose only failure mode was
                    // a local IO error. Rather than silently reporting the whole folder deleted
                    // when some objects survived, count the failures and surface them below.
                    do {
                        _ = try await ClusterReplicationService.deleteObjectClusterWide(
                            req: req, bucketName: bucketName, key: object.key,
                            versionId: targetVersionId, versioningStatus: .disabled)
                    } catch {
                        failedKeys += 1
                        req.logger.warning(
                            "Folder delete: failed to delete '\(object.key)' (version \(targetVersionId ?? "null")) under '\(sanitizedPrefix)': \(error)"
                        )
                        continue
                    }
                    // A delete marker being removed isn't itself an object-removed event; only emit
                    // for a real object version.
                    if !object.isDeleteMarker {
                        await NotificationService.emit(
                            event: .objectRemovedDelete, bucketName: bucketName, key: object.key,
                            size: nil, etag: nil, versionId: targetVersionId,
                            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)
                    }
                    await ReplicationService.enqueueDelete(
                        app: req.application, bucketName: bucketName, key: object.key,
                        versionId: targetVersionId)
                }
                // Advance past what was just processed. Marker positions are absolute in the sorted
                // key/version space, so deleting items at or before the marker never skips the rest.
                keyMarker = isTruncated ? nextKeyMarker : nil
                versionIdMarker = isTruncated ? nextVersionIdMarker : nil
            } while keyMarker != nil

            if failedKeys > 0 {
                throw Abort(
                    .internalServerError,
                    reason:
                        "Failed to delete \(failedKeys) object(s) under the prefix; some may remain. Please retry."
                )
            }
            // The per-key deletes prune their own parents, but a folder that was already empty
            // (a phantom directory) has no keys to delete - this is what actually removes it.
            await ClusterReplicationService.pruneEmptyPrefix(
                app: req.application, bucketName: bucketName, prefix: sanitizedPrefix)
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

    /// Copy, move/rename and version restore for the object browser - all one server-side copy
    /// (restore is S3's own idiom: copy the old version onto its key as a new current version),
    /// routed to the destination's owner before anything is read, like every other write.
    @Sendable
    func copyObject(req: Request) async throws -> Response {
        let auth = try req.auth.require(AuthenticatedUser.self)
        let input = try req.content.decode(ObjectCopyRequestDTO.self)
        let deleteSource = input.deleteSource ?? false

        guard !input.sourceKey.isEmpty, !input.sourceKey.hasSuffix("/") else {
            throw Abort(.badRequest, reason: "Source key must name an object, not a folder")
        }
        guard !input.destinationKey.hasSuffix("/") else {
            throw Abort(.badRequest, reason: "Destination key must name an object, not a folder")
        }
        try ObjectKeyValidation.validate(input.destinationKey)

        let sameObject =
            input.sourceBucket == input.destinationBucket
            && input.sourceKey == input.destinationKey
        if sameObject && input.sourceVersionId == nil {
            throw Abort(.badRequest, reason: "Source and destination are the same object")
        }
        if sameObject && deleteSource {
            // Deleting the source afterwards would delete (or mark) the copy just made.
            throw Abort(.badRequest, reason: "An object cannot be moved onto itself")
        }

        try await requireOwnedBucketExists(
            req: req, bucketName: input.sourceBucket, userId: auth.userId)
        if input.sourceBucket != input.destinationBucket {
            try await requireOwnedBucketExists(
                req: req, bucketName: input.destinationBucket, userId: auth.userId)
        }

        let peers: [ClusterNodeInfo]
        let ecWriteFanOut: (peers: [ClusterNodeInfo], config: ClusterErasureCodingConfig)?
        switch try await S3Controller.routeSingleObjectWrite(
            req: req, bucketName: input.destinationBucket, key: input.destinationKey)
        {
        case .local(let localPeers, let fanOut):
            peers = localPeers
            ecWriteFanOut = fanOut
        case .forwarded(let response):
            return response
        }

        let versioningStatus = await BucketVersioningCache.shared.resolvedStatus(
            app: req.application, bucket: input.destinationBucket)
        let outcome: S3Controller.CopyOutcome
        do {
            outcome = try await S3Controller().copyObject(
                req: req, destinationBucket: input.destinationBucket,
                destinationKey: input.destinationKey,
                copySource: CopySource(
                    bucketName: input.sourceBucket, key: input.sourceKey,
                    versionId: input.sourceVersionId),
                versioningStatus: versioningStatus, peers: peers, ecWriteFanOut: ecWriteFanOut
            ) { .carriedOver(from: $0) }
        } catch {
            throw abort(for: error, serverFault: "Copying the object failed.")
        }

        if deleteSource {
            // The copy is durable, so the source goes the same way a console delete takes it:
            // a delete marker on a versioned bucket, a hard delete otherwise.
            let sourceVersioning =
                input.sourceBucket == input.destinationBucket
                ? versioningStatus
                : await BucketVersioningCache.shared.resolvedStatus(
                    app: req.application, bucket: input.sourceBucket)
            let removed = try await ClusterReplicationService.deleteObjectClusterWide(
                req: req, bucketName: input.sourceBucket, key: input.sourceKey, versionId: nil,
                versioningStatus: sourceVersioning)
            await NotificationService.emit(
                event: removed.isDeleteMarker
                    ? .objectRemovedDeleteMarkerCreated : .objectRemovedDelete,
                bucketName: input.sourceBucket, key: input.sourceKey, size: nil, etag: nil,
                versionId: removed.versionId,
                requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)
            await ReplicationService.enqueueDelete(
                app: req.application, bucketName: input.sourceBucket, key: input.sourceKey,
                versionId: removed.versionId)
        }

        return try await ObjectMeta.ResponseDTO(from: outcome.meta).encodeResponse(for: req)
    }

    @Sendable
    func downloadObjects(req: Request) async throws -> Response {
        let auth = try req.auth.require(AuthenticatedUser.self)
        let input = try req.content.decode(DownloadRequestDTO.self)
        return try await download(
            req: req, userId: auth.userId, bucket: input.bucket, keys: input.keys,
            versionId: input.versionId, inline: false, downloadName: nil)
    }

    /// Hands out a `DownloadTicket` for what the caller may download, so the browser can fetch
    /// it directly (`DownloadTicketController`). Ownership is checked here and again when the
    /// ticket is redeemed.
    @Sendable
    func issueDownloadTicket(req: Request) async throws -> DownloadTicketDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)
        let input = try req.content.decode(DownloadTicketRequestDTO.self)
        guard !input.keys.isEmpty else {
            throw Abort(.badRequest, reason: "No keys provided for download")
        }
        try await requireOwnedBucketExists(req: req, bucketName: input.bucket, userId: auth.userId)
        guard let key = req.application.storage[DownloadTicketSigner.Key.self] else {
            throw Abort(.internalServerError, reason: "Download tickets are not configured")
        }
        let ticket = DownloadTicket(
            bucket: input.bucket, keys: input.keys, versionId: input.versionId, userId: auth.userId,
            inline: input.inline ?? false, downloadName: input.downloadName,
            expiresAt: Date().addingTimeInterval(DownloadTicketSigner.lifetime))
        return DownloadTicketDTO(
            ticket: try DownloadTicketSigner.issue(ticket, key: key), expiresAt: ticket.expiresAt)
    }

    /// The download itself, for the bearer-authenticated POST and for a redeemed ticket alike.
    func download(
        req: Request, userId: UUID, bucket: String, keys: [String], versionId: String?,
        inline: Bool, downloadName: String?
    ) async throws -> Response {
        guard !keys.isEmpty else {
            throw Abort(.badRequest, reason: "No keys provided for download")
        }
        try await requireOwnedBucketExists(req: req, bucketName: bucket, userId: userId)

        // If single file, download directly
        if keys.count == 1 && !keys[0].hasSuffix("/") {
            return try await downloadSingleFile(
                req: req, bucketName: bucket, key: keys[0], versionId: versionId, inline: inline,
                downloadName: downloadName)
        }

        // Multiple files or folders - create ZIP
        return try await downloadAsZip(req: req, bucketName: bucket, keys: keys)
    }

    private func downloadSingleFile(
        req: Request, bucketName: String, key: String, versionId: String? = nil, inline: Bool = false,
        downloadName: String? = nil
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
                responsible.contains(where: { $0.id == config.nodeId }),
                ErasureCodedObjectHandler.holdsAnyLocalShard(
                    bucketName: bucketName, key: key, versionId: versionId)
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
                return streamedResponse(req: req, rawTempFile: tempPath, meta: meta, key: key, inline: inline, downloadName: downloadName)
            }

            // Resolve to the on-disk file so large objects can stream straight from it - a
            // console download of a multi-GB object must not buffer it in memory (same pattern
            // as SharedLinkController and the S3 GET handler).
            let located: (path: String, location: (meta: ObjectMeta, payloadOffset: Int, payloadSize: Int))? =
                try? await S3Service.offloadBlockingIO(req) {
                    var path = try? ObjectFileHandler.resolvePath(
                        bucketName: bucketName, key: key, versionId: versionId)
                    if path == nil, versionId == "null" {
                        // The "null" version of a never-versioned key lives at the plain path
                        let plainPath = ObjectFileHandler.storagePath(for: bucketName, key: key)
                        if ObjectFileHandler.keyExists(for: bucketName, key: key, path: plainPath) {
                            path = plainPath
                        }
                    }
                    guard let path else { return nil }
                    return (path, try ObjectFileHandler.payloadLocation(path: path))
                }

            if let (path, location) = located ?? nil, !location.meta.isDeleteMarker {
                let meta = location.meta
                let response: Response
                if location.payloadSize > Constants.streamingThreshold {
                    response = S3Service.buildStreamingObjectResponse(
                        req: req, meta: meta, path: path, payloadOffset: location.payloadOffset)
                } else {
                    // Headers and body from the same read - one consistent snapshot of the file
                    guard
                        let (freshMeta, data) = try? await S3Service.offloadBlockingIO(req, {
                            try ObjectFileHandler.read(from: path, loadData: true)
                        }),
                        let data
                    else {
                        throw Abort(.internalServerError, reason: "Failed to read object data")
                    }
                    response = S3Service.buildVersionedObjectMetadataResponse(
                        meta: freshMeta, includeBody: true, data: data)
                }
                return Self.attachDownloadHeaders(to: response, key: key, inline: inline, downloadName: downloadName)
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
        return streamedResponse(req: req, rawTempFile: tempPath, meta: meta, key: key, inline: inline, downloadName: downloadName)
    }

    /// Streams a raw (header-less) temp file - the payload `ClusterReplicationClient
    /// .fetchObjectToTempFile` spooled from a peer - straight to the client in fixed windows,
    /// unlinking it once the stream drains (or fails). Distinct from
    /// `S3Service.buildStreamingObjectResponse`, which streams an on-disk *Alarik object file*
    /// (metadata header + payload); this file is bare bytes with `meta` supplied separately.
    private func streamedResponse(
        req: Request, rawTempFile tempPath: String, meta: ObjectMeta, key: String, inline: Bool,
        downloadName: String?
    ) -> Response {
        let response = S3Service.buildVersionedObjectMetadataResponse(meta: meta, includeBody: false)
        Self.streamRawFile(req: req, path: tempPath, count: meta.size, into: response)
        return Self.attachDownloadHeaders(to: response, key: key, inline: inline, downloadName: downloadName)
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

    /// `inline` is honoured only for types a browser renders natively without running anything;
    /// an HTML or SVG body shown inline would run in the API's origin. A `.pdf` key is served as
    /// a PDF whatever type it was uploaded with, so the viewer opens it and nothing else does.
    private static func attachDownloadHeaders(
        to response: Response, key: String, inline: Bool, downloadName: String?
    ) -> Response {
        let fileName = downloadName ?? String(key.split(separator: "/").last ?? "download")
        var disposition = "attachment"
        if inline {
            if fileName.lowercased().hasSuffix(".pdf") {
                response.headers.contentType = HTTPMediaType(type: "application", subType: "pdf")
            }
            if let type = response.headers.contentType, isInlineSafe(type) {
                disposition = "inline"
            }
        }
        response.headers.replaceOrAdd(
            name: "Content-Disposition",
            value: "\(disposition); filename=\"\(fileName.contentDispositionFilenameEscaped)\""
        )
        response.headers.replaceOrAdd(name: "X-Content-Type-Options", value: "nosniff")
        response.headers.replaceOrAdd(name: .cacheControl, value: "private, no-store")
        return response
    }

    private static func isInlineSafe(_ type: HTTPMediaType) -> Bool {
        switch type.type.lowercased() {
        case "image": return type.subType.lowercased() != "svg+xml"
        case "video", "audio": return true
        case "application": return type.subType.lowercased() == "pdf"
        default: return false
        }
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
            req.logger.error("Folder download could not build the ZIP archive: \(error)")
            throw Abort(.internalServerError, reason: "Failed to create the ZIP archive.")
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
            if let path = try? await S3Service.offloadBlockingIO(req, {
                try ObjectFileHandler.resolvePath(bucketName: bucketName, key: key, versionId: nil)
            }) ?? nil {
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

        let resolvedPath = try await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.resolvePath(bucketName: bucketName, key: key, versionId: nil)
        }
        guard let path = resolvedPath else {
            throw Abort(.notFound, reason: "Object not found")
        }

        let updatedMeta = try await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.rewriteMetadata(bucketName: bucketName, key: key, at: path) {
                $0.tags = nil
            }
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

        let versions = try await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.listVersions(bucketName: bucketName, key: key)
        }
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

        let versioningStatus = await BucketVersioningCache.shared.resolvedStatus(app: req.application, bucket: bucketName)
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
