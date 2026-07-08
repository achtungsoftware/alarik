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
import Foundation
import Vapor
import XMLCoder
import ZIPFoundation

struct InternalBucketController: RouteCollection {
    struct UploadInput: Content {
        let data: File
    }

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
            self.id = delivery.id!
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
            self.id = task.id!
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

    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("buckets").get(use: self.listBuckets)
        routes.grouped("buckets").post(use: self.createBucket)
        routes.grouped("buckets").grouped(":bucketName").delete(use: self.deleteBucket)
        routes.grouped("buckets").grouped(":bucketName").grouped("versioning").get(
            use: self.getVersioning)
        routes.grouped("buckets").grouped(":bucketName").grouped("versioning").put(
            use: self.setVersioning)
        routes.grouped("buckets").grouped(":bucketName").grouped("policy").get(
            use: self.getPolicy)
        routes.grouped("buckets").grouped(":bucketName").grouped("policy").put(
            use: self.setPolicy)
        routes.grouped("buckets").grouped(":bucketName").grouped("policy").delete(
            use: self.deletePolicy)
        routes.grouped("buckets").grouped(":bucketName").grouped("tags").get(
            use: self.getBucketTags)
        routes.grouped("buckets").grouped(":bucketName").grouped("tags").put(
            use: self.setBucketTags)
        routes.grouped("buckets").grouped(":bucketName").grouped("tags").delete(
            use: self.deleteBucketTags)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications").get(
            use: self.getBucketNotifications)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications").put(
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
            .grouped("targets").put(use: self.setReplicationTargets)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("rules").get(use: self.getReplicationRules)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("rules").put(use: self.setReplicationRules)
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
        routes.grouped("objects").post(use: self.uploadObject)
        routes.grouped("objects").delete(use: self.deleteObject)
        routes.grouped("objects", "download").post(use: self.downloadObjects)
        routes.grouped("objects", "versions").get(use: self.listObjectVersions)
        routes.grouped("objects", "version").delete(use: self.deleteObjectVersion)
        routes.grouped("objects", "tags").get(use: self.getObjectTags)
        routes.grouped("objects", "tags").put(use: self.setObjectTags)
        routes.grouped("objects", "tags").delete(use: self.deleteObjectTags)
        routes.grouped("objects", "metadata").get(use: self.getObjectMetadata)
        routes.grouped("objects", "metadata").put(use: self.setObjectMetadata)
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
        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .filter(\.$user.$id == userId)
                .first()
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
    func listBuckets(req: Request) async throws -> Page<Bucket> {
        let auth = try req.auth.require(AuthenticatedUser.self)
        let search = req.query[String.self, at: "search"]?.trimmingCharacters(in: .whitespaces)

        let query = Bucket.query(on: req.db)
            .filter(\.$user.$id == auth.userId)
            .sort(\.$creationDate, .descending)

        if let search, !search.isEmpty {
            query.filter(\.$name ~~ search)
        }

        return try await query.paginate(for: req)
    }

    @Sendable
    func createBucket(req: Request) async throws -> Bucket.ResponseDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        try Bucket.Create.validate(content: req)

        let create: Bucket.Create = try req.content.decode(Bucket.Create.self)

        if (try await Bucket.query(on: req.db).filter(\.$name == create.name).first()) != nil {
            throw Abort(.conflict, reason: "The requested bucket name is not available.")
        }

        try await BucketService.create(
            on: req.db, bucketName: create.name, userId: auth.userId,
            versioningEnabled: create.versioningEnabled)

        // Fetch the created bucket from the database to get the ID
        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == create.name)
                .filter(\.$user.$id == auth.userId)
                .first()
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
            on: req.db, bucketName: bucketName, userId: auth.userId, force: true)

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

        let (objects, commonPrefixes, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            prefix: prefix,
            delimiter: delimiter,
            maxKeys: 10000
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

        let (sizeBytes, objectCount) = BucketHandler.calculateStats(
            bucketName: bucketName, prefix: prefix)
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

        guard
            try ObjectFileHandler.readCurrentObject(
                bucketName: input.bucket, key: input.key, loadData: false) != nil
        else {
            throw Abort(.notFound, reason: "Object not found")
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
        try await link.save(on: req.db)

        let url = "\(apiBaseURL)/api/v1/shared/\(link.id!.uuidString)"
        return ShareResponseDTO(url: url, expiresAt: expiresAt)
    }

    /// Lists shared links created by the authenticated user, across all of their buckets.
    @Sendable
    func listSharedLinks(req: Request) async throws -> Page<SharedLink.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)

        let page: Page<SharedLink> = try await SharedLink.query(on: req.db)
            .filter(\.$user.$id == auth.userId)
            .sort(\.$createdAt, .descending)
            .paginate(for: req)

        return page.map { $0.toResponseDTO() }
    }

    /// Revokes a shared link early, before it would otherwise expire.
    @Sendable
    func deleteSharedLink(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let sharedLinkId = req.parameters.get("sharedLinkId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid shared link ID.")
        }

        guard
            let link = try await SharedLink.query(on: req.db)
                .filter(\.$id == sharedLinkId)
                .filter(\.$user.$id == auth.userId)
                .first()
        else {
            throw Abort(.notFound, reason: "Shared link not found.")
        }

        try await link.delete(on: req.db)

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

        // Parse multipart form data
        let input = try req.content.decode(UploadInput.self)

        let filename = input.data.filename
        guard !filename.isEmpty else {
            throw Abort(.badRequest, reason: "File must have a filename")
        }

        // Construct the full key path (prefix + filename)
        let keyPath = prefix.isEmpty ? filename : "\(prefix)\(filename)"

        // Read file data
        let fileData = Data(buffer: input.data.data)

        let etag = S3Service.computeETag(fileData)

        // Create object metadata
        var meta = ObjectMeta(
            bucketName: bucketName,
            key: keyPath,
            size: fileData.count,
            contentType: input.data.contentType?.description ?? "application/octet-stream",
            etag: etag,
            updatedAt: Date()
        )

        // Get bucket versioning status from cache
        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        // Write object with versioning support - real blocking file IO, offloaded to the
        // blocking-IO thread pool rather than tying up the async executor.
        let initialMeta = meta
        let writtenVersionId: String? = try await S3Service.offloadBlockingIO(req) {
            if versioningStatus != .disabled {
                return try ObjectFileHandler.writeVersioned(
                    metadata: initialMeta,
                    data: fileData,
                    bucketName: bucketName,
                    key: keyPath,
                    versioningStatus: versioningStatus
                )
            } else {
                let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
                try ObjectFileHandler.write(metadata: initialMeta, data: fileData, to: path)
                return nil
            }
        }
        if let writtenVersionId {
            meta.versionId = writtenVersionId
            meta.isLatest = true
        }

        await NotificationService.emit(
            event: .objectCreatedPut, bucketName: bucketName, key: keyPath,
            size: meta.size, etag: etag, versionId: meta.versionId,
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, on: req.db)
        await ReplicationService.enqueuePut(
            bucketName: bucketName, key: keyPath, versionId: meta.versionId, on: req.db)

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
            // Delete all objects with this prefix, emitting one event per removed object
            let deletedKeys = try ObjectFileHandler.deletePrefix(bucketName: bucketName, prefix: key)
            for deletedKey in deletedKeys {
                await NotificationService.emit(
                    event: .objectRemovedDelete, bucketName: bucketName, key: deletedKey,
                    size: nil, etag: nil, versionId: nil,
                    requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, on: req.db)
                await ReplicationService.enqueueDelete(
                    bucketName: bucketName, key: deletedKey, versionId: nil, on: req.db)
            }
        } else if versioningStatus == .enabled {
            // Versioning enabled - create delete marker instead of permanent delete
            let marker = try ObjectFileHandler.createDeleteMarker(bucketName: bucketName, key: key)
            await NotificationService.emit(
                event: .objectRemovedDeleteMarkerCreated, bucketName: bucketName, key: key,
                size: nil, etag: nil, versionId: marker.versionId,
                requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, on: req.db)
            await ReplicationService.enqueueDelete(
                bucketName: bucketName, key: key, versionId: marker.versionId, on: req.db)
        } else {
            // Versioning disabled or suspended - permanent delete
            // Check versioned storage first
            if ObjectFileHandler.isVersioned(bucketName: bucketName, key: key) {
                // Delete all versions
                let versions = try ObjectFileHandler.listVersions(bucketName: bucketName, key: key)
                for version in versions {
                    if let vid = version.versionId {
                        try? ObjectFileHandler.deleteVersion(
                            bucketName: bucketName, key: key, versionId: vid)
                    }
                }
            }

            // Also delete non-versioned path if exists
            let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
            if FileManager.default.fileExists(atPath: path) {
                try FileManager.default.removeItem(atPath: path)
            }

            await NotificationService.emit(
                event: .objectRemovedDelete, bucketName: bucketName, key: key,
                size: nil, etag: nil, versionId: nil,
                requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, on: req.db)
            await ReplicationService.enqueueDelete(
                bucketName: bucketName, key: key, versionId: nil, on: req.db)
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
        guard let path else {
            throw Abort(.notFound, reason: "Object not found")
        }

        guard let location = try? ObjectFileHandler.payloadLocation(path: path),
            !location.meta.isDeleteMarker
        else {
            throw Abort(.notFound, reason: "Object not found")
        }
        let meta = location.meta

        let response: Response
        if location.payloadSize > Constants.streamingThreshold {
            response = S3Service.buildStreamingObjectResponse(
                req: req, meta: meta, path: path, payloadOffset: location.payloadOffset)
        } else {
            // Headers and body from the same read - one consistent snapshot of the file
            guard let (freshMeta, data) = try? ObjectFileHandler.read(from: path, loadData: true),
                let data
            else {
                throw Abort(.internalServerError, reason: "Failed to read object data")
            }
            response = S3Service.buildVersionedObjectMetadataResponse(
                meta: freshMeta, includeBody: true, data: data)
        }

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

        // Create ZIP archive
        let archive: Archive
        do {
            archive = try Archive(
                url: zipURL, accessMode: .create)
        } catch {
            throw Abort(.internalServerError, reason: "Failed to create ZIP archive: \(error)")
        }

        var addedFiles = 0

        // Helper function to read object data (versioned or non-versioned)
        func readObjectData(bucketName: String, key: String) -> Data? {
            guard
                let (_, data) = try? ObjectFileHandler.readCurrentObject(
                    bucketName: bucketName, key: key, loadData: true)
            else {
                return nil
            }
            return data
        }

        for key in keys {
            if key.hasSuffix("/") {
                // It's a folder - add all files with this prefix
                let (objects, _, _, _) = try ObjectFileHandler.listObjects(
                    bucketName: bucketName,
                    prefix: key,
                    delimiter: nil,  // No delimiter to get all nested files
                    maxKeys: 10000
                )

                for object in objects {
                    // Skip delete markers
                    if object.isDeleteMarker { continue }

                    if let fileData = readObjectData(bucketName: bucketName, key: object.key) {
                        // Use relative path from the folder prefix
                        let relativePath = String(object.key.dropFirst(key.count))
                        let zipEntryPath = relativePath.isEmpty ? object.key : relativePath

                        try archive.addEntry(
                            with: zipEntryPath, type: .file,
                            uncompressedSize: Int64(fileData.count),
                            bufferSize: 4096,
                            provider: { position, size in
                                let start = Int(position)
                                let end = min(start + size, fileData.count)
                                return fileData[start..<end]
                            })
                        addedFiles += 1
                    }
                }
            } else {
                // Single file - use just the filename without path
                if let fileData = readObjectData(bucketName: bucketName, key: key) {
                    // Extract just the filename from the full key path
                    let filename = key.split(separator: "/").last.map(String.init) ?? key

                    try archive.addEntry(
                        with: filename, type: .file, uncompressedSize: Int64(fileData.count),
                        bufferSize: 4096,
                        provider: { position, size in
                            let start = Int(position)
                            let end = min(start + size, fileData.count)
                            return fileData[start..<end]
                        })
                    addedFiles += 1
                }
            }
        }

        guard addedFiles > 0 else {
            try? FileManager.default.removeItem(at: zipURL)
            throw Abort(.notFound, reason: "No files found to download")
        }

        // Read the ZIP file
        let zipData = try Data(contentsOf: zipURL)

        // Clean up
        try? FileManager.default.removeItem(at: zipURL)

        let response = Response(status: .ok, body: .init(data: zipData))
        response.headers.contentType = .zip
        response.headers.replaceOrAdd(
            name: "Content-Disposition",
            value: "attachment; filename=\"\(bucketName)-download.zip\""
        )
        response.headers.replaceOrAdd(
            name: .contentLength,
            value: String(zipData.count)
        )

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
        try await bucket.save(on: req.db)

        await BucketVersioningCache.shared.setStatus(for: bucketName, status: newStatus)

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
        try await bucket.save(on: req.db)

        await BucketPolicyCache.shared.setPolicy(for: bucketName, policy: policy)

        return PolicyDTO(policy: rawJSON)
    }

    static func deletePolicy(req: Request, bucket: Bucket, bucketName: String) async throws {
        bucket.policy = nil
        try await bucket.save(on: req.db)

        await BucketPolicyCache.shared.removePolicy(for: bucketName)
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
        try await bucket.save(on: req.db)

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
        try await bucket.save(on: req.db)

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
        return NotificationConfigDTO(rules: NotificationConfiguration.fromJSON(raw).rules)
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
            }
            return normalized
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        let config = NotificationConfiguration(rules: normalizedRules)
        bucket.notificationConfig = normalizedRules.isEmpty ? nil : config.toJSON()
        try await bucket.save(on: req.db)

        await NotificationConfigCache.shared.setConfig(for: bucketName, config: config)

        // Stop delivering already-queued events for rules that were just removed - if a rule
        // is deleted because its endpoint is wrong or compromised, in-flight deliveries to the
        // old URL must not keep firing until they exhaust their retries.
        let keptIds = normalizedRules.map(\.id)
        let purge = NotificationDelivery.query(on: req.db)
            .filter(\.$bucketName == bucketName)
        // `!~ []` is driver-dependent, so when every rule was removed just purge the whole
        // bucket's outbox rather than relying on a NOT IN () clause.
        if !keptIds.isEmpty {
            purge.filter(\.$ruleId !~ keptIds)
        }
        try await purge.delete()

        return NotificationConfigDTO(rules: normalizedRules)
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
            rule: rule, bucketName: bucketName, requestId: req.id, on: req.db)

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

        let deliveries = try await NotificationDelivery.query(on: req.db)
            .filter(\.$bucketName == bucketName)
            .sort(\.$createdAt, .descending)
            .limit(100)
            .all()

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
        guard
            let delivery = try await NotificationDelivery.query(on: req.db)
                .filter(\.$id == deliveryId)
                .filter(\.$bucketName == bucketName)
                .first()
        else {
            throw Abort(.notFound, reason: "Delivery not found")
        }

        delivery.state = NotificationDelivery.State.pending.rawValue
        delivery.attempts = 0
        delivery.nextAttemptAt = Date()
        try await delivery.save(on: req.db)

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
        return ReplicationTargetsDTO(targets: ReplicationConfiguration.fromJSON(raw).targets)
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
            guard !target.accessKeyId.isEmpty, !target.secretAccessKey.isEmpty else {
                throw Abort(.badRequest, reason: "Each replication target requires credentials.")
            }

            var normalized = target
            if normalized.id == ReplicationTarget.zeroUUID {
                normalized.id = UUID()
            }
            return normalized
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        let existingRules =
            bucket.replicationConfig.map { ReplicationConfiguration.fromJSON($0).rules } ?? []
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
        try await bucket.save(on: req.db)

        await ReplicationConfigCache.shared.setConfig(for: bucketName, config: config)

        return ReplicationTargetsDTO(targets: normalizedTargets)
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
        try await bucket.save(on: req.db)

        await ReplicationConfigCache.shared.setConfig(for: bucketName, config: config)

        // Stop delivering already-queued tasks for rules that were just removed - same
        // reasoning as the equivalent webhook-delivery purge above.
        let keptIds = normalizedRules.map(\.id)
        let purge = ReplicationTask.query(on: req.db)
            .filter(\.$bucketName == bucketName)
        if !keptIds.isEmpty {
            purge.filter(\.$ruleId !~ keptIds)
        }
        try await purge.delete()

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
        let db = req.application.db
        let logger = req.application.logger
        Task {
            do {
                let enqueued = try await ReplicationService.resync(
                    bucketName: bucketName, rule: rule, target: target, on: db)
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

        let tasks = try await ReplicationTask.query(on: req.db)
            .filter(\.$bucketName == bucketName)
            .sort(\.$createdAt, .descending)
            .limit(100)
            .all()

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
        guard
            let task = try await ReplicationTask.query(on: req.db)
                .filter(\.$id == taskId)
                .filter(\.$bucketName == bucketName)
                .first()
        else {
            throw Abort(.notFound, reason: "Replication task not found")
        }

        task.state = ReplicationTask.State.pending.rawValue
        task.attempts = 0
        task.nextAttemptAt = Date()
        try await task.save(on: req.db)

        ReplicationDispatcher.shared.wake()

        return ReplicationTaskDTO(from: task)
    }

    /// Returns the tags of the *current* version of an object. Internal API doesn't expose
    /// per-version tag management (the S3-protocol endpoints already do, via `versionId`).
    @Sendable
    func getObjectTags(req: Request) async throws -> TagsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing 'bucket' or 'key' query parameter")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        guard
            let (meta, _) = try ObjectFileHandler.readCurrentObject(
                bucketName: bucketName, key: key, loadData: false)
        else {
            throw Abort(.notFound, reason: "Object not found")
        }

        return TagsDTO(tags: meta.tags ?? [:])
    }

    /// Sets the tags of the *current* version of an object, overwriting any existing tags
    /// entirely. Does not create a new version - modifies the existing version's metadata in
    /// place, matching the S3-protocol PutObjectTagging semantics.
    @Sendable
    func setObjectTags(req: Request) async throws -> TagsDTO {
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

        guard
            let path = try ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: nil)
        else {
            throw Abort(.notFound, reason: "Object not found")
        }

        _ = try await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.rewriteMetadata(at: path) { $0.tags = input.tags }
        }

        return TagsDTO(tags: input.tags)
    }

    @Sendable
    func getObjectMetadata(req: Request) async throws -> ObjectMetadataDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing 'bucket' or 'key' query parameter")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        guard
            let (meta, _) = try ObjectFileHandler.readCurrentObject(
                bucketName: bucketName, key: key, loadData: false)
        else {
            throw Abort(.notFound, reason: "Object not found")
        }

        return ObjectMetadataDTO(contentType: meta.contentType, metadata: meta.metadata)
    }

    /// Updates the Content-Type and custom (`x-amz-meta-*`) metadata of the *current* version
    /// of an object, in place - same shape as `setObjectTags`: no new version, no re-upload of
    /// the body, and (also matching S3's PutObjectTagging/metadata-only semantics) no
    /// webhook notification, since the object's data hasn't changed.
    @Sendable
    func setObjectMetadata(req: Request) async throws -> ObjectMetadataDTO {
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

        guard
            let path = try ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: nil)
        else {
            throw Abort(.notFound, reason: "Object not found")
        }

        // Lowercase keys, same normalization handleObjectPut applies to x-amz-meta-* headers
        let normalizedMetadata = Dictionary(
            uniqueKeysWithValues: input.metadata.map { ($0.key.lowercased(), $0.value) })

        _ = try await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.rewriteMetadata(at: path) {
                $0.contentType = input.contentType
                $0.metadata = normalizedMetadata
            }
        }

        return ObjectMetadataDTO(contentType: input.contentType, metadata: normalizedMetadata)
    }

    @Sendable
    func deleteObjectTags(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing 'bucket' or 'key' query parameter")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        guard
            let path = try ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: nil)
        else {
            throw Abort(.notFound, reason: "Object not found")
        }

        _ = try await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.rewriteMetadata(at: path) { $0.tags = nil }
        }

        return .noContent
    }

    @Sendable
    func listObjectVersions(req: Request) async throws -> [ObjectMeta.ResponseDTO] {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        guard let key = req.query[String.self, at: "key"] else {
            throw Abort(.badRequest, reason: "Missing 'key' query parameter")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let versions = try ObjectFileHandler.listVersions(bucketName: bucketName, key: key)

        return versions.map { ObjectMeta.ResponseDTO(from: $0) }
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

        try ObjectFileHandler.deleteVersion(bucketName: bucketName, key: key, versionId: versionId)

        await NotificationService.emit(
            event: .objectRemovedDelete, bucketName: bucketName, key: key,
            size: nil, etag: nil, versionId: versionId,
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, on: req.db)
        // Not replicated: this permanently prunes one specific historical version, which has
        // no meaningful equivalent on the replication target (see
        // ReplicationClient.replicateDelete).

        return .noContent
    }
}
