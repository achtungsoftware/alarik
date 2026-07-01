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

    struct ShareRequestDTO: Content {
        let bucket: String
        let key: String
        let expiresInSeconds: Int
    }

    struct ShareResponseDTO: Content {
        let url: String
        let expiresAt: Date
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
        routes.grouped("objects").get(use: self.listObjects)
        routes.grouped("objects").post(use: self.uploadObject)
        routes.grouped("objects").delete(use: self.deleteObject)
        routes.grouped("objects", "download").post(use: self.downloadObjects)
        routes.grouped("objects", "versions").get(use: self.listObjectVersions)
        routes.grouped("objects", "version").delete(use: self.deleteObjectVersion)
        routes.grouped("objects", "tags").get(use: self.getObjectTags)
        routes.grouped("objects", "tags").put(use: self.setObjectTags)
        routes.grouped("objects", "tags").delete(use: self.deleteObjectTags)
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

    @Sendable
    func listBuckets(req: Request) async throws -> Page<Bucket> {
        let auth = try req.auth.require(AuthenticatedUser.self)
        return try await Bucket.query(on: req.db)
            .filter(\.$user.$id == auth.userId)
            .sort(\.$creationDate, .descending)
            .paginate(for: req)
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
        let delimiter = req.query[String.self, at: "delimiter"] ?? "/"

        let (objects, commonPrefixes, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            prefix: prefix,
            delimiter: delimiter,
            maxKeys: 10000
        )

        // Convert objects to DTOs
        var items: [ObjectMeta.ResponseDTO] = []

        // Add folders (common prefixes)
        for commonPrefix in commonPrefixes {
            items.append(ObjectMeta.ResponseDTO(folderKey: commonPrefix))
        }

        // Add files
        for object in objects {
            items.append(ObjectMeta.ResponseDTO(from: object))
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

    /// Cap on how long a shared link can stay valid. Kept at the same 7 days as a real S3
    /// presigned URL would allow, for familiarity - but this is an Alarik-specific limit, not
    /// a SigV4 constraint, since shared links don't use SigV4 at all.
    static let maxShareExpirySeconds = 604_800

    /// Creates a time-limited public link to an object you own. Nothing about your account or
    /// credentials is exposed - the link is just an opaque, unguessable token (the new row's own
    /// id) that `SharedLinkController` looks up. Revoking access just means deleting the row,
    /// which happens automatically once it expires (see the cleanup task in configure.swift).
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

        guard input.expiresInSeconds > 0,
            input.expiresInSeconds <= Self.maxShareExpirySeconds
        else {
            throw Abort(
                .badRequest,
                reason:
                    "expiresInSeconds must be between 1 and \(Self.maxShareExpirySeconds) (7 days)"
            )
        }

        let expiresAt = Date().addingTimeInterval(TimeInterval(input.expiresInSeconds))
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

        // Write object with versioning support
        if versioningStatus != .disabled {
            let versionId = try ObjectFileHandler.writeVersioned(
                metadata: meta,
                data: fileData,
                bucketName: bucketName,
                key: keyPath,
                versioningStatus: versioningStatus
            )
            meta.versionId = versionId
            meta.isLatest = true
        } else {
            let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
            try ObjectFileHandler.write(metadata: meta, data: fileData, to: path)
        }

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
            // Delete all objects with this prefix
            _ = try ObjectFileHandler.deletePrefix(bucketName: bucketName, prefix: key)
        } else if versioningStatus == .enabled {
            // Versioning enabled - create delete marker instead of permanent delete
            _ = try ObjectFileHandler.createDeleteMarker(bucketName: bucketName, key: key)
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
        // Try versioned storage first, then fall back to non-versioned
        let meta: ObjectMeta
        let fileData: Data

        do {
            let (m, data) = try ObjectFileHandler.readVersion(
                bucketName: bucketName,
                key: key,
                versionId: versionId,
                loadData: true
            )

            // Check if latest version is a delete marker
            if m.isDeleteMarker {
                throw Abort(.notFound, reason: "Object not found")
            }

            guard let d = data else {
                throw Abort(.internalServerError, reason: "Failed to read object data")
            }

            meta = m
            fileData = d
        } catch let error as Abort {
            throw error
        } catch {
            // Fall back to non-versioned path
            let path = ObjectFileHandler.storagePath(for: bucketName, key: key)

            guard FileManager.default.fileExists(atPath: path) else {
                throw Abort(.notFound, reason: "Object not found")
            }

            let (m, data) = try ObjectFileHandler.read(from: path, loadData: true)

            guard let d = data else {
                throw Abort(.internalServerError, reason: "Failed to read object data")
            }

            meta = m
            fileData = d
        }

        let response = Response(status: .ok, body: .init(data: fileData))
        response.headers.contentType = HTTPMediaType(
            type: meta.contentType.split(separator: "/").first.map(String.init) ?? "application",
            subType: meta.contentType.split(separator: "/").last.map(String.init)
                ?? "octet-stream"
        )
        let fileName = String(key.split(separator: "/").last ?? "download")
        response.headers.replaceOrAdd(
            name: "Content-Disposition",
            value: "attachment; filename=\"\(fileName.contentDispositionFilenameEscaped)\""
        )
        response.headers.replaceOrAdd(
            name: .contentLength,
            value: String(fileData.count)
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

    @Sendable
    func setVersioning(req: Request) async throws -> VersioningStatusDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(VersioningStatusDTO.self)

        guard let newStatus = VersioningStatus(rawValue: input.status) else {
            throw Abort(
                .badRequest,
                reason: "Invalid versioning status. Use 'Enabled', 'Suspended', or 'Disabled'")
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

        let (meta, data) = try ObjectFileHandler.read(from: path, loadData: true)
        guard let data = data else {
            throw Abort(.internalServerError, reason: "Could not read object data")
        }

        var updatedMeta = meta
        updatedMeta.tags = input.tags
        try ObjectFileHandler.write(metadata: updatedMeta, data: data, to: path)

        return TagsDTO(tags: input.tags)
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

        let (meta, data) = try ObjectFileHandler.read(from: path, loadData: true)
        guard let data = data else {
            throw Abort(.internalServerError, reason: "Could not read object data")
        }

        var updatedMeta = meta
        updatedMeta.tags = nil
        try ObjectFileHandler.write(metadata: updatedMeta, data: data, to: path)

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

        return .noContent
    }
}
