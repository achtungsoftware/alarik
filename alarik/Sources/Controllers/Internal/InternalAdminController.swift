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
import XMLCoder

#if os(Linux)
    import Glibc
#endif

struct InternalAdminController: RouteCollection {

    struct StorageStats: Content {
        let totalBytes: Int64
        let availableBytes: Int64
        let usedBytes: Int64
        let alarikUsedBytes: Int64
        let bucketCount: Int
        let userCount: Int
    }

    /// Runtime metrics (CPU/RAM/traffic + last-hour history) plus cheap resource counts that
    /// don't require walking the storage tree - the dashboard polls this every few seconds, so
    /// it must stay inexpensive (unlike `storageStats`, which walks the whole bucket dir).
    struct SystemStats: Content {
        let metrics: MetricsCollector.Snapshot
        let accessKeyCount: Int
        let sharedLinkCount: Int
        let oidcProviderCount: Int
        let multipartUploadCount: Int
        /// `nil` when this node isn't part of a cluster. Lets the console tell the difference
        /// between "these metrics describe the whole deployment" (single-node mode) and "these
        /// describe only the one node that happened to answer this request" (cluster mode) -
        /// `metrics`, the local disk stats in `storageStats`, and `multipartUploadCount` are all
        /// genuinely per-node in cluster mode, unlike the cluster-wide counts above.
        let clusterNode: ClusterNodeIdentityDTO?
    }

    struct ClusterNodeIdentityDTO: Content {
        let nodeId: UUID
        let address: String
    }

    struct PolicyDTO: Content {
        let policy: String?
    }

    struct BucketStatsDTO: Content {
        let sizeBytes: Int64
        let objectCount: Int
    }

    /// `listBuckets`'s response shape - deliberately NOT the raw `Bucket` model. Fluent's
    /// `Fields.encode` serializes every `@Field` with no exclusions, and `Bucket.$user`'s
    /// eager-loaded `User` would do the same for `passwordHash` - `Bucket.ResponseDTO` (used
    /// elsewhere) also doesn't carry ownership at all, so this exists specifically to answer
    /// "which user owns this bucket" for the admin console without ever serializing a full
    /// `User`/`Bucket` model to the wire.
    struct AdminBucketDTO: Content {
        let id: UUID?
        let name: String
        let creationDate: Date?
        let versioningStatus: String
        let user: User.ResponseDTO?
        /// Raw bucket-policy JSON, or nil when unset - see `Bucket.ResponseDTO.policy`. The admin
        /// bucket list badges each bucket public/private from this; without it every bucket reads
        /// as private regardless of what its policy actually says.
        let policy: String?
    }

    func boot(routes: any RoutesBuilder) throws {

        routes.grouped("admin").grouped("users")
            .get(use: listUsers)

        routes.grouped("admin").grouped("users")
            .post(use: createUser)

        routes.grouped("admin").grouped("users")
            .put(use: editUser)

        routes.grouped("admin").grouped("users")
            .delete(":userId", use: deleteUser)

        routes.grouped("admin")
            .get("storageStats", use: getStorageStats)

        routes.grouped("admin")
            .get("systemStats", use: getSystemStats)

        routes.grouped("admin")
            .get("buckets", use: self.listBuckets)

        routes.grouped("admin").grouped("buckets").grouped(":bucketName").delete(
            use: self.deleteBucket)

        routes.grouped("admin").grouped("buckets").grouped(":bucketName").grouped("policy").get(
            use: self.getBucketPolicy)
        routes.grouped("admin").grouped("buckets").grouped(":bucketName").grouped("policy").put(
            use: self.setBucketPolicy)
        routes.grouped("admin").grouped("buckets").grouped(":bucketName").grouped("policy").delete(
            use: self.deleteBucketPolicy)

        routes.grouped("admin").grouped("buckets").grouped(":bucketName").grouped("stats").get(
            use: self.getBucketStats)
    }

    @Sendable
    func deleteBucket(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        guard let bucket = try await Bucket.find(app: req.application, name: bucketName) else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        try await BucketService.delete(
            req: req, bucketName: bucketName, userId: bucket.userId, force: true)

        return .noContent
    }

    /// Fetches any bucket by name (no ownership filter - the admin can manage any user's
    /// bucket) or throws the standard "Bucket not found" 404.
    private func fetchAnyBucket(req: Request, bucketName: String) async throws -> Bucket {
        guard let bucket = try await Bucket.find(app: req.application, name: bucketName) else {
            throw Abort(.notFound, reason: "Bucket not found")
        }
        return bucket
    }

    @Sendable
    func getBucketPolicy(req: Request) async throws -> PolicyDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await fetchAnyBucket(req: req, bucketName: bucketName)

        return PolicyDTO(policy: InternalBucketController.policyResponse(for: bucket).policy)
    }

    @Sendable
    func setBucketPolicy(req: Request) async throws -> PolicyDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let rawJSON = try InternalBucketController.requirePolicyBody(req: req)
        let bucket = try await fetchAnyBucket(req: req, bucketName: bucketName)

        let result = try await InternalBucketController.setPolicy(
            req: req, bucket: bucket, bucketName: bucketName, rawJSON: rawJSON)
        return PolicyDTO(policy: result.policy)
    }

    @Sendable
    func deleteBucketPolicy(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await fetchAnyBucket(req: req, bucketName: bucketName)

        try await InternalBucketController.deletePolicy(
            req: req, bucket: bucket, bucketName: bucketName)

        return .noContent
    }

    /// Disk usage and object count for a single bucket. Deliberately its own on-demand
    /// endpoint rather than a field on `listBuckets` - a recursive directory walk per bucket
    /// on every page load would make the bucket list slow to scale; the console fetches this
    /// lazily, one call per row already on screen.
    @Sendable
    func getBucketStats(req: Request) async throws -> BucketStatsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }
        _ = try await fetchAnyBucket(req: req, bucketName: bucketName)

        let (sizeBytes, objectCount) = BucketHandler.calculateStats(bucketName: bucketName)
        return BucketStatsDTO(sizeBytes: sizeBytes, objectCount: objectCount)
    }

    @Sendable
    func listBuckets(req: Request) async throws -> Page<AdminBucketDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let allBuckets = try await Bucket.all(app: req.application)
            .sorted { ($0.creationDate ?? .distantPast) > ($1.creationDate ?? .distantPast) }
        let page = try allBuckets.paginated(for: req)

        // Point lookup per bucket ON this page only (never the whole collection) - small,
        // bounded N, the same "shallow, per-page" cost `getBucketStats` already accepts.
        var dto: [AdminBucketDTO] = []
        dto.reserveCapacity(page.items.count)
        for bucket in page.items {
            let user = try await User.find(app: req.application, id: bucket.userId)
            dto.append(
                AdminBucketDTO(
                    id: bucket.id, name: bucket.name, creationDate: bucket.creationDate,
                    versioningStatus: bucket.versioningStatus, user: user?.toResponseDTO(),
                    policy: bucket.policy))
        }
        return Page(items: dto, metadata: page.metadata)
    }

    @Sendable
    func listUsers(req: Request) async throws -> Page<User.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let users = await MetadataListingService.list(
            app: req.application, collection: MetadataCollections.users
        )
        .compactMap { try? JSONDecoder().decode(User.self, from: $0.value) }
        .sorted { $0.name > $1.name }

        return try users.paginated(for: req).map { $0.toResponseDTO() }
    }

    @Sendable
    func createUser(req: Request) async throws -> User.ResponseDTO {
        try User.Create.validate(content: req)

        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let create: User.Create = try req.content.decode(User.Create.self)
        let user: User = try User(
            name: create.name,
            username: create.username,
            passwordHash: Bcrypt.hash(create.password),
            isAdmin: create.isAdmin
        )

        do {
            try await user.create(app: req.application)
        } catch is User.UserError {
            throw Abort(.conflict, reason: "Username already exists.")
        }

        return user.toResponseDTO()
    }

    @Sendable
    func editUser(req: Request) async throws -> User.ResponseDTO {
        try User.EditAdmin.validate(content: req)

        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let editUser: User.EditAdmin = try req.content.decode(User.EditAdmin.self)

        guard let existingUser = try await User.find(app: req.application, id: editUser.id) else {
            throw Abort(.notFound, reason: "User not found")
        }

        // Every admin endpoint (including this one) requires an authenticated admin, so
        // stripping the last admin's status would permanently lock every admin-only action -
        // including re-promoting anyone - behind a login no account can pass anymore.
        if existingUser.isAdmin && !editUser.isAdmin {
            let remainingAdmins = await MetadataListingService.list(
                app: req.application, collection: MetadataCollections.users
            )
            .compactMap { try? JSONDecoder().decode(User.self, from: $0.value) }
            .filter { $0.isAdmin && $0.id != editUser.id }
            .count
            guard remainingAdmins > 0 else {
                throw Abort(
                    .conflict, reason: "Cannot remove admin status from the last administrator.")
            }
        }

        let previousUsername = existingUser.username
        existingUser.name = editUser.name
        existingUser.username = editUser.username
        existingUser.isAdmin = editUser.isAdmin

        do {
            try await existingUser.rename(app: req.application, from: previousUsername)
        } catch is User.UserError {
            throw Abort(.conflict, reason: "Username already exists.")
        }

        return editUser.toUserResponseDTO()
    }

    @Sendable
    func deleteUser(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let userIdString = req.parameters.get("userId"),
            let userId = UUID(uuidString: userIdString)
        else {
            throw Abort(.badRequest, reason: "Invalid user ID")
        }

        guard let userToDelete = try await User.find(app: req.application, id: userId) else {
            throw Abort(.notFound, reason: "User not found")
        }

        // Prevent deleting yourself
        if userToDelete.id == auth.userId {
            throw Abort(.forbidden, reason: "Cannot delete yourself")
        }

        // Force-delete every bucket the user owns through BucketService, not a raw local-disk
        // removal - the same cluster-wide object cleanup, cache invalidation, and outbox purge
        // every other bucket-teardown path in this codebase goes through. A raw
        // `BucketHandler.forceDelete` only wipes the requesting node's own directory, leaving
        // every other cluster node's physical copies orphaned - invisible until a bucket with
        // the same name is created again (bucket paths are name-derived, not id-derived), at
        // which point the "deleted" data silently resurfaces under the new bucket.
        let buckets = try await Bucket.all(app: req.application).filter { $0.userId == userId }

        for bucket in buckets {
            try await BucketService.delete(
                req: req, bucketName: bucket.name, userId: userId, force: true)
        }

        // Delete each access key (also clears all 3 caches, including the secret-key one -
        // skipping that one would leave a deleted user's S3 credentials valid until restart)
        let accessKeys = try await AccessKey.findAll(app: req.application, userId: userId)

        for accessKey in accessKeys {
            try await AccessKeyService.delete(
                app: req.application, accessKey: accessKey.accessKey, id: accessKey.id)
        }

        // Delete the user - buckets and access keys are already fully torn down above.
        try await userToDelete.delete(app: req.application)

        return .noContent
    }

    @Sendable
    func getStorageStats(req: Request) async throws -> StorageStats {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let storageURL = URL(fileURLWithPath: BucketHandler.rootPath)

        // Get disk space info
        let (totalBytes, availableBytes) = DiskSpace.availableAndTotal(for: storageURL)

        let usedBytes = totalBytes - availableBytes

        // Calculate storage used by alarik (size of Storage/buckets directory)
        let alarikUsedBytes = Self.calculateDirectorySize(at: storageURL)

        // Count buckets and objects
        let bucketCount = await MetadataListingService.count(
            app: req.application, collection: MetadataCollections.buckets)
        let userCount = await MetadataListingService.count(
            app: req.application, collection: MetadataCollections.users)

        return StorageStats(
            totalBytes: totalBytes,
            availableBytes: availableBytes,
            usedBytes: usedBytes,
            alarikUsedBytes: alarikUsedBytes,
            bucketCount: bucketCount,
            userCount: userCount
        )
    }

    @Sendable
    func getSystemStats(req: Request) async throws -> SystemStats {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let metrics = await MetricsCollector.shared.snapshot()

        let accessKeyCount = await MetadataListingService.count(
            app: req.application, collection: MetadataCollections.accessKeys)
        let sharedLinkCount = await MetadataListingService.count(
            app: req.application, collection: MetadataCollections.sharedLinks)
        let oidcProviderCount = await MetadataListingService.count(
            app: req.application, collection: MetadataCollections.oidcProviders)

        let clusterNode = req.application.storage[ClusterConfigurationKey.self].map {
            ClusterNodeIdentityDTO(nodeId: $0.nodeId, address: $0.address)
        }

        return SystemStats(
            metrics: metrics,
            accessKeyCount: accessKeyCount,
            sharedLinkCount: sharedLinkCount,
            oidcProviderCount: oidcProviderCount,
            multipartUploadCount: Self.countMultipartUploads(),
            clusterNode: clusterNode
        )
    }

    /// Counts in-progress multipart uploads: one upload == one
    /// `Storage/multipart/{bucket}/{uploadId}/` directory. Two shallow directory listings,
    /// never a recursive walk.
    private static func countMultipartUploads() -> Int {
        let fileManager = FileManager.default
        guard
            let buckets = try? fileManager.contentsOfDirectory(
                atPath: MultipartFileHandler.rootPath)
        else { return 0 }

        var count = 0
        for bucket in buckets {
            let bucketPath = MultipartFileHandler.rootPath + bucket
            if let uploads = try? fileManager.contentsOfDirectory(atPath: bucketPath) {
                count += uploads.count
            }
        }
        return count
    }


    private static func calculateDirectorySize(at url: URL) -> Int64 {
        let fileManager = FileManager.default
        guard fileManager.fileExists(atPath: url.path) else {
            return 0
        }

        guard
            let enumerator = fileManager.enumerator(
                at: url,
                includingPropertiesForKeys: [.fileSizeKey],
                options: [.skipsHiddenFiles]
            )
        else {
            return 0
        }

        var totalSize: Int64 = 0
        while let fileURL = enumerator.nextObject() as? URL {
            if let resourceValues = try? fileURL.resourceValues(forKeys: [.fileSizeKey]),
                let fileSize = resourceValues.fileSize
            {
                totalSize += Int64(fileSize)
            }
        }
        return totalSize
    }
}
