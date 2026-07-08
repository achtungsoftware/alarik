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
    }

    struct PolicyDTO: Content {
        let policy: String?
    }

    struct BucketStatsDTO: Content {
        let sizeBytes: Int64
        let objectCount: Int
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

        guard
            let bucket =
                try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .with(\.$user)
                .first()
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        try await BucketService.delete(
            on: req.db, bucketName: bucketName, userId: bucket.user.id!, force: true)

        return .noContent
    }

    /// Fetches any bucket by name (no ownership filter - the admin can manage any user's
    /// bucket) or throws the standard "Bucket not found" 404.
    private func fetchAnyBucket(req: Request, bucketName: String) async throws -> Bucket {
        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .first()
        else {
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
    func listBuckets(req: Request) async throws -> Page<Bucket> {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        return try await Bucket.query(on: req.db)
            .sort(\.$creationDate, .descending)
            .with(\.$user)
            .paginate(for: req)
    }

    @Sendable
    func listUsers(req: Request) async throws -> Page<User.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let user: Page<User> = try await User.query(on: req.db)
            .sort(\.$name, .descending)
            .paginate(for: req)

        return user.map { $0.toResponseDTO() }
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
            try await user.save(on: req.db)
        } catch {
            if let dbError = error as? any DatabaseError,
                dbError.isConstraintFailure
            {
                throw Abort(.conflict, reason: "Username already exists.")
            }
            throw error
        }

        return user.toResponseDTO()
    }

    @Sendable
    func editUser(req: Request) async throws -> User.ResponseDTO {
        try User.EditAdmin.validate(content: req)

        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let editUser: User.EditAdmin = try req.content.decode(User.EditAdmin.self)

        do {
            try await User.query(on: req.db)
                .filter(\.$id == editUser.id)
                .set(\.$name, to: editUser.name)
                .set(\.$username, to: editUser.username)
                .set(\.$isAdmin, to: editUser.isAdmin)
                .update()
        } catch {
            if let dbError = error as? any DatabaseError,
                dbError.isConstraintFailure
            {
                throw Abort(.conflict, reason: "Username already exists.")
            }
            throw error
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

        guard let userToDelete = try await User.find(userId, on: req.db) else {
            throw Abort(.notFound, reason: "User not found")
        }

        // Prevent deleting yourself
        if userToDelete.id == auth.userId {
            throw Abort(.forbidden, reason: "Cannot delete yourself")
        }

        // Delete all bucket folders from disk
        let buckets = try await Bucket.query(on: req.db)
            .filter(\.$user.$id == userId)
            .all()

        for bucket in buckets {
            try BucketHandler.forceDelete(name: bucket.name)
            await BucketVersioningCache.shared.removeBucket(bucket.name)
            CacheInvalidationService.notify(
                on: req.db, cache: "bucketVersioning", op: .remove, key: bucket.name)
        }

        // Delete each access key (also clears all 3 caches, including the secret-key one -
        // skipping that one would leave a deleted user's S3 credentials valid until restart)
        let accessKeys = try await AccessKey.query(on: req.db)
            .filter(\.$user.$id == userId)
            .all()

        for accessKey in accessKeys {
            try await AccessKeyService.delete(on: req.db, accessKey: accessKey.accessKey)
        }

        // Delete the user (buckets cascade delete in DB; access keys are already gone above)
        try await userToDelete.delete(on: req.db)

        return .noContent
    }

    @Sendable
    func getStorageStats(req: Request) async throws -> StorageStats {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let storageURL = URL(fileURLWithPath: BucketHandler.rootPath)

        // Get disk space info
        let (totalBytes, availableBytes) = Self.getDiskSpace(for: storageURL)

        let usedBytes = totalBytes - availableBytes

        // Calculate storage used by alarik (size of Storage/buckets directory)
        let alarikUsedBytes = Self.calculateDirectorySize(at: storageURL)

        // Count buckets and objects
        let bucketCount = try await Bucket.query(on: req.db).count()
        let userCount = try await User.query(on: req.db).count()

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

        let accessKeyCount = try await AccessKey.query(on: req.db).count()
        let sharedLinkCount = try await SharedLink.query(on: req.db).count()
        let oidcProviderCount = try await OIDCProvider.query(on: req.db).count()

        return SystemStats(
            metrics: metrics,
            accessKeyCount: accessKeyCount,
            sharedLinkCount: sharedLinkCount,
            oidcProviderCount: oidcProviderCount,
            multipartUploadCount: Self.countMultipartUploads()
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

    private static func getDiskSpace(for url: URL) -> (total: Int64, available: Int64) {
        let path =
            FileManager.default.fileExists(atPath: url.path)
            ? url.path
            : url.deletingLastPathComponent().path

        #if os(Linux)
            var stat = statvfs()
            guard statvfs(path, &stat) == 0 else {
                return (0, 0)
            }
            let blockSize = UInt64(stat.f_frsize)
            let totalBytes = Int64(UInt64(stat.f_blocks) * blockSize)
            let availableBytes = Int64(UInt64(stat.f_bavail) * blockSize)
            return (totalBytes, availableBytes)
        #else
            do {
                let values = try URL(fileURLWithPath: path).resourceValues(forKeys: [
                    .volumeAvailableCapacityForImportantUsageKey,
                    .volumeTotalCapacityKey,
                ])
                let total = Int64(values.volumeTotalCapacity ?? 0)
                let available = Int64(values.volumeAvailableCapacityForImportantUsage ?? 0)
                return (total, available)
            } catch {
                return (0, 0)
            }
        #endif
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
