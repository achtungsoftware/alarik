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

    struct PolicyDTO: Content {
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
            .get("buckets", use: self.listBuckets)

        routes.grouped("admin").grouped("buckets").grouped(":bucketName").delete(
            use: self.deleteBucket)

        routes.grouped("admin").grouped("buckets").grouped(":bucketName").grouped("policy").get(
            use: self.getBucketPolicy)
        routes.grouped("admin").grouped("buckets").grouped(":bucketName").grouped("policy").put(
            use: self.setBucketPolicy)
        routes.grouped("admin").grouped("buckets").grouped(":bucketName").grouped("policy").delete(
            use: self.deleteBucketPolicy)
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

    @Sendable
    func getBucketPolicy(req: Request) async throws -> PolicyDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .first()
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        return PolicyDTO(policy: bucket.policy)
    }

    @Sendable
    func setBucketPolicy(req: Request) async throws -> PolicyDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(PolicyDTO.self)

        guard let rawJSON = input.policy else {
            throw Abort(.badRequest, reason: "Missing policy")
        }

        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .first()
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

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

    @Sendable
    func deleteBucketPolicy(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .first()
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        bucket.policy = nil
        try await bucket.save(on: req.db)

        await BucketPolicyCache.shared.removePolicy(for: bucketName)

        return .noContent
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
        }

        // Remove from all caches
        let accessKeys = try await AccessKey.query(on: req.db)
            .filter(\.$user.$id == userId)
            .all()

        for accessKey in accessKeys {
            await AccessKeyUserMapCache.shared.remove(accessKey: accessKey.accessKey)
            await AccessKeyBucketMapCache.shared.removeAccessKey(accessKey.accessKey)
        }

        // Delete the user (buckets and access keys cascade delete in DB)
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
