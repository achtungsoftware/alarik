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

struct InternalAdminController: RouteCollection {

    struct StorageStats: Content {
        let totalBytes: Int64
        let availableBytes: Int64
        let usedBytes: Int64
        let alarikUsedBytes: Int64
        let bucketCount: Int
        let userCount: Int
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
    }

    @Sendable
    func listUsers(req: Request) async throws -> Page<User.ResponseDTO> {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let fetchedAdminUser: User = try await User.find(sessionToken.userId, on: req.db)
        else {
            throw Abort(.unauthorized, reason: "User not found")
        }

        guard fetchedAdminUser.isAdmin else {
            throw Abort(.unauthorized, reason: "User not admin")
        }

        let user: Page<User> = try await User.query(on: req.db)
            .sort(\.$name, .descending)
            .paginate(for: req)

        return user.map { $0.toResponseDTO() }
    }

    @Sendable
    func createUser(req: Request) async throws -> User.ResponseDTO {
        try User.Create.validate(content: req)

        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let fetchedAdminUser: User = try await User.find(sessionToken.userId, on: req.db)
        else {
            throw Abort(.unauthorized, reason: "User not found")
        }

        guard fetchedAdminUser.isAdmin else {
            throw Abort(.unauthorized, reason: "User not admin")
        }

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

        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let fetchedAdminUser: User = try await User.find(sessionToken.userId, on: req.db)
        else {
            throw Abort(.unauthorized, reason: "User not found")
        }

        guard fetchedAdminUser.isAdmin else {
            throw Abort(.unauthorized, reason: "User not admin")
        }

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
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let fetchedAdminUser: User = try await User.find(sessionToken.userId, on: req.db)
        else {
            throw Abort(.unauthorized, reason: "User not found")
        }

        guard fetchedAdminUser.isAdmin else {
            throw Abort(.unauthorized, reason: "User not admin")
        }

        guard let userIdString = req.parameters.get("userId"),
            let userId = UUID(uuidString: userIdString)
        else {
            throw Abort(.badRequest, reason: "Invalid user ID")
        }

        guard let userToDelete = try await User.find(userId, on: req.db) else {
            throw Abort(.notFound, reason: "User not found")
        }

        // Prevent deleting yourself
        if userToDelete.id == fetchedAdminUser.id {
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
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let fetchedAdminUser: User = try await User.find(sessionToken.userId, on: req.db)
        else {
            throw Abort(.unauthorized, reason: "User not found")
        }

        guard fetchedAdminUser.isAdmin else {
            throw Abort(.unauthorized, reason: "User not admin")
        }

        let storageURL = URL(fileURLWithPath: BucketHandler.rootPath)

        let availableBytes: Int64
        let totalBytes: Int64

        do {
            let values = try storageURL.resourceValues(forKeys: [
                .volumeAvailableCapacityForImportantUsageKey,
                .volumeTotalCapacityKey,
            ])
            availableBytes = Int64(values.volumeAvailableCapacityForImportantUsage ?? 0)
            totalBytes = Int64(values.volumeTotalCapacity ?? 0)
        } catch {
            // Fallback: try the parent directory if storage path doesn't exist yet
            let parentURL = storageURL.deletingLastPathComponent()
            let values = try parentURL.resourceValues(forKeys: [
                .volumeAvailableCapacityForImportantUsageKey,
                .volumeTotalCapacityKey,
            ])
            availableBytes = Int64(values.volumeAvailableCapacityForImportantUsage ?? 0)
            totalBytes = Int64(values.volumeTotalCapacity ?? 0)
        }

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
