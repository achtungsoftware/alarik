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

struct InternalUserController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("users").post(use: self.createUser)
        routes.grouped("users").grouped("login")
            .grouped(User.credentialsAuthenticator())
            .post(use: login)

        routes.grouped("users").grouped("auth")
            .grouped(SessionToken.authenticator())
            .post(use: auth)

        routes.grouped("users").grouped("accessKeys")
            .grouped(SessionToken.authenticator())
            .get(use: listAccessKeys)

        routes.grouped("users")
            .grouped(SessionToken.authenticator())
            .put(use: editUser)

        routes.grouped("users").grouped("accessKeys")
            .grouped(SessionToken.authenticator())
            .post(use: createAccessKey)

        routes.grouped("users").grouped("accessKeys").grouped(":accessKeyId")
            .grouped(SessionToken.authenticator())
            .delete(use: deleteAccessKey)

        routes.grouped("users")
            .grouped(SessionToken.authenticator())
            .delete(use: deleteUser)
    }

    @Sendable
    func editUser(req: Request) async throws -> User.ResponseDTO {
        try User.Edit.validate(content: req)

        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let user = try await User.find(sessionToken.userId, on: req.db)
        else {
            throw Abort(.notFound, reason: "User not found")
        }

        let editUser: User.Edit = try req.content.decode(User.Edit.self)

        // Handle password change if requested
        if let currentPassword = editUser.currentPassword,
            let newPassword = editUser.newPassword,
            !currentPassword.isEmpty,
            !newPassword.isEmpty
        {
            guard try user.verify(password: currentPassword) else {
                throw Abort(.unauthorized, reason: "Current password is incorrect")
            }

            let newPasswordHash = try Bcrypt.hash(newPassword)
            try await User.query(on: req.db)
                .filter(\.$id == sessionToken.userId)
                .set(\.$passwordHash, to: newPasswordHash)
                .update()
        }

        do {
            try await User.query(on: req.db)
                .filter(\.$id == sessionToken.userId)
                .set(\.$name, to: editUser.name)
                .set(\.$username, to: editUser.username)
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
    func createAccessKey(req: Request) async throws -> AccessKey.ResponseDTO {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        try AccessKey.Create.validate(content: req)

        let create: AccessKey.Create = try req.content.decode(AccessKey.Create.self)
        let accessKey: AccessKey = AccessKey(
            userId: sessionToken.userId, accessKey: create.accessKey, secretKey: create.secretKey,
            expirationDate: create.expirationDate
        )

        do {
            try await accessKey.save(on: req.db)
        } catch {
            if let dbError = error as? any DatabaseError,
                dbError.isConstraintFailure
            {
                throw Abort(.conflict, reason: "Access key already exists.")
            }
            throw error
        }

        // Add to caches
        await AccessKeySecretKeyMapCache.shared.add(
            accessKey: create.accessKey,
            secretKey: create.secretKey
        )
        await AccessKeyUserMapCache.shared.add(
            accessKey: create.accessKey,
            userId: sessionToken.userId
        )
        // Note: AccessKeyBucketMapCache is updated when buckets are created,
        // not when access keys are created (new access keys have no buckets yet)

        return accessKey.toResponseDTO()
    }

    @Sendable
    func deleteAccessKey(req: Request) async throws -> HTTPStatus {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let accessKeyId = req.parameters.get("accessKeyId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid access key ID.")
        }

        guard
            let accessKey = try await AccessKey.query(on: req.db)
                .filter(\.$id == accessKeyId)
                .filter(\.$user.$id == sessionToken.userId)
                .first()
        else {
            throw Abort(.notFound, reason: "Access key not found.")
        }

        try await AccessKeyService.delete(on: req.db, accessKey: accessKey.accessKey)

        return .noContent
    }

    @Sendable
    func listAccessKeys(req: Request) async throws -> Page<AccessKey.ResponseDTO> {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        let page: Page<AccessKey> = try await AccessKey.query(on: req.db)
            .filter(\.$user.$id == sessionToken.userId)
            .sort(\.$createdAt, .descending)
            .paginate(for: req)

        return page.map { $0.toResponseDTO() }
    }

    @Sendable
    func auth(req: Request) async throws -> User.ResponseDTO {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let user: User = try await User.find(sessionToken.userId, on: req.db) else {
            throw Abort(.unauthorized, reason: "User not found")
        }

        return user.toResponseDTO()
    }

    @Sendable
    func login(req: Request) async throws -> ClientTokenResponse {
        let user: User = try req.auth.require(User.self)
        let payload: SessionToken = try SessionToken(with: user)
        return ClientTokenResponse(token: try await req.jwt.sign(payload))
    }

    @Sendable
    func deleteUser(req: Request) async throws -> HTTPStatus {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let userToDelete = try await User.find(sessionToken.userId, on: req.db) else {
            throw Abort(.notFound, reason: "User not found")
        }

        // Delete all bucket folders from disk
        let buckets = try await Bucket.query(on: req.db)
            .filter(\.$user.$id == sessionToken.userId)
            .all()

        for bucket in buckets {
            try BucketHandler.forceDelete(name: bucket.name)
            await BucketVersioningCache.shared.removeBucket(bucket.name)
        }

        // Remove from all caches
        let accessKeys = try await AccessKey.query(on: req.db)
            .filter(\.$user.$id == sessionToken.userId)
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
    func createUser(req: Request) async throws -> User.ResponseDTO {
        #if DEBUG
        #else
            if let allowAccountCreation = Environment.get("ALLOW_ACCOUNT_CREATION") {
                if allowAccountCreation != "true" {

                    throw Abort(
                        .unauthorized,
                        reason: "User creation is disabled in production.")
                }
            } else {
                throw Abort(
                    .unauthorized,
                    reason: "User creation is disabled in production.")
            }
        #endif

        try User.FormCreate.validate(content: req)

        let create: User.FormCreate = try req.content.decode(User.FormCreate.self)
        let user: User = try User(
            name: create.name,
            username: create.username,
            passwordHash: Bcrypt.hash(create.password),
            isAdmin: false
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
}
