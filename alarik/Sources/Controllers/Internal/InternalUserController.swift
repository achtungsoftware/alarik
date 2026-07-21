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

struct InternalUserController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("users").post(use: self.createUser)
        routes.grouped("users").grouped("login")
            .post(use: login)

        routes.grouped("users").grouped("auth")
            .grouped(InternalAuthenticator())
            .post(use: auth)

        routes.grouped("users").grouped("accessKeys")
            .grouped(InternalAuthenticator())
            .get(use: listAccessKeys)

        routes.grouped("users")
            .grouped(InternalAuthenticator())
            .put(use: editUser)

        routes.grouped("users").grouped("accessKeys")
            .grouped(InternalAuthenticator())
            .post(use: createAccessKey)

        routes.grouped("users").grouped("accessKeys").grouped(":accessKeyId")
            .grouped(InternalAuthenticator())
            .delete(use: deleteAccessKey)

        routes.grouped("users")
            .grouped(InternalAuthenticator())
            .delete(use: deleteUser)
    }

    @Sendable
    func editUser(req: Request) async throws -> User.ResponseDTO {
        try User.Edit.validate(content: req)

        let auth = try req.auth.require(AuthenticatedUser.self)

        let editUser: User.Edit = try req.content.decode(User.Edit.self)

        // Handle password change if requested
        if let currentPassword = editUser.currentPassword,
            let newPassword = editUser.newPassword,
            !currentPassword.isEmpty,
            !newPassword.isEmpty
        {
            guard try auth.user.verify(password: currentPassword) else {
                throw Abort(.unauthorized, reason: "Current password is incorrect")
            }
            auth.user.passwordHash = try Bcrypt.hash(newPassword)
        }

        let previousUsername = auth.user.username
        auth.user.name = editUser.name
        auth.user.username = editUser.username

        do {
            try await auth.user.rename(app: req.application, from: previousUsername)
        } catch is User.UserError {
            throw Abort(.conflict, reason: "Username already exists.")
        }

        return editUser.toUserResponseDTO()
    }

    @Sendable
    func createAccessKey(req: Request) async throws -> AccessKey.ResponseDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        try AccessKey.Create.validate(content: req)

        let create: AccessKey.Create = try req.content.decode(AccessKey.Create.self)
        let accessKey: AccessKey = AccessKey(
            userId: auth.userId, accessKey: create.accessKey, secretKey: create.secretKey,
            expirationDate: create.expirationDate
        )

        guard try await accessKey.create(app: req.application) else {
            throw Abort(.conflict, reason: "Access key already exists.")
        }

        // Add to caches
        await AccessKeySecretKeyMapCache.shared.add(
            accessKey: create.accessKey,
            secretKey: create.secretKey
        )
        CacheInvalidationService.notify(
            app: req.application, cache: "accessKeySecret", op: .upsert, key: create.accessKey)
        await AccessKeyUserMapCache.shared.add(
            accessKey: create.accessKey,
            userId: auth.userId
        )
        CacheInvalidationService.notify(
            app: req.application, cache: "accessKeyUser", op: .upsert, key: create.accessKey)

        // Map the new access key to all existing buckets for this user
        let userBuckets = try await Bucket.all(app: req.application).filter {
            $0.userId == auth.userId
        }

        for bucket in userBuckets {
            await AccessKeyBucketMapCache.shared.add(
                accessKey: create.accessKey,
                bucketName: bucket.name
            )
        }
        // One notify for the whole key, not per bucket - accessKeyBucket/upsert reloads this
        // key's entire bucket set from the DB in one shot on the receiving end.
        CacheInvalidationService.notify(
            app: req.application, cache: "accessKeyBucket", op: .upsert, key: create.accessKey)

        return accessKey.toResponseDTO()
    }

    @Sendable
    func deleteAccessKey(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let accessKeyId = req.parameters.get("accessKeyId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid access key ID.")
        }

        // Pointer first: a direct store read whose availability doesn't depend on every peer
        // answering a cluster-wide listing in time. Revocation is the one operation that must
        // not degrade to a wrong 404 just because a listing was momentarily partial.
        if let pointer = try await AccessKey.findIdPointer(app: req.application, id: accessKeyId) {
            guard pointer.userId == auth.userId else {
                throw Abort(.notFound, reason: "Access key not found.")
            }
            try await AccessKeyService.delete(
                app: req.application, accessKey: pointer.accessKey, id: accessKeyId)
            return .noContent
        }

        // Legacy fallback: keys created before the by-id pointer existed have no pointer record,
        // so resolve them through the listing as before.
        guard
            let accessKey = try await AccessKey.findAll(app: req.application, userId: auth.userId)
                .first(where: { $0.id == accessKeyId })
        else {
            throw Abort(.notFound, reason: "Access key not found.")
        }

        try await AccessKeyService.delete(
            app: req.application, accessKey: accessKey.accessKey, id: accessKey.id)

        return .noContent
    }

    @Sendable
    func listAccessKeys(req: Request) async throws -> Page<AccessKey.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)

        let keys = try await AccessKey.findAll(app: req.application, userId: auth.userId)
            .sorted { $0.createdAt > $1.createdAt }

        return try keys.paginated(for: req).map { $0.toResponseDTO() }
    }

    @Sendable
    func auth(req: Request) async throws -> User.ResponseDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)
        return auth.user.toResponseDTO()
    }

    struct LoginCredentials: Content {
        let username: String
        let password: String
    }

    @Sendable
    func login(req: Request) async throws -> ClientTokenResponse {
        guard let credentials = try? req.content.decode(LoginCredentials.self) else {
            throw Abort(.unauthorized)
        }
        guard
            let user = try await User.findByUsername(
                app: req.application, username: credentials.username),
            try user.verify(password: credentials.password)
        else {
            throw Abort(.unauthorized)
        }
        let payload: SessionToken = try SessionToken(with: user)
        return ClientTokenResponse(token: try await req.jwt.sign(payload))
    }

    @Sendable
    func deleteUser(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        // Force-delete every bucket the user owns through BucketService, not a raw local-disk
        // removal - see InternalAdminController.deleteUser for why a raw `forceDelete` leaves
        // every other cluster node's physical copies orphaned, ready to silently resurface if a
        // bucket with the same name is ever created again.
        let buckets = try await Bucket.all(app: req.application).filter {
            $0.userId == auth.userId
        }

        for bucket in buckets {
            try await BucketService.delete(
                req: req, bucketName: bucket.name, userId: auth.userId, force: true)
        }

        // Delete each access key (also clears all 3 caches, including the secret-key one -
        // skipping that one would leave a deleted user's S3 credentials valid until restart)
        let accessKeys = try await AccessKey.findAll(app: req.application, userId: auth.userId)

        for accessKey in accessKeys {
            try await AccessKeyService.delete(
                app: req.application, accessKey: accessKey.accessKey, id: accessKey.id)
        }

        // Delete the user - buckets and access keys are already fully torn down above.
        try await auth.user.delete(app: req.application)

        return .noContent
    }

    @Sendable
    func createUser(req: Request) async throws -> User.ResponseDTO {
        #if DEBUG
        #else
            if let allowAccountCreation = Environment.sanitizedGet("ALLOW_ACCOUNT_CREATION") {
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
            try await user.create(app: req.application)
        } catch is User.UserError {
            throw Abort(.conflict, reason: "Username already exists.")
        }

        return user.toResponseDTO()
    }
}
