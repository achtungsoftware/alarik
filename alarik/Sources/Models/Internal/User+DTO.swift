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

import struct Foundation.UUID

/// Backed by `MetadataStore`, not Fluent - primary record at `users/<id>`. Usernames are unique
/// but editable, unlike the immutable id, so uniqueness is enforced through a separate pointer
/// object (`users/by-username/<username>` -> `{"userId": "<uuid>"}`) rather than the primary key
/// itself - see `findByUsername`/`create`/`rename` below.
final class User: @unchecked Sendable, MetadataRecord, Authenticatable {
    let id: UUID
    var name: String
    var username: String
    var passwordHash: String
    var isAdmin: Bool

    // Subject values are only unique within a single provider's namespace, not globally - so
    // matching/linking a user to an OIDC identity always keys on this pair together, never on
    // oidcSubject alone.
    var oidcSubject: String?
    var oidcProviderId: UUID?

    init(
        id: UUID = UUID(), name: String, username: String, passwordHash: String, isAdmin: Bool
    ) {
        self.id = id
        self.name = name
        self.username = username
        self.passwordHash = passwordHash
        self.isAdmin = isAdmin
        self.oidcSubject = nil
        self.oidcProviderId = nil
    }

    func toResponseDTO() -> User.ResponseDTO {
        .init(
            id: self.id,
            name: self.name,
            username: self.username,
            isAdmin: self.isAdmin,
        )
    }

    func verify(password: String) throws -> Bool {
        try Bcrypt.verify(password, created: self.passwordHash)
    }
}

// MARK: - MetadataStore access

extension User {
    struct UsernamePointer: Codable {
        let userId: UUID
    }

    enum UserError: Error {
        /// Another user already holds this username - `putIfAbsent` on the secondary index
        /// failed closed.
        case usernameTaken
    }

    static var metadataCollection: String { MetadataCollections.users }
    var metadataId: String { id.uuidString }

    static func find(app: Application, id: UUID) async throws -> User? {
        try await find(app: app, key: id.uuidString)
    }

    /// Resolves the username pointer, then the primary record - self-healing if either half is
    /// stale (target deleted, or the primary record has since been renamed away from this
    /// username, e.g. a crash between steps in `rename` below).
    static func findByUsername(app: Application, username: String) async throws -> User? {
        guard
            let pointer = try await MetadataStore.get(
                UsernamePointer.self, app: app, collection: MetadataCollections.usersByUsername,
                id: username)
        else { return nil }

        guard let user = try await find(app: app, id: pointer.userId), user.username == username
        else {
            try? await MetadataStore.delete(
                app: app, collection: MetadataCollections.usersByUsername, id: username)
            return nil
        }
        return user
    }

    /// Creates a brand-new user, atomically claiming `username`. Throws `UserError.usernameTaken`
    /// if another user already holds it. Overrides the `MetadataRecord` default, which knows
    /// nothing about the username pointer.
    func create(app: Application) async throws {
        let claimed = try await MetadataStore.putIfAbsent(
            app: app, collection: MetadataCollections.usersByUsername, id: username,
            value: UsernamePointer(userId: id))
        guard claimed else { throw UserError.usernameTaken }

        do {
            try await MetadataStore.put(
                app: app, collection: MetadataCollections.users, id: id.uuidString, value: self)
        } catch {
            try? await MetadataStore.delete(
                app: app, collection: MetadataCollections.usersByUsername, id: username)
            throw error
        }
    }

    /// Retargets the username pointer before updating the primary record, in that order, so a
    /// crash mid-way leaves at worst a harmless stale pointer (self-healed by `findByUsername`)
    /// rather than a primary record no pointer resolves to. No-op (falls through to a plain
    /// `save`) when `username` is unchanged.
    func rename(app: Application, from previousUsername: String) async throws {
        guard previousUsername != username else {
            try await save(app: app)
            return
        }
        let claimed = try await MetadataStore.putIfAbsent(
            app: app, collection: MetadataCollections.usersByUsername, id: username,
            value: UsernamePointer(userId: id))
        guard claimed else { throw UserError.usernameTaken }

        try await MetadataStore.put(
            app: app, collection: MetadataCollections.users, id: id.uuidString, value: self)
        try? await MetadataStore.delete(
            app: app, collection: MetadataCollections.usersByUsername, id: previousUsername)
    }

    /// Overrides the `MetadataRecord` default - the username pointer has to go too.
    func delete(app: Application) async throws {
        try await MetadataStore.delete(
            app: app, collection: MetadataCollections.users, id: id.uuidString)
        try? await MetadataStore.delete(
            app: app, collection: MetadataCollections.usersByUsername, id: username)
    }
}

extension User {
    struct Edit: Content {
        var name: String
        var username: String
        var currentPassword: String?
        var newPassword: String?

        func toUserResponseDTO() -> User.ResponseDTO {
            .init(
                name: self.name,
                username: self.username,
            )
        }
    }

    struct EditAdmin: Content {
        var id: UUID
        var name: String
        var username: String
        var isAdmin: Bool

        func toUserResponseDTO() -> User.ResponseDTO {
            .init(
                id: self.id,
                name: self.name,
                username: self.username,
                isAdmin: self.isAdmin,
            )
        }
    }

    struct FormCreate: Content {
        var name: String
        var username: String
        var password: String
    }

    struct Create: Content {
        var name: String
        var username: String
        var password: String
        var isAdmin: Bool
    }

    struct ResponseDTO: Content {
        var id: UUID?
        var name: String?
        var username: String?
        var isAdmin: Bool?
    }
}

extension User.Create: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("name", as: String.self, is: !.empty)
        validations.add("username", as: String.self, is: !.empty)
        validations.add("password", as: String.self, is: !.empty)
    }
}

extension User.FormCreate: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("name", as: String.self, is: !.empty)
        validations.add("username", as: String.self, is: !.empty)
        validations.add("password", as: String.self, is: !.empty)
    }
}

extension User.EditAdmin: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("id", as: UUID.self)
        validations.add("name", as: String.self, is: !.empty)
        validations.add("username", as: String.self, is: !.empty)
        validations.add("isAdmin", as: Bool.self)
    }
}

extension User.Edit: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("name", as: String.self, is: !.empty)
        validations.add("username", as: String.self, is: !.empty)
    }
}
