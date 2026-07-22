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

/// Backed by `MetadataStore`, not Fluent - keyed by the access key VALUE itself (`accessKey`),
/// not `id`. It's already a natural unique identifier, and the hot-path lookup (SigV4 auth) is
/// always by that value, never by id - so unlike `User`, no secondary index is needed at all.
/// `id` is kept only for API compatibility (the console addresses a key by id for deletion) - see
/// `find(app:id:userId:)`, which lists this small per-user collection and filters in memory.
final class AccessKey: @unchecked Sendable, MetadataRecord {
    let id: UUID
    var userId: UUID
    var accessKey: String
    var secretKey: String
    var createdAt: Date
    var expirationDate: Date?

    init(
        id: UUID = UUID(),
        userId: UUID,
        accessKey: String,
        secretKey: String,
        createdAt: Date = Date(),
        expirationDate: Date? = nil
    ) {
        self.id = id
        self.userId = userId
        self.accessKey = accessKey
        self.secretKey = secretKey
        self.createdAt = createdAt
        self.expirationDate = expirationDate
    }
}

// MARK: - MetadataStore access

extension AccessKey {
    static var metadataCollection: String { MetadataCollections.accessKeys }
    var metadataId: String { accessKey }

    static func find(app: Application, accessKey: String) async throws -> AccessKey? {
        try await find(app: app, key: accessKey)
    }

    /// Every access key belonging to `userId` - a full-collection listing filtered in memory.
    /// Access keys are a shallow, low-churn collection (see `MetadataListingService`'s doc
    /// comment), so this is only ever called from admin/console paths, never per-S3-request.
    static func findAll(app: Application, userId: UUID) async throws -> [AccessKey] {
        await all(app: app).filter { $0.userId == userId }
    }

    /// Secondary index record: `access-keys-by-id/<uuid>` -> the key's value and owner. Carries
    /// `userId` so a delete-by-id can check ownership without reading the primary record at all -
    /// revocation must stay possible even while the primary is temporarily unreconstructable.
    struct IdPointer: Codable, Sendable {
        let accessKey: String
        let userId: UUID
    }

    /// Resolves the by-id pointer, self-healing a dangling one (primary deleted, or a crash
    /// between the two writes in `create`) - mirrors `User.findByUsername`.
    static func findIdPointer(app: Application, id: UUID) async throws -> IdPointer? {
        guard
            let pointer = try await MetadataStore.get(
                IdPointer.self, app: app, collection: MetadataCollections.accessKeysById,
                id: id.uuidString)
        else { return nil }

        guard
            let key = try await MetadataStore.get(
                AccessKey.self, app: app, collection: MetadataCollections.accessKeys,
                id: pointer.accessKey), key.id == id
        else {
            try? await MetadataStore.delete(
                app: app, collection: MetadataCollections.accessKeysById, id: id.uuidString)
            return nil
        }
        return pointer
    }

    /// Creates the key, failing if `accessKey`'s value is already taken by another key.
    /// Overrides the `MetadataRecord` default to keep the by-id pointer in step with the primary.
    func create(app: Application) async throws -> Bool {
        let claimed = try await MetadataStore.putIfAbsent(
            app: app, collection: MetadataCollections.accessKeys, id: accessKey, value: self)
        guard claimed else { return false }
        do {
            try await MetadataStore.put(
                app: app, collection: MetadataCollections.accessKeysById, id: id.uuidString,
                value: IdPointer(accessKey: accessKey, userId: userId))
        } catch {
            // Keep the pair consistent: without the pointer the key would be un-revocable
            // by id whenever a listing hiccups, which defeats the pointer's purpose.
            try? await MetadataStore.delete(
                app: app, collection: MetadataCollections.accessKeys, id: accessKey)
            throw error
        }
        return true
    }

    /// Overrides the `MetadataRecord` default - the by-id pointer has to go too.
    func delete(app: Application) async throws {
        try await AccessKeyService.delete(app: app, accessKey: accessKey, id: id)
    }
}

extension AccessKey {
    struct Create: Content {
        var accessKey: String
        var secretKey: String
        var expirationDate: Date?
    }

    struct ResponseDTO: Content {
        var id: UUID?
        var accessKey: String?
        var createdAt: Date?
        var expirationDate: Date??
    }

    func toResponseDTO() -> AccessKey.ResponseDTO {
        .init(
            id: self.id,
            accessKey: self.accessKey,
            createdAt: self.createdAt,
            expirationDate: self.expirationDate
        )
    }
}

extension AccessKey.Create: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("accessKey", as: String.self, is: !.empty)
        validations.add("secretKey", as: String.self, is: !.empty)
    }
}
