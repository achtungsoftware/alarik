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

import Foundation
import Vapor

/// A public link to a single object, optionally time-limited. The row's own id is used
/// directly as the opaque, unguessable public token - no credential of any kind is exposed,
/// and revoking a link is just deleting (or letting expire) this row.
///
/// Backed by `MetadataStore`, not Fluent - `id` doubles as the primary key directly (already a
/// natural immutable identifier, unlike `User`'s mutable username).
final class SharedLink: @unchecked Sendable, Codable {
    let id: UUID
    var userId: UUID
    var bucketName: String
    var key: String

    /// When the link stops working, or nil for a link that never expires. Non-expiring links
    /// live until explicitly revoked - the hourly cleanup task's `expiresAt <= now` filter
    /// naturally never matches a nil.
    var expiresAt: Date?

    var createdAt: Date

    init(
        id: UUID = UUID(),
        userId: UUID,
        bucketName: String,
        key: String,
        expiresAt: Date?,
        createdAt: Date = Date()
    ) {
        self.id = id
        self.userId = userId
        self.bucketName = bucketName
        self.key = key
        self.expiresAt = expiresAt
        self.createdAt = createdAt
    }
}

// MARK: - MetadataStore access

extension SharedLink {
    static func find(app: Application, id: UUID) async throws -> SharedLink? {
        try await MetadataStore.get(
            SharedLink.self, app: app, collection: MetadataCollections.sharedLinks,
            id: id.uuidString)
    }

    static func all(app: Application) async throws -> [SharedLink] {
        await MetadataListingService.list(app: app, collection: MetadataCollections.sharedLinks)
            .compactMap { try? JSONDecoder().decode(SharedLink.self, from: $0.value) }
    }

    func save(app: Application) async throws {
        try await MetadataStore.put(
            app: app, collection: MetadataCollections.sharedLinks, id: id.uuidString, value: self)
    }

    func delete(app: Application) async throws {
        try await MetadataStore.delete(
            app: app, collection: MetadataCollections.sharedLinks, id: id.uuidString)
    }
}
