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

/// Holds the per-attempt `providerId`/`nonce`/PKCE `code_verifier` for an in-flight OIDC login
/// between the `/login/:providerId` redirect and the shared `/callback` round-trip, keyed by the
/// `state` value round-tripped through the IdP. The callback route is a single fixed URL shared
/// by every provider (so admins only ever register one redirect URI with their IdP); `providerId`
/// is how the callback knows which provider an incoming `code` belongs to.
///
/// Backed by `MetadataStore`, giving `consumeIfPresent` an atomic single-use guarantee uniform
/// across standalone and clustered deployments: behind a load balancer, the login redirect can
/// land on node A while the IdP's callback lands on node B - `MetadataStore` resolves that by
/// fetching from whoever holds the shards, not by relying on both requests landing on one node.
enum OIDCStateCache {
    struct Entry: Codable {
        let providerId: UUID
        let nonce: String
        let codeVerifier: String
        let createdAt: Date
    }

    static func store(
        app: Application, state: String, providerId: UUID, nonce: String, codeVerifier: String
    ) async throws {
        try await MetadataStore.put(
            app: app, collection: MetadataCollections.oidcStates, id: state,
            value: Entry(
                providerId: providerId, nonce: nonce, codeVerifier: codeVerifier,
                createdAt: Date()))
    }

    /// Single-use: removes the entry on read so a `state` value can never be replayed.
    static func consume(app: Application, state: String) async throws -> Entry? {
        try await MetadataStore.consumeIfPresent(
            Entry.self, app: app, collection: MetadataCollections.oidcStates, id: state)
    }

    static func removeExpired(app: Application, olderThan ttl: TimeInterval) async throws {
        let cutoff = Date().addingTimeInterval(-ttl)
        let entries = await MetadataListingService.list(
            app: app, collection: MetadataCollections.oidcStates)
        for entry in entries {
            guard let decoded = try? JSONDecoder().decode(Entry.self, from: entry.value),
                decoded.createdAt < cutoff
            else { continue }
            try? await MetadataStore.delete(
                app: app, collection: MetadataCollections.oidcStates, id: entry.id)
        }
    }
}
