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

import struct Foundation.UUID

/// Holds the per-attempt `providerId`/`nonce`/PKCE `code_verifier` for an in-flight OIDC login
/// between the `/login/:providerId` redirect and the shared `/callback` round-trip, keyed by the
/// `state` value round-tripped through the IdP. There's no server-side session to store this in
/// (the server is otherwise stateless JWT auth), so it lives here instead of a cookie or DB
/// table - it's short-lived and only needed for the duration of a single login attempt. The
/// callback route is a single fixed URL shared by every provider (so admins only ever register
/// one redirect URI with their IdP); `providerId` is how the callback knows which provider an
/// incoming `code` belongs to.
final actor OIDCStateCache {

    public static let shared = OIDCStateCache()

    struct Entry {
        let providerId: UUID
        let nonce: String
        let codeVerifier: String
        let createdAt: Date
    }

    private var map: [String: Entry] = [:]

    func store(state: String, providerId: UUID, nonce: String, codeVerifier: String) {
        map[state] = Entry(
            providerId: providerId, nonce: nonce, codeVerifier: codeVerifier, createdAt: Date())
    }

    /// Single-use: removes the entry on read so a `state` value can never be replayed.
    func consume(state: String) -> Entry? {
        map.removeValue(forKey: state)
    }

    func removeExpired(olderThan ttl: TimeInterval) {
        guard !map.isEmpty else { return }
        let cutoff = Date().addingTimeInterval(-ttl)
        map = map.filter { $0.value.createdAt > cutoff }
    }
}
