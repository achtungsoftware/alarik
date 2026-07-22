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

/// `MetadataStore`/`MetadataListingService` collection names for Alarik's own control-plane
/// records - mirrors `OutboxCollections`' role for the outbox tables.
enum MetadataCollections {
    static let users = "users"

    /// Secondary index `users-by-username/<username>` -> `{"userId": "<uuid>"}`, since a user's
    /// primary key is its immutable id but usernames must stay unique and are editable - see
    /// `User.findByUsername(app:username:)`.
    static let usersByUsername = "users-by-username"

    /// Keyed by the access key value itself (not a UUID) - it's already a natural unique
    /// identifier, and every hot-path lookup (SigV4 auth) is by that value, never by id.
    static let accessKeys = "access-keys"

    /// Secondary index `access-keys-by-id/<uuid>` -> `{accessKey, userId}`. The console
    /// addresses a key by its UUID for revocation; resolving that through a cluster-wide
    /// listing makes revocation only as available as the *least* available peer (one busy
    /// node's timeout turns "revoke" into a wrong 404). This pointer makes it a direct store
    /// read instead - same pattern as `usersByUsername`.
    static let accessKeysById = "access-keys-by-id"

    static let sharedLinks = "shared-links"
    static let buckets = "buckets"
    static let clusterNodes = "cluster-nodes"
    static let oidcProviders = "oidc-providers"

    /// Keyed by the OIDC `state` value itself - single-use by construction via
    /// `MetadataStore.consumeIfPresent`.
    static let oidcStates = "oidc-states"

    /// Every collection, for maintenance passes that must cover the whole store
    /// (`MetadataMaintenance`'s tombstone GC and migration sweep). A new collection added above
    /// and forgotten here is simply never swept, so keep the two together.
    static let all: [String] = [
        users, usersByUsername, accessKeys, accessKeysById, sharedLinks, buckets, clusterNodes,
        oidcProviders, oidcStates,
    ]

    /// Collections whose deletes remove the record outright instead of leaving a tombstone.
    ///
    /// Tombstones exist so a replica that was offline during a delete can't resurrect the record
    /// on return. Neither collection here has that exposure, and both would pay for it:
    /// - `clusterNodes` is self-coordinated at k=1/m=0 and rewritten by every heartbeat, so a
    ///   stale copy is corrected within one beat anyway - and it has no delete path at all.
    /// - `oidcStates` entries are single-use (`consumeIfPresent`) and TTL-swept, so they churn
    ///   constantly and a resurrected one is inert: it is still expired, and replay is already
    ///   prevented by the consume. Tombstoning them would accumulate garbage for no gain.
    static let tombstoneExempt: Set<String> = [clusterNodes, oidcStates]
}
