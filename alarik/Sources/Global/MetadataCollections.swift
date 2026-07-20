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

    /// Secondary index `users/by-username/<username>` -> `{"userId": "<uuid>"}`, since a user's
    /// primary key is its immutable id but usernames must stay unique and are editable - see
    /// `User.findByUsername(app:username:)`.
    static let usersByUsername = "users-by-username"

    /// Keyed by the access key value itself (not a UUID) - it's already a natural unique
    /// identifier, and every hot-path lookup (SigV4 auth) is by that value, never by id.
    static let accessKeys = "access-keys"

    static let sharedLinks = "shared-links"
    static let buckets = "buckets"
    static let clusterNodes = "cluster-nodes"
    static let oidcProviders = "oidc-providers"

    /// Keyed by the OIDC `state` value itself - single-use by construction via
    /// `MetadataStore.consumeIfPresent`.
    static let oidcStates = "oidc-states"
}
