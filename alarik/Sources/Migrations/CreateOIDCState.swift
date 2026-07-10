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

/// Backs `OIDCStateCache`'s Postgres-mode storage: on a single node the in-flight OIDC login
/// state (state/nonce/PKCE verifier) can live in memory, but behind a load balancer the login
/// redirect and its callback can land on two different nodes, so it has to live somewhere every
/// node can reach - this table, only actually used when `DATABASE_URL` is set. Created
/// unconditionally on both backends (portable Fluent DSL), harmlessly unused on SQLite.
struct CreateOIDCState: AsyncMigration {
    func prepare(on database: any Database) async throws {
        try await database.schema("oidc_states")
            .id()
            .field("state", .string, .required)
            .field(
                "provider_id", .uuid, .required,
                .references("oidc_providers", "id", onDelete: .cascade))
            .field("nonce", .string, .required)
            .field("code_verifier", .string, .required)
            .field("created_at", .datetime, .required)
            .unique(on: "state")
            .create()
    }

    func revert(on database: any Database) async throws {
        try await database.schema("oidc_states").delete()
    }
}
