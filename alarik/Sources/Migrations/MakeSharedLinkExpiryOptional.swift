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
import SQLKit

/// Drops the NOT NULL constraint from `shared_links.expires_at` so a NULL can mean "never
/// expires". SQLite has no `ALTER COLUMN`, so this is the standard rebuild dance: rename the
/// old table aside, create the new shape, copy every row, drop the old table. The column list
/// is spelled out explicitly on both sides of the copy - relying on positional `SELECT *`
/// would silently corrupt data if the two tables' column orders ever diverged.
struct MakeSharedLinkExpiryOptional: AsyncMigration {
    private static let columns = "id, user_id, bucket_name, key, expires_at, created_at"

    func prepare(on database: any Database) async throws {
        guard let sql = database as? any SQLDatabase else { return }

        try await sql.raw("ALTER TABLE shared_links RENAME TO shared_links_old").run()

        try await database.schema("shared_links")
            .id()
            .field("user_id", .uuid, .required, .references("users", "id", onDelete: .cascade))
            .field("bucket_name", .string, .required)
            .field("key", .string, .required)
            .field("expires_at", .datetime)
            .field("created_at", .datetime, .required)
            .create()

        try await sql.raw(
            "INSERT INTO shared_links (\(unsafeRaw: Self.columns)) SELECT \(unsafeRaw: Self.columns) FROM shared_links_old"
        ).run()
        try await sql.raw("DROP TABLE shared_links_old").run()
    }

    func revert(on database: any Database) async throws {
        guard let sql = database as? any SQLDatabase else { return }

        try await sql.raw("ALTER TABLE shared_links RENAME TO shared_links_old").run()

        try await database.schema("shared_links")
            .id()
            .field("user_id", .uuid, .required, .references("users", "id", onDelete: .cascade))
            .field("bucket_name", .string, .required)
            .field("key", .string, .required)
            .field("expires_at", .datetime, .required)
            .field("created_at", .datetime, .required)
            .create()

        // Never-expiring rows can't survive the reinstated NOT NULL - dropping them mirrors
        // what the old behavior would have done to them anyway (no such links could exist).
        try await sql.raw(
            "INSERT INTO shared_links (\(unsafeRaw: Self.columns)) SELECT \(unsafeRaw: Self.columns) FROM shared_links_old WHERE expires_at IS NOT NULL"
        ).run()
        try await sql.raw("DROP TABLE shared_links_old").run()
    }
}
