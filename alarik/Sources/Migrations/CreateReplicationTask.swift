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

struct CreateReplicationTask: AsyncMigration {
    func prepare(on database: any Database) async throws {
        try await database.schema("replication_tasks")
            .id()
            .field("bucket_name", .string, .required)
            .field("rule_id", .uuid, .required)
            .field("target_id", .uuid, .required)
            .field("endpoint", .string, .required)
            .field("target_bucket", .string, .required)
            .field("access_key_id", .string, .required)
            .field("secret_access_key", .string, .required)
            .field("region", .string, .required)
            .field("key", .string, .required)
            .field("version_id", .string)
            .field("operation", .string, .required)
            .field("attempts", .int, .required)
            .field("next_attempt_at", .datetime, .required)
            .field("state", .string, .required)
            .field("last_error", .string)
            .field("created_at", .datetime, .required)
            .create()

        // Same shape as idx_notification_deliveries_due - the dispatcher polls
        // WHERE state = pending AND next_attempt_at <= now on every tick.
        if let sql = database as? any SQLDatabase {
            try await sql.create(index: "idx_replication_tasks_due")
                .on("replication_tasks")
                .column("state")
                .column("next_attempt_at")
                .run()
        }
    }

    func revert(on database: any Database) async throws {
        try await database.schema("replication_tasks").delete()
    }
}
