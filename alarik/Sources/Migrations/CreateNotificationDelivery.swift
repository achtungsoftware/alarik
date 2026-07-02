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

struct CreateNotificationDelivery: AsyncMigration {
    func prepare(on database: any Database) async throws {
        try await database.schema("notification_deliveries")
            .id()
            .field("bucket_name", .string, .required)
            .field("rule_id", .uuid, .required)
            .field("url", .string, .required)
            .field("secret", .string)
            .field("payload", .string, .required)
            .field("attempts", .int, .required)
            .field("next_attempt_at", .datetime, .required)
            .field("state", .string, .required)
            .field("created_at", .datetime, .required)
            .create()

        // The dispatcher polls WHERE state = pending AND next_attempt_at <= now on every
        // tick - this index keeps that query cheap no matter how big the backlog gets
        if let sql = database as? any SQLDatabase {
            try await sql.create(index: "idx_notification_deliveries_due")
                .on("notification_deliveries")
                .column("state")
                .column("next_attempt_at")
                .run()
        }
    }

    func revert(on database: any Database) async throws {
        try await database.schema("notification_deliveries").delete()
    }
}
