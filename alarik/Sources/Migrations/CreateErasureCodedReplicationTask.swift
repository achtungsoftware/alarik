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

/// The erasure-coding sibling of `cluster_replication_tasks`: a durable outbox row for one
/// shard's push/delete/reconstruction on one peer node, same drain-loop shape (via
/// `GenericOutboxDispatcher`) but keyed by `(bucket_name, key, version_id, shard_index)` instead
/// of a whole object.
struct CreateErasureCodedReplicationTask: AsyncMigration {
    func prepare(on database: any Database) async throws {
        try await database.schema("erasure_coded_replication_tasks")
            .id()
            .field("bucket_name", .string, .required)
            .field("key", .string, .required)
            .field("version_id", .string)
            .field("shard_index", .int, .required)
            .field("operation", .string, .required)
            .field("target_node_id", .uuid, .required)
            .field("reason", .string, .required)
            .field("attempts", .int, .required)
            .field("next_attempt_at", .datetime, .required)
            .field("state", .string, .required)
            .field("last_error", .string)
            .field("created_at", .datetime, .required)
            .create()

        // Same shape as idx_cluster_replication_tasks_due - the dispatcher polls
        // WHERE state = pending AND next_attempt_at <= now on every tick.
        if let sql = database as? any SQLDatabase {
            try await sql.create(index: "idx_erasure_coded_replication_tasks_due")
                .on("erasure_coded_replication_tasks")
                .column("state")
                .column("next_attempt_at")
                .run()
        }
    }

    func revert(on database: any Database) async throws {
        try await database.schema("erasure_coded_replication_tasks").delete()
    }
}
