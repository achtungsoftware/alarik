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

/// The internal-cluster sibling of `replication_tasks`: a durable
/// outbox row for pushing/deleting one object on one peer node, structurally identical to
/// `ReplicationTask` (same drain-loop shape in `ClusterReplicationDispatcher`) but targeting a
/// `target_node_id` in this cluster instead of an external S3-compatible endpoint. Kept as a
/// separate table rather than reusing `replication_tasks` - the two have different targets
/// (node vs. external endpoint+credentials), different lifecycles, and internal cluster
/// replication applies to every bucket, not just versioning-enabled ones with configured rules.
struct CreateClusterReplicationTask: AsyncMigration {
    func prepare(on database: any Database) async throws {
        try await database.schema("cluster_replication_tasks")
            .id()
            .field("bucket_name", .string, .required)
            .field("key", .string, .required)
            .field("version_id", .string)
            .field("operation", .string, .required)
            .field("target_node_id", .uuid, .required)
            .field("reason", .string, .required)
            .field("attempts", .int, .required)
            .field("next_attempt_at", .datetime, .required)
            .field("state", .string, .required)
            .field("last_error", .string)
            .field("created_at", .datetime, .required)
            .create()

        // Same shape as idx_replication_tasks_due - the dispatcher polls
        // WHERE state = pending AND next_attempt_at <= now on every tick.
        if let sql = database as? any SQLDatabase {
            try await sql.create(index: "idx_cluster_replication_tasks_due")
                .on("cluster_replication_tasks")
                .column("state")
                .column("next_attempt_at")
                .run()
        }
    }

    func revert(on database: any Database) async throws {
        try await database.schema("cluster_replication_tasks").delete()
    }
}
