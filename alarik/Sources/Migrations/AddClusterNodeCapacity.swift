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

/// Self-reported disk capacity for capacity-aware write placement. Nullable - existing rows and
/// a node that hasn't heartbeated since upgrading simply read `nil`, which every consumer
/// (`ClusterCapacityPolicy`) already treats as "unknown, fail open, never near-full."
struct AddClusterNodeCapacity: AsyncMigration {
    func prepare(on database: any Database) async throws {
        try await database.schema("cluster_nodes")
            .field("total_bytes", .int64)
            .update()
        try await database.schema("cluster_nodes")
            .field("available_bytes", .int64)
            .update()
    }

    func revert(on database: any Database) async throws {
        try await database.schema("cluster_nodes")
            .deleteField("total_bytes")
            .update()
        try await database.schema("cluster_nodes")
            .deleteField("available_bytes")
            .update()
    }
}
