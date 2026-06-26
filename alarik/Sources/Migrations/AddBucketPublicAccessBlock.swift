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

struct AddBucketPublicAccessBlock: AsyncMigration {
    // SQLite's ALTER TABLE only supports adding one column per statement - unlike CreateBucket/
    // CreateUser/etc., which add multiple fields in one .create() (a single CREATE TABLE
    // naturally allows multiple columns), each field here needs its own separate .update() call.
    func prepare(on database: any Database) async throws {
        try await database.schema("buckets")
            .field("block_public_acls", .bool, .required, .sql(.default(false)))
            .update()
        try await database.schema("buckets")
            .field("ignore_public_acls", .bool, .required, .sql(.default(false)))
            .update()
        try await database.schema("buckets")
            .field("block_public_policy", .bool, .required, .sql(.default(false)))
            .update()
        try await database.schema("buckets")
            .field("restrict_public_buckets", .bool, .required, .sql(.default(false)))
            .update()
    }

    func revert(on database: any Database) async throws {
        try await database.schema("buckets")
            .deleteField("block_public_acls")
            .update()
        try await database.schema("buckets")
            .deleteField("ignore_public_acls")
            .update()
        try await database.schema("buckets")
            .deleteField("block_public_policy")
            .update()
        try await database.schema("buckets")
            .deleteField("restrict_public_buckets")
            .update()
    }
}
