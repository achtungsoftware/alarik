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

import Foundation
import Testing
import Vapor
import VaporTesting

@testable import Alarik

/// Re-requesting the same shard repair must not stack rows: the row's id is its identity, and
/// the owning mailbox keeps a pending row and revives a dead-lettered one in place.
@Suite("Outbox identity dedup", .serialized)
struct OutboxDedupTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await test(app)
        } catch {
            try? StorageHelper.cleanStorage()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    @Test("a shard task's id is its identity")
    func identityIds() {
        let target = UUID()
        let a = ErasureCodedReplicationTask(
            bucketName: "b", key: "k", versionId: "v", shardIndex: 2, operation: .put, targetNodeId: target, reason: .reconstruct)
        let b = ErasureCodedReplicationTask(
            bucketName: "b", key: "k", versionId: "v", shardIndex: 2, operation: .put, targetNodeId: target, reason: .rebalance)
        #expect(a.id == b.id)
        let otherIndex = ErasureCodedReplicationTask(
            bucketName: "b", key: "k", versionId: "v", shardIndex: 3, operation: .put, targetNodeId: target, reason: .reconstruct)
        #expect(otherIndex.id != a.id)
        let otherVersion = ErasureCodedReplicationTask(
            bucketName: "b", key: "k", versionId: nil, shardIndex: 2, operation: .put, targetNodeId: target, reason: .reconstruct)
        #expect(otherVersion.id != a.id)
    }

    @Test("storing the same task again keeps the pending row and revives a dead-lettered one")
    func storeDeduplicates() async throws {
        try await withApp { app in
            let collection = OutboxCollections.erasureCodedReplicationTasks
            let target = OutboxMailbox.selfNodeId(app: app)
            func task() -> ErasureCodedReplicationTask {
                ErasureCodedReplicationTask(
                    bucketName: "b", key: "k", versionId: nil, shardIndex: 1, operation: .put,
                    targetNodeId: target, reason: .reconstruct)
            }
            await OutboxMailbox.enqueue(app: app, collection: collection, row: task())
            await OutboxMailbox.enqueue(app: app, collection: collection, row: task())
            var rows = OutboxMailbox.allOwnedTasks(ErasureCodedReplicationTask.self, app: app, collection: collection)
            #expect(rows.count == 1)

            // Dead-lettered after exhausting its attempts: the next request revives it in place.
            let failed = try #require(rows.first)
            failed.state = .failed
            failed.attempts = 8
            failed.lastError = "boom"
            try OutboxMailbox.update(failed, collection: collection)
            await OutboxMailbox.enqueue(app: app, collection: collection, row: task())
            rows = OutboxMailbox.allOwnedTasks(ErasureCodedReplicationTask.self, app: app, collection: collection)
            #expect(rows.count == 1)
            #expect(rows.first?.state == .pending)
            #expect(rows.first?.attempts == 0)
            #expect(rows.first?.lastError == nil)
        }
    }
}
