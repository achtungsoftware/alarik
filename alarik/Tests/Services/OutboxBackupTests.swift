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

/// The outbox durability scheme for webhook and bucket-replication rows: mirrored to peers, and
/// adopted by a surviving node once their owner is gone (`OutboxMailbox.runBackupSweep`).
@Suite("Outbox backup mirroring", .serialized)
struct OutboxBackupTests {
    private func withClusterApp(_ test: (Application, UUID) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            let selfId = UUID()
            app.storage[ClusterConfigurationKey.self] = ClusterConfiguration(
                nodeId: selfId, address: "http://self.internal", secret: "test-secret")
            try await test(app, selfId)
        } catch {
            await ClusterNodeCache.shared.load(initialData: [])
            try? StorageHelper.cleanStorage()
            try await app.asyncShutdown()
            throw error
        }
        await ClusterNodeCache.shared.load(initialData: [])
        try await app.asyncShutdown()
    }

    private func backupRow(owner: UUID) throws -> (NotificationDelivery, Data) {
        let row = NotificationDelivery(
            bucketName: "b", ruleId: UUID(), url: "http://127.0.0.1:1/hook", secret: nil,
            payload: "{}", ownerNodeId: owner)
        return (row, try JSONEncoder().encode(row))
    }

    @Test("a mirrored row is adopted once its owner has dropped out of the cluster, and only then")
    func adoptsRowsOfDepartedOwners() async throws {
        try await withClusterApp { app, selfId in
            let liveOwner = UUID()
            let deadOwner = UUID()
            let now = Date()
            await ClusterNodeCache.shared.load(initialData: [
                ClusterNodeInfo(id: selfId, address: "http://self.internal", status: .active, lastHeartbeatAt: now),
                ClusterNodeInfo(id: liveOwner, address: "http://live.internal", status: .active, lastHeartbeatAt: now),
                // Registered but silent for far longer than the heartbeat staleness.
                ClusterNodeInfo(
                    id: deadOwner, address: "http://dead.internal", status: .active,
                    lastHeartbeatAt: now.addingTimeInterval(-3600)),
            ])
            let collection = OutboxCollections.notificationDeliveries
            let (liveRow, liveData) = try backupRow(owner: liveOwner)
            let (deadRow, deadData) = try backupRow(owner: deadOwner)
            try OutboxMailbox.receiveBackup(data: liveData, collection: collection, ownerNodeId: liveOwner, taskId: liveRow.id)
            try OutboxMailbox.receiveBackup(data: deadData, collection: collection, ownerNodeId: deadOwner, taskId: deadRow.id)

            let promoted = await OutboxMailbox.promoteOrphanedBackups(
                NotificationDelivery.self, app: app, collection: collection)
            #expect(promoted == 1)

            let owned = OutboxMailbox.allOwnedTasks(NotificationDelivery.self, app: app, collection: collection)
            #expect(owned.map(\.id) == [deadRow.id])
            #expect(owned.first?.ownerNodeId == selfId)
            let remainingBackups = OutboxMailboxFileHandler.listAllOwnerTaskIds(
                root: OutboxMailboxFileHandler.backupRootPath, collection: collection)
            #expect(remainingBackups.map(\.1) == [liveRow.id])

            // Running again adopts nothing new.
            #expect(await OutboxMailbox.promoteOrphanedBackups(NotificationDelivery.self, app: app, collection: collection) == 0)
        }
    }

    @Test("the replica count honours the new variable and falls back to the historical name")
    func replicaCountFallsBack() async throws {
        try await withClusterApp { app, _ in
            #expect(OutboxMailbox.replicaCount(app: app) == OutboxMailbox.defaultReplicaCount)
            setenv("CLUSTER_METADATA_REPLICA_COUNT", "3", 1)
            defer { unsetenv("CLUSTER_METADATA_REPLICA_COUNT") }
            #expect(OutboxMailbox.replicaCount(app: app) == 3)
            setenv("CLUSTER_OUTBOX_REPLICA_COUNT", "4", 1)
            defer { unsetenv("CLUSTER_OUTBOX_REPLICA_COUNT") }
            #expect(OutboxMailbox.replicaCount(app: app) == 4)
        }
    }
}
