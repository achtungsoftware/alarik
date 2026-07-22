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

import Crypto
import Foundation
import Testing
import Vapor

@testable import Alarik

/// `rebalanceBucket` fire-and-forget wakes the process-wide `ClusterReplicationDispatcher`
/// singleton via a detached, un-awaited Task whenever it creates outbox work. That Task can
/// outlive this suite's short-lived per-test `Application`/database (`withApp` tears both down
/// as soon as a test body returns), and when it finally runs against a torn-down app it can crash
/// the whole test *process*, not just one test - confirmed empirically, not just theoretically,
/// while developing this suite. So only the one scenario below that provably never creates any
/// outbox row (and therefore never wakes the dispatcher) is covered here. The row-creation paths
/// (retrying a dead-lettered copy task, enqueueing a fresh reclaim) are exercised indirectly by
/// `ClusterRebalanceService.swift`'s own reasoning/comments and by `cluster_tests.sh`'s real
/// multi-node suite, which doesn't have this ephemeral-app hazard.
@Suite("ClusterRebalanceService tests", .serialized)
struct ClusterRebalanceServiceTests {
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

    /// Finds a key for which `selfId` is NOT among `PlacementService.responsibleNodes` under
    /// `activeNodes` - i.e. a genuine reclaim candidate. Needs at least 4 active nodes, since
    /// with `replicationFactor == 3` and 3 or fewer nodes every node is always responsible for
    /// everything.
    private func findReclaimCandidateKey(
        selfId: UUID, bucketName: String, activeNodes: [ClusterNodeInfo]
    ) -> String {
        for i in 0..<1000 {
            let key = "object-\(i).txt"
            let responsible = PlacementService.responsibleNodes(
                bucketName: bucketName, key: key, activeNodes: activeNodes)
            if !responsible.contains(where: { $0.id == selfId }) {
                return key
            }
        }
        fatalError("Could not find a reclaim-candidate key in 1000 tries")
    }

    /// Finds a key for which `selfId` IS among `PlacementService.responsibleNodes` under
    /// `activeNodes` - i.e. a genuine copy candidate (this node would push it to the other
    /// responsible nodes).
    private func findResponsibleCandidateKey(
        selfId: UUID, bucketName: String, activeNodes: [ClusterNodeInfo]
    ) -> String {
        for i in 0..<1000 {
            let key = "object-\(i).txt"
            let responsible = PlacementService.responsibleNodes(
                bucketName: bucketName, key: key, activeNodes: activeNodes)
            if responsible.contains(where: { $0.id == selfId }) {
                return key
            }
        }
        fatalError("Could not find a responsible-candidate key in 1000 tries")
    }

    private func setUpBucketAndFourNodeCluster(
        _ app: Application, bucketName: String
    ) async throws -> (selfId: UUID, activeNodes: [ClusterNodeInfo]) {
        let user = User(
            name: "Rebalance Test User", username: UUID().uuidString,
            passwordHash: try Bcrypt.hash("TestPass123!"), isAdmin: false)
        try await user.create(app: app)
        try await Bucket(name: bucketName, userId: user.id).save(app: app)
        try BucketHandler.create(name: bucketName)

        let selfId = UUID()
        let config = ClusterConfiguration(
            nodeId: selfId, address: "http://self.internal", secret: "test-secret")
        app.storage[ClusterConfigurationKey.self] = config

        let now = Date()
        let activeNodes = [selfId, UUID(), UUID(), UUID()].map {
            ClusterNodeInfo(id: $0, address: "http://\($0).internal", status: .active, lastHeartbeatAt: now)
        }
        await ClusterNodeCache.shared.load(initialData: activeNodes)

        return (selfId, activeNodes)
    }

    @Test(
        "rebalanceBucket does not reclaim a key while a copy task to a still-responsible node is genuinely pending"
    )
    func reclaimGatedOnPendingCopyTask() async throws {
        try await withApp { app in
            let bucketName = "rebalance-pending-gate-bucket"
            let (selfId, activeNodes) = try await setUpBucketAndFourNodeCluster(
                app, bucketName: bucketName)
            let key = findReclaimCandidateKey(
                selfId: selfId, bucketName: bucketName, activeNodes: activeNodes)

            let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
            let meta = ObjectMeta(
                bucketName: bucketName, key: key, size: 5, contentType: "text/plain",
                etag: "\"etag\"", updatedAt: Date())
            try ObjectFileHandler.write(metadata: meta, data: Data("hello".utf8), to: path)

            let responsible = PlacementService.responsibleNodes(
                bucketName: bucketName, key: key, activeNodes: activeNodes)
            // Already-pending (not dead-lettered) - this rebalance pass creates no new outbox
            // rows for this key at all (self isn't responsible, so no copy task; the key is
            // already covered by this existing pending task, so no reclaim task either), which
            // is exactly why this is the one scenario safe to exercise through the real
            // `rebalance()` entry point - see the suite-level doc comment.
            let pendingTask = ClusterReplicationTask(
                bucketName: bucketName, key: key, versionId: nil, operation: .put,
                targetNodeId: responsible[0].id, reason: .rebalance, ownerNodeId: selfId)
            try OutboxMailbox.update(pendingTask, collection: OutboxCollections.clusterReplicationTasks)

            try await ClusterRebalanceService.rebalance(app: app, reason: .manualResync)

            // The local copy must survive: a copy to a currently-responsible node is still
            // genuinely in flight, so deleting the only source of truth here would risk losing
            // the object entirely if that copy hasn't landed yet.
            #expect(FileManager.default.fileExists(atPath: path))
        }
    }

    @Test(
        "rebalanceBucket does not insert a duplicate copy task when one is already pending for the same (key, target)"
    )
    func copyPhaseSkipsDuplicateForAlreadyPendingTarget() async throws {
        try await withApp { app in
            let bucketName = "rebalance-copy-dedup-bucket"
            let (selfId, activeNodes) = try await setUpBucketAndFourNodeCluster(
                app, bucketName: bucketName)
            let key = findResponsibleCandidateKey(
                selfId: selfId, bucketName: bucketName, activeNodes: activeNodes)

            let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
            let meta = ObjectMeta(
                bucketName: bucketName, key: key, size: 5, contentType: "text/plain",
                etag: "\"etag\"", updatedAt: Date())
            try ObjectFileHandler.write(metadata: meta, data: Data("hello".utf8), to: path)

            // Self is responsible for this key, so `rebalanceBucket` wants to push a copy to
            // every *other* responsible node - pre-cover all of them with an already-pending
            // task each, so this pass has no genuinely new copy work to enqueue at all (and
            // therefore never wakes the dispatcher - see the suite-level doc comment).
            let responsible = PlacementService.responsibleNodes(
                bucketName: bucketName, key: key, activeNodes: activeNodes)
            let otherResponsibleIds = responsible.filter { $0.id != selfId }.map(\.id)
            #expect(!otherResponsibleIds.isEmpty)
            var preInsertedIds: Set<UUID> = []
            for targetId in otherResponsibleIds {
                let task = ClusterReplicationTask(
                    bucketName: bucketName, key: key, versionId: nil, operation: .put,
                    targetNodeId: targetId, reason: .rebalance, ownerNodeId: selfId)
                try OutboxMailbox.update(task, collection: OutboxCollections.clusterReplicationTasks)
                preInsertedIds.insert(task.id)
            }

            try await ClusterRebalanceService.rebalance(app: app, reason: .manualResync)

            let copyTasksForKey = OutboxMailbox.allOwnedTasks(
                ClusterReplicationTask.self, app: app, collection: OutboxCollections.clusterReplicationTasks
            ).filter {
                $0.bucketName == bucketName && $0.key == key
                    && $0.operation == .put
            }

            // Exactly the pre-inserted rows survive, one per other-responsible target - no
            // duplicates were piled on top of the already-pending work.
            #expect(copyTasksForKey.count == otherResponsibleIds.count)
            let survivingIds = Set(copyTasksForKey.map(\.id))
            #expect(survivingIds == preInsertedIds)
        }
    }
}
