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
import Foundation
import Testing
import Vapor
import VaporTesting

@testable import Alarik

@Suite("InternalClusterController tests", .serialized)
struct InternalClusterControllerTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()
            try await test(app)
            try await app.autoRevert()
        } catch {
            try? StorageHelper.cleanStorage()
            try? await app.autoRevert()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    @Test(
        "rebalanceStatus tallies pending-by-reason and failed counts correctly without loading every row"
    )
    func testRebalanceStatusCounts() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            // Two `.write`, one `.rebalance`, zero `.reclaim` - the zero case matters too, since
            // the aggregation must never report a reason with a zero count.
            for _ in 0..<2 {
                try await ClusterReplicationTask(
                    bucketName: "b", key: "k\(UUID())", versionId: nil, operation: .put,
                    targetNodeId: UUID(), reason: .write
                ).save(on: app.db)
            }
            try await ClusterReplicationTask(
                bucketName: "b", key: "k\(UUID())", versionId: nil, operation: .put,
                targetNodeId: UUID(), reason: .rebalance
            ).save(on: app.db)

            let failedTask = ClusterReplicationTask(
                bucketName: "b", key: "k\(UUID())", versionId: nil, operation: .delete,
                targetNodeId: UUID(), reason: .reclaim)
            failedTask.state = ClusterReplicationTask.State.failed.rawValue
            try await failedTask.save(on: app.db)

            try await app.test(
                .GET, "/api/v1/admin/cluster/rebalance/status",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let dto = try res.content.decode(
                        InternalClusterController.RebalanceStatusDTO.self)
                    #expect(dto.pendingCount == 3)
                    #expect(dto.failedCount == 1)
                    #expect(dto.pendingByReason == ["write": 2, "rebalance": 1])
                    #expect(dto.replicationFactor == PlacementService.replicationFactor)
                })
        }
    }

    @Test("rebalanceStatus without auth fails")
    func testRebalanceStatusWithoutAuth() async throws {
        try await withApp { app in
            try await app.test(
                .GET, "/api/v1/admin/cluster/rebalance/status",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }
}
