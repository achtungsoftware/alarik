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

@testable import Alarik

/// `coordinateDelete` with an empty `peers` list never touches cluster configuration or the
/// network at all (`replicateWrite` returns immediately when `peers.isEmpty`), so its local
/// delete-marker-vs-permanent-delete branching - the logic `S3Controller.handleObjectDelete` and
/// the Multi-Object-Delete per-key routing now share - is fully testable against plain SQLite,
/// no Postgres/cluster mode required. Multi-node delivery/fan-out behavior itself is covered by
/// `cluster_tests.sh`'s real multi-node suite, not here.
@Suite("ClusterReplicationService tests (no cluster required)", .serialized)
struct ClusterReplicationServiceTests {
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

    @Test("coordinateDelete on a non-versioned bucket removes the object and reports no marker")
    func permanentDeleteOutcome() async throws {
        try await withApp { app in
            let bucketName = "coordinate-delete-bucket"
            let key = "plain-object.txt"
            let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
            let meta = ObjectMeta(
                bucketName: bucketName, key: key, size: 5, contentType: "text/plain",
                etag: "\"etag\"", updatedAt: Date())
            try ObjectFileHandler.write(metadata: meta, data: Data("hello".utf8), to: path)
            #expect(FileManager.default.fileExists(atPath: path))

            let outcome = try await ClusterReplicationService.coordinateDelete(
                app: app, bucketName: bucketName, key: key, versionId: nil,
                versioningStatus: .disabled, peers: [])

            #expect(outcome.isDeleteMarker == false)
            #expect(outcome.versionId == nil)
            #expect(!FileManager.default.fileExists(atPath: path))
        }
    }

    @Test("coordinateDelete on a versioned bucket creates a delete marker and reports its id")
    func deleteMarkerOutcome() async throws {
        try await withApp { app in
            let bucketName = "coordinate-delete-versioned-bucket"
            let key = "versioned-object.txt"

            let outcome = try await ClusterReplicationService.coordinateDelete(
                app: app, bucketName: bucketName, key: key, versionId: nil,
                versioningStatus: .enabled, peers: [])

            #expect(outcome.isDeleteMarker == true)
            #expect(outcome.versionId != nil)
        }
    }

    @Test("coordinateDelete with an explicit versionId prunes that version, never a marker")
    func explicitVersionPruneOutcome() async throws {
        try await withApp { app in
            let bucketName = "coordinate-delete-prune-bucket"
            let key = "versioned-object.txt"
            let versionId = ObjectMeta.generateVersionId()
            let path = ObjectFileHandler.versionedPath(
                for: bucketName, key: key, versionId: versionId)
            let meta = ObjectMeta(
                bucketName: bucketName, key: key, size: 5, contentType: "text/plain",
                etag: "\"etag\"", updatedAt: Date(), versionId: versionId, isLatest: false)
            try ObjectFileHandler.write(metadata: meta, data: Data("hello".utf8), to: path)

            let outcome = try await ClusterReplicationService.coordinateDelete(
                app: app, bucketName: bucketName, key: key, versionId: versionId,
                versioningStatus: .enabled, peers: [])

            #expect(outcome.isDeleteMarker == false)
            #expect(outcome.versionId == versionId)
            #expect(!FileManager.default.fileExists(atPath: path))
        }
    }

    @Test(
        "ClusterReplicationDispatcher skips a .put task it can't fulfill locally, without counting a failed attempt"
    )
    func dispatcherSkipsUndeliverablePutWithoutBurningAttempts() async throws {
        try await withApp { app in
            // Every node runs this dispatcher independently against the same shared table, so any
            // node's tick can pick up a .put task for an object it doesn't actually hold locally -
            // this simulates exactly that: a task with no corresponding file on disk at all.
            let task = ClusterReplicationTask(
                bucketName: "no-local-copy-bucket", key: "missing.txt", versionId: nil,
                operation: .put, targetNodeId: UUID(), reason: .write)
            try await task.save(on: app.db)
            let taskId = try task.requireID()

            await ClusterReplicationDispatcher.shared.drain()

            let reloaded = try #require(
                try await ClusterReplicationTask.find(taskId, on: app.db))
            #expect(reloaded.state == ClusterReplicationTask.State.pending.rawValue)
            #expect(reloaded.attempts == 0)
            #expect(reloaded.lastError == nil)
        }
    }
}
