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

/// No cluster vars set - exercises `MetadataListingService`'s local-only path (no network
/// fan-out, since there are no peers). Multi-node fan-out/merge/dedup correctness needs real
/// separate processes and is covered by `cluster_tests.sh`, not this suite.
@Suite("MetadataListingService tests (standalone)", .serialized)
struct MetadataListingServiceTests {
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

    @Test("list returns an empty array for a collection with no records")
    func listEmptyCollection() async throws {
        try await withApp { app in
            let entries = await MetadataListingService.list(
                app: app, collection: "empty-collection-\(UUID().uuidString)")
            #expect(entries.isEmpty)
        }
    }

    @Test("list returns every record written to a collection")
    func listReturnsAllRecords() async throws {
        try await withApp { app in
            let collection = "list-test-\(UUID().uuidString)"
            var expected: [String: Data] = [:]
            for i in 0..<5 {
                let id = "item-\(i)"
                let value = Data("value-\(i)".utf8)
                try await MetadataStore.put(app: app, collection: collection, id: id, value: value)
                expected[id] = value
            }

            let entries = await MetadataListingService.list(app: app, collection: collection)
            #expect(entries.count == 5)
            for entry in entries {
                #expect(expected[entry.id] == entry.value)
            }
        }
    }

    @Test("list excludes records from other collections")
    func listExcludesOtherCollections() async throws {
        try await withApp { app in
            let collectionA = "collection-a-\(UUID().uuidString)"
            let collectionB = "collection-b-\(UUID().uuidString)"
            try await MetadataStore.put(
                app: app, collection: collectionA, id: "x", value: Data("a".utf8))
            try await MetadataStore.put(
                app: app, collection: collectionB, id: "y", value: Data("b".utf8))

            let entriesA = await MetadataListingService.list(app: app, collection: collectionA)
            #expect(entriesA.count == 1)
            #expect(entriesA.first?.id == "x")
        }
    }

    @Test("list excludes a similarly-prefixed but distinct collection name")
    func listDoesNotMatchPrefixCollisionAcrossCollections() async throws {
        try await withApp { app in
            let base = UUID().uuidString
            try await MetadataStore.put(
                app: app, collection: "users-\(base)", id: "1", value: Data("real".utf8))
            try await MetadataStore.put(
                app: app, collection: "users-\(base)-archive", id: "1", value: Data("decoy".utf8))

            let entries = await MetadataListingService.list(app: app, collection: "users-\(base)")
            #expect(entries.count == 1)
            #expect(entries.first?.value == Data("real".utf8))
        }
    }

    @Test("list reflects a deletion - a removed record is no longer listed")
    func listReflectsDeletion() async throws {
        try await withApp { app in
            let collection = "list-delete-\(UUID().uuidString)"
            try await MetadataStore.put(
                app: app, collection: collection, id: "a", value: Data("1".utf8))
            try await MetadataStore.put(
                app: app, collection: collection, id: "b", value: Data("2".utf8))
            try await MetadataStore.delete(app: app, collection: collection, id: "a")

            let entries = await MetadataListingService.list(app: app, collection: collection)
            #expect(entries.count == 1)
            #expect(entries.first?.id == "b")
        }
    }

    @Test("count matches the number of records in the collection")
    func countMatchesListCount() async throws {
        try await withApp { app in
            let collection = "count-test-\(UUID().uuidString)"
            for i in 0..<3 {
                try await MetadataStore.put(
                    app: app, collection: collection, id: "id-\(i)", value: Data("v".utf8))
            }
            let count = await MetadataListingService.count(app: app, collection: collection)
            #expect(count == 3)
        }
    }

    @Test("localEnvelopeEntries matches list's result when there are no peers")
    func localEntriesMatchesListWhenStandalone() async throws {
        try await withApp { app in
            let collection = "local-entries-test-\(UUID().uuidString)"
            try await MetadataStore.put(
                app: app, collection: collection, id: "only", value: Data("v".utf8))

            let local = await MetadataListingService.localEnvelopeEntries(
                app: app, collection: collection)
            let full = await MetadataListingService.list(app: app, collection: collection)
            #expect(local.count == full.count)
            #expect(local.first?.id == full.first?.id)
            // The local half carries envelopes so peers can merge them; `list` hands back payloads.
            #expect(local.first?.envelope.payload == full.first?.value)
        }
    }
}
