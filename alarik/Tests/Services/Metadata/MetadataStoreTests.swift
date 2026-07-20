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

/// No cluster vars set in the test process - every case here exercises `MetadataStore`'s
/// standalone (k=1/m=0, zero-network) path, the one every non-clustered deployment (the
/// overwhelming majority) takes on every call. Multi-node forwarding/quorum/auto-cap behavior
/// needs real separate processes and is covered by `cluster_tests.sh`, not this suite.
@Suite("MetadataStore tests (standalone)", .serialized)
struct MetadataStoreTests {
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

    private struct Record: Codable, Equatable {
        let name: String
        let count: Int
    }

    // MARK: - Round trip

    @Test("get returns nil for a record that was never written")
    func getReturnsNilForMissingRecord() async throws {
        try await withApp { app in
            let value = try await MetadataStore.get(
                app: app, collection: "users", id: UUID().uuidString)
            #expect(value == nil)
        }
    }

    @Test("put then get round-trips the exact bytes")
    func putThenGetRoundTrips() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            let payload = Data("hello metadata".utf8)
            try await MetadataStore.put(app: app, collection: "users", id: id, value: payload)

            let readBack = try await MetadataStore.get(app: app, collection: "users", id: id)
            #expect(readBack == payload)
        }
    }

    @Test("put overwrites an existing record unconditionally")
    func putOverwritesExistingRecord() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            try await MetadataStore.put(
                app: app, collection: "buckets", id: id, value: Data("v1".utf8))
            try await MetadataStore.put(
                app: app, collection: "buckets", id: id, value: Data("v2".utf8))

            let readBack = try await MetadataStore.get(app: app, collection: "buckets", id: id)
            #expect(readBack == Data("v2".utf8))
        }
    }

    @Test("zero-byte values round-trip correctly (no special-cased failure)")
    func zeroByteValueRoundTrips() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            try await MetadataStore.put(app: app, collection: "oidc-states", id: id, value: Data())
            let readBack = try await MetadataStore.get(app: app, collection: "oidc-states", id: id)
            #expect(readBack == Data())
        }
    }

    @Test("delete removes the record; a later get returns nil")
    func deleteRemovesRecord() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            try await MetadataStore.put(
                app: app, collection: "access-keys", id: id, value: Data("secret".utf8))
            try await MetadataStore.delete(app: app, collection: "access-keys", id: id)

            let readBack = try await MetadataStore.get(app: app, collection: "access-keys", id: id)
            #expect(readBack == nil)
        }
    }

    @Test("delete on a record that never existed does not throw")
    func deleteOnMissingRecordIsNoop() async throws {
        try await withApp { app in
            try await MetadataStore.delete(
                app: app, collection: "shared-links", id: UUID().uuidString)
        }
    }

    // MARK: - Codable convenience

    @Test("Codable put/get round-trips a typed value")
    func codableConvenienceRoundTrips() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            let record = Record(name: "alice", count: 42)
            try await MetadataStore.put(app: app, collection: "users", id: id, value: record)

            let readBack = try await MetadataStore.get(Record.self, app: app, collection: "users", id: id)
            #expect(readBack == record)
        }
    }

    // MARK: - putIfAbsent (CAS / uniqueness)

    @Test("putIfAbsent creates the record and returns true when absent")
    func putIfAbsentCreatesWhenAbsent() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            let created = try await MetadataStore.putIfAbsent(
                app: app, collection: "users", id: id, value: Data("first".utf8))
            #expect(created)

            let readBack = try await MetadataStore.get(app: app, collection: "users", id: id)
            #expect(readBack == Data("first".utf8))
        }
    }

    @Test("putIfAbsent returns false and leaves the existing value untouched when present")
    func putIfAbsentRejectsWhenPresent() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            let firstCreated = try await MetadataStore.putIfAbsent(
                app: app, collection: "users", id: id, value: Data("first".utf8))
            #expect(firstCreated)

            let secondCreated = try await MetadataStore.putIfAbsent(
                app: app, collection: "users", id: id, value: Data("second".utf8))
            #expect(!secondCreated)

            let readBack = try await MetadataStore.get(app: app, collection: "users", id: id)
            #expect(readBack == Data("first".utf8))
        }
    }

    @Test("concurrent putIfAbsent calls on the same id: exactly one wins")
    func concurrentPutIfAbsentHasExactlyOneWinner() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            let results = await withTaskGroup(of: Bool.self, returning: [Bool].self) { group in
                for i in 0..<10 {
                    group.addTask {
                        (try? await MetadataStore.putIfAbsent(
                            app: app, collection: "buckets", id: id,
                            value: Data("attempt-\(i)".utf8))) ?? false
                    }
                }
                var collected: [Bool] = []
                for await result in group { collected.append(result) }
                return collected
            }

            #expect(results.filter { $0 }.count == 1)
        }
    }

    @Test("putIfAbsent Codable convenience creates only once")
    func putIfAbsentCodableConvenience() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            let first = try await MetadataStore.putIfAbsent(
                app: app, collection: "users", id: id, value: Record(name: "a", count: 1))
            let second = try await MetadataStore.putIfAbsent(
                app: app, collection: "users", id: id, value: Record(name: "b", count: 2))
            #expect(first)
            #expect(!second)

            let readBack = try await MetadataStore.get(Record.self, app: app, collection: "users", id: id)
            #expect(readBack == Record(name: "a", count: 1))
        }
    }

    // MARK: - consumeIfPresent (atomic read-then-delete, single-use)

    @Test("consumeIfPresent returns nil for a record that never existed")
    func consumeIfPresentReturnsNilWhenAbsent() async throws {
        try await withApp { app in
            let value = try await MetadataStore.consumeIfPresent(
                app: app, collection: "oidc-states", id: UUID().uuidString)
            #expect(value == nil)
        }
    }

    @Test("consumeIfPresent returns the value once, then nil - single use")
    func consumeIfPresentIsSingleUse() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            try await MetadataStore.put(
                app: app, collection: "oidc-states", id: id, value: Data("state-payload".utf8))

            let firstConsume = try await MetadataStore.consumeIfPresent(
                app: app, collection: "oidc-states", id: id)
            #expect(firstConsume == Data("state-payload".utf8))

            let secondConsume = try await MetadataStore.consumeIfPresent(
                app: app, collection: "oidc-states", id: id)
            #expect(secondConsume == nil)

            // Confirmed genuinely gone, not just unconsumable.
            let readBack = try await MetadataStore.get(app: app, collection: "oidc-states", id: id)
            #expect(readBack == nil)
        }
    }

    @Test("concurrent consumeIfPresent calls on the same id: exactly one consumer wins")
    func concurrentConsumeIfPresentHasExactlyOneWinner() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            try await MetadataStore.put(
                app: app, collection: "oidc-states", id: id, value: Data("only-once".utf8))

            let results = await withTaskGroup(of: Data?.self, returning: [Data?].self) { group in
                for _ in 0..<10 {
                    group.addTask {
                        try? await MetadataStore.consumeIfPresent(
                            app: app, collection: "oidc-states", id: id)
                    }
                }
                var collected: [Data?] = []
                for await result in group { collected.append(result) }
                return collected
            }

            #expect(results.compactMap { $0 }.count == 1)
        }
    }

    // MARK: - Collections are independent namespaces

    @Test("the same id in two different collections does not collide")
    func differentCollectionsDoNotCollide() async throws {
        try await withApp { app in
            let id = UUID().uuidString
            try await MetadataStore.put(
                app: app, collection: "users", id: id, value: Data("user-record".utf8))
            try await MetadataStore.put(
                app: app, collection: "buckets", id: id, value: Data("bucket-record".utf8))

            let userValue = try await MetadataStore.get(app: app, collection: "users", id: id)
            let bucketValue = try await MetadataStore.get(app: app, collection: "buckets", id: id)
            #expect(userValue == Data("user-record".utf8))
            #expect(bucketValue == Data("bucket-record".utf8))
        }
    }
}
