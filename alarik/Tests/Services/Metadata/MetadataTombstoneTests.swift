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

/// Tombstone semantics at the `MetadataStore` level: a delete must leave a durable marker rather
/// than removing bytes, and that marker must be invisible to every ordinary reader while still
/// being reclaimable once it has outlived its purpose.
///
/// Standalone (k=1/m=0, no network). The multi-node half - a node that was *offline* during the
/// delete returning and losing to the tombstone - needs real separate processes and lives in
/// `cluster_tests.sh`.
@Suite("Metadata tombstone tests (standalone)", .serialized)
struct MetadataTombstoneTests {
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

    private func collection() -> String { "tombstone-test-\(UUID().uuidString)" }
    private let value = Data("secret-material".utf8)

    // MARK: - Delete leaves a tombstone

    @Test("a delete hides the record but leaves a tombstone behind")
    func deleteLeavesTombstone() async throws {
        try await withApp { app in
            let collection = collection()
            try await MetadataStore.put(app: app, collection: collection, id: "k", value: value)
            try await MetadataStore.delete(app: app, collection: collection, id: "k")

            // Ordinary readers see absence...
            let readBack = try await MetadataStore.get(app: app, collection: collection, id: "k")
            #expect(readBack == nil)
            // ...but the record is still physically there, marked deleted. This is what a
            // returning stale replica loses against.
            let envelope = try await MetadataStore.getEnvelope(
                app: app, collection: collection, id: "k")
            #expect(envelope?.isTombstone == true)
            #expect(envelope?.payload == nil)
        }
    }

    @Test("a tombstoned record is excluded from listings")
    func tombstoneExcludedFromListing() async throws {
        try await withApp { app in
            let collection = collection()
            try await MetadataStore.put(app: app, collection: collection, id: "live", value: value)
            try await MetadataStore.put(app: app, collection: collection, id: "gone", value: value)
            try await MetadataStore.delete(app: app, collection: collection, id: "gone")

            let ids = await MetadataListingService.list(app: app, collection: collection)
                .map(\.id)
            #expect(ids == ["live"])
        }
    }

    @Test("deleting a record that was never there is not an error")
    func deleteMissingRecord() async throws {
        try await withApp { app in
            let collection = collection()
            try await MetadataStore.delete(app: app, collection: collection, id: "never-existed")
            let readBack = try await MetadataStore.get(
                app: app, collection: collection, id: "never-existed")
            #expect(readBack == nil)
        }
    }

    // MARK: - Reclaiming a tombstoned id

    @Test("putIfAbsent can claim an id whose previous record was deleted")
    func putIfAbsentOverTombstone() async throws {
        try await withApp { app in
            let collection = collection()
            #expect(
                try await MetadataStore.putIfAbsent(
                    app: app, collection: collection, id: "name", value: Data("first".utf8)))
            try await MetadataStore.delete(app: app, collection: collection, id: "name")

            // A released username or bucket name has to be reusable, so a tombstone must read as
            // absent to `putIfAbsent` - not as "taken".
            #expect(
                try await MetadataStore.putIfAbsent(
                    app: app, collection: collection, id: "name", value: Data("second".utf8)))
            let readBack = try await MetadataStore.get(app: app, collection: collection, id: "name")
            #expect(readBack == Data("second".utf8))
        }
    }

    @Test("putIfAbsent still refuses an id held by a live record")
    func putIfAbsentRespectsLiveRecord() async throws {
        try await withApp { app in
            let collection = collection()
            #expect(
                try await MetadataStore.putIfAbsent(
                    app: app, collection: collection, id: "name", value: Data("first".utf8)))
            let claimedAgain = try await MetadataStore.putIfAbsent(
                app: app, collection: collection, id: "name", value: Data("second".utf8))
            #expect(claimedAgain == false)

            let readBack = try await MetadataStore.get(
                app: app, collection: collection, id: "name")
            #expect(readBack == Data("first".utf8))
        }
    }

    @Test("re-putting a deleted id brings it back")
    func putOverTombstone() async throws {
        try await withApp { app in
            let collection = collection()
            try await MetadataStore.put(app: app, collection: collection, id: "k", value: value)
            try await MetadataStore.delete(app: app, collection: collection, id: "k")
            try await MetadataStore.put(
                app: app, collection: collection, id: "k", value: Data("again".utf8))

            let readBack = try await MetadataStore.get(app: app, collection: collection, id: "k")
            #expect(readBack == Data("again".utf8))
        }
    }

    // MARK: - consumeIfPresent

    @Test("consumeIfPresent returns the value once and tombstones it")
    func consumeLeavesTombstone() async throws {
        try await withApp { app in
            let collection = collection()
            try await MetadataStore.put(app: app, collection: collection, id: "k", value: value)

            let first = try await MetadataStore.consumeIfPresent(
                app: app, collection: collection, id: "k")
            #expect(first == value)

            // Single-use: a second consume must find nothing, tombstone or not.
            let second = try await MetadataStore.consumeIfPresent(
                app: app, collection: collection, id: "k")
            #expect(second == nil)
        }
    }

    // MARK: - Exempt collections

    @Test("an exempt collection hard-deletes instead of tombstoning")
    func exemptCollectionHardDeletes() async throws {
        try await withApp { app in
            // `oidc-states` is single-use and TTL-swept; tombstoning every expired state would
            // accumulate garbage for no safety gain.
            let collection = MetadataCollections.oidcStates
            let id = "state-\(UUID().uuidString)"
            try await MetadataStore.put(app: app, collection: collection, id: id, value: value)
            try await MetadataStore.delete(app: app, collection: collection, id: id)

            let envelope = try await MetadataStore.getEnvelope(
                app: app, collection: collection, id: id)
            #expect(envelope == nil, "an exempt collection should leave nothing behind at all")
        }
    }

    // MARK: - Garbage collection
    //
    // These use a real registered collection deliberately: the sweep walks
    // `MetadataCollections.all`, so a collection missing from that list is never swept at all -
    // which is exactly the failure mode `allCollectionsSwept` guards against.

    @Test("GC reclaims a tombstone that has outlived the grace period")
    func gcReclaimsExpiredTombstone() async throws {
        try await withApp { app in
            let collection = MetadataCollections.buckets
            try await MetadataStore.put(app: app, collection: collection, id: "k", value: value)
            try await MetadataStore.delete(app: app, collection: collection, id: "k")

            // Grace of zero: every tombstone is immediately past it.
            setenv("CLUSTER_METADATA_TOMBSTONE_GRACE_SECONDS", "0", 1)
            defer { unsetenv("CLUSTER_METADATA_TOMBSTONE_GRACE_SECONDS") }

            await MetadataMaintenance.runTombstoneGC(app: app)

            let envelope = try await MetadataStore.getEnvelope(
                app: app, collection: collection, id: "k")
            #expect(envelope == nil, "an expired tombstone should be physically gone")
        }
    }

    @Test("GC keeps a tombstone that is still inside the grace period")
    func gcRetainsFreshTombstone() async throws {
        try await withApp { app in
            let collection = MetadataCollections.buckets
            try await MetadataStore.put(app: app, collection: collection, id: "k", value: value)
            try await MetadataStore.delete(app: app, collection: collection, id: "k")

            setenv("CLUSTER_METADATA_TOMBSTONE_GRACE_SECONDS", "3600", 1)
            defer { unsetenv("CLUSTER_METADATA_TOMBSTONE_GRACE_SECONDS") }

            await MetadataMaintenance.runTombstoneGC(app: app)

            // Reclaiming early is what re-opens the resurrection window, so this must survive.
            let envelope = try await MetadataStore.getEnvelope(
                app: app, collection: collection, id: "k")
            #expect(envelope?.isTombstone == true)
            let readBack = try await MetadataStore.get(app: app, collection: collection, id: "k")
            #expect(readBack == nil)
        }
    }

    @Test("GC leaves live records untouched")
    func gcLeavesLiveRecords() async throws {
        try await withApp { app in
            let collection = MetadataCollections.buckets
            try await MetadataStore.put(app: app, collection: collection, id: "keep", value: value)

            setenv("CLUSTER_METADATA_TOMBSTONE_GRACE_SECONDS", "0", 1)
            defer { unsetenv("CLUSTER_METADATA_TOMBSTONE_GRACE_SECONDS") }

            await MetadataMaintenance.runTombstoneGC(app: app)

            let readBack = try await MetadataStore.get(
                app: app, collection: collection, id: "keep")
            #expect(readBack == value)
        }
    }

    @Test("the configured grace period is read from the environment")
    func graceIsConfigurable() {
        setenv("CLUSTER_METADATA_TOMBSTONE_GRACE_SECONDS", "42", 1)
        #expect(MetadataMaintenance.tombstoneGrace() == 42)
        unsetenv("CLUSTER_METADATA_TOMBSTONE_GRACE_SECONDS")

        setenv("CLUSTER_METADATA_TOMBSTONE_GRACE_DAYS", "2", 1)
        #expect(MetadataMaintenance.tombstoneGrace() == 2 * 24 * 3600)
        unsetenv("CLUSTER_METADATA_TOMBSTONE_GRACE_DAYS")

        #expect(
            MetadataMaintenance.tombstoneGrace()
                == TimeInterval(MetadataMaintenance.defaultGraceDays) * 24 * 3600)
    }

    // MARK: - Upgrading an existing deployment

    @Test("a record stored before envelopes existed is still readable end to end")
    func legacyRecordRemainsReadable() async throws {
        try await withApp { app in
            let collection = collection()
            let bare = Data(#"{"name":"pre-envelope"}"#.utf8)

            // `executeLocalPut` writes bytes verbatim (it is the receiving side of a forward, so
            // it must never re-wrap), which makes it the exact seam for planting a record in the
            // pre-envelope on-disk format an existing deployment would already have.
            try await MetadataStore.executeLocalPut(
                app: app, collection: collection, id: "legacy", value: bare)

            let readBack = try await MetadataStore.get(
                app: app, collection: collection, id: "legacy")
            #expect(readBack == bare, "an unwrapped record must not read back as missing")

            let listed = await MetadataListingService.list(app: app, collection: collection)
            #expect(listed.count == 1)
            #expect(listed.first?.value == bare)
        }
    }

    @Test("a legacy record is superseded by any later write, then behaves normally")
    func legacyRecordCanBeOverwrittenAndDeleted() async throws {
        try await withApp { app in
            let collection = collection()
            try await MetadataStore.executeLocalPut(
                app: app, collection: collection, id: "legacy", value: Data("old".utf8))

            try await MetadataStore.put(
                app: app, collection: collection, id: "legacy", value: Data("new".utf8))
            let afterWrite = try await MetadataStore.get(
                app: app, collection: collection, id: "legacy")
            #expect(afterWrite == Data("new".utf8))

            try await MetadataStore.delete(app: app, collection: collection, id: "legacy")
            let afterDelete = try await MetadataStore.get(
                app: app, collection: collection, id: "legacy")
            #expect(afterDelete == nil)
        }
    }

    // MARK: - Model-level behaviour

    @Test("a deleted access key stays deleted and stays out of listings")
    func deletedAccessKeyStaysDeleted() async throws {
        try await withApp { app in
            let userId = UUID()
            let key = AccessKey(
                userId: userId, accessKey: "AKIA\(UUID().uuidString.prefix(12))",
                secretKey: "secret")
            #expect(try await key.create(app: app))

            try await key.delete(app: app)

            let found = try await AccessKey.find(app: app, accessKey: key.accessKey)
            #expect(found == nil)
            let all = await AccessKey.all(app: app).map(\.accessKey)
            #expect(all.contains(key.accessKey) == false)
        }
    }
}
