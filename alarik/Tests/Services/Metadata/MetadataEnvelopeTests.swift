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
import Logging
import Testing

@testable import Alarik

/// The envelope, its conflict-resolution rule, and the migration engine - all pure, no app or
/// cluster required. The behaviour these protect is subtle and security-relevant (a tombstone
/// losing a comparison means a revoked credential comes back), so it is pinned here rather than
/// left to the multi-node harness alone.
@Suite("Metadata envelope tests")
struct MetadataEnvelopeTests {
    private func payload(_ string: String) -> Data { Data(string.utf8) }

    // MARK: - Round trip

    @Test("a live record round-trips through the envelope unchanged")
    func liveRoundTrip() throws {
        let envelope = MetadataEnvelope.live(payload: payload("{\"a\":1}"), schemaVersion: 3)
        let decoded = MetadataEnvelope.decode(try envelope.encoded())

        #expect(decoded.deleted == false)
        #expect(decoded.schemaVersion == 3)
        #expect(decoded.payload == payload("{\"a\":1}"))
        #expect(decoded.updatedAtMillis == envelope.updatedAtMillis)
        #expect(decoded.format == MetadataEnvelope.currentFormat)
    }

    @Test("a tombstone round-trips and carries no payload")
    func tombstoneRoundTrip() throws {
        let envelope = MetadataEnvelope.tombstone(schemaVersion: 1)
        let decoded = MetadataEnvelope.decode(try envelope.encoded())

        #expect(decoded.isTombstone)
        #expect(decoded.payload == nil)
    }

    @Test("a non-JSON payload survives the envelope (base64, not re-parsed)")
    func binaryPayloadRoundTrip() throws {
        let raw = Data([0x00, 0xFF, 0x10, 0x7F, 0xAB])
        let decoded = MetadataEnvelope.decode(
            try MetadataEnvelope.live(payload: raw, schemaVersion: 1).encoded())
        #expect(decoded.payload == raw)
    }

    // MARK: - Backward compatibility

    @Test("bytes written before envelopes existed read back as a live record, not as corrupt")
    func legacyBareBytesAreReadable() {
        let legacy = payload("{\"username\":\"alarik\"}")
        let decoded = MetadataEnvelope.decode(legacy)

        #expect(decoded.deleted == false)
        #expect(decoded.payload == legacy)
        #expect(decoded.schemaVersion == MetadataMigrations.baseVersion)
        // Zero timestamp, so any rewritten copy wins over an unwrapped one.
        #expect(decoded.updatedAtMillis == 0)
        #expect(MetadataEnvelope.isLegacyBare(legacy))
    }

    @Test("a wrapped record is not mistaken for a legacy bare one")
    func wrappedIsNotLegacy() throws {
        let wrapped = try MetadataEnvelope.live(payload: payload("{}"), schemaVersion: 1).encoded()
        #expect(MetadataEnvelope.isLegacyBare(wrapped) == false)
    }

    @Test("a bare record whose fields resemble an envelope is still treated as legacy")
    func lookalikeBareRecordIsLegacy() {
        // No `_alarik_env` marker, so this must not be parsed as an envelope even though it
        // carries every other field name.
        let lookalike = payload(
            "{\"schemaVersion\":9,\"updatedAtMillis\":123,\"deleted\":true,\"payload\":\"eA==\"}")
        let decoded = MetadataEnvelope.decode(lookalike)

        #expect(decoded.deleted == false, "a bare record must never be read as a tombstone")
        #expect(decoded.payload == lookalike)
    }

    // MARK: - Conflict resolution

    @Test("the newer write wins regardless of which side is the tombstone")
    func newerWins() {
        let older = MetadataEnvelope.live(payload: payload("old"), schemaVersion: 1, updatedAtMillis: 1_000)
        let newer = MetadataEnvelope.live(payload: payload("new"), schemaVersion: 1, updatedAtMillis: 2_000)

        #expect(newer.supersedes(older))
        #expect(older.supersedes(newer) == false)

        let newerTombstone = MetadataEnvelope.tombstone(schemaVersion: 1, updatedAtMillis: 2_000)
        #expect(newerTombstone.supersedes(older))
        #expect(older.supersedes(newerTombstone) == false)
    }

    @Test("a stale replica returning after a delete loses to the tombstone")
    func staleReplicaLosesToTombstone() {
        // The exact resurrection scenario: a node held this record, went offline, and the record
        // was deleted while it was away. Its copy is necessarily older than the tombstone.
        let staleLiveCopy = MetadataEnvelope.live(
            payload: payload("revoked-access-key"), schemaVersion: 1, updatedAtMillis: 1_000)
        let tombstone = MetadataEnvelope.tombstone(schemaVersion: 1, updatedAtMillis: 5_000)

        #expect(tombstone.supersedes(staleLiveCopy))
        #expect(staleLiveCopy.supersedes(tombstone) == false)
    }

    @Test("on an exact timestamp tie the tombstone wins")
    func tombstoneWinsTies() {
        let live = MetadataEnvelope.live(payload: payload("x"), schemaVersion: 1, updatedAtMillis: 7)
        let tombstone = MetadataEnvelope.tombstone(schemaVersion: 1, updatedAtMillis: 7)

        #expect(tombstone.supersedes(live))
        #expect(live.supersedes(tombstone) == false)
    }

    @Test("ties between two live records resolve deterministically, not by argument order")
    func liveTiesAreDeterministic() {
        let a = MetadataEnvelope.live(payload: payload("aaa"), schemaVersion: 1, updatedAtMillis: 7)
        let b = MetadataEnvelope.live(payload: payload("bbb"), schemaVersion: 1, updatedAtMillis: 7)

        // Exactly one direction holds - whichever it is, every node computes the same answer.
        #expect(a.supersedes(b) != b.supersedes(a))
    }

    // MARK: - Migrations

    private var logger: Logger { Logger(label: "metadata-envelope-tests") }

    @Test("migration steps compose in ascending order across multiple versions")
    func stepsComposeInOrder() throws {
        let steps: [Int: MetadataMigrations.Step] = [
            1: { $0["trail"] = (($0["trail"] as? String) ?? "") + "1to2;" },
            2: { $0["trail"] = (($0["trail"] as? String) ?? "") + "2to3;" },
        ]
        let upgraded = MetadataMigrations.apply(
            payload: Data("{\"name\":\"x\"}".utf8), steps: steps, from: 1, to: 3,
            context: "test", logger: logger)

        let object = try #require(
            try JSONSerialization.jsonObject(with: upgraded) as? [String: Any])
        #expect(object["trail"] as? String == "1to2;2to3;")
        #expect(object["name"] as? String == "x", "untouched fields survive the upgrade")
    }

    @Test("a record already at the current version is returned untouched")
    func noOpWhenCurrent() {
        let original = Data("{\"a\":1}".utf8)
        let result = MetadataMigrations.apply(
            payload: original, steps: [1: { $0["mutated"] = true }], from: 2, to: 2,
            context: "test", logger: logger)
        #expect(result == original)
    }

    @Test("a record from a newer binary is passed through rather than dropped")
    func forwardVersionPassesThrough() {
        // The rolling-upgrade case: an old node reading a record a newer node wrote. Dropping it
        // would take a live credential out of service; Codable ignores unknown keys, so passing
        // it through is both safe and the only non-destructive option.
        let fromNewerBinary = Data("{\"a\":1,\"addedLater\":true}".utf8)
        let result = MetadataMigrations.apply(
            payload: fromNewerBinary, steps: [:], from: 5, to: 2, context: "test", logger: logger)
        #expect(result == fromNewerBinary)
    }

    @Test("an unparseable payload is left alone instead of being destroyed")
    func unparseablePayloadSurvives() {
        let garbage = Data([0x00, 0x01, 0x02])
        let result = MetadataMigrations.apply(
            payload: garbage, steps: [1: { $0["x"] = 1 }], from: 1, to: 2, context: "test",
            logger: logger)
        #expect(result == garbage)
    }

    @Test("collections default to the base schema version")
    func defaultSchemaVersion() {
        #expect(MetadataMigrations.currentVersion(for: MetadataCollections.users)
            == MetadataMigrations.baseVersion)
        #expect(
            MetadataMigrations.needsPersistedUpgrade(
                collection: MetadataCollections.users,
                storedVersion: MetadataMigrations.baseVersion) == false)
    }

    // MARK: - Tombstone policy

    @Test("only the collections that cannot resurrect data are exempt from tombstones")
    func exemptCollections() {
        #expect(MetadataCollections.tombstoneExempt.contains(MetadataCollections.clusterNodes))
        #expect(MetadataCollections.tombstoneExempt.contains(MetadataCollections.oidcStates))
        // The credential-bearing collections must never be exempt - this is the security property.
        #expect(MetadataCollections.tombstoneExempt.contains(MetadataCollections.accessKeys) == false)
        #expect(MetadataCollections.tombstoneExempt.contains(MetadataCollections.users) == false)
        #expect(MetadataCollections.tombstoneExempt.contains(MetadataCollections.buckets) == false)
    }

    @Test("every collection is covered by the maintenance sweeps")
    func allCollectionsSwept() {
        // A collection added to the store but forgotten in `all` would silently never have its
        // tombstones reclaimed or its schema upgraded.
        for collection in [
            MetadataCollections.users, MetadataCollections.usersByUsername,
            MetadataCollections.accessKeys, MetadataCollections.sharedLinks,
            MetadataCollections.buckets, MetadataCollections.clusterNodes,
            MetadataCollections.oidcProviders, MetadataCollections.oidcStates,
        ] {
            #expect(MetadataCollections.all.contains(collection), "\(collection) is not swept")
        }
    }
}
