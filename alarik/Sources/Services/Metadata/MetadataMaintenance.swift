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
import Vapor

/// Background upkeep for `MetadataStore`: expiring tombstones once they've outlived their purpose,
/// and persisting schema upgrades that reads have been applying in memory.
///
/// Both passes walk only this node's **local** records, never a cluster-wide listing - so the cost
/// per node stays proportional to what it stores rather than to how many nodes exist, which is what
/// keeps this viable at large cluster sizes.
///
/// Discovery is any-shard-index (see `ErasureCodedObjectHandler.listLocalShardEntries`), so with
/// replicated metadata every holder sweeps every record it has - `replicationFactor` passes per
/// record, not one. That redundancy is deliberate rather than merely tolerated: it means neither
/// sweep depends on one particular node being up. Both operations converge under repetition - a
/// purge routes to the record's coordinator, and a migration rewrite carries the original
/// `updatedAtMillis`, so re-running one is a no-op rather than a conflicting write.
enum MetadataMaintenance {
    /// How long a tombstone is kept before its bytes are reclaimed.
    ///
    /// This is the **maximum time a node may be offline and still rejoin safely**. A node away
    /// longer than this can return holding a record whose tombstone has already been collected,
    /// with nothing left to say it was deleted - the resurrection this whole mechanism exists to
    /// prevent. Raise it if your replacement/repair cycle is slower than the default week.
    static let defaultGraceDays = 7

    /// `CLUSTER_METADATA_TOMBSTONE_GRACE_SECONDS` wins when set - it exists so tests can compress
    /// a week into seconds. `CLUSTER_METADATA_TOMBSTONE_GRACE_DAYS` is the operator-facing knob.
    static func tombstoneGrace() -> TimeInterval {
        if let raw = Environment.sanitizedGet("CLUSTER_METADATA_TOMBSTONE_GRACE_SECONDS"),
            let seconds = TimeInterval(raw), seconds >= 0
        {
            return seconds
        }
        if let raw = Environment.sanitizedGet("CLUSTER_METADATA_TOMBSTONE_GRACE_DAYS"),
            let days = TimeInterval(raw), days >= 0
        {
            return days * 24 * 3600
        }
        return TimeInterval(defaultGraceDays) * 24 * 3600
    }

    /// Reclaims tombstones that have outlived the grace period.
    static func runTombstoneGC(app: Application) async {
        let cutoffMillis =
            MetadataEnvelope.nowMillis() - Int64(tombstoneGrace() * 1000)
        var reclaimed = 0

        for collection in MetadataCollections.all
        where !MetadataCollections.tombstoneExempt.contains(collection) {
            for (id, envelope) in await localEnvelopes(app: app, collection: collection) {
                guard envelope.isTombstone, envelope.updatedAtMillis < cutoffMillis else { continue }
                do {
                    try await MetadataStore.purge(app: app, collection: collection, id: id)
                    reclaimed += 1
                } catch {
                    // Best-effort: a tombstone that fails to reclaim is only wasted bytes, and
                    // the next pass retries it. Never worth failing the sweep over.
                    app.logger.warning(
                        "Could not reclaim expired tombstone '\(collection)/\(id)': \(error)")
                }
            }
        }

        if reclaimed > 0 {
            app.logger.info("Reclaimed \(reclaimed) expired metadata tombstone(s).")
        }
    }

    /// Persists schema upgrades that `MetadataStore.get` has been applying in memory, so the work
    /// happens once rather than on every read forever.
    static func runMigrationSweep(app: Application) async {
        var upgraded = 0

        for collection in MetadataCollections.all {
            for (id, envelope) in await localEnvelopes(app: app, collection: collection) {
                guard !envelope.isTombstone, let payload = envelope.payload else { continue }
                guard
                    MetadataMigrations.needsPersistedUpgrade(
                        collection: collection, storedVersion: envelope.schemaVersion)
                else { continue }

                let migrated = MetadataMigrations.upgrade(
                    payload: payload, collection: collection,
                    storedVersion: envelope.schemaVersion, logger: app.logger)

                // `updatedAtMillis` is carried over deliberately. A schema upgrade is a
                // representation change, not a new write - re-stamping it would let a migrating
                // node's copy beat a genuinely newer write made elsewhere.
                let rewritten = MetadataEnvelope(
                    format: MetadataEnvelope.currentFormat,
                    schemaVersion: MetadataMigrations.currentVersion(for: collection),
                    updatedAtMillis: envelope.updatedAtMillis,
                    deleted: false,
                    payload: migrated)
                do {
                    try await MetadataStore.putEnvelope(
                        app: app, collection: collection, id: id, envelope: rewritten)
                    upgraded += 1
                } catch {
                    app.logger.warning(
                        "Could not persist schema upgrade for '\(collection)/\(id)': \(error)")
                }
            }
        }

        if upgraded > 0 {
            app.logger.info("Persisted \(upgraded) metadata schema upgrade(s).")
        }
    }

    /// This node's own shard-0 records for `collection`, paired with their envelopes.
    private static func localEnvelopes(
        app: Application, collection: String
    ) async -> [(id: String, envelope: MetadataEnvelope)] {
        await MetadataListingService.localEnvelopeEntries(app: app, collection: collection)
            .map { (id: $0.id, envelope: $0.envelope) }
    }
}
