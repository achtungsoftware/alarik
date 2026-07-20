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

/// The on-disk wrapper around every `MetadataStore` record: the record's own bytes (`payload`)
/// plus the metadata needed to reconcile replicas that disagree.
///
/// Three properties this exists for, none of which a bare record can express:
/// - **Tombstones** (`deleted`): a delete stores a marker rather than removing bytes, so a replica
///   that was offline during the delete loses to the tombstone on return instead of resurrecting
///   the record. Without this, a revoked access key comes back to life.
/// - **Last-writer-wins** (`updatedAtMillis`): gives replica disagreement a deterministic winner.
/// - **Schema evolution** (`schemaVersion`): lets a record be upgraded on read instead of failing
///   to decode and being silently dropped.
struct MetadataEnvelope: Sendable, Equatable {
    /// Current envelope *format* version - the wrapper's own shape, independent of the record
    /// schema versions tracked per collection in `MetadataMigrations`.
    static let currentFormat = 1

    var format: Int
    var schemaVersion: Int
    /// Milliseconds since the Unix epoch. Deliberately a plain integer rather than a `Date`: the
    /// value is compared across nodes and across encoder configurations, so it must not depend on
    /// any `JSONEncoder.DateEncodingStrategy` agreeing cluster-wide.
    var updatedAtMillis: Int64
    var deleted: Bool
    /// The record's own encoded bytes. `nil` exactly when `deleted` - a tombstone carries no body.
    var payload: Data?

    var updatedAt: Date { Date(timeIntervalSince1970: Double(updatedAtMillis) / 1000) }

    var isTombstone: Bool { deleted }
}

// MARK: - Construction

extension MetadataEnvelope {
    static func nowMillis() -> Int64 { Int64(Date().timeIntervalSince1970 * 1000) }

    static func live(
        payload: Data, schemaVersion: Int, updatedAtMillis: Int64 = nowMillis()
    ) -> MetadataEnvelope {
        MetadataEnvelope(
            format: currentFormat, schemaVersion: schemaVersion,
            updatedAtMillis: updatedAtMillis, deleted: false, payload: payload)
    }

    static func tombstone(
        schemaVersion: Int, updatedAtMillis: Int64 = nowMillis()
    ) -> MetadataEnvelope {
        MetadataEnvelope(
            format: currentFormat, schemaVersion: schemaVersion,
            updatedAtMillis: updatedAtMillis, deleted: true, payload: nil)
    }
}

// MARK: - Conflict resolution

extension MetadataEnvelope {
    /// Whether `self` should win over `other` when two replicas disagree about the same key.
    ///
    /// Newest write wins. On an exact timestamp tie a tombstone wins, deliberately: the two
    /// outcomes are "a record briefly reappears" and "a record stays deleted", and only the first
    /// is a security problem. Remaining ties fall back to a payload byte comparison purely so the
    /// result is deterministic across nodes rather than dependent on gather order.
    func supersedes(_ other: MetadataEnvelope) -> Bool {
        if updatedAtMillis != other.updatedAtMillis {
            return updatedAtMillis > other.updatedAtMillis
        }
        if deleted != other.deleted { return deleted }
        return (payload ?? Data()).lexicographicallyPrecedes(other.payload ?? Data())
    }
}

// MARK: - Wire format

extension MetadataEnvelope {
    /// Marker key identifying wrapped bytes. Detection keys off this rather than "does an envelope
    /// decode succeed", so a bare legacy record whose fields happen to line up can never be
    /// mistaken for an envelope.
    private static let markerKey = "_alarik_env"

    private enum Key {
        static let schemaVersion = "schemaVersion"
        static let updatedAtMillis = "updatedAtMillis"
        static let deleted = "deleted"
        static let payload = "payload"
    }

    func encoded() throws -> Data {
        var object: [String: Any] = [
            Self.markerKey: format,
            Key.schemaVersion: schemaVersion,
            Key.updatedAtMillis: updatedAtMillis,
            Key.deleted: deleted,
        ]
        if let payload { object[Key.payload] = payload.base64EncodedString() }
        return try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
    }

    /// Parses stored bytes. Bytes written before envelopes existed are read as a legacy live
    /// record at schema version 1 with a zero timestamp, so an unwrapped record loses to any
    /// rewritten one and existing deployments keep working rather than reading back as corrupt.
    static func decode(_ data: Data) -> MetadataEnvelope {
        guard
            let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
            let format = object[markerKey] as? Int
        else {
            return MetadataEnvelope(
                format: 0, schemaVersion: 1, updatedAtMillis: 0, deleted: false, payload: data)
        }

        let deleted = object[Key.deleted] as? Bool ?? false
        let payload = (object[Key.payload] as? String).flatMap { Data(base64Encoded: $0) }
        return MetadataEnvelope(
            format: format,
            schemaVersion: object[Key.schemaVersion] as? Int ?? 1,
            updatedAtMillis: (object[Key.updatedAtMillis] as? NSNumber)?.int64Value ?? 0,
            deleted: deleted,
            payload: deleted ? nil : payload)
    }

    /// True when `data` was written before envelopes existed - used only to decide whether a
    /// record is worth rewriting during the migration sweep.
    static func isLegacyBare(_ data: Data) -> Bool {
        guard let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            return true
        }
        return object[markerKey] == nil
    }
}
