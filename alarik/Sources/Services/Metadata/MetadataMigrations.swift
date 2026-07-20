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

/// Schema evolution for `MetadataStore` records.
///
/// Records upgrade lazily: a read applies any outstanding steps in memory, and a background sweep
/// (`MetadataMigrationSweep`) persists them. Deliberately not a big-bang migration at boot - in a
/// cluster there is no safe single moment to run one, and during a rolling upgrade both binary
/// versions are live at once.
///
/// ## Adding a migration
///
/// Say `User` gains a required `email` field. Two edits:
///
/// 1. Add a step keyed by the version it upgrades *from*:
///    ```swift
///    MetadataCollections.users: [
///        1: { object in object["email"] = object["email"] ?? "" }
///    ]
///    ```
/// 2. Bump the collection to the new version:
///    ```swift
///    MetadataCollections.users: 2
///    ```
///
/// Steps run in ascending order, so 1 -> 2 -> 3 composes automatically. Keep every step
/// total (never fail) and idempotent; it may run on a record another node is upgrading too.
enum MetadataMigrations {
    /// One upgrade step, mutating the decoded record JSON in place.
    typealias Step = @Sendable (inout [String: Any]) -> Void

    /// Version every collection starts at, and the version of any collection not listed in
    /// `currentVersions`.
    static let baseVersion = 1

    /// Ordered upgrade steps per collection, keyed by the version each step upgrades *from*.
    /// Empty until the first schema change - the machinery is what matters, not its contents.
    private static let registry: [String: [Int: Step]] = [:]

    /// The schema version this binary writes for each collection. Unlisted collections are at
    /// `baseVersion`.
    private static let currentVersions: [String: Int] = [:]

    static func currentVersion(for collection: String) -> Int {
        currentVersions[collection] ?? baseVersion
    }

    /// Applies every outstanding step to `payload`, returning the upgraded bytes.
    ///
    /// A record written by a *newer* binary than this one is passed through untouched rather than
    /// dropped: `Codable` ignores unknown keys, so a forward-version record almost always still
    /// decodes correctly, and refusing it would take a live access key or bucket out of service
    /// during a rolling upgrade. It is logged so the mixed-version window is visible.
    static func upgrade(
        payload: Data, collection: String, storedVersion: Int, logger: Logger
    ) -> Data {
        apply(
            payload: payload, steps: registry[collection] ?? [:], from: storedVersion,
            to: currentVersion(for: collection), context: collection, logger: logger)
    }

    /// The pure core of `upgrade`, with the registry passed in rather than looked up - so the
    /// step-composition and forward-version rules can be exercised directly without a schema
    /// change existing in the real registry.
    static func apply(
        payload: Data, steps: [Int: Step], from storedVersion: Int, to target: Int,
        context: String, logger: Logger
    ) -> Data {
        guard storedVersion != target else { return payload }

        guard storedVersion < target else {
            logger.warning(
                "Metadata record in '\(context)' has schema version \(storedVersion), newer than this node's \(target) - passing through unmigrated. Expected only during a rolling upgrade."
            )
            return payload
        }

        guard !steps.isEmpty else { return payload }
        guard var object = try? JSONSerialization.jsonObject(with: payload) as? [String: Any]
        else {
            logger.error(
                "Could not parse a '\(context)' record for migration from version \(storedVersion) - leaving it unmigrated."
            )
            return payload
        }

        for version in storedVersion..<target {
            steps[version]?(&object)
        }

        guard let upgraded = try? JSONSerialization.data(withJSONObject: object) else {
            logger.error(
                "Migrating a '\(context)' record from version \(storedVersion) produced invalid JSON - leaving it unmigrated."
            )
            return payload
        }
        return upgraded
    }

    /// Whether a stored record is behind the current schema and worth rewriting during the sweep.
    static func needsPersistedUpgrade(collection: String, storedVersion: Int) -> Bool {
        storedVersion < currentVersion(for: collection)
    }
}
