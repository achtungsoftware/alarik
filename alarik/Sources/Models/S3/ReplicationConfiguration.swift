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

/// A remote S3-compatible destination a bucket can replicate to - endpoint, destination
/// bucket, and its own credentials. Scoped to one source bucket rather than a reusable
/// account-wide credential library.
struct ReplicationTarget: Codable, Equatable {
    var id: UUID
    /// Base endpoint of the remote S3-compatible service. http/https only; private-range
    /// targets require an admin owner (SSRF guard - enforced at save time, see
    /// InternalBucketController), same rule as webhook URLs.
    var endpoint: String
    var targetBucket: String
    var accessKeyId: String
    var secretAccessKey: String
    /// Region to sign requests for - must exactly match the target's own configured region.
    /// Real AWS S3 always validates this; a target Alarik instance does too (`ALARIK_REGION`,
    /// see `AlarikRegion`), so this can't be left as a fire-and-forget default.
    var region: String
    var enabled: Bool

    /// The all-zeros UUID a client sends for a not-yet-persisted target; the server assigns a
    /// real id on save so target ids are always server-owned.
    static let zeroUUID = UUID(uuid: (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    /// Stable ARN identity for this target, surfaced in the S3 `?replication` XML response's
    /// `Destination.Bucket` field - reuses the exact `arn:alarik:*:::{id}` scheme
    /// `NotificationConfiguration` already established for webhook rule ARNs.
    var arn: String { "arn:alarik:replication:::\(id.uuidString)" }
}

/// One replication rule: which objects (by key prefix) get pushed to which target, and what
/// operations are mirrored. References a target by id rather than embedding connection details
/// itself - multiple rules on one bucket can share a target.
struct ReplicationRule: Codable, Equatable {
    var id: UUID
    var targetId: UUID
    /// Optional key prefix filter - empty/nil means the whole bucket.
    var prefix: String?
    /// Whether permanent deletes and delete-marker creation are mirrored to the target.
    /// Defaults to false - replicating deletes is opt-in, never a silent default, since a bug
    /// or misconfiguration here can destroy the remote copy.
    var replicateDeletes: Bool
    /// Whether objects that already existed before this rule was created/enabled should be
    /// replicated. Doesn't happen automatically even when true - see the resync endpoint,
    /// which is the explicit "do it now" trigger.
    var replicateExisting: Bool
    /// Whether the triggering PUT/DELETE waits for this rule's delivery before responding to
    /// the client (up to `ReplicationService.synchronousTimeout`), falling back to the normal
    /// async outbox on failure or timeout - see `ReplicationService.enqueue`. The local write
    /// itself is never blocked or failed by this, only delayed; a slow/unreachable target costs
    /// latency here, never correctness. Defaults to `false` (async)
    var synchronous: Bool
    var enabled: Bool

    static let zeroUUID = ReplicationTarget.zeroUUID

    init(
        id: UUID, targetId: UUID, prefix: String?, replicateDeletes: Bool,
        replicateExisting: Bool, synchronous: Bool = false, enabled: Bool
    ) {
        self.id = id
        self.targetId = targetId
        self.prefix = prefix
        self.replicateDeletes = replicateDeletes
        self.replicateExisting = replicateExisting
        self.synchronous = synchronous
        self.enabled = enabled
    }

    // Custom Decodable: `synchronous` must tolerate being absent from JSON saved by an older
    // version of this struct (Swift's synthesized Decodable only does that for Optional
    // properties - see ObjectMeta.tags for the same constraint) rather than failing the whole
    // bucket's replication config to decode.
    enum CodingKeys: String, CodingKey {
        case id, targetId, prefix, replicateDeletes, replicateExisting, synchronous, enabled
    }

    init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(UUID.self, forKey: .id)
        targetId = try container.decode(UUID.self, forKey: .targetId)
        prefix = try container.decodeIfPresent(String.self, forKey: .prefix)
        replicateDeletes = try container.decode(Bool.self, forKey: .replicateDeletes)
        replicateExisting = try container.decode(Bool.self, forKey: .replicateExisting)
        synchronous = try container.decodeIfPresent(Bool.self, forKey: .synchronous) ?? false
        enabled = try container.decode(Bool.self, forKey: .enabled)
    }

    /// Whether this rule wants replication for `key` - prefix-only (no event-type filtering
    /// like webhook rules have; replication cares whether the key was touched, not why).
    func matches(key: String) -> Bool {
        guard enabled else { return false }
        if let prefix, !prefix.isEmpty, !key.hasPrefix(prefix) { return false }
        return true
    }
}

/// A bucket's full replication configuration (`?replication` subresource / console
/// "Replication" settings). Stored JSON-encoded on the bucket row, like tags, lifecycle rules,
/// and webhook notification rules.
struct ReplicationConfiguration: Codable, Equatable {
    var targets: [ReplicationTarget]
    var rules: [ReplicationRule]

    static let maxTargetCount = 4
    static let maxRuleCount = 4

    static let empty = ReplicationConfiguration(targets: [], rules: [])

    func target(for id: UUID) -> ReplicationTarget? {
        targets.first { $0.id == id }
    }

    func toJSON() -> String {
        guard let data = try? JSONEncoder().encode(self),
            let json = String(data: data, encoding: .utf8)
        else {
            return #"{"targets":[],"rules":[]}"#
        }
        return json
    }

    static func fromJSON(_ json: String) -> ReplicationConfiguration {
        guard let data = json.data(using: .utf8),
            let config = try? JSONDecoder().decode(ReplicationConfiguration.self, from: data)
        else {
            return .empty
        }
        return config
    }

    /// Builds the S3 `GET ?replication` response, AWS's `ReplicationConfiguration` XML shape.
    /// Only enabled rules with a resolvable target are emitted. An empty configuration is an
    /// empty root element, matching how `?notification`/`?lifecycle` behave when unset.
    func toXML() -> String {
        // Explicit `String` typing throughout: SQLKit's SQLQueryString is (transitively)
        // ExpressibleByStringInterpolation and visible in this module, so unannotated string
        // literals here can otherwise resolve to it instead of String (see
        // NotificationConfiguration.toXML, which hit this exact bug).
        let ruleElements: String = rules.compactMap { (rule: ReplicationRule) -> String? in
            guard rule.enabled, let target = target(for: rule.targetId) else { return nil }

            let prefixXML: String =
                rule.prefix.flatMap { $0.isEmpty ? nil : "<Filter><Prefix>\($0.xmlEscaped)</Prefix></Filter>" }
                ?? "<Filter><Prefix></Prefix></Filter>"

            let deleteMarkerStatus = rule.replicateDeletes ? "Enabled" : "Disabled"
            let id: String = rule.id.uuidString

            return """
                <Rule>\
                <ID>\(id)</ID>\
                <Status>Enabled</Status>\
                \(prefixXML)\
                <DeleteMarkerReplication><Status>\(deleteMarkerStatus)</Status></DeleteMarkerReplication>\
                <Destination><Bucket>\(target.arn.xmlEscaped)</Bucket></Destination>\
                </Rule>
                """
        }.joined()

        return
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ReplicationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\(ruleElements)</ReplicationConfiguration>"
    }
}
