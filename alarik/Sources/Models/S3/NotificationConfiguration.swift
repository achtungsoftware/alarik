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

/// One webhook rule of a bucket's notification configuration. Unlike AWS (which can only
/// target SNS/SQS/Lambda ARNs), Alarik delivers events straight to an HTTP(S) endpoint.
struct NotificationRule: Codable, Equatable {
    var id: UUID
    /// Delivery endpoint. http/https only; private-range targets require an admin owner
    /// (SSRF guard - enforced at save time, see InternalBucketController).
    var url: String
    /// Optional shared secret. When set, every delivery carries
    /// `X-Alarik-Signature-256: <hex hmacSHA256(body, secret)>` (GitHub-webhook style).
    var secret: String?
    /// Subscribed event types, e.g. "s3:ObjectCreated:*" or "s3:ObjectRemoved:Delete".
    var events: [String]
    /// Optional key filters, matching S3's prefix/suffix filter rules.
    var prefix: String?
    var suffix: String?
    var enabled: Bool

    /// The all-zeros UUID a client sends for a not-yet-persisted rule; the server assigns a
    /// real id on save so rule ids are always server-owned.
    static let zeroUUID = UUID(uuid: (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    /// All event types Alarik can emit. Wildcard family variants ("s3:ObjectCreated:*") are
    /// accepted in `events` and match every member of the family.
    static let supportedEvents: Set<String> = [
        "s3:ObjectCreated:*",
        "s3:ObjectCreated:Put",
        "s3:ObjectCreated:Copy",
        "s3:ObjectCreated:CompleteMultipartUpload",
        "s3:ObjectRemoved:*",
        "s3:ObjectRemoved:Delete",
        "s3:ObjectRemoved:DeleteMarkerCreated",
        "s3:LifecycleExpiration:*",
        "s3:LifecycleExpiration:Delete",
        "s3:LifecycleExpiration:DeleteMarkerCreated",
    ]

    /// Whether this rule wants `eventName` (full form, e.g. "s3:ObjectCreated:Put") for `key`.
    func matches(eventName: String, key: String) -> Bool {
        guard enabled else { return false }

        let eventMatches = events.contains { subscribed in
            if subscribed == eventName { return true }
            if subscribed.hasSuffix(":*") {
                return eventName.hasPrefix(String(subscribed.dropLast(1)))
            }
            return false
        }
        guard eventMatches else { return false }

        if let prefix, !prefix.isEmpty, !key.hasPrefix(prefix) { return false }
        if let suffix, !suffix.isEmpty, !key.hasSuffix(suffix) { return false }
        return true
    }
}

/// A bucket's full notification configuration (`?notification` subresource / console
/// "Webhooks" settings). Stored JSON-encoded on the bucket row, like tags and lifecycle rules.
struct NotificationConfiguration: Codable, Equatable {
    var rules: [NotificationRule]

    /// Real S3 caps notification configurations at 100 rules; Alarik uses a tighter,
    /// self-hosting-appropriate limit.
    static let maxRuleCount = 16

    static let empty = NotificationConfiguration(rules: [])

    func toJSON() -> String {
        guard let data = try? JSONEncoder().encode(self),
            let json = String(data: data, encoding: .utf8)
        else {
            return #"{"rules":[]}"#
        }
        return json
    }

    static func fromJSON(_ json: String) -> NotificationConfiguration {
        guard let data = json.data(using: .utf8),
            let config = try? JSONDecoder().decode(NotificationConfiguration.self, from: data)
        else {
            return .empty
        }
        return config
    }

    /// Builds the S3 `GET ?notification` response. Webhook rules are surfaced as
    /// QueueConfigurations with an `arn:alarik:webhook:::{id}` ARN - S3's XML shape has no
    /// URL field, so the ARN carries the rule identity. An empty configuration is an empty
    /// root element, per the spec.
    func toXML() -> String {
        // Explicit `String` typing throughout: SQLKit's SQLQueryString is (transitively)
        // ExpressibleByStringInterpolation and visible in this module, so unannotated string
        // literals here can otherwise resolve to it instead of String.
        let ruleElements: String = rules.map { (rule: NotificationRule) -> String in
            var filterRules: [String] = []
            if let prefix = rule.prefix, !prefix.isEmpty {
                filterRules.append(
                    "<FilterRule><Name>prefix</Name><Value>\(prefix.xmlEscaped)</Value></FilterRule>")
            }
            if let suffix = rule.suffix, !suffix.isEmpty {
                filterRules.append(
                    "<FilterRule><Name>suffix</Name><Value>\(suffix.xmlEscaped)</Value></FilterRule>")
            }
            let filterXML: String =
                filterRules.isEmpty ? "" : "<Filter><S3Key>\(filterRules.joined())</S3Key></Filter>"

            let eventElements: String = rule.events.map { (event: String) -> String in
                "<Event>\(event.xmlEscaped)</Event>"
            }.joined()

            let id: String = rule.id.uuidString
            return
                "<QueueConfiguration><Id>\(id)</Id><Queue>arn:alarik:webhook:::\(id)</Queue>\(eventElements)\(filterXML)</QueueConfiguration>"
        }.joined()

        return
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\(ruleElements)</NotificationConfiguration>"
    }
}
