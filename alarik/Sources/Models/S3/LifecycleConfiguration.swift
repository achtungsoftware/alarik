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

/// A single Lifecycle rule, scoped to the most useful, implementable subset of the AWS spec
/// (verified against the PutBucketLifecycleConfiguration API reference) - Prefix-only filters,
/// no tag/size-based filtering, no Transition actions (no storage classes exist to transition
/// to). `prefix` is empty for "the whole bucket".
struct LifecycleRule: Codable, Equatable {
    var id: String
    var enabled: Bool
    var prefix: String
    var expirationDays: Int?
    var noncurrentVersionExpirationDays: Int?
    var abortIncompleteMultipartUploadDays: Int?
}

struct LifecycleConfiguration: Equatable {
    var rules: [LifecycleRule]

    /// Real S3 elements this system doesn't support - rejected explicitly at parse time rather
    /// than silently accepted and ignored, mirroring `BucketPolicy.parseAndValidate`'s philosophy.
    private static let unsupportedElementNames = [
        "Transition", "NoncurrentVersionTransition", "Tag", "And",
        "ObjectSizeGreaterThan", "ObjectSizeLessThan", "ExpiredObjectDeleteMarker",
    ]

    /// Parses a `LifecycleConfiguration` request body. Matches the block-regex-extraction style
    /// already used for Tagging's `<Tag>` blocks / multipart's `<Part>` blocks.
    static func parse(xml: String, requestId: String) throws -> LifecycleConfiguration {
        for name in unsupportedElementNames {
            if xml.contains("<\(name)>") || xml.contains("<\(name)/>") {
                throw S3Error(
                    status: .badRequest, code: "MalformedXML",
                    message:
                        "Unsupported lifecycle element <\(name)> - only Prefix-based filters, Expiration.Days, NoncurrentVersionExpiration.NoncurrentDays, and AbortIncompleteMultipartUpload.DaysAfterInitiation are currently supported.",
                    requestId: requestId)
            }
        }

        let ruleBlockPattern = #"<Rule>(.*?)</Rule>"#
        let ruleBlockRegex = try NSRegularExpression(
            pattern: ruleBlockPattern, options: [.dotMatchesLineSeparators])
        let range = NSRange(xml.startIndex..., in: xml)
        let blocks = ruleBlockRegex.matches(in: xml, options: [], range: range)

        var rules: [LifecycleRule] = []

        for block in blocks {
            guard let blockRange = Range(block.range(at: 1), in: xml) else { continue }
            let content = String(xml[blockRange])

            guard let status = extract(tag: "Status", from: content),
                status == "Enabled" || status == "Disabled"
            else {
                throw S3Error(
                    status: .badRequest, code: "MalformedXML",
                    message: "Each Rule must have a Status of \"Enabled\" or \"Disabled\".",
                    requestId: requestId)
            }

            let id = extract(tag: "ID", from: content) ?? UUID().uuidString
            let prefix = extractPrefix(from: content)
            let expirationDays = extractNestedInt(outer: "Expiration", inner: "Days", from: content)
            let noncurrentDays = extractNestedInt(
                outer: "NoncurrentVersionExpiration", inner: "NoncurrentDays", from: content)
            let abortDays = extractNestedInt(
                outer: "AbortIncompleteMultipartUpload", inner: "DaysAfterInitiation",
                from: content)

            guard expirationDays != nil || noncurrentDays != nil || abortDays != nil else {
                throw S3Error(
                    status: .badRequest, code: "MalformedXML",
                    message:
                        "Each Rule must specify at least one supported action (Expiration, NoncurrentVersionExpiration, or AbortIncompleteMultipartUpload).",
                    requestId: requestId)
            }

            rules.append(
                LifecycleRule(
                    id: id, enabled: status == "Enabled", prefix: prefix,
                    expirationDays: expirationDays,
                    noncurrentVersionExpirationDays: noncurrentDays,
                    abortIncompleteMultipartUploadDays: abortDays))
        }

        guard !rules.isEmpty else {
            throw S3Error(
                status: .badRequest, code: "MalformedXML",
                message: "LifecycleConfiguration must contain at least one Rule.",
                requestId: requestId)
        }

        return LifecycleConfiguration(rules: rules)
    }

    private static func extract(tag: String, from xml: String) -> String? {
        guard
            let regex = try? NSRegularExpression(
                pattern: "<\(tag)>\\s*(.*?)\\s*</\(tag)>", options: [.dotMatchesLineSeparators])
        else { return nil }
        let range = NSRange(xml.startIndex..., in: xml)
        guard let match = regex.firstMatch(in: xml, options: [], range: range),
            let valueRange = Range(match.range(at: 1), in: xml)
        else { return nil }
        return String(xml[valueRange])
    }

    /// Prefers the modern `<Filter><Prefix>` nesting, falling back to the legacy bare `<Prefix>`
    /// directly under `<Rule>` for backward compatibility (both verified against the
    /// PutBucketLifecycleConfiguration API reference). Absent entirely means "whole bucket".
    private static func extractPrefix(from xml: String) -> String {
        if let filterContent = extract(tag: "Filter", from: xml),
            let prefix = extract(tag: "Prefix", from: filterContent)
        {
            return prefix
        }
        return extract(tag: "Prefix", from: xml) ?? ""
    }

    private static func extractNestedInt(outer: String, inner: String, from xml: String) -> Int? {
        guard let outerContent = extract(tag: outer, from: xml),
            let innerValue = extract(tag: inner, from: outerContent)
        else { return nil }
        return Int(innerValue)
    }

    /// Builds the `GetBucketLifecycleConfiguration` response XML.
    func toXML() -> String {
        let ruleElements = rules.map { rule -> String in
            var inner = "<ID>\(rule.id.xmlEscaped)</ID>"
            inner += "<Filter><Prefix>\(rule.prefix.xmlEscaped)</Prefix></Filter>"
            inner += "<Status>\(rule.enabled ? "Enabled" : "Disabled")</Status>"
            if let days = rule.expirationDays {
                inner += "<Expiration><Days>\(days)</Days></Expiration>"
            }
            if let days = rule.noncurrentVersionExpirationDays {
                inner +=
                    "<NoncurrentVersionExpiration><NoncurrentDays>\(days)</NoncurrentDays></NoncurrentVersionExpiration>"
            }
            if let days = rule.abortIncompleteMultipartUploadDays {
                inner +=
                    "<AbortIncompleteMultipartUpload><DaysAfterInitiation>\(days)</DaysAfterInitiation></AbortIncompleteMultipartUpload>"
            }
            return "<Rule>\(inner)</Rule>"
        }.joined()

        return
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\(ruleElements)</LifecycleConfiguration>"
    }

    /// JSON-encoded storage representation - parsed rules, not raw XML passthrough like
    /// `BucketPolicy`, since these need to be evaluated programmatically by `LifecycleService`.
    func toJSON() -> String {
        guard let data = try? JSONEncoder().encode(rules),
            let json = String(data: data, encoding: .utf8)
        else {
            return "[]"
        }
        return json
    }

    static func fromJSON(_ json: String) -> LifecycleConfiguration {
        guard let data = json.data(using: .utf8),
            let rules = try? JSONDecoder().decode([LifecycleRule].self, from: data)
        else {
            return LifecycleConfiguration(rules: [])
        }
        return LifecycleConfiguration(rules: rules)
    }
}
