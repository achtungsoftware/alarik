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

/// A bucket's or an object's tag-set (`?tagging`). Shared between bucket and object tagging -
/// both use the identical `<Tagging><TagSet><Tag>...` XML shape (verified against the AWS API
/// reference for PutBucketTagging/PutObjectTagging).
struct Tagging: Equatable {
    let tags: [String: String]

    /// Real S3 limits objects to 10 tags - verified against the PutObjectTagging API reference.
    static let maxTagCount = 10

    /// Parses a `Tagging` request body, matching the block-regex-extraction style already used
    /// elsewhere for request XML (e.g. `parseCompleteMultipartUploadBody`'s `<Part>` blocks).
    static func parse(xml: String, requestId: String) throws -> Tagging {
        var tags: [String: String] = [:]

        let tagBlockPattern = #"<Tag>(.*?)</Tag>"#
        let tagBlockRegex = try NSRegularExpression(
            pattern: tagBlockPattern, options: [.dotMatchesLineSeparators])
        let range = NSRange(xml.startIndex..., in: xml)
        let blocks = tagBlockRegex.matches(in: xml, options: [], range: range)

        let keyPattern = #"<Key>\s*(.*?)\s*</Key>"#
        let valuePattern = #"<Value>\s*(.*?)\s*</Value>"#
        let keyRegex = try NSRegularExpression(
            pattern: keyPattern, options: [.dotMatchesLineSeparators])
        let valueRegex = try NSRegularExpression(
            pattern: valuePattern, options: [.dotMatchesLineSeparators])

        for block in blocks {
            guard let blockRange = Range(block.range(at: 1), in: xml) else { continue }
            let blockContent = String(xml[blockRange])
            let blockNSRange = NSRange(blockContent.startIndex..., in: blockContent)

            guard
                let keyMatch = keyRegex.firstMatch(in: blockContent, options: [], range: blockNSRange),
                let keyRange = Range(keyMatch.range(at: 1), in: blockContent)
            else { continue }
            let key = String(blockContent[keyRange]).xmlUnescaped

            var value = ""
            if let valueMatch = valueRegex.firstMatch(
                in: blockContent, options: [], range: blockNSRange),
                let valueRange = Range(valueMatch.range(at: 1), in: blockContent)
            {
                value = String(blockContent[valueRange]).xmlUnescaped
            }

            tags[key] = value
        }

        guard tags.count <= maxTagCount else {
            throw S3Error(
                status: .badRequest, code: "InvalidTag",
                message: "Object tags cannot be greater than \(maxTagCount).",
                requestId: requestId)
        }

        return Tagging(tags: tags)
    }

    /// Parses the `x-amz-tagging` PutObject header - URL query-string encoded, e.g.
    /// "key1=value1&key2=value2" (verified against the PutObject API reference).
    static func parseHeaderValue(_ value: String) -> Tagging {
        var tags: [String: String] = [:]
        for pair in value.split(separator: "&") where !pair.isEmpty {
            guard let eqIndex = pair.firstIndex(of: "=") else { continue }
            let rawKey = String(pair[..<eqIndex])
            let rawValue = String(pair[pair.index(after: eqIndex)...])
            let key = rawKey.removingPercentEncoding ?? rawKey
            let value = rawValue.removingPercentEncoding ?? rawValue
            tags[key] = value
        }
        return Tagging(tags: tags)
    }

    /// Builds the `GetBucketTagging`/`GetObjectTagging` response XML.
    func toXML() -> String {
        let tagElements = tags.map { key, value in
            "<Tag><Key>\(key.xmlEscaped)</Key><Value>\(value.xmlEscaped)</Value></Tag>"
        }.joined()
        return
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>\(tagElements)</TagSet></Tagging>"
    }

    /// JSON-encoded storage representation, mirroring how `BucketPolicy`/`policy` stores its
    /// raw form as a single string column.
    func toJSON() -> String {
        guard let data = try? JSONEncoder().encode(tags), let json = String(data: data, encoding: .utf8)
        else {
            return "{}"
        }
        return json
    }

    static func fromJSON(_ json: String) -> Tagging {
        guard let data = json.data(using: .utf8),
            let tags = try? JSONDecoder().decode([String: String].self, from: data)
        else {
            return Tagging(tags: [:])
        }
        return Tagging(tags: tags)
    }
}
