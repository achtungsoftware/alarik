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

/// One `<CORSRule>` (verified against the CORSRule API reference). Origins and headers take a
/// single `*` wildcard each; methods are the five S3 allows.
struct CORSRule: Codable, Equatable {
    var id: String?
    var allowedOrigins: [String]
    var allowedMethods: [String]
    var allowedHeaders: [String]
    var exposeHeaders: [String]
    var maxAgeSeconds: Int?

    init(
        id: String? = nil, allowedOrigins: [String], allowedMethods: [String],
        allowedHeaders: [String] = [], exposeHeaders: [String] = [], maxAgeSeconds: Int? = nil
    ) {
        self.id = id
        self.allowedOrigins = allowedOrigins
        self.allowedMethods = allowedMethods
        self.allowedHeaders = allowedHeaders
        self.exposeHeaders = exposeHeaders
        self.maxAgeSeconds = maxAgeSeconds
    }

    /// Whether this rule admits the request: the origin and method must match, and every
    /// header the preflight asks for must be covered.
    func admits(origin: String, method: String, requestHeaders: [String]) -> Bool {
        guard allowedMethods.contains(method.uppercased()) else { return false }
        guard allowedOrigins.contains(where: { CORSConfiguration.matches(pattern: $0, origin) })
        else { return false }
        return requestHeaders.allSatisfy { header in
            allowedHeaders.contains {
                CORSConfiguration.matches(pattern: $0.lowercased(), header.lowercased())
            }
        }
    }
}

struct CORSConfiguration: Equatable {
    var rules: [CORSRule]

    /// S3's own ceiling (verified against the PutBucketCors API reference).
    static let maxRuleCount = 100
    static let allowedMethodNames: Set<String> = ["GET", "PUT", "HEAD", "POST", "DELETE"]

    struct ValidationError: Error {
        let message: String
    }

    /// The checks every rule set must pass, whichever API delivers it; the S3 XML parser and
    /// the console endpoint share this.
    static func validate(_ rules: [CORSRule]) throws {
        guard !rules.isEmpty else {
            throw ValidationError(message: "CORSConfiguration must contain at least one CORSRule.")
        }
        guard rules.count <= maxRuleCount else {
            throw ValidationError(
                message: "CORSConfiguration may contain at most \(maxRuleCount) CORSRules.")
        }
        for rule in rules {
            if let id = rule.id, id.utf8.count > 255 {
                throw ValidationError(message: "A CORSRule ID may be at most 255 characters long.")
            }
            guard !rule.allowedOrigins.isEmpty else {
                throw ValidationError(message: "Each CORSRule must have at least one AllowedOrigin.")
            }
            guard !rule.allowedMethods.isEmpty else {
                throw ValidationError(message: "Each CORSRule must have at least one AllowedMethod.")
            }
            for method in rule.allowedMethods where !allowedMethodNames.contains(method) {
                throw ValidationError(
                    message:
                        "Found unsupported HTTP method \"\(method)\" in CORS config. Unsupported methods are: \(method)."
                )
            }
            for value in rule.allowedOrigins + rule.allowedHeaders {
                guard !value.isEmpty else {
                    throw ValidationError(message: "AllowedOrigin and AllowedHeader values cannot be empty.")
                }
                guard value.filter({ $0 == "*" }).count <= 1 else {
                    throw ValidationError(
                        message: "AllowedOrigin \"\(value)\" can not have more than one wildcard.")
                }
            }
            for value in rule.exposeHeaders where value.isEmpty || value.contains("*") {
                throw ValidationError(message: "ExposeHeader values cannot be empty or contain a wildcard.")
            }
            if let maxAge = rule.maxAgeSeconds, maxAge < 0 {
                throw ValidationError(message: "MaxAgeSeconds must be a non-negative integer.")
            }
        }
    }

    /// S3's single-`*` wildcard: the one `*` matches any run of characters, none included.
    static func matches(pattern: String, _ value: String) -> Bool {
        guard let star = pattern.firstIndex(of: "*") else { return pattern == value }
        let prefix = pattern[..<star]
        let suffix = pattern[pattern.index(after: star)...]
        return value.count >= prefix.count + suffix.count && value.hasPrefix(prefix)
            && value.hasSuffix(suffix)
    }

    /// The first rule admitting the request - S3 applies rules in order, first match wins.
    func rule(admitting origin: String, method: String, requestHeaders: [String] = []) -> CORSRule? {
        rules.first { $0.admits(origin: origin, method: method, requestHeaders: requestHeaders) }
    }

    // MARK: - XML

    static func parse(xml: String, requestId: String) throws -> CORSConfiguration {
        let blocks = elements(named: "CORSRule", in: xml)
        var rules: [CORSRule] = []
        for block in blocks {
            rules.append(
                CORSRule(
                    id: elements(named: "ID", in: block).first,
                    allowedOrigins: elements(named: "AllowedOrigin", in: block),
                    allowedMethods: elements(named: "AllowedMethod", in: block),
                    allowedHeaders: elements(named: "AllowedHeader", in: block),
                    exposeHeaders: elements(named: "ExposeHeader", in: block),
                    maxAgeSeconds: elements(named: "MaxAgeSeconds", in: block).first.flatMap {
                        Int($0)
                    }))
        }
        do {
            try validate(rules)
        } catch let error as ValidationError {
            throw S3Error(
                status: .badRequest, code: "MalformedXML", message: error.message,
                requestId: requestId)
        }
        return CORSConfiguration(rules: rules)
    }

    /// Every `<name>…</name>` in document order, unescaped and trimmed.
    private static func elements(named name: String, in xml: String) -> [String] {
        guard
            let regex = try? NSRegularExpression(
                pattern: "<\(name)>(.*?)</\(name)>", options: [.dotMatchesLineSeparators])
        else { return [] }
        let range = NSRange(xml.startIndex..., in: xml)
        return regex.matches(in: xml, options: [], range: range).compactMap { match in
            guard let valueRange = Range(match.range(at: 1), in: xml) else { return nil }
            return String(xml[valueRange]).trimmingCharacters(in: .whitespacesAndNewlines)
                .xmlUnescaped
        }
    }

    /// Builds the `GetBucketCors` response XML.
    func toXML() -> String {
        let ruleElements = rules.map { rule -> String in
            var inner = ""
            if let id = rule.id { inner += "<ID>\(id.xmlEscaped)</ID>" }
            inner += rule.allowedOrigins.map { "<AllowedOrigin>\($0.xmlEscaped)</AllowedOrigin>" }.joined()
            inner += rule.allowedMethods.map { "<AllowedMethod>\($0.xmlEscaped)</AllowedMethod>" }.joined()
            inner += rule.allowedHeaders.map { "<AllowedHeader>\($0.xmlEscaped)</AllowedHeader>" }.joined()
            inner += rule.exposeHeaders.map { "<ExposeHeader>\($0.xmlEscaped)</ExposeHeader>" }.joined()
            if let maxAge = rule.maxAgeSeconds { inner += "<MaxAgeSeconds>\(maxAge)</MaxAgeSeconds>" }
            return "<CORSRule>\(inner)</CORSRule>"
        }.joined()
        return
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CORSConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\(ruleElements)</CORSConfiguration>"
    }
}
