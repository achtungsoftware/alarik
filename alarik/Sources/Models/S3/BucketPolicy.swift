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

/// Actions a bucket policy is currently allowed to grant. Deliberately a small whitelist -
/// scoped to the "anonymous public read" use case. Extend this (and the auth call sites that
/// consult it) when broader policy support is needed.
enum S3PolicyAction: String, CaseIterable {
    case getObject = "s3:GetObject"
    case getObjectVersion = "s3:GetObjectVersion"
    case listBucket = "s3:ListBucket"
}

/// Modeled fully (not just `.allow`) so the type already has the shape for when Deny support
/// is added. `BucketPolicy.parseAndValidate` is what rejects `.deny` in v1, not this type.
enum PolicyEffect: String, Codable, Equatable {
    case allow = "Allow"
    case deny = "Deny"
}

/// AWS allows Principal to be the bare string "*" (anyone, including unauthenticated callers)
/// or an object like {"AWS": "..."} (a specific account/role). Only the bare "*" form is
/// supported in v1; anything else decodes to `.other` and is rejected at validation time.
enum PolicyPrincipal: Equatable, Codable {
    case anyone
    case other

    init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let value = try? container.decode(String.self), value == "*" {
            self = .anyone
        } else {
            self = .other
        }
    }

    func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .anyone: try container.encode("*")
        case .other: try container.encode("other")
        }
    }
}

/// A parsed `Resource` ARN (`arn:aws:s3:::bucket` or `arn:aws:s3:::bucket/key-pattern`),
/// pre-split at validation time so request-time matching is just string comparisons -
/// no per-request parsing or regex on the hot path.
struct ResourceMatcher: Equatable {
    enum KeyPattern: Equatable {
        case exact(String)
        case prefix(String)
    }

    let rawValue: String
    let bucketName: String
    /// nil means this resource targets the bucket itself (used by bucket-level actions like ListBucket)
    let keyPattern: KeyPattern?

    init(arn: String) {
        rawValue = arn
        let arnPrefix = "arn:aws:s3:::"
        let withoutPrefix = arn.hasPrefix(arnPrefix) ? String(arn.dropFirst(arnPrefix.count)) : arn

        guard let slashIndex = withoutPrefix.firstIndex(of: "/") else {
            bucketName = withoutPrefix
            keyPattern = nil
            return
        }

        bucketName = String(withoutPrefix[..<slashIndex])
        let keyPart = String(withoutPrefix[withoutPrefix.index(after: slashIndex)...])
        keyPattern = keyPart.hasSuffix("*") ? .prefix(String(keyPart.dropLast())) : .exact(keyPart)
    }

    /// `key` is nil for bucket-level actions (e.g. ListBucket); a bucket-level resource can only
    /// satisfy a bucket-level action, and an object-level resource only an object-level action.
    func matches(bucketName: String, key: String?) -> Bool {
        guard self.bucketName == bucketName else { return false }

        switch (keyPattern, key) {
        case (nil, nil):
            return true
        case (nil, .some), (.some, nil):
            return false
        case (.some(.exact(let exact)), .some(let key)):
            return key == exact
        case (.some(.prefix(let prefix)), .some(let key)):
            return key.hasPrefix(prefix)
        }
    }
}

struct PolicyStatement: Decodable {
    let sid: String?
    let effect: PolicyEffect
    let principal: PolicyPrincipal
    /// Raw action strings (not yet checked against `S3PolicyAction`) - unsupported actions
    /// are a validation error with a specific message, not a generic decode failure.
    let actions: [String]
    let resources: [ResourceMatcher]

    enum CodingKeys: String, CodingKey {
        case sid = "Sid"
        case effect = "Effect"
        case principal = "Principal"
        case actions = "Action"
        case resources = "Resource"
    }

    init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        sid = try container.decodeIfPresent(String.self, forKey: .sid)
        effect = try container.decode(PolicyEffect.self, forKey: .effect)
        principal = try container.decode(PolicyPrincipal.self, forKey: .principal)
        actions = try Self.decodeStringOrArray(container, forKey: .actions)
        resources = try Self.decodeStringOrArray(container, forKey: .resources).map(ResourceMatcher.init)
    }

    /// AWS allows Action/Resource to be either a single string or an array of strings
    private static func decodeStringOrArray(
        _ container: KeyedDecodingContainer<CodingKeys>, forKey key: CodingKeys
    ) throws -> [String] {
        if let single = try? container.decode(String.self, forKey: key) {
            return [single]
        }
        return try container.decode([String].self, forKey: key)
    }
}

struct BucketPolicy: Equatable {
    let version: String
    let statements: [PolicyStatement]
    /// The exact JSON that was PUT, returned as-is by GetBucketPolicy
    let rawJSON: String

    static func == (lhs: BucketPolicy, rhs: BucketPolicy) -> Bool {
        lhs.rawJSON == rhs.rawJSON
    }

    /// Whether this policy grants `action` on `bucketName`/`key` to an anonymous (unauthenticated)
    /// caller. v1 only ever matches Allow + Principal "*" statements - see parseAndValidate.
    func allowsAnonymous(action: S3PolicyAction, bucketName: String, key: String?) -> Bool {
        statements.contains { statement in
            statement.effect == .allow
                && statement.principal == .anyone
                && statement.actions.contains(action.rawValue)
                && statement.resources.contains { $0.matches(bucketName: bucketName, key: key) }
        }
    }

    /// Parses and validates a bucket policy document, rejecting anything outside the v1-supported
    /// subset (Allow + Principal "*" + a small action whitelist + Resource matching this bucket)
    /// with a specific error - unsupported elements are never silently accepted-but-ignored.
    static func parseAndValidate(rawJSON: String, bucketName: String, requestId: String) throws
        -> BucketPolicy
    {
        struct PolicyDocument: Decodable {
            let version: String
            let statements: [PolicyStatement]

            enum CodingKeys: String, CodingKey {
                case version = "Version"
                case statements = "Statement"
            }
        }

        guard let data = rawJSON.data(using: .utf8) else {
            throw S3Error(
                status: .badRequest, code: "MalformedPolicy",
                message: "Policy must be valid UTF-8 encoded JSON.", requestId: requestId)
        }

        let document: PolicyDocument
        do {
            document = try JSONDecoder().decode(PolicyDocument.self, from: data)
        } catch {
            throw S3Error(
                status: .badRequest, code: "MalformedPolicy",
                message: "The policy is not valid JSON.",
                requestId: requestId)
        }

        guard !document.statements.isEmpty else {
            throw S3Error(
                status: .badRequest, code: "MalformedPolicy",
                message: "Policy must contain at least one Statement.", requestId: requestId)
        }

        let supportedActions = Set(S3PolicyAction.allCases.map(\.rawValue))

        for statement in document.statements {
            guard statement.effect == .allow else {
                throw S3Error(
                    status: .badRequest, code: "MalformedPolicy",
                    message: "Only \"Effect\": \"Allow\" is currently supported.",
                    requestId: requestId)
            }
            guard statement.principal == .anyone else {
                throw S3Error(
                    status: .badRequest, code: "MalformedPolicy",
                    message: "Only \"Principal\": \"*\" is currently supported.",
                    requestId: requestId)
            }
            guard !statement.actions.isEmpty else {
                throw S3Error(
                    status: .badRequest, code: "MalformedPolicy",
                    message: "Each Statement must specify at least one Action.",
                    requestId: requestId)
            }
            for action in statement.actions {
                guard supportedActions.contains(action) else {
                    throw S3Error(
                        status: .badRequest, code: "MalformedPolicy",
                        message:
                            "Unsupported Action \"\(action)\". Supported actions: \(S3PolicyAction.allCases.map(\.rawValue).joined(separator: ", ")).",
                        requestId: requestId)
                }
            }
            guard !statement.resources.isEmpty else {
                throw S3Error(
                    status: .badRequest, code: "MalformedPolicy",
                    message: "Each Statement must specify at least one Resource.",
                    requestId: requestId)
            }
            for resource in statement.resources {
                guard resource.bucketName == bucketName else {
                    throw S3Error(
                        status: .badRequest, code: "MalformedPolicy",
                        message:
                            "Policy has invalid resource. \"\(resource.rawValue)\" does not match bucket \"\(bucketName)\".",
                        requestId: requestId)
                }
            }
        }

        return BucketPolicy(version: document.version, statements: document.statements, rawJSON: rawJSON)
    }
}
