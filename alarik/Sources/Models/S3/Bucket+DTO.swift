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
import XMLCoder

/// Backed by `MetadataStore`, not Fluent - primary record at `buckets/<name>`. The bucket name is
/// already a natural, immutable, globally-unique identifier (S3 buckets can't be renamed), so it
/// doubles as the primary key directly, the same pattern `AccessKey`/`SharedLink`/`OIDCProvider`
/// already use for their own natural keys - no secondary index needed.
final class Bucket: Content, @unchecked Sendable, Codable {
    let id: UUID
    var name: String
    var userId: UUID
    var creationDate: Date?
    var versioningStatus: String

    /// Raw JSON bucket policy document, or nil if no policy has been set
    var policy: String?

    /// Public Access Block settings - see `PublicAccessBlockConfiguration`. `blockPublicAcls`/
    /// `ignorePublicAcls` are accepted/stored for client compatibility (e.g. Terraform sets all
    /// 4 unconditionally) but are no-ops, since this system has no ACL concept at all.
    var blockPublicAcls: Bool
    var ignorePublicAcls: Bool
    var blockPublicPolicy: Bool
    var restrictPublicBuckets: Bool

    /// JSON-encoded `[String: String]` tag-set, or nil if no tags have been set - see `Tagging`.
    var tags: String?

    /// JSON-encoded `[LifecycleRule]`, or nil if no lifecycle configuration has been set - see
    /// `LifecycleConfiguration`.
    var lifecycleRules: String?

    /// JSON-encoded `NotificationConfiguration` (webhook rules), or nil if none configured.
    var notificationConfig: String?

    /// JSON-encoded `ReplicationConfiguration` (remote targets + rules), or nil if none
    /// configured.
    var replicationConfig: String?

    init(id: UUID = UUID(), name: String, userId: UUID) {
        self.id = id
        self.name = name
        self.userId = userId
        self.creationDate = Date()
        self.versioningStatus = VersioningStatus.disabled.rawValue
        self.blockPublicAcls = false
        self.ignorePublicAcls = false
        self.blockPublicPolicy = false
        self.restrictPublicBuckets = false
    }

    init(id: UUID = UUID(), name: String, userId: UUID, creationDate: Date) {
        self.id = id
        self.name = name
        self.userId = userId
        self.creationDate = creationDate
        self.versioningStatus = VersioningStatus.disabled.rawValue
        self.blockPublicAcls = false
        self.ignorePublicAcls = false
        self.blockPublicPolicy = false
        self.restrictPublicBuckets = false
    }

    init(id: UUID = UUID(), name: String, userId: UUID, versioningStatus: VersioningStatus) {
        self.id = id
        self.name = name
        self.userId = userId
        self.creationDate = Date()
        self.versioningStatus = versioningStatus.rawValue
        self.blockPublicAcls = false
        self.ignorePublicAcls = false
        self.blockPublicPolicy = false
        self.restrictPublicBuckets = false
    }

    /// Current public access block configuration for this bucket.
    var publicAccessBlock: PublicAccessBlockConfiguration {
        PublicAccessBlockConfiguration(
            blockPublicAcls: blockPublicAcls,
            ignorePublicAcls: ignorePublicAcls,
            blockPublicPolicy: blockPublicPolicy,
            restrictPublicBuckets: restrictPublicBuckets
        )
    }

    func toResponseDTO() -> Bucket.ResponseDTO {
        .init(
            id: self.id,
            name: self.name,
            creationDate: self.creationDate,
            versioningStatus: self.versioningStatus
        )
    }

    /// Returns true if versioning is enabled for this bucket
    var isVersioningEnabled: Bool {
        versioningStatus == VersioningStatus.enabled.rawValue
    }

    /// Returns true if versioning is suspended (was enabled, now paused)
    var isVersioningSuspended: Bool {
        versioningStatus == VersioningStatus.suspended.rawValue
    }
}

// MARK: - MetadataStore access

extension Bucket {
    static func find(app: Application, name: String) async throws -> Bucket? {
        try await MetadataStore.get(
            Bucket.self, app: app, collection: MetadataCollections.buckets, id: name)
    }

    /// Every bucket cluster-wide - a full-collection fan-out (see `MetadataListingService`'s doc
    /// comment). Only ever called from admin/console/background-sweep paths (bucket listing,
    /// cache warm/reload, rebalance/lifecycle walks), never per-S3-request - the hot per-request
    /// bucket lookup is always `find(app:name:)`, a single point read.
    static func all(app: Application) async throws -> [Bucket] {
        await MetadataListingService.list(app: app, collection: MetadataCollections.buckets)
            .compactMap { try? JSONDecoder().decode(Bucket.self, from: $0.value) }
    }

    /// Creates the bucket, failing if the name is already taken.
    func create(app: Application) async throws -> Bool {
        try await MetadataStore.putIfAbsent(
            app: app, collection: MetadataCollections.buckets, id: name, value: self)
    }

    func save(app: Application) async throws {
        try await MetadataStore.put(
            app: app, collection: MetadataCollections.buckets, id: name, value: self)
    }

    func delete(app: Application) async throws {
        try await MetadataStore.delete(
            app: app, collection: MetadataCollections.buckets, id: name)
    }
}

extension Bucket {
    struct Create: Content {
        var name: String
        var versioningEnabled: Bool
    }

    struct ResponseDTO: Content {
        var id: UUID?
        var name: String?
        var creationDate: Date?
        var versioningStatus: String?
    }
}

extension Bucket.Create: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("name", as: String.self, is: .bucketName)
        validations.add("versioningEnabled", as: Bool.self, is: .valid)
    }
}

struct ListAllMyBucketsResultDTO: Encodable {
    let owner: Owner
    let buckets: BucketsContainer

    struct Owner: Encodable {
        let id: String
        let displayName: String

        enum CodingKeys: String, CodingKey {
            case id = "ID"
            case displayName = "DisplayName"
        }
    }

    struct BucketsContainer: Encodable {
        let bucket: [BucketEntry]

        enum CodingKeys: String, CodingKey {
            case bucket = "Bucket"
        }
    }

    struct BucketEntry: Encodable {
        let name: String
        let creationDate: String  // ISO8601
        enum CodingKeys: String, CodingKey {
            case name = "Name"
            case creationDate = "CreationDate"
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(owner, forKey: .owner)
        try container.encode(buckets, forKey: .buckets)
    }

    enum CodingKeys: String, CodingKey {
        case owner = "Owner"
        case buckets = "Buckets"
    }
}

extension ListAllMyBucketsResultDTO {
    static func s3XMLContainer(_ buckets: [Bucket]) throws -> Data {
        let result = ListAllMyBucketsResultDTO(
            owner: .init(id: "vapor-user", displayName: "vapor-user"),
            buckets: .init(
                bucket: buckets.map { bucket in
                    let formatter = ISO8601DateFormatter()
                    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
                    let dateStr =
                        bucket.creationDate.map { formatter.string(from: $0) }
                        ?? "2025-01-01T00:00:00.000Z"
                    return .init(name: bucket.name, creationDate: dateStr)
                })
        )

        let encoder = XMLEncoder()
        return try encoder.encodeWithS3XMLContainer(
            result, withRootKey: "ListAllMyBucketsResult")
    }
}
