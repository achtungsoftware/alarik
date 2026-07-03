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

import Fluent
import Foundation
import Vapor
import XMLCoder

final class Bucket: Content, Model, @unchecked Sendable {
    static let schema = "buckets"

    @ID(key: .id)
    var id: UUID?

    @Field(key: "name")
    var name: String

    @Parent(key: "user_id")
    var user: User

    @Field(key: "creation_date")
    var creationDate: Date?

    @Field(key: "versioning_status")
    var versioningStatus: String

    /// Raw JSON bucket policy document, or nil if no policy has been set
    @Field(key: "policy")
    var policy: String?

    /// Public Access Block settings - see `PublicAccessBlockConfiguration`. `blockPublicAcls`/
    /// `ignorePublicAcls` are accepted/stored for client compatibility (e.g. Terraform sets all
    /// 4 unconditionally) but are no-ops, since this system has no ACL concept at all.
    @Field(key: "block_public_acls")
    var blockPublicAcls: Bool

    @Field(key: "ignore_public_acls")
    var ignorePublicAcls: Bool

    @Field(key: "block_public_policy")
    var blockPublicPolicy: Bool

    @Field(key: "restrict_public_buckets")
    var restrictPublicBuckets: Bool

    /// JSON-encoded `[String: String]` tag-set, or nil if no tags have been set - see `Tagging`.
    @Field(key: "tags")
    var tags: String?

    /// JSON-encoded `[LifecycleRule]`, or nil if no lifecycle configuration has been set - see
    /// `LifecycleConfiguration`.
    @Field(key: "lifecycle_rules")
    var lifecycleRules: String?

    /// JSON-encoded `NotificationConfiguration` (webhook rules), or nil if none configured.
    @Field(key: "notification_config")
    var notificationConfig: String?

    /// JSON-encoded `ReplicationConfiguration` (remote targets + rules), or nil if none
    /// configured.
    @Field(key: "replication_config")
    var replicationConfig: String?

    init() {
        self.versioningStatus = VersioningStatus.disabled.rawValue
        self.blockPublicAcls = false
        self.ignorePublicAcls = false
        self.blockPublicPolicy = false
        self.restrictPublicBuckets = false
    }

    init(name: String, userId: UUID) {
        self.name = name
        self.creationDate = Date()
        self.$user.id = userId
        self.versioningStatus = VersioningStatus.disabled.rawValue
        self.blockPublicAcls = false
        self.ignorePublicAcls = false
        self.blockPublicPolicy = false
        self.restrictPublicBuckets = false
    }

    init(name: String, userId: UUID, creationDate: Date) {
        self.name = name
        self.creationDate = creationDate
        self.$user.id = userId
        self.versioningStatus = VersioningStatus.disabled.rawValue
        self.blockPublicAcls = false
        self.ignorePublicAcls = false
        self.blockPublicPolicy = false
        self.restrictPublicBuckets = false
    }

    init(name: String, userId: UUID, versioningStatus: VersioningStatus) {
        self.name = name
        self.creationDate = Date()
        self.$user.id = userId
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
            name: self.$name.value,
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

        func toModel() -> Bucket {
            let model = Bucket()

            model.id = self.id
            if let name = self.name {
                model.name = name
            }
            if let creationDate = self.creationDate {
                model.creationDate = creationDate
            }
            if let versioningStatus = self.versioningStatus {
                model.versioningStatus = versioningStatus
            }
            return model
        }
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
