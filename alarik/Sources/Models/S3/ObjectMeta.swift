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

struct ObjectMeta: Codable {
    var bucketName: String
    var key: String
    var size: Int
    var contentType: String
    var etag: String
    var metadata: [String: String] = [:]
    var updatedAt: Date

    var versionId: String?
    var isLatest: Bool = true
    var isDeleteMarker: Bool = false

    init(
        bucketName: String,
        key: String,
        size: Int,
        contentType: String,
        etag: String,
        metadata: [String: String] = [:],
        updatedAt: Date,
        versionId: String? = nil,
        isLatest: Bool = true,
        isDeleteMarker: Bool = false
    ) {
        self.bucketName = bucketName
        self.key = key
        self.size = size
        self.contentType = contentType
        self.etag = etag
        self.metadata = metadata
        self.updatedAt = updatedAt
        self.versionId = versionId
        self.isLatest = isLatest
        self.isDeleteMarker = isDeleteMarker
    }

    /// Generates a new version ID (UUID without dashes)
    static func generateVersionId() -> String {
        UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
    }
}

struct ListBucketResult: Encodable {
    let name: String
    let prefix: String
    let marker: String?
    let nextMarker: String?
    let maxKeys: Int
    let isTruncated: Bool
    let contents: [ObjectEntry]
    let commonPrefixes: [CommonPrefix]
    
    enum CodingKeys: String, CodingKey {
        case name = "Name"
        case prefix = "Prefix"
        case marker = "Marker"
        case nextMarker = "NextMarker"
        case maxKeys = "MaxKeys"
        case isTruncated = "IsTruncated"
        case contents = "Contents"
        case commonPrefixes = "CommonPrefixes"
    }
}

struct ListBucketResultV2: Encodable {
    let name: String
    let prefix: String
    let startAfter: String?
    let continuationToken: String?
    let nextContinuationToken: String?
    let keyCount: Int
    let maxKeys: Int
    let isTruncated: Bool
    let contents: [ObjectEntry]
    let commonPrefixes: [CommonPrefix]
    
    enum CodingKeys: String, CodingKey {
        case name = "Name"
        case prefix = "Prefix"
        case startAfter = "StartAfter"
        case continuationToken = "ContinuationToken"
        case nextContinuationToken = "NextContinuationToken"
        case keyCount = "KeyCount"
        case maxKeys = "MaxKeys"
        case isTruncated = "IsTruncated"
        case contents = "Contents"
        case commonPrefixes = "CommonPrefixes"
    }
}

struct ObjectEntry: Encodable {
    let key: String
    let lastModified: String
    let etag: String
    let size: Int
    let storageClass: String
    
    enum CodingKeys: String, CodingKey {
        case key = "Key"
        case lastModified = "LastModified"
        case etag = "ETag"
        case size = "Size"
        case storageClass = "StorageClass"
    }
}

struct CommonPrefix: Encodable {
    let prefix: String

    enum CodingKeys: String, CodingKey {
        case prefix = "Prefix"
    }
}

// API response DTO for object browser
import Vapor

extension ObjectMeta {
    struct ResponseDTO: Content {
        var key: String
        var size: Int
        var contentType: String
        var etag: String
        var lastModified: Date
        var isFolder: Bool = false
        var versionId: String?
        var isLatest: Bool?
        var isDeleteMarker: Bool?

        init(from meta: ObjectMeta) {
            self.key = meta.key
            self.size = meta.size
            self.contentType = meta.contentType
            self.etag = meta.etag
            self.lastModified = meta.updatedAt
            self.isFolder = false
            self.versionId = meta.versionId
            self.isLatest = meta.isLatest
            self.isDeleteMarker = meta.isDeleteMarker
        }

        init(folderKey: String) {
            self.key = folderKey
            self.size = 0
            self.contentType = "application/x-directory"
            self.etag = ""
            self.lastModified = Date()
            self.isFolder = true
            self.versionId = nil
            self.isLatest = nil
            self.isDeleteMarker = nil
        }
    }
}

struct ListVersionsResult: Encodable {
    let name: String
    let prefix: String
    let delimiter: String?
    let keyMarker: String?
    let versionIdMarker: String?
    let nextKeyMarker: String?
    let nextVersionIdMarker: String?
    let maxKeys: Int
    let isTruncated: Bool
    let versions: [VersionEntry]
    let deleteMarkers: [DeleteMarkerEntry]
    let commonPrefixes: [CommonPrefix]

    enum CodingKeys: String, CodingKey {
        case name = "Name"
        case prefix = "Prefix"
        case delimiter = "Delimiter"
        case keyMarker = "KeyMarker"
        case versionIdMarker = "VersionIdMarker"
        case nextKeyMarker = "NextKeyMarker"
        case nextVersionIdMarker = "NextVersionIdMarker"
        case maxKeys = "MaxKeys"
        case isTruncated = "IsTruncated"
        case versions = "Version"
        case deleteMarkers = "DeleteMarker"
        case commonPrefixes = "CommonPrefixes"
    }
}

struct VersionEntry: Encodable {
    let key: String
    let versionId: String
    let isLatest: Bool
    let lastModified: String
    let etag: String
    let size: Int
    let storageClass: String

    enum CodingKeys: String, CodingKey {
        case key = "Key"
        case versionId = "VersionId"
        case isLatest = "IsLatest"
        case lastModified = "LastModified"
        case etag = "ETag"
        case size = "Size"
        case storageClass = "StorageClass"
    }

    init(from meta: ObjectMeta) {
        self.key = meta.key
        self.versionId = meta.versionId ?? "null"
        self.isLatest = meta.isLatest
        self.lastModified = meta.updatedAt.iso8601String
        self.etag = "\"\(meta.etag)\""
        self.size = meta.size
        self.storageClass = "STANDARD"
    }
}

struct DeleteMarkerEntry: Encodable {
    let key: String
    let versionId: String
    let isLatest: Bool
    let lastModified: String

    enum CodingKeys: String, CodingKey {
        case key = "Key"
        case versionId = "VersionId"
        case isLatest = "IsLatest"
        case lastModified = "LastModified"
    }

    init(from meta: ObjectMeta) {
        self.key = meta.key
        self.versionId = meta.versionId ?? "null"
        self.isLatest = meta.isLatest
        self.lastModified = meta.updatedAt.iso8601String
    }
}

struct VersioningConfiguration: Codable {
    var status: String?

    enum CodingKeys: String, CodingKey {
        case status = "Status"
    }
}

enum VersioningStatus: String, Codable {
    case disabled = "Disabled"
    case enabled = "Enabled"
    case suspended = "Suspended"
}