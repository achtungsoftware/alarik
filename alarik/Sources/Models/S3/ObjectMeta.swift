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
    
    init(
        bucketName: String,
        key: String,
        size: Int,
        contentType: String,
        etag: String,
        metadata: [String: String] = [:],
        updatedAt: Date
    ) {
        self.bucketName = bucketName
        self.key = key
        self.size = size
        self.contentType = contentType
        self.etag = etag
        self.metadata = metadata
        self.updatedAt = updatedAt
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

        init(from meta: ObjectMeta) {
            self.key = meta.key
            self.size = meta.size
            self.contentType = meta.contentType
            self.etag = meta.etag
            self.lastModified = meta.updatedAt
            self.isFolder = false
        }

        init(folderKey: String) {
            self.key = folderKey
            self.size = 0
            self.contentType = "application/x-directory"
            self.etag = ""
            self.lastModified = Date()
            self.isFolder = true
        }
    }
}