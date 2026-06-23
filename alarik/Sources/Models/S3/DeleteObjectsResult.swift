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

/// A single `<Object>` entry parsed from a DeleteObjects request body.
struct DeleteObjectRequestEntry {
    let key: String
    let versionId: String?
}

struct DeleteObjectsResult: Encodable {
    let deleted: [DeletedEntry]
    let errors: [DeleteErrorEntry]

    enum CodingKeys: String, CodingKey {
        case deleted = "Deleted"
        case errors = "Error"
    }
}

struct DeletedEntry: Encodable {
    let key: String
    let versionId: String?
    let deleteMarker: Bool?
    let deleteMarkerVersionId: String?

    enum CodingKeys: String, CodingKey {
        case key = "Key"
        case versionId = "VersionId"
        case deleteMarker = "DeleteMarker"
        case deleteMarkerVersionId = "DeleteMarkerVersionId"
    }
}

struct DeleteErrorEntry: Encodable {
    let key: String
    let code: String
    let message: String

    enum CodingKeys: String, CodingKey {
        case key = "Key"
        case code = "Code"
        case message = "Message"
    }
}
