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

/// Metadata stored for each multipart upload
struct MultipartUploadMeta: Codable {
    let uploadId: String
    let bucketName: String
    let key: String
    let contentType: String
    let metadata: [String: String]
    let initiated: Date
    /// The flexible-checksum algorithm pinned at CreateMultipartUpload (lowercase suffix, e.g.
    /// `sha256`), used to combine the parts into the object's checksum. Optional - uploads created
    /// without a checksum algorithm, or before this field existed, decode as nil.
    var checksumAlgorithm: String? = nil
    /// The checksum type the client requested (`FULL_OBJECT` / `COMPOSITE`), if any. The effective
    /// type is still constrained by the algorithm at completion (see `multipartChecksumType`).
    var checksumType: String? = nil
}

/// Metadata stored for each uploaded part
struct MultipartPartMeta: Codable {
    let partNumber: Int
    let etag: String
    let size: Int
    let lastModified: Date
    /// This part's base64 checksum in the upload's pinned algorithm (the raw digest, base64), or
    /// nil when the part was uploaded without one. Feeds the COMPOSITE object checksum at complete.
    var checksum: String? = nil
}
