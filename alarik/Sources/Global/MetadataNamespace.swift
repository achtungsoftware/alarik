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

/// The hidden internal namespace holding Alarik's own control-plane metadata (users, buckets,
/// access keys, cluster membership, outbox tasks, ...) as small objects erasure-coded through
/// the same engine as regular object data. Lives under `BucketHandler.rootURL` as an ordinary-
/// looking (but reserved) bucket directory, so `ErasureCodedRebalanceService`'s membership-change
/// walk and `ErasureCodedScrubber`'s bit-rot scrub cover metadata objects for free.
enum MetadataNamespace {
    /// Reserved pseudo-bucket name. Already unreachable through the S3 API on its own - the
    /// bucket-name validator's character class requires `[a-z0-9]` as the first character (see
    /// `Sources/Validation/BucketName.swift`) - but every path that walks bucket names/directories
    /// directly, rather than going through that validator, must still explicitly exclude it.
    static let bucketName = ".alarik.sys"

    /// True when `name` is the reserved metadata pseudo-bucket - every path that enumerates,
    /// creates, or validates real bucket names must exclude/reject it.
    static func isReserved(_ name: String) -> Bool {
        name == bucketName
    }

    /// Builds the object key for one record within a collection, e.g.
    /// `key(collection: "users", id: "1234") == "users/1234"`.
    static func key(collection: String, id: String) -> String {
        "\(collection)/\(id)"
    }

    /// The inverse of `key(collection:id:)` - splits on the first `/` only, since an id (a UUID,
    /// access key, or username) never legitimately contains one, but a collection name never
    /// does either, so this is unambiguous either way.
    static func splitKey(_ key: String) -> (collection: String, id: String)? {
        guard let slash = key.firstIndex(of: "/") else { return nil }
        return (String(key[key.startIndex..<slash]), String(key[key.index(after: slash)...]))
    }
}
