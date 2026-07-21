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

final actor AccessKeyBucketMapCache: StoreBackedCache {
    public static let shared = AccessKeyBucketMapCache()

    private var map: [String: Set<String>] = [:]

    /// Full replace, not merge - see `AccessKeySecretKeyMapCache.load` for why this matters on a
    /// LISTEN-outage reload, not just at boot: a bucket unmapped from a key while disconnected
    /// must actually disappear from that key's set, not just have currently-mapped buckets
    /// re-inserted into a set that still remembers the removed one forever.
    func load(initialData: [(accessKey: String, bucketName: String)]) {
        map = Dictionary(grouping: initialData, by: \.accessKey)
            .mapValues { Set($0.map(\.bucketName)) }
    }

    func add(accessKey: String, bucketName: String) {
        if map[accessKey] != nil {
            map[accessKey]?.insert(bucketName)
        } else {
            map[accessKey] = [bucketName]
        }
    }

    func remove(accessKey: String, bucketName: String) {
        map[accessKey]?.remove(bucketName)
        if map[accessKey]?.isEmpty == true {
            map.removeValue(forKey: accessKey)
        }
    }

    // Check if a bucket exists for this access key
    func bucket(for bucketName: String) -> String? {
        // Check if ANY access key has this bucket
        for (_, buckets) in map {
            if buckets.contains(bucketName) {
                return bucketName
            }
        }
        return nil
    }

    // Check if this access key can access this bucket
    func canAccess(accessKey: String, bucket: String) -> Bool {
        map[accessKey]?.contains(bucket) ?? false
    }

    func exists(accessKey: String) -> Bool {
        map[accessKey] != nil
    }

    func buckets(for accessKey: String) -> Set<String>? {
        map[accessKey]
    }

    func removeAccessKey(_ accessKey: String) {
        map.removeValue(forKey: accessKey)
    }

    func keys(for bucketName: String) -> [String] {
        map.filter { $0.value.contains(bucketName) }.map { $0.key }
    }

    func removeAll(for bucketName: String) {
        for key in map.keys {
            map[key]?.remove(bucketName)
            if map[key]?.isEmpty == true {
                map.removeValue(forKey: key)
            }
        }
    }

    func getMap() -> [String: Set<String>] {
        map
    }

    // MARK: - StoreBackedCache

    /// Keyed on the pair, because the question this cache answers is "may this key touch this
    /// bucket" - a per-access-key entry alone cannot express a miss for one specific bucket.
    struct Grant: Hashable, Sendable {
        let accessKey: String
        let bucket: String
    }

    var missLedger = CacheMissLedger<Grant>()

    /// Whether `accessKey` may act on `bucket`, consulting the store when this node has no
    /// cached grant - the accessor every caller should use.
    func canAccess(app: Application, accessKey: String, bucket: String) async -> Bool {
        await resolve(app: app, key: Grant(accessKey: accessKey, bucket: bucket)) ?? false
    }

    func cachedValue(for key: Grant) -> Bool? {
        // Only a positive is cached knowledge. Absence means "not known here", never "denied" -
        // answering `false` from a cold cache is exactly the false `AccessDenied` this avoids.
        map[key.accessKey]?.contains(key.bucket) == true ? true : nil
    }

    func absorb(_ value: Bool, for key: Grant) {
        guard value else { return }
        map[key.accessKey, default: []].insert(key.bucket)
    }

    /// Authoritative answer: the bucket exists and is owned by this access key's user. Resolves
    /// the owner through `AccessKeyUserMapCache` rather than reading the key record directly, so
    /// that lookup gets the same cache-then-store treatment instead of a private shortcut.
    func loadFromStore(app: Application, key: Grant) async throws -> Bool? {
        guard
            let ownerId = await AccessKeyUserMapCache.shared.resolvedUserId(
                app: app, accessKey: key.accessKey)
        else { return nil }
        guard let bucket = try await Bucket.find(app: app, name: key.bucket) else { return nil }
        return bucket.userId == ownerId ? true : nil
    }
}
