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

/// Caches already-parsed bucket policies (and public access block settings) so the request hot
/// path (every anonymous request) never touches the database or re-parses JSON. Both live in one
/// cache, rather than as two separate actors, since they're loaded/invalidated at the exact same
/// lifecycle points and together answer one question: "can an anonymous caller in?" Mirrors
/// BucketVersioningCache.
final actor BucketPolicyCache: StoreBackedCache {
    public static let shared = BucketPolicyCache()

    private var map: [String: BucketPolicy] = [:]
    private var publicAccessBlockMap: [String: PublicAccessBlockConfiguration] = [:]

    /// Full replace, not merge - see `AccessKeySecretKeyMapCache.load` for why this matters on a
    /// LISTEN-outage reload, not just at boot.
    func load(initialData: [(bucketName: String, policy: BucketPolicy)]) {
        map = Dictionary(uniqueKeysWithValues: initialData.map { ($0.bucketName, $0.policy) })
    }

    /// Get the parsed policy for a bucket, or nil if none has been set
    func policy(for bucketName: String) -> BucketPolicy? {
        map[bucketName]
    }

    /// Set/replace the policy for a bucket
    func setPolicy(for bucketName: String, policy: BucketPolicy) {
        map[bucketName] = policy
    }

    /// Remove a bucket's policy (DeleteBucketPolicy, or bucket deletion)
    func removePolicy(for bucketName: String) {
        map.removeValue(forKey: bucketName)
    }

    func getMap() -> [String: BucketPolicy] {
        map
    }

    /// Full replace, not merge - see `AccessKeySecretKeyMapCache.load` for why this matters on a
    /// LISTEN-outage reload, not just at boot.
    func loadPublicAccessBlocks(
        initialData: [(bucketName: String, configuration: PublicAccessBlockConfiguration)]
    ) {
        publicAccessBlockMap = Dictionary(
            uniqueKeysWithValues: initialData.map { ($0.bucketName, $0.configuration) })
    }

    /// Get the public access block configuration for a bucket, or nil if never configured
    /// (equivalent to all-false - nothing blocked).
    func publicAccessBlock(for bucketName: String) -> PublicAccessBlockConfiguration? {
        publicAccessBlockMap[bucketName]
    }

    /// Set/replace the public access block configuration for a bucket
    func setPublicAccessBlock(
        for bucketName: String, configuration: PublicAccessBlockConfiguration
    ) {
        publicAccessBlockMap[bucketName] = configuration
    }

    /// Remove a bucket's public access block configuration (DeletePublicAccessBlock, or bucket
    /// deletion)
    func removePublicAccessBlock(for bucketName: String) {
        publicAccessBlockMap.removeValue(forKey: bucketName)
    }

    func getPublicAccessBlockMap() -> [String: PublicAccessBlockConfiguration] {
        publicAccessBlockMap
    }

    // MARK: - StoreBackedCache

    /// Both bucket-authorization values together: one `Bucket` read answers both, and caching
    /// them as a unit keeps them from disagreeing about whether the bucket was resolvable.
    struct Authorization: Sendable {
        let policy: BucketPolicy?
        let publicAccessBlock: PublicAccessBlockConfiguration?
    }

    var missLedger = CacheMissLedger<String>()

    func cachedValue(for key: String) -> Authorization? {
        guard map[key] != nil || publicAccessBlockMap[key] != nil else { return nil }
        return Authorization(policy: map[key], publicAccessBlock: publicAccessBlockMap[key])
    }

    func absorb(_ value: Authorization, for key: String) {
        if let policy = value.policy { map[key] = policy }
        if let pab = value.publicAccessBlock { publicAccessBlockMap[key] = pab }
    }

    func loadFromStore(app: Application, key: String) async throws -> Authorization? {
        guard let bucket = try await Bucket.find(app: app, name: key) else { return nil }
        return Authorization(
            policy: LoadCacheLifecycle.parsedPolicy(for: bucket, logger: app.logger),
            publicAccessBlock: LoadCacheLifecycle.publicAccessBlockIfNonDefault(for: bucket))
    }

    /// The bucket's policy, consulting the store on a miss - a miss otherwise reads as "no
    /// policy", which silently changes who is allowed to do what.
    func resolvedPolicy(app: Application, bucket: String) async -> BucketPolicy? {
        await resolve(app: app, key: bucket)?.policy
    }

    /// The bucket's public-access-block configuration, consulting the store on a miss.
    func resolvedPublicAccessBlock(app: Application, bucket: String) async
        -> PublicAccessBlockConfiguration?
    {
        await resolve(app: app, key: bucket)?.publicAccessBlock
    }
}
