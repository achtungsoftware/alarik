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

    /// Buckets an authoritative read proved have NEITHER a policy nor a public-access-block.
    ///
    /// Without this, "exists but has neither" is uncacheable: both maps stay empty for such a
    /// bucket, so `cachedValue` reports a miss forever and every single anonymous request pays a
    /// metadata-store read. That is the common case (most buckets have no policy), and it sits on
    /// the anonymous hot path, so the negative answer has to be cacheable exactly like a positive
    /// one. Kept in sync by every mutator below - a policy or PAB appearing must evict the entry,
    /// or a bucket that just became public would keep reading as "nothing set".
    private var knownWithoutAuthorization: Set<String> = []

    /// Full replace, not merge - see `AccessKeySecretKeyMapCache.load` for why this matters on a
    /// LISTEN-outage reload, not just at boot.
    func load(initialData: [(bucketName: String, policy: BucketPolicy)]) {
        map = Dictionary(uniqueKeysWithValues: initialData.map { ($0.bucketName, $0.policy) })
        knownWithoutAuthorization.subtract(map.keys)
    }

    /// Get the parsed policy for a bucket, or nil if none has been set
    func policy(for bucketName: String) -> BucketPolicy? {
        map[bucketName]
    }

    /// Set/replace the policy for a bucket
    func setPolicy(for bucketName: String, policy: BucketPolicy) {
        map[bucketName] = policy
        knownWithoutAuthorization.remove(bucketName)
    }

    /// Remove a bucket's policy (DeleteBucketPolicy, or bucket deletion)
    func removePolicy(for bucketName: String) {
        map.removeValue(forKey: bucketName)
        // Not inserted into `knownWithoutAuthorization` here: this only proves the POLICY is
        // gone, and says nothing about the public-access-block. Let the next read establish it.
        knownWithoutAuthorization.remove(bucketName)
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
        knownWithoutAuthorization.subtract(publicAccessBlockMap.keys)
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
        knownWithoutAuthorization.remove(bucketName)
    }

    /// Remove a bucket's public access block configuration (DeletePublicAccessBlock, or bucket
    /// deletion)
    func removePublicAccessBlock(for bucketName: String) {
        publicAccessBlockMap.removeValue(forKey: bucketName)
        // See `removePolicy`: proves nothing about the policy half.
        knownWithoutAuthorization.remove(bucketName)
    }

    func getPublicAccessBlockMap() -> [String: PublicAccessBlockConfiguration] {
        publicAccessBlockMap
    }

    /// Records that `bucketName` has neither a policy nor a public-access-block, from a source
    /// that already knows authoritatively (the bulk reload, which reads every bucket anyway).
    /// Without this the anonymous path pays one store read per policy-less bucket after every
    /// restart to learn the same thing. Ignores buckets that do have either, so it can never
    /// mask a policy that another pass already installed.
    func markWithoutAuthorization(_ bucketName: String) {
        guard map[bucketName] == nil, publicAccessBlockMap[bucketName] == nil else { return }
        knownWithoutAuthorization.insert(bucketName)
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
        if map[key] != nil || publicAccessBlockMap[key] != nil {
            return Authorization(policy: map[key], publicAccessBlock: publicAccessBlockMap[key])
        }
        // A bucket proven to have neither is a real cached answer, not a miss.
        if knownWithoutAuthorization.contains(key) {
            return Authorization(policy: nil, publicAccessBlock: nil)
        }
        return nil
    }

    func absorb(_ value: Authorization, for key: String) {
        if let policy = value.policy { map[key] = policy }
        if let pab = value.publicAccessBlock { publicAccessBlockMap[key] = pab }
        if value.policy == nil && value.publicAccessBlock == nil {
            knownWithoutAuthorization.insert(key)
        } else {
            knownWithoutAuthorization.remove(key)
        }
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
