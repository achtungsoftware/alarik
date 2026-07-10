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
import Testing

@testable import Alarik

/// `LoadCacheLifecycle.reloadAll` runs both at boot and as the safety-net reload after a
/// Postgres LISTEN reconnect - in the latter case, a row deleted/revoked while disconnected must
/// actually disappear from the cache, not just have currently-existing rows re-upserted on top
/// of a map that still remembers the stale one forever. Each test below simulates that exact
/// sequence directly against a fresh (non-`.shared`) cache instance: `load` with an initial
/// dataset, then `load` again with a strict subset, and confirms the removed entry is gone -
/// pure in-memory actor tests, no DB/app required.
@Suite("Cache reload purges stale entries, not just merges")
struct CacheReloadPurgeTests {
    @Test("AccessKeySecretKeyMapCache.load replaces rather than merges")
    func accessKeySecretKeyMapCachePurges() async {
        let cache = AccessKeySecretKeyMapCache()
        await cache.load(initialData: [("keyA", "secretA"), ("keyB", "secretB")])
        #expect(await cache.secretKey(for: "keyB") == "secretB")

        // Simulate keyB being revoked while disconnected, then a reload seeing only keyA.
        await cache.load(initialData: [("keyA", "secretA")])

        #expect(await cache.secretKey(for: "keyA") == "secretA")
        #expect(await cache.secretKey(for: "keyB") == nil)
    }

    @Test("AccessKeyUserMapCache.load replaces rather than merges")
    func accessKeyUserMapCachePurges() async {
        let cache = AccessKeyUserMapCache()
        let userA = UUID()
        await cache.load(initialData: [("keyA", userA), ("keyB", UUID())])
        #expect(await cache.userId(for: "keyB") != nil)

        await cache.load(initialData: [("keyA", userA)])

        #expect(await cache.userId(for: "keyA") == userA)
        #expect(await cache.userId(for: "keyB") == nil)
    }

    @Test("AccessKeyBucketMapCache.load replaces rather than merges")
    func accessKeyBucketMapCachePurges() async {
        let cache = AccessKeyBucketMapCache()
        await cache.load(initialData: [
            ("keyA", "bucket1"), ("keyA", "bucket2"), ("keyB", "bucket3"),
        ])
        #expect(await cache.buckets(for: "keyA") == ["bucket1", "bucket2"])
        #expect(await cache.exists(accessKey: "keyB"))

        // Simulate bucket2 being unmapped from keyA, and keyB's access key being revoked
        // entirely, while disconnected.
        await cache.load(initialData: [("keyA", "bucket1")])

        #expect(await cache.buckets(for: "keyA") == ["bucket1"])
        #expect(await cache.exists(accessKey: "keyB") == false)
    }

    @Test("BucketVersioningCache.load replaces rather than merges")
    func bucketVersioningCachePurges() async {
        let cache = BucketVersioningCache()
        await cache.load(initialData: [
            ("bucketA", VersioningStatus.enabled.rawValue),
            ("bucketB", VersioningStatus.enabled.rawValue),
        ])
        #expect(await cache.isVersioningEnabled(for: "bucketB"))

        // Simulate bucketB being deleted while disconnected.
        await cache.load(initialData: [("bucketA", VersioningStatus.enabled.rawValue)])

        #expect(await cache.isVersioningEnabled(for: "bucketA"))
        // A deleted bucket must fall back to the "not in cache" default, not keep reporting
        // whatever status it last had.
        #expect(await cache.getStatus(for: "bucketB") == .disabled)
    }

    @Test("BucketPolicyCache.load replaces rather than merges")
    func bucketPolicyCachePurges() async {
        let cache = BucketPolicyCache()
        let policy = BucketPolicy(version: "2012-10-17", statements: [], rawJSON: "{}")
        await cache.load(initialData: [("bucketA", policy), ("bucketB", policy)])
        #expect(await cache.policy(for: "bucketB") != nil)

        // Simulate bucketB's policy being deleted while disconnected.
        await cache.load(initialData: [("bucketA", policy)])

        #expect(await cache.policy(for: "bucketA") != nil)
        #expect(await cache.policy(for: "bucketB") == nil)
    }

    @Test("BucketPolicyCache.loadPublicAccessBlocks replaces rather than merges")
    func publicAccessBlockCachePurges() async {
        let cache = BucketPolicyCache()
        let config = PublicAccessBlockConfiguration(
            blockPublicAcls: true, ignorePublicAcls: true, blockPublicPolicy: true,
            restrictPublicBuckets: true)
        await cache.loadPublicAccessBlocks(initialData: [
            ("bucketA", config), ("bucketB", config),
        ])
        #expect(await cache.publicAccessBlock(for: "bucketB") != nil)

        await cache.loadPublicAccessBlocks(initialData: [("bucketA", config)])

        #expect(await cache.publicAccessBlock(for: "bucketA") != nil)
        #expect(await cache.publicAccessBlock(for: "bucketB") == nil)
    }

    @Test("NotificationConfigCache.load replaces rather than merges")
    func notificationConfigCachePurges() async {
        let cache = NotificationConfigCache()
        let config = NotificationConfiguration(rules: [
            NotificationRule(
                id: UUID(), url: "https://example.com/hook", secret: nil,
                events: ["s3:ObjectCreated:*"], prefix: nil, suffix: nil, enabled: true)
        ])
        await cache.load(initialData: [("bucketA", config), ("bucketB", config)])
        #expect(await cache.config(for: "bucketB") != nil)

        // Simulate bucketB's webhook config being removed while disconnected.
        await cache.load(initialData: [("bucketA", config)])

        #expect(await cache.config(for: "bucketA") != nil)
        #expect(await cache.config(for: "bucketB") == nil)
    }

    @Test("ReplicationConfigCache.load replaces rather than merges")
    func replicationConfigCachePurges() async {
        let cache = ReplicationConfigCache()
        let config = ReplicationConfiguration(
            targets: [],
            rules: [
                ReplicationRule(
                    id: UUID(), targetId: UUID(), prefix: nil, replicateDeletes: false,
                    replicateExisting: false, synchronous: false, enabled: true)
            ])
        await cache.load(initialData: [("bucketA", config), ("bucketB", config)])
        #expect(await cache.config(for: "bucketB") != nil)

        // Simulate bucketB's replication config being removed while disconnected.
        await cache.load(initialData: [("bucketA", config)])

        #expect(await cache.config(for: "bucketA") != nil)
        #expect(await cache.config(for: "bucketB") == nil)
    }
}
