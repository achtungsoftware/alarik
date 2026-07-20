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
import Vapor

@testable import Alarik

/// `CacheInvalidationMessage` -> `CacheReloadDispatch.apply` reload path against a real test
/// app. This is the bulk of the invalidation logic's test coverage; only the actual HTTP
/// broadcast delivery needs a real multi-node cluster, covered separately by `cluster_tests.sh`.
@Suite("CacheInvalidationService / CacheReloadDispatch tests", .serialized)
struct CacheInvalidationServiceTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await test(app)
        } catch {
            try? StorageHelper.cleanStorage()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    private func createUser(_ app: Application) async throws -> UUID {
        let user = User(
            name: "Cache Invalidation Test User", username: UUID().uuidString,
            passwordHash: try Bcrypt.hash("TestPass123!"), isAdmin: false)
        try await user.create(app: app)
        return user.id
    }

    private func createBucket(_ app: Application, userId: UUID, name: String) async throws -> Bucket {
        let bucket = Bucket(name: name, userId: userId)
        try await bucket.save(app: app)
        return bucket
    }

    private func createAccessKey(_ app: Application, userId: UUID, accessKey: String, secretKey: String)
        async throws
    {
        let key = AccessKey(userId: userId, accessKey: accessKey, secretKey: secretKey)
        _ = try await key.create(app: app)
    }

    private func payload(cache: String, op: CacheInvalidationMessage.Op, key: String) -> String {
        let message = CacheInvalidationMessage(cache: cache, op: op, key: key)
        return String(decoding: try! JSONEncoder().encode(message), as: UTF8.self)
    }

    // MARK: - notify() is a true no-op outside cluster mode

    @Test("notify(app:) on a non-clustered app returns without throwing or hanging")
    func notifyIsNoOpWhenNotClustered() async throws {
        try await withApp { app in
            // Nothing to assert beyond "this returns promptly and doesn't crash" - with no
            // `ClusterConfigurationKey` stashed (the test process sets no CLUSTER_NODE_ADDRESS/
            // CLUSTER_SECRET), the guard in `notify` returns before any Task is even spawned, so
            // there is no async broadcast to observe. Real HTTP broadcast delivery needs a real
            // multi-node cluster and is covered by `cluster_tests.sh`.
            CacheInvalidationService.notify(
                app: app, cache: "bucketVersioning", op: .upsert, key: "does-not-matter")
        }
    }

    // MARK: - CacheInvalidationMessage JSON round-trip

    @Test("CacheInvalidationMessage round-trips through JSON for every op")
    func messageRoundTrips() throws {
        for op: CacheInvalidationMessage.Op in [.upsert, .remove, .removeBucket] {
            let message = CacheInvalidationMessage(cache: "bucketVersioning", op: op, key: "my-bucket")
            let data = try JSONEncoder().encode(message)
            let decoded = try JSONDecoder().decode(CacheInvalidationMessage.self, from: data)
            #expect(decoded.cache == message.cache)
            #expect(decoded.op == message.op)
            #expect(decoded.key == message.key)
        }
    }

    // MARK: - CacheReloadDispatch: bucketVersioning

    @Test("apply bucketVersioning upsert reloads the current status from the DB")
    func dispatchBucketVersioningUpsert() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            let bucket = try await createBucket(app, userId: userId, name: "versioning-bucket")
            bucket.versioningStatus = VersioningStatus.enabled.rawValue
            try await bucket.save(app: app)

            await CacheReloadDispatch.apply(
                payload: payload(cache: "bucketVersioning", op: .upsert, key: "versioning-bucket"),
                app: app)

            #expect(
                await BucketVersioningCache.shared.getStatus(for: "versioning-bucket") == .enabled)
        }
    }

    @Test("apply bucketVersioning remove clears the cache entry")
    func dispatchBucketVersioningRemove() async throws {
        try await withApp { app in
            await BucketVersioningCache.shared.setStatus(for: "gone-bucket", status: .enabled)

            await CacheReloadDispatch.apply(
                payload: payload(cache: "bucketVersioning", op: .remove, key: "gone-bucket"),
                app: app)

            #expect(await BucketVersioningCache.shared.getStatus(for: "gone-bucket") == .disabled)
        }
    }

    // MARK: - CacheReloadDispatch: bucketPolicy / publicAccessBlock

    @Test("apply bucketPolicy upsert reloads and parses the stored policy JSON")
    func dispatchBucketPolicyUpsert() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            let bucket = try await createBucket(app, userId: userId, name: "policy-bucket")
            bucket.policy = """
                {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Sid": "PublicRead",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::policy-bucket/*"
                    }]
                }
                """
            try await bucket.save(app: app)

            await CacheReloadDispatch.apply(
                payload: payload(cache: "bucketPolicy", op: .upsert, key: "policy-bucket"),
                app: app)

            #expect(await BucketPolicyCache.shared.policy(for: "policy-bucket") != nil)
        }
    }

    @Test("apply bucketPolicy upsert with no stored policy removes the cache entry")
    func dispatchBucketPolicyUpsertNoPolicyRemoves() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            _ = try await createBucket(app, userId: userId, name: "no-policy-bucket")
            // Seed a stale cache entry as if a previous policy existed
            await BucketPolicyCache.shared.setPolicy(
                for: "no-policy-bucket",
                policy: try BucketPolicy.parseAndValidate(
                    rawJSON: """
                        {"Version":"2012-10-17","Statement":[{"Sid":"x","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::no-policy-bucket/*"}]}
                        """, bucketName: "no-policy-bucket", requestId: "test"))

            await CacheReloadDispatch.apply(
                payload: payload(cache: "bucketPolicy", op: .upsert, key: "no-policy-bucket"),
                app: app)

            #expect(await BucketPolicyCache.shared.policy(for: "no-policy-bucket") == nil)
        }
    }

    @Test("apply bucketPublicAccessBlock upsert reloads the flags from the DB")
    func dispatchPublicAccessBlockUpsert() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            let bucket = try await createBucket(app, userId: userId, name: "pab-bucket")
            bucket.blockPublicAcls = true
            bucket.restrictPublicBuckets = true
            try await bucket.save(app: app)

            await CacheReloadDispatch.apply(
                payload: payload(cache: "bucketPublicAccessBlock", op: .upsert, key: "pab-bucket"),
                app: app)

            let config = await BucketPolicyCache.shared.publicAccessBlock(for: "pab-bucket")
            #expect(config?.blockPublicAcls == true)
            #expect(config?.restrictPublicBuckets == true)
        }
    }

    // MARK: - CacheReloadDispatch: notification / replication config

    @Test("apply notificationConfig upsert reloads webhook rules from the DB")
    func dispatchNotificationConfigUpsert() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            let bucket = try await createBucket(app, userId: userId, name: "webhook-bucket")
            let config = NotificationConfiguration(rules: [
                NotificationRule(
                    id: UUID(), url: "https://example.com/hook", secret: nil,
                    events: ["s3:ObjectCreated:*"], prefix: nil, suffix: nil, enabled: true)
            ])
            bucket.notificationConfig = config.toJSON()
            try await bucket.save(app: app)

            await CacheReloadDispatch.apply(
                payload: payload(cache: "notificationConfig", op: .upsert, key: "webhook-bucket"),
                app: app)

            #expect(await NotificationConfigCache.shared.config(for: "webhook-bucket")?.rules.count == 1)
        }
    }

    @Test("apply replicationConfig upsert reloads targets and rules from the DB")
    func dispatchReplicationConfigUpsert() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            let bucket = try await createBucket(app, userId: userId, name: "replication-bucket")
            let targetId = UUID()
            let config = ReplicationConfiguration(
                targets: [
                    ReplicationTarget(
                        id: targetId, endpoint: "https://remote.example", targetBucket: "backup",
                        accessKeyId: "AKIA", secretAccessKey: "secret", region: "us-east-1",
                        enabled: true)
                ],
                rules: [
                    ReplicationRule(
                        id: UUID(), targetId: targetId, prefix: nil, replicateDeletes: false,
                        replicateExisting: false, enabled: true)
                ])
            bucket.replicationConfig = config.toJSON()
            try await bucket.save(app: app)

            await CacheReloadDispatch.apply(
                payload: payload(
                    cache: "replicationConfig", op: .upsert, key: "replication-bucket"),
                app: app)

            let reloaded = await ReplicationConfigCache.shared.config(for: "replication-bucket")
            #expect(reloaded?.targets.count == 1)
            #expect(reloaded?.rules.count == 1)
        }
    }

    // MARK: - CacheReloadDispatch: access key caches

    @Test("apply accessKeySecret upsert reloads the secret key from the DB")
    func dispatchAccessKeySecretUpsert() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            try await createAccessKey(
                app, userId: userId, accessKey: "reload-key", secretKey: "reload-secret")

            await CacheReloadDispatch.apply(
                payload: payload(cache: "accessKeySecret", op: .upsert, key: "reload-key"),
                app: app)

            #expect(
                await AccessKeySecretKeyMapCache.shared.secretKey(for: "reload-key")
                    == "reload-secret")
        }
    }

    @Test("apply accessKeySecret remove clears the cache entry")
    func dispatchAccessKeySecretRemove() async throws {
        try await withApp { app in
            await AccessKeySecretKeyMapCache.shared.add(accessKey: "gone-key", secretKey: "s")

            await CacheReloadDispatch.apply(
                payload: payload(cache: "accessKeySecret", op: .remove, key: "gone-key"),
                app: app)

            #expect(await AccessKeySecretKeyMapCache.shared.secretKey(for: "gone-key") == nil)
        }
    }

    @Test("apply accessKeyUser upsert reloads the owning user id from the DB")
    func dispatchAccessKeyUserUpsert() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            try await createAccessKey(
                app, userId: userId, accessKey: "owner-key", secretKey: "s")

            await CacheReloadDispatch.apply(
                payload: payload(cache: "accessKeyUser", op: .upsert, key: "owner-key"),
                app: app)

            #expect(await AccessKeyUserMapCache.shared.userId(for: "owner-key") == userId)
        }
    }

    @Test("apply accessKeyBucket upsert additively merges the key's bucket set from the DB, never dropping entries")
    func dispatchAccessKeyBucketUpsertIsAdditiveOnly() async throws {
        try await withApp { app in
            let userId = try await createUser(app)
            _ = try await createBucket(app, userId: userId, name: "bucket-one")
            _ = try await createBucket(app, userId: userId, name: "bucket-two")
            try await createAccessKey(
                app, userId: userId, accessKey: "multi-bucket-key", secretKey: "s")

            // Seed an entry for a bucket this key doesn't actually own in the DB. Upsert must be
            // a pure additive merge (see `CacheReloadDispatch`'s doc comment on this case): it
            // only ever ADDS what a successful read finds, never drops what it doesn't - a wipe-
            // and-rebuild here is exactly the bug that caused real, currently-valid buckets to
            // intermittently vanish from a key's cached access on any single incomplete fan-out.
            // Genuine removal has its own dedicated `.removeBucket` signal, exercised below.
            await AccessKeyBucketMapCache.shared.add(
                accessKey: "multi-bucket-key", bucketName: "not-actually-owned-bucket")

            await CacheReloadDispatch.apply(
                payload: payload(cache: "accessKeyBucket", op: .upsert, key: "multi-bucket-key"),
                app: app)

            let buckets = await AccessKeyBucketMapCache.shared.buckets(for: "multi-bucket-key")
            #expect(buckets == Set(["bucket-one", "bucket-two", "not-actually-owned-bucket"]))
        }
    }

    @Test("apply accessKeyBucket removeBucket strips the bucket from every key without touching the DB")
    func dispatchAccessKeyBucketRemoveBucket() async throws {
        try await withApp { app in
            await AccessKeyBucketMapCache.shared.add(accessKey: "key-a", bucketName: "shared-bucket")
            await AccessKeyBucketMapCache.shared.add(accessKey: "key-b", bucketName: "shared-bucket")

            await CacheReloadDispatch.apply(
                payload: payload(cache: "accessKeyBucket", op: .removeBucket, key: "shared-bucket"),
                app: app)

            #expect(await AccessKeyBucketMapCache.shared.canAccess(accessKey: "key-a", bucket: "shared-bucket") == false)
            #expect(await AccessKeyBucketMapCache.shared.canAccess(accessKey: "key-b", bucket: "shared-bucket") == false)
        }
    }

    // MARK: - Malformed payloads

    @Test("apply with a malformed payload logs and does not throw")
    func dispatchMalformedPayloadDoesNotCrash() async throws {
        try await withApp { app in
            await CacheReloadDispatch.apply(payload: "not valid json", app: app)
            // No assertion beyond "this didn't crash" - malformed input from a NOTIFY payload
            // (which could in principle come from anything listening on that channel) must
            // never take the process down.
        }
    }
}
