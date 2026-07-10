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

import Fluent
import Vapor

final class LoadCacheLifecycle: LifecycleHandler {
    func didBootAsync(_ app: Application) async throws {
        do {
            try await LoadCacheLifecycle.reloadAll(app: app)
        } catch {
            app.logger.error("Failed to load cache: \(error)")
        }

        #if DEBUG  // Print all caches in debug
            if app.environment != .testing {
                dump(await AccessKeyUserMapCache.shared.getMap())
                dump(await AccessKeyBucketMapCache.shared.getMap())
                dump(await AccessKeySecretKeyMapCache.shared.getMap())
                dump(await BucketVersioningCache.shared.getMap())
                dump(await BucketPolicyCache.shared.getMap())
                dump(await BucketPolicyCache.shared.getPublicAccessBlockMap())
            }
        #endif
    }

    /// The full boot-time bulk load, factored out so it can also be run as the safety-net
    /// reload after `CacheInvalidationListener` reconnects a dropped LISTEN connection -
    /// Postgres does not redeliver missed NOTIFYs, so a full reload from the DB is the only
    /// sound way to recover from a gap in the notification stream. Throws on failure (unlike
    /// `didBootAsync`, which catches and only logs) so each caller can decide how to react.
    static func reloadAll(app: Application) async throws {
        // Load all access keys with their parent user
        let keys = try await AccessKey.query(on: app.db)
            .group(.or) {
                $0.filter(\.$expirationDate == nil)
                $0.filter(\.$expirationDate > Date.now)
            }
            .with(\.$user)
            .all()

        // Load all buckets for the users referenced by access keys
        let userIDs = keys.compactMap { $0.user.id }
        let buckets = try await Bucket.query(on: app.db)
            .filter(\.$user.$id ~~ userIDs)
            .all()

        // Map userID -> buckets
        let bucketsByUser = Dictionary(grouping: buckets, by: { $0.$user.id })

        // Build cache mappings
        var bucketData: [(accessKey: String, bucketName: String)] = []
        var userMappingData: [(accessKey: String, userId: UUID)] = []

        for key in keys {

            // Add to AccessKeySecretKeyMapCache
            await AccessKeySecretKeyMapCache.shared.add(
                accessKey: key.accessKey,
                secretKey: key.secretKey
            )

            let userID = key.$user.id

            // Add to user mapping cache
            userMappingData.append((accessKey: key.accessKey, userId: userID))

            guard let userBuckets = bucketsByUser[userID] else { continue }

            for bucket in userBuckets {
                bucketData.append((accessKey: key.accessKey, bucketName: bucket.name))
            }
        }

        await AccessKeyUserMapCache.shared.load(initialData: userMappingData)
        await AccessKeyBucketMapCache.shared.load(initialData: bucketData)

        // Load bucket versioning status cache
        let allBuckets = try await Bucket.query(on: app.db).all()
        let versioningData = allBuckets.map {
            (bucketName: $0.name, versioningStatus: $0.versioningStatus)
        }
        await BucketVersioningCache.shared.load(initialData: versioningData)

        // Load bucket policy cache - skip (not crash) any policy that fails to
        // re-validate, since it should always have been valid when it was saved
        let policyData: [(bucketName: String, policy: BucketPolicy)] = allBuckets.compactMap {
            bucket in
            guard let policy = parsedPolicy(for: bucket, logger: app.logger) else { return nil }
            return (bucketName: bucket.name, policy: policy)
        }
        await BucketPolicyCache.shared.load(initialData: policyData)

        // Load public access block cache - only buckets with at least one flag set are
        // worth caching, an all-false bucket behaves identically to "not in the map".
        let publicAccessBlockData:
            [(bucketName: String, configuration: PublicAccessBlockConfiguration)] =
                allBuckets.compactMap { bucket in
                    guard let config = publicAccessBlockIfNonDefault(for: bucket) else {
                        return nil
                    }
                    return (bucketName: bucket.name, configuration: config)
                }
        await BucketPolicyCache.shared.loadPublicAccessBlocks(
            initialData: publicAccessBlockData)

        // Load notification (webhook) configuration cache
        let notificationData: [(bucketName: String, config: NotificationConfiguration)] =
            allBuckets.compactMap { bucket in
                guard let config = notificationConfig(for: bucket) else { return nil }
                return (bucketName: bucket.name, config: config)
            }
        await NotificationConfigCache.shared.load(initialData: notificationData)

        // Load replication configuration cache
        let replicationData: [(bucketName: String, config: ReplicationConfiguration)] =
            allBuckets.compactMap { bucket in
                guard let config = replicationConfig(for: bucket) else { return nil }
                return (bucketName: bucket.name, config: config)
            }
        await ReplicationConfigCache.shared.load(initialData: replicationData)

        // Load cluster membership cache - harmless empty load when
        // cluster mode is off, since no node ever registers itself into `cluster_nodes` then.
        let clusterNodes = try await ClusterNode.query(on: app.db).all()
        let clusterNodeData = clusterNodes.compactMap { node -> ClusterNodeInfo? in
            guard let id = node.id else { return nil }
            return ClusterNodeInfo(
                id: id, address: node.address,
                status: ClusterNode.Status(rawValue: node.status) ?? .active,
                lastHeartbeatAt: node.lastHeartbeatAt,
                totalBytes: node.totalBytes, availableBytes: node.availableBytes)
        }
        await ClusterNodeCache.shared.load(initialData: clusterNodeData)
    }

    // MARK: - Per-bucket mappers

    /// Shared by the bulk load above and `CacheReloadDispatch`'s single-item reload, so the two
    /// paths can never drift apart on what a stored JSON policy actually decodes to. Returns
    /// nil (and logs) for a policy that fails to re-validate - it should always have been valid
    /// when it was saved, so this is a defensive skip, not an expected case.
    static func parsedPolicy(for bucket: Bucket, logger: Logger) -> BucketPolicy? {
        guard let rawPolicy = bucket.policy else { return nil }
        guard
            let policy = try? BucketPolicy.parseAndValidate(
                rawJSON: rawPolicy, bucketName: bucket.name, requestId: "cache-reload")
        else {
            logger.error("Failed to load stored policy for bucket '\(bucket.name)' - skipping")
            return nil
        }
        return policy
    }

    /// nil when every flag is false - a bucket with no restrictions behaves identically to not
    /// being in the cache at all, so there's nothing worth caching.
    static func publicAccessBlockIfNonDefault(for bucket: Bucket) -> PublicAccessBlockConfiguration? {
        let config = bucket.publicAccessBlock
        guard
            config.blockPublicAcls || config.ignorePublicAcls || config.blockPublicPolicy
                || config.restrictPublicBuckets
        else { return nil }
        return config
    }

    /// nil when there are no rules - an empty rule set behaves identically to not being in the
    /// cache (`NotificationConfigCache.config(for:)`'s nil fast path).
    static func notificationConfig(for bucket: Bucket) -> NotificationConfiguration? {
        guard let raw = bucket.notificationConfig else { return nil }
        let config = NotificationConfiguration.fromJSON(raw)
        guard !config.rules.isEmpty else { return nil }
        return config
    }

    /// nil when there are no rules - same reasoning as `notificationConfig(for:)`.
    static func replicationConfig(for bucket: Bucket) -> ReplicationConfiguration? {
        guard let raw = bucket.replicationConfig else { return nil }
        let config = ReplicationConfiguration.fromJSON(raw)
        guard !config.rules.isEmpty else { return nil }
        return config
    }
}
