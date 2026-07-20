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

import Vapor

final class LoadCacheLifecycle: LifecycleHandler {
    /// Delays for the boot-time catch-up retries below - short and front-loaded, since the gap
    /// they're closing is a one-time race during a node's very first seconds alive, not a
    /// steady-state condition worth polling for long. The server starts accepting real traffic
    /// the instant `didBootAsync` returns - this catch-up runs in a detached background task, so
    /// it does NOT block that - meaning a request can legitimately arrive before even the first
    /// retry fires. Starting at 1s (not 3s) shrinks that specific window as much as is reasonable
    /// without spamming a peer that's still mid-boot itself; the remaining delays stay spread out
    /// since by then a genuinely slow peer needs real time, not tighter polling.
    static let bootCatchUpDelays: [Duration] = [
        .seconds(1), .seconds(2), .seconds(4), .seconds(8), .seconds(15),
    ]

    func didBootAsync(_ app: Application) async throws {
        do {
            try await LoadCacheLifecycle.reloadAll(app: app)
        } catch {
            app.logger.error("Failed to load cache: \(error)")
        }

        // The boot-time fan-out this reload just ran is a genuine one-shot race: a node started
        // early in a simultaneous multi-node cold start (most notably the very first node, before
        // any peer even exists yet to fan out to) can complete its own boot before a peer that
        // holds records this node needs (the seeded admin key chief among them) has finished ITS
        // boot and started answering `/internal/cluster/metadata/list`. Every cache load this
        // function performs is upsert-only (see `reloadAll`'s doc comment), so re-running it a few
        // more times over the following seconds costs nothing beyond the fan-out itself and can
        // only ever fill gaps the first attempt missed - never re-introduce one it already closed.
        // This is what keeps a fresh cluster's convergence bounded and fast without resorting to a
        // live per-request lookup on the hot SigV4 auth path.
        if app.storage[ClusterConfigurationKey.self] != nil {
            Task {
                for delay in LoadCacheLifecycle.bootCatchUpDelays {
                    try? await Task.sleep(for: delay)
                    do {
                        try await LoadCacheLifecycle.reloadAll(app: app)
                    } catch {
                        app.logger.warning("Boot-time cache catch-up reload failed: \(error)")
                    }
                }
            }
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

    /// The full boot-time bulk load, factored out so it can also be run as the periodic safety
    /// net that recovers from any gap in the cache-invalidation broadcast stream. Throws on
    /// failure (unlike `didBootAsync`, which catches and only logs) so each caller can decide
    /// how to react.
    ///
    /// Every cache load below is upsert-only (merged in, nothing already cached is ever dropped)
    /// - this function is called more than once in a node's lifetime: at boot, again whenever
    /// `ClusterMembershipLifecycle` discovers a new peer (see its `reseedFromConfiguredSeeds` doc
    /// comment), and on a 60s periodic timer - and `MetadataListingService`'s cluster-wide fan-out
    /// every one of these loads relies on is a best-effort snapshot, not a guaranteed-complete one
    /// (a peer that's merely slow to answer, or transiently unreachable during a node
    /// kill/restart, just contributes nothing to *that* call). A destructive full replace on any
    /// call whose fan-out happens to be less complete than a *previous* call's would silently wipe
    /// out entries that were already correctly cached - including, worst case, a live access key
    /// or bucket config still very much in use, on every node, until the next periodic tick
    /// happens to see a more complete fan-out. This was the root cause of intermittent
    /// `InvalidAccessKeyId` failures reappearing well after a cluster had already converged,
    /// whenever the periodic reload's fan-out raced a `kill_node`/`restart_node` scenario.
    ///
    /// Genuine removal (a key revoked, a bucket deleted) is never this function's job to detect -
    /// it already has its own authoritative, targeted signal via `CacheInvalidationService`'s
    /// `.remove` broadcasts (see `AccessKeyService.delete`/`BucketService`'s delete path and
    /// `CacheReloadDispatch`'s corresponding `.remove` cases), exactly mirroring
    /// `ClusterNodeCache.reconcile`'s reasoning below - a merely-incomplete fan-out and a genuine
    /// departure are indistinguishable from a listing snapshot alone, so this function must never
    /// treat "absent from this one fan-out" as removal.
    static func reloadAll(app: Application) async throws {
        // Load all non-expired access keys
        let keys = try await AccessKey.all(app: app).filter {
            $0.expirationDate == nil || $0.expirationDate! > Date.now
        }

        // Every bucket, cluster-wide - loaded once and reused below for the versioning/policy/
        // public-access-block/notification/replication caches too, rather than re-listing per
        // cache.
        let allBuckets = try await Bucket.all(app: app)

        // Map userID -> buckets, scoped to the users referenced by access keys
        let userIDs = Set(keys.map { $0.userId })
        let bucketsByUser = Dictionary(
            grouping: allBuckets.filter { userIDs.contains($0.userId) }, by: { $0.userId })

        // Build cache mappings
        var bucketData: [(accessKey: String, bucketName: String)] = []
        var userMappingData: [(accessKey: String, userId: UUID)] = []
        var secretKeyData: [(accessKey: String, secretKey: String)] = []

        for key in keys {
            secretKeyData.append((accessKey: key.accessKey, secretKey: key.secretKey))

            let userID = key.userId

            // Add to user mapping cache
            userMappingData.append((accessKey: key.accessKey, userId: userID))

            guard let userBuckets = bucketsByUser[userID] else { continue }

            for bucket in userBuckets {
                bucketData.append((accessKey: key.accessKey, bucketName: bucket.name))
            }
        }

        for (accessKey, secretKey) in secretKeyData {
            await AccessKeySecretKeyMapCache.shared.add(accessKey: accessKey, secretKey: secretKey)
        }
        for (accessKey, userId) in userMappingData {
            await AccessKeyUserMapCache.shared.add(accessKey: accessKey, userId: userId)
        }
        for (accessKey, bucketName) in bucketData {
            await AccessKeyBucketMapCache.shared.add(accessKey: accessKey, bucketName: bucketName)
        }

        // Bucket versioning status - upsert only, see the doc comment above.
        for bucket in allBuckets {
            let status = VersioningStatus(rawValue: bucket.versioningStatus) ?? .disabled
            await BucketVersioningCache.shared.addBucket(bucket.name, versioningStatus: status)
        }

        // Bucket policy - skip (not crash) any policy that fails to re-validate, since it should
        // always have been valid when it was saved.
        for bucket in allBuckets {
            guard let policy = parsedPolicy(for: bucket, logger: app.logger) else { continue }
            await BucketPolicyCache.shared.setPolicy(for: bucket.name, policy: policy)
        }

        // Public access block - only buckets with at least one flag set are worth caching, an
        // all-false bucket behaves identically to "not in the map".
        for bucket in allBuckets {
            guard let config = publicAccessBlockIfNonDefault(for: bucket) else { continue }
            await BucketPolicyCache.shared.setPublicAccessBlock(
                for: bucket.name, configuration: config)
        }

        // Notification (webhook) configuration
        for bucket in allBuckets {
            guard let config = notificationConfig(for: bucket) else { continue }
            await NotificationConfigCache.shared.setConfig(for: bucket.name, config: config)
        }

        // Replication configuration
        for bucket in allBuckets {
            guard let config = replicationConfig(for: bucket) else { continue }
            await ReplicationConfigCache.shared.setConfig(for: bucket.name, config: config)
        }

        // Load cluster membership cache - harmless empty load when cluster mode is off, since no
        // node ever registers itself into the `cluster-nodes` collection then. `reconcile`, not
        // `load`: `ClusterNode.all` is a best-effort cluster-wide fan-out
        // (`MetadataListingService`), so a peer merely slow to answer must not have its
        // (still-fresher) cached entry clobbered by its absence here.
        let clusterNodes = try await ClusterNode.all(app: app)
        let clusterNodeData = clusterNodes.compactMap { node -> ClusterNodeInfo? in
            guard let status = ClusterNode.Status(rawValue: node.status) else { return nil }
            return ClusterNodeInfo(
                id: node.id, address: node.address, status: status,
                lastHeartbeatAt: node.lastHeartbeatAt,
                totalBytes: node.totalBytes, availableBytes: node.availableBytes)
        }
        await ClusterNodeCache.shared.reconcile(snapshot: clusterNodeData)
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
