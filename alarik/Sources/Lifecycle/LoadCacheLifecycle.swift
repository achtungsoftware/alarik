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
    /// Delays for the boot-time catch-up retries, which run in the background *after* the server
    /// is already accepting traffic. Every cache load is upsert-only (see `reloadAll`'s doc
    /// comment), so repeating it can only fill gaps the first attempt missed, never reintroduce one.
    ///
    /// These must stay off the boot path. `didBootAsync` runs before the server binds, and a
    /// single `reloadAll` fans out to every peer across several collections - with an unreachable
    /// peer costing the full probe timeout plus its retry, per collection. Making boot wait on
    /// even a couple of extra rounds pushes startup past the point where a supervisor (or
    /// `cluster_tests.sh`'s restart health check) gives up on the node entirely, turning a
    /// convergence optimization into a node that never comes back.
    static let bootCatchUpDelays: [Duration] = [
        .seconds(1), .seconds(2), .seconds(4), .seconds(8), .seconds(15),
    ]

    func didBootAsync(_ app: Application) async throws {
        do {
            try await LoadCacheLifecycle.reloadAll(app: app)
        } catch {
            app.logger.error("Failed to load cache: \(error)")
        }
        // Marked even when the load threw. A failed boot load leaves this node no worse off than
        // any node whose fan-out came back partial, and the catch-up retries below keep filling
        // gaps - whereas never becoming ready would keep a recoverable node out of service
        // permanently, which is the worse failure.
        await NodeReadiness.shared.markCacheLoaded()

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
    /// `gated` is for the periodic safety-net tick ONLY, and defaults to off.
    ///
    /// The digest gate reasons "nothing in the store changed, so every cache is already correct".
    /// That inference only holds when the caches were correct to begin with - it says nothing
    /// about a cache that is empty or has been cleared, because emptying a cache doesn't change
    /// the store. Boot is exactly that case: the caches start empty while the store sits
    /// unchanged, so a gated load there would skip the very work boot exists to do. Every caller
    /// that runs because the cache state is in question (boot, boot catch-up, discovering a peer)
    /// must therefore stay ungated; only the timer, which runs when nothing is known to have
    /// happened, may skip.
    static func reloadAll(app: Application, gated: Bool = false) async throws {
        // Membership first, and never gated: this is the durable backstop for membership now
        // that the refresh tick is bounded gossip rather than a full collection pull - it recovers
        // a node that is registered on disk but has fallen out of every live peer's cache. The
        // records are replicated like all metadata, so a node that is down is still listed.
        // `reconcile`, not `load`: `ClusterNode.all` is a best-effort fan-out, so a peer merely
        // slow to answer must not have its (still-fresher) cached entry clobbered by its absence.
        let clusterNodeData = await ClusterNode.all(app: app).map { node in
            ClusterNodeInfo(
                id: node.id, address: node.address, status: node.status,
                lastHeartbeatAt: node.lastHeartbeatAt,
                totalBytes: node.totalBytes, availableBytes: node.availableBytes)
        }
        await ClusterNodeCache.shared.reconcile(snapshot: clusterNodeData)

        // Everything below re-pulls whole collections from every peer. In a cluster where nothing
        // has been created or revoked since the last cycle - the normal state - that is pure
        // waste, so a digest comparison decides whether it is worth doing at all. The gate fails
        // open: any unreachable peer or probe error counts as "changed".
        //
        // The digests are refreshed even when `gated` is false, so an ungated load still updates
        // the baseline - otherwise the next gated tick would compare against a stale one and
        // redo work that just happened.
        let changed = await MetadataReloadGate.shared.changedSinceLastCheck(
            app: app,
            collections: [MetadataCollections.accessKeys, MetadataCollections.buckets])
        if gated && !changed { return }

        // Load all non-expired access keys
        let keyListing = await AccessKey.allVerified(app: app)
        let keys = keyListing.records.filter {
            $0.expirationDate == nil || $0.expirationDate! > Date.now
        }

        // Every bucket, cluster-wide - loaded once and reused below for the versioning/policy/
        // public-access-block/notification/replication caches too, rather than re-listing per
        // cache.
        let bucketListing = await Bucket.allVerified(app: app)
        let allBuckets = bucketListing.records

        // Map userID -> buckets, scoped to the users referenced by access keys
        let userIDs = Set(keys.map { $0.userId })
        let bucketsByUser = Dictionary(
            grouping: allBuckets.filter { userIDs.contains($0.userId) }, by: { $0.userId })

        // Build cache mappings
        var bucketData: [(accessKey: String, bucketName: String)] = []
        var userMappingData: [(accessKey: String, userId: UUID)] = []
        var secretKeyData: [(accessKey: String, secretKey: String, expiresAt: Date?)] = []

        for key in keys {
            secretKeyData.append(
                (accessKey: key.accessKey, secretKey: key.secretKey,
                 expiresAt: key.expirationDate))

            let userID = key.userId

            // Add to user mapping cache
            userMappingData.append((accessKey: key.accessKey, userId: userID))

            guard let userBuckets = bucketsByUser[userID] else { continue }

            for bucket in userBuckets {
                bucketData.append((accessKey: key.accessKey, bucketName: bucket.name))
            }
        }

        for (accessKey, secretKey, expiresAt) in secretKeyData {
            await AccessKeySecretKeyMapCache.shared.add(
                accessKey: accessKey, secretKey: secretKey, expiresAt: expiresAt)
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

        // Bucket policy and public access block, in one pass over the buckets (each is parsed
        // once, and buckets with neither are recorded as such so the anonymous request path can
        // answer "nothing set" from cache rather than paying a store read per bucket to
        // rediscover it after every restart). A policy that fails to re-validate is skipped, not
        // crashed on, since it should always have been valid when it was saved.
        for bucket in allBuckets {
            let policy = parsedPolicy(for: bucket, logger: app.logger)
            let publicAccessBlock = publicAccessBlockIfNonDefault(for: bucket)

            if let policy {
                await BucketPolicyCache.shared.setPolicy(for: bucket.name, policy: policy)
            }
            if let publicAccessBlock {
                await BucketPolicyCache.shared.setPublicAccessBlock(
                    for: bucket.name, configuration: publicAccessBlock)
            }
            if policy == nil, publicAccessBlock == nil {
                await BucketPolicyCache.shared.markWithoutAuthorization(bucket.name)
            }
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

        // Removal reconciliation - the upsert-only loads above can never DROP an entry, which
        // fixed wrongful drops but leaves the mirror problem: a node that missed every retry of
        // a `.remove` broadcast keeps a revoked key or deleted bucket cached. Two sources say a
        // cached entry is gone. A tombstone seen on any peer is positive evidence and counts
        // even from a partial listing - without it, one unreachable node kept a revoked key
        // valid everywhere else indefinitely. Mere absence counts only against a verifiably
        // COMPLETE listing (every peer answered, no record skipped): "absent from a partial
        // snapshot" and "deleted" are indistinguishable.
        //
        // Both are confirmed against the store before dropping: a key created DURING the
        // listing fan-out is absent from the snapshot yet already broadcast into this cache, and
        // a bucket name can be deleted and re-created. A store error keeps the entry
        // (conservative; retried next cycle).
        var staleKeys = keyListing.tombstonedIds
        if keyListing.complete {
            let cachedKeys = await AccessKeySecretKeyMapCache.shared.getMap().keys
            staleKeys.formUnion(cachedKeys.filter { !keyListing.presentIds.contains($0) })
        }
        for cached in staleKeys where await AccessKeySecretKeyMapCache.shared.exists(accessKey: cached) {
            let confirmed: AccessKey?
            do { confirmed = try await AccessKey.find(app: app, accessKey: cached) } catch { continue }
            guard confirmed == nil else { continue }
            app.logger.notice(
                "Cache reconcile: dropping access key '\(cached)' deleted while this node missed the removal broadcast."
            )
            await dropAccessKey(cached)
        }

        var staleBuckets = bucketListing.tombstonedIds
        if bucketListing.complete {
            let cachedBuckets = await BucketVersioningCache.shared.getMap().keys
            staleBuckets.formUnion(cachedBuckets.filter { !bucketListing.presentIds.contains($0) })
        }
        for cached in staleBuckets where await BucketVersioningCache.shared.getMap()[cached] != nil {
            let confirmedBucket: Bucket?
            do { confirmedBucket = try await Bucket.find(app: app, name: cached) } catch { continue }
            guard confirmedBucket == nil else { continue }
            app.logger.notice(
                "Cache reconcile: dropping bucket '\(cached)' deleted while this node missed the removal broadcast."
            )
            await dropBucket(cached)
        }
    }

    /// Every cache that remembers an access key.
    static func dropAccessKey(_ accessKey: String) async {
        await AccessKeySecretKeyMapCache.shared.remove(accessKey: accessKey)
        await AccessKeyUserMapCache.shared.remove(accessKey: accessKey)
        await AccessKeyBucketMapCache.shared.removeAccessKey(accessKey)
    }

    /// Every cache that remembers a bucket.
    static func dropBucket(_ bucketName: String) async {
        await BucketVersioningCache.shared.removeBucket(bucketName)
        await BucketPolicyCache.shared.removePolicy(for: bucketName)
        await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)
        await NotificationConfigCache.shared.removeBucket(bucketName)
        await ReplicationConfigCache.shared.removeBucket(bucketName)
        await AccessKeyBucketMapCache.shared.removeAll(for: bucketName)
    }

    // MARK: - Per-bucket mappers

    /// Shared by the bulk load above and `CacheReloadDispatch`'s single-item reload, so the two
    /// paths can never drift apart on what a stored JSON policy actually decodes to. Returns
    /// nil (and logs) for a policy that fails to re-validate - it should always have been valid
    /// when it was saved, so this is a defensive skip, not an expected case.
    static func parsedPolicy(for bucket: Bucket, logger: Logger) -> BucketPolicy? {
        guard let rawPolicy = bucket.policy else { return nil }
        do {
            return try BucketPolicy.parseAndValidate(
                rawJSON: rawPolicy, bucketName: bucket.name, requestId: "cache-reload")
        } catch {
            // Fail closed, but loudly: GetBucketPolicy still returns the stored document.
            let reason = (error as? S3Error)?.message ?? "\(error)"
            logger.error(
                "Stored bucket policy for '\(bucket.name)' no longer validates and is NOT enforced, while GetBucketPolicy still returns it - delete or re-put the policy. Reason: \(reason)"
            )
            return nil
        }
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
        guard let config = bucket.notificationConfig, !config.rules.isEmpty else { return nil }
        return config
    }

    /// nil when there are no rules - same reasoning as `notificationConfig(for:)`.
    static func replicationConfig(for bucket: Bucket) -> ReplicationConfiguration? {
        guard let config = bucket.replicationConfig, !config.rules.isEmpty else { return nil }
        return config
    }
}
