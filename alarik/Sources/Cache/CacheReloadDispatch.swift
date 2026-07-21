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

/// Applies an incoming `CacheInvalidationMessage` (broadcast by `CacheInvalidationService`) to
/// this node's own in-memory caches. A flat switch over `(cache, op)` rather than a registry/
/// protocol abstraction - the set of caches is small and fixed, so a registry would be
/// indirection with no payoff. Every `upsert` re-reads the relevant record via
/// `LoadCacheLifecycle`'s shared mapper helpers, the same parsing the boot-time bulk load uses,
/// so the two paths can never silently drift apart on what a stored JSON blob decodes to.
enum CacheReloadDispatch {
    static func apply(payload: String, app: Application) async {
        guard let data = payload.data(using: .utf8),
            let message = try? JSONDecoder().decode(CacheInvalidationMessage.self, from: data)
        else {
            app.logger.error("Malformed cache invalidation payload: \(payload)")
            return
        }

        do {
            try await apply(message: message, app: app)
        } catch {
            app.logger.error("Failed to apply cache invalidation \(message): \(error)")
        }
    }

    static func apply(message: CacheInvalidationMessage, app: Application) async throws {
        let bucketName = message.key

        switch (message.cache, message.op) {
        case ("bucketVersioning", .upsert):
            // `Bucket.find` is a live, best-effort read - see `("accessKeySecret", .upsert)`'s
            // doc comment below for the full reasoning (identical here): only set on success,
            // never remove on a miss, since genuine bucket deletion already has its own explicit
            // `.remove` signal right below. Removing here on a transient miss didn't just risk a
            // stale cache entry - it silently reset the bucket to the versioning-disabled default,
            // a real behavior change for a bucket that was never actually touched.
            if let bucket = try await Bucket.find(app: app, name: bucketName) {
                let status = VersioningStatus(rawValue: bucket.versioningStatus) ?? .disabled
                await BucketVersioningCache.shared.setStatus(for: bucketName, status: status)
            }
        case ("bucketVersioning", .remove):
            await BucketVersioningCache.shared.removeBucket(bucketName)

        case ("accessKeySecret", .upsert):
            // `AccessKey.find` is a live, best-effort read (`MetadataStore.get`, subject to the
            // same placement-drift/transient-unavailability windows documented throughout
            // `MetadataStore`) - a single failed lookup here does NOT mean the key was deleted,
            // only that this one read didn't land. Only ADD on success; never remove on a miss -
            // genuine deletion already has its own explicit, targeted signal
            // (`("accessKeySecret", .remove)` right below, from `AccessKeyService.delete`), so
            // this upsert path removing on top of that was pure downside: it could - and did -
            // silently evict a perfectly valid, currently-in-use key's cache entry on nothing
            // more than a transient read hiccup, with no explicit deletion involved at all.
            let accessKey = message.key
            if let key = try await AccessKey.find(app: app, accessKey: accessKey) {
                await AccessKeySecretKeyMapCache.shared.add(
                    accessKey: key.accessKey, secretKey: key.secretKey)
            }
        case ("accessKeySecret", .remove):
            await AccessKeySecretKeyMapCache.shared.remove(accessKey: message.key)

        case ("accessKeyUser", .upsert):
            // Same reasoning as `("accessKeySecret", .upsert)` immediately above - only add on a
            // successful read, never remove on a miss.
            let accessKey = message.key
            if let key = try await AccessKey.find(app: app, accessKey: accessKey) {
                await AccessKeyUserMapCache.shared.add(accessKey: key.accessKey, userId: key.userId)
            }
        case ("accessKeyUser", .remove):
            await AccessKeyUserMapCache.shared.remove(accessKey: message.key)

        case ("accessKeyBucket", .upsert):
            // Additive merge, NOT wipe-then-rebuild (see git history for the previous approach
            // and why it was wrong): this fires on every single bucket creation for a user (see
            // `BucketService.create`, which notifies this for every access key that user owns) -
            // a long-running cluster creates many buckets over its lifetime, and every one of
            // them used to trigger a full wipe of every OTHER node's entire cached bucket set for
            // this key, rebuilt from `Bucket.all(app:)` - a best-effort, eventually-consistent
            // cluster-wide fan-out (`MetadataListingService.list`). Any single incomplete fan-out
            // (one transiently slow/unreachable peer, one listing race) meant a bucket this node
            // correctly had cached a moment ago would be missing from the rebuild and silently
            // disappear - permanently, until the next unrelated bucket creation happened to
            // trigger another (possibly also incomplete) rebuild. With dozens of bucket-creation
            // events across a long suite run, the odds of at least one incomplete fan-out costing
            // a real, currently-valid bucket its cached access were high - this is what was
            // actually behind the recurring "AccessDenied for a bucket that unquestionably
            // exists" failures. Genuine bucket deletion already has its own explicit, targeted
            // signal (`.removeBucket` below) - the upsert path only ever needs to ADD what the
            // read finds, never remove what it doesn't.
            let accessKey = message.key
            if let key = try await AccessKey.find(app: app, accessKey: accessKey) {
                let buckets = try await Bucket.all(app: app).filter { $0.userId == key.userId }
                for bucket in buckets {
                    await AccessKeyBucketMapCache.shared.add(accessKey: accessKey, bucketName: bucket.name)
                }
            }
        case ("accessKeyBucket", .remove):
            await AccessKeyBucketMapCache.shared.removeAccessKey(message.key)
        case ("accessKeyBucket", .removeBucket):
            await AccessKeyBucketMapCache.shared.removeAll(for: bucketName)

        case ("bucketPolicy", .upsert):
            // Never remove on a `Bucket.find` miss - same reasoning as `("bucketVersioning",
            // .upsert)` above: an unreadable bucket is ambiguous (transient hiccup vs. genuine
            // absence), and genuine bucket deletion already has its own signal. But once the
            // bucket IS found, "no policy" is no longer ambiguous in the same way: `bucket.policy
            // == nil` is a definitive, successfully-confirmed fact (as opposed to
            // `parsedPolicy` returning nil because a *non-nil* stored policy failed to parse,
            // which is a genuine data problem, not "no policy" - that case must NOT clear the
            // cache, or a real but currently-unparseable policy would silently stop being
            // enforced). Only the confirmed-absent case is safe to clear a stale cache entry for.
            if let bucket = try await Bucket.find(app: app, name: bucketName) {
                if let policy = LoadCacheLifecycle.parsedPolicy(for: bucket, logger: app.logger) {
                    await BucketPolicyCache.shared.setPolicy(for: bucketName, policy: policy)
                } else if bucket.policy == nil {
                    await BucketPolicyCache.shared.removePolicy(for: bucketName)
                }
            }
        case ("bucketPolicy", .remove):
            await BucketPolicyCache.shared.removePolicy(for: bucketName)

        case ("bucketPublicAccessBlock", .upsert):
            // Never remove on a `Bucket.find` miss (ambiguous), same as `bucketPolicy` above -
            // but `publicAccessBlockIfNonDefault` returning nil (unlike `parsedPolicy`) has no
            // parse-failure path at all, only "every flag reads false" - always a definitive,
            // successfully-confirmed fact, so it's always safe to clear a stale entry here.
            if let bucket = try await Bucket.find(app: app, name: bucketName) {
                if let config = LoadCacheLifecycle.publicAccessBlockIfNonDefault(for: bucket) {
                    await BucketPolicyCache.shared.setPublicAccessBlock(for: bucketName, configuration: config)
                } else {
                    await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)
                }
            }
        case ("bucketPublicAccessBlock", .remove):
            await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)

        case ("notificationConfig", .upsert):
            // Never remove on a `Bucket.find` miss (ambiguous) - but `notificationConfig`
            // returning nil, like `publicAccessBlockIfNonDefault`, has no parse-failure path
            // (`fromJSON` never throws, always definitive), so it's always safe to clear a stale
            // entry once the bucket is confirmed found.
            if let bucket = try await Bucket.find(app: app, name: bucketName) {
                if let config = LoadCacheLifecycle.notificationConfig(for: bucket) {
                    await NotificationConfigCache.shared.setConfig(for: bucketName, config: config)
                } else {
                    await NotificationConfigCache.shared.removeBucket(bucketName)
                }
            }
        case ("notificationConfig", .remove):
            await NotificationConfigCache.shared.removeBucket(bucketName)

        case ("replicationConfig", .upsert):
            // Same reasoning as `notificationConfig` above - always safe to clear once the
            // bucket is confirmed found.
            if let bucket = try await Bucket.find(app: app, name: bucketName) {
                if let config = LoadCacheLifecycle.replicationConfig(for: bucket) {
                    await ReplicationConfigCache.shared.setConfig(for: bucketName, config: config)
                } else {
                    await ReplicationConfigCache.shared.removeBucket(bucketName)
                }
            }
        case ("replicationConfig", .remove):
            await ReplicationConfigCache.shared.removeBucket(bucketName)

        case ("clusterNode", .upsert):
            let nodeId = message.key
            guard let uuid = UUID(uuidString: nodeId) else { break }
            // Use the broadcaster-supplied `nodeInfo` directly - never re-read this node's
            // record via `MetadataStore` here, see `CacheInvalidationService.notify`'s doc
            // comment on `nodeInfo` for the circular-placement reason why. A message from an
            // older binary with no `nodeInfo` (or a caller that genuinely couldn't supply one)
            // falls back to the old re-read, best-effort.
            // `reconcile`, not raw upsert: broadcast delivery is retried, so a pre-restart
            // heartbeat message can land AFTER newer post-restart data and would otherwise
            // regress the entry into staleness. `reconcile` keeps whichever is fresher.
            if let wire = message.nodeInfo {
                await ClusterNodeCache.shared.reconcile(snapshot: [
                    ClusterNodeInfo(
                        id: wire.id, address: wire.address,
                        status: ClusterNode.Status(rawValue: wire.status) ?? .active,
                        lastHeartbeatAt: wire.lastHeartbeatAt, totalBytes: wire.totalBytes,
                        availableBytes: wire.availableBytes)
                ])
            } else if let node = try await ClusterNode.find(app: app, id: uuid) {
                await ClusterNodeCache.shared.reconcile(snapshot: [
                    ClusterNodeInfo(
                        id: uuid, address: node.address,
                        status: ClusterNode.Status(rawValue: node.status) ?? .active,
                        lastHeartbeatAt: node.lastHeartbeatAt,
                        totalBytes: node.totalBytes, availableBytes: node.availableBytes)
                ])
            }
            // Membership genuinely changed (join, status flip, or a heartbeat refresh) - kick
            // off a rebalance walk. Cheap no-op when nothing actually needs to move: the walk
            // only enqueues tasks for objects whose responsible set changed.
            await ClusterRebalanceService.scheduleRebalance(app: app, reason: .membershipChange)
            await ErasureCodedRebalanceService.scheduleRebalance(app: app, reason: .membershipChange)
        case ("clusterNode", .remove):
            if let uuid = UUID(uuidString: message.key) {
                await ClusterNodeCache.shared.remove(id: uuid)
                await ClusterRebalanceService.scheduleRebalance(app: app, reason: .membershipChange)
                await ErasureCodedRebalanceService.scheduleRebalance(app: app, reason: .membershipChange)
            }

        case ("clusterRebalance", _):
            // Operator-triggered resync: unlike a genuine membership change (whose NOTIFY carries
            // a specific node id), this is a deliberate "re-check placement everywhere" broadcast,
            // so every node runs its own self-scoped walk. That's what makes resync actually
            // cluster-wide - the walk only ever sees this node's local disk, so a node holding an
            // under-replicated object must run its own walk to push the missing copies out.
            await ClusterRebalanceService.scheduleRebalance(app: app, reason: .manualResync)
            await ErasureCodedRebalanceService.scheduleRebalance(app: app, reason: .manualResync)

        case ("clusterScrub", _):
            // Operator-triggered bit-rot scrub, broadcast to every node for the same reason as a
            // resync: a scrub only sees this node's own local shards, so cluster-wide verification
            // needs every node to run its own pass.
            Task { await ErasureCodedScrubber.scrub(app: app) }

        default:
            app.logger.error("Unknown cache invalidation message: \(message)")
        }
    }
}
