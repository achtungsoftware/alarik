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
import Foundation
import Vapor

/// Applies an incoming `CacheInvalidationMessage` (received over the Postgres LISTEN channel
/// by `CacheInvalidationListener`) to this node's own in-memory caches. A flat switch over
/// `(cache, op)` rather than a registry/protocol abstraction - the set of caches is small and
/// fixed, so a registry would be indirection with no payoff.
///
/// Every `upsert` re-reads the relevant row from this node's own DB via
/// `LoadCacheLifecycle`'s shared mapper helpers - the exact same parsing `LoadCacheLifecycle`
/// uses for the boot-time bulk load, so the two paths can never silently drift apart on what a
/// stored JSON blob decodes to.
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

    private static func apply(message: CacheInvalidationMessage, app: Application) async throws {
        let bucketName = message.key

        switch (message.cache, message.op) {
        case ("bucketVersioning", .upsert):
            if let bucket = try await Bucket.query(on: app.db).filter(\.$name == bucketName).first() {
                let status = VersioningStatus(rawValue: bucket.versioningStatus) ?? .disabled
                await BucketVersioningCache.shared.setStatus(for: bucketName, status: status)
            } else {
                await BucketVersioningCache.shared.removeBucket(bucketName)
            }
        case ("bucketVersioning", .remove):
            await BucketVersioningCache.shared.removeBucket(bucketName)

        case ("accessKeySecret", .upsert):
            let accessKey = message.key
            if let key = try await AccessKey.query(on: app.db).filter(\.$accessKey == accessKey).first() {
                await AccessKeySecretKeyMapCache.shared.add(
                    accessKey: key.accessKey, secretKey: key.secretKey)
            } else {
                await AccessKeySecretKeyMapCache.shared.remove(accessKey: accessKey)
            }
        case ("accessKeySecret", .remove):
            await AccessKeySecretKeyMapCache.shared.remove(accessKey: message.key)

        case ("accessKeyUser", .upsert):
            let accessKey = message.key
            if let key = try await AccessKey.query(on: app.db).filter(\.$accessKey == accessKey).first() {
                await AccessKeyUserMapCache.shared.add(accessKey: key.accessKey, userId: key.$user.id)
            } else {
                await AccessKeyUserMapCache.shared.remove(accessKey: accessKey)
            }
        case ("accessKeyUser", .remove):
            await AccessKeyUserMapCache.shared.remove(accessKey: message.key)

        case ("accessKeyBucket", .upsert):
            // Derived cache: recompute this one access key's entire bucket set from scratch
            // rather than trying to apply a single delta - it's the only representation that
            // can't drift from the DB.
            let accessKey = message.key
            await AccessKeyBucketMapCache.shared.removeAccessKey(accessKey)
            if let key = try await AccessKey.query(on: app.db).filter(\.$accessKey == accessKey).first() {
                let buckets = try await Bucket.query(on: app.db)
                    .filter(\.$user.$id == key.$user.id)
                    .all()
                for bucket in buckets {
                    await AccessKeyBucketMapCache.shared.add(accessKey: accessKey, bucketName: bucket.name)
                }
            }
        case ("accessKeyBucket", .remove):
            await AccessKeyBucketMapCache.shared.removeAccessKey(message.key)
        case ("accessKeyBucket", .removeBucket):
            await AccessKeyBucketMapCache.shared.removeAll(for: bucketName)

        case ("bucketPolicy", .upsert):
            if let bucket = try await Bucket.query(on: app.db).filter(\.$name == bucketName).first(),
                let policy = LoadCacheLifecycle.parsedPolicy(for: bucket, logger: app.logger)
            {
                await BucketPolicyCache.shared.setPolicy(for: bucketName, policy: policy)
            } else {
                await BucketPolicyCache.shared.removePolicy(for: bucketName)
            }
        case ("bucketPolicy", .remove):
            await BucketPolicyCache.shared.removePolicy(for: bucketName)

        case ("bucketPublicAccessBlock", .upsert):
            if let bucket = try await Bucket.query(on: app.db).filter(\.$name == bucketName).first(),
                let config = LoadCacheLifecycle.publicAccessBlockIfNonDefault(for: bucket)
            {
                await BucketPolicyCache.shared.setPublicAccessBlock(for: bucketName, configuration: config)
            } else {
                await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)
            }
        case ("bucketPublicAccessBlock", .remove):
            await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)

        case ("notificationConfig", .upsert):
            if let bucket = try await Bucket.query(on: app.db).filter(\.$name == bucketName).first(),
                let config = LoadCacheLifecycle.notificationConfig(for: bucket)
            {
                await NotificationConfigCache.shared.setConfig(for: bucketName, config: config)
            } else {
                await NotificationConfigCache.shared.removeBucket(bucketName)
            }
        case ("notificationConfig", .remove):
            await NotificationConfigCache.shared.removeBucket(bucketName)

        case ("replicationConfig", .upsert):
            if let bucket = try await Bucket.query(on: app.db).filter(\.$name == bucketName).first(),
                let config = LoadCacheLifecycle.replicationConfig(for: bucket)
            {
                await ReplicationConfigCache.shared.setConfig(for: bucketName, config: config)
            } else {
                await ReplicationConfigCache.shared.removeBucket(bucketName)
            }
        case ("replicationConfig", .remove):
            await ReplicationConfigCache.shared.removeBucket(bucketName)

        case ("clusterNode", .upsert):
            let nodeId = message.key
            guard let uuid = UUID(uuidString: nodeId) else { break }
            if let node = try await ClusterNode.find(uuid, on: app.db) {
                await ClusterNodeCache.shared.upsert(
                    ClusterNodeInfo(
                        id: uuid, address: node.address,
                        status: ClusterNode.Status(rawValue: node.status) ?? .active,
                        lastHeartbeatAt: node.lastHeartbeatAt,
                        totalBytes: node.totalBytes, availableBytes: node.availableBytes))
            } else {
                await ClusterNodeCache.shared.remove(id: uuid)
            }
            // Membership genuinely changed (join, status flip, or a heartbeat refresh) - kick
            // off a rebalance walk. Cheap no-op when nothing actually needs to move: the walk
            // only enqueues tasks for objects whose responsible set changed.
            await ClusterRebalanceService.scheduleRebalance(app: app, reason: .membershipChange)
        case ("clusterNode", .remove):
            if let uuid = UUID(uuidString: message.key) {
                await ClusterNodeCache.shared.remove(id: uuid)
                await ClusterRebalanceService.scheduleRebalance(app: app, reason: .membershipChange)
            }

        case ("clusterRebalance", _):
            // Operator-triggered resync: unlike a genuine membership change (whose NOTIFY carries
            // a specific node id), this is a deliberate "re-check placement everywhere" broadcast,
            // so every node runs its own self-scoped walk. That's what makes resync actually
            // cluster-wide - the walk only ever sees this node's local disk, so a node holding an
            // under-replicated object must run its own walk to push the missing copies out.
            await ClusterRebalanceService.scheduleRebalance(app: app, reason: .manualResync)

        default:
            app.logger.error("Unknown cache invalidation message: \(message)")
        }
    }
}
