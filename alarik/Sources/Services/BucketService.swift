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
import XMLCoder

struct BucketService {
    static func create(
        app: Application,
        bucketName: String,
        userId: UUID,
        versioningEnabled: Bool = false
    )
        async throws
    {
        // Load-bearing, not merely defense in depth. `.alarik.sys` is indeed already unreachable
        // via the bucket-name validator's character class (it can't start with `.`), but
        // `MetadataNamespace.reservedRootPaths` also covers the probe endpoints, and those ARE
        // valid S3 bucket names - nothing upstream rejects them. Since this is the single
        // low-level chokepoint every creation path funnels through, it is the only place that
        // reliably stops a bucket being created that path-style routing could never reach again.
        guard !MetadataNamespace.isReserved(bucketName) else {
            throw S3Error(
                status: .forbidden, code: "InvalidBucketName",
                message: "This bucket name is reserved.")
        }

        let bucket: Bucket = Bucket(
            name: bucketName, userId: userId,
            versioningStatus: versioningEnabled ? .enabled : .disabled)

        // Deliberately outside the do/catch below: if this fails (the name is already taken -
        // `create` is a `putIfAbsent`), nothing has been created yet, so there is nothing to
        // roll back. Rolling back here would otherwise delete an *existing* bucket's directory
        // that this call never created, just because it shares the requested name.
        guard try await bucket.create(app: app) else {
            throw S3Error(
                status: .conflict, code: "BucketAlreadyExists",
                message: "The requested bucket name is not available.")
        }

        do {
            try BucketHandler.create(name: bucketName)

            // Get all access keys for this user
            let userAccessKeys = await AccessKeyUserMapCache.shared.accessKeys(for: userId)

            // Map the bucket to ALL of the user's access keys. Uses `notifyAndWait`, not the
            // fire-and-forget `notify`: a brand-new bucket is routinely accessed from every node
            // within milliseconds of this call returning, so this broadcast needs to actually
            // land before this function returns, not just be attempted.
            for accessKey in userAccessKeys {
                await AccessKeyBucketMapCache.shared.add(
                    accessKey: accessKey,
                    bucketName: bucketName
                )
                await CacheInvalidationService.notifyAndWait(
                    app: app, cache: "accessKeyBucket", op: .upsert, key: accessKey)
            }

            await BucketVersioningCache.shared.addBucket(
                bucketName, versioningStatus: versioningEnabled ? .enabled : .disabled)
            CacheInvalidationService.notify(
                app: app, cache: "bucketVersioning", op: .upsert, key: bucketName)
        } catch {
            // Best-effort: each step rolls back independently, so a failure in one (e.g. the
            // directory was never created) doesn't prevent the others from running, and the
            // original error - not a secondary rollback failure - is always what's thrown.
            try? await bucket.delete(app: app)
            try? BucketHandler.delete(name: bucketName, force: true)
            await BucketVersioningCache.shared.removeBucket(bucketName)
            CacheInvalidationService.notify(
                app: app, cache: "bucketVersioning", op: .remove, key: bucketName)
            throw error
        }
    }

    static func delete(
        req: Request,
        bucketName: String,
        userId: UUID,
        force: Bool = false
    )
        async throws
    {
        // A force delete may remove a genuinely non-empty bucket - every object, across every
        // version and delete marker, on every node, must be explicitly deleted first, or other
        // nodes' physical copies would be left orphaned. Propagates a per-object failure: if any
        // object can't be confirmed deleted everywhere, the Bucket row below must not be dropped.
        if force {
            try await Self.deleteAllObjectsClusterWide(req: req, bucketName: bucketName)
        }

        if let bucket = try await Bucket.find(app: req.application, name: bucketName),
            bucket.userId == userId
        {
            try await bucket.delete(app: req.application)
        }

        await AccessKeyBucketMapCache.shared.removeAll(for: bucketName)
        CacheInvalidationService.notify(
            app: req.application, cache: "accessKeyBucket", op: .removeBucket, key: bucketName)
        await BucketVersioningCache.shared.removeBucket(bucketName)
        CacheInvalidationService.notify(
            app: req.application, cache: "bucketVersioning", op: .remove, key: bucketName)
        await BucketPolicyCache.shared.removePolicy(for: bucketName)
        CacheInvalidationService.notify(
            app: req.application, cache: "bucketPolicy", op: .remove, key: bucketName)
        await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)
        CacheInvalidationService.notify(
            app: req.application, cache: "bucketPublicAccessBlock", op: .remove, key: bucketName)
        await NotificationConfigCache.shared.removeBucket(bucketName)
        CacheInvalidationService.notify(
            app: req.application, cache: "notificationConfig", op: .remove, key: bucketName)
        await ReplicationConfigCache.shared.removeBucket(bucketName)
        CacheInvalidationService.notify(
            app: req.application, cache: "replicationConfig", op: .remove, key: bucketName)

        // Drop any queued webhook deliveries / replication tasks for the deleted bucket -
        // retrying them would announce or push objects for a bucket that no longer exists.
        // Cluster-wide: each of the 3 task types can be owned by any node, not just this one.
        await OutboxMailbox.purgeBucketAcrossCluster(
            NotificationDelivery.self, app: req.application,
            collection: OutboxCollections.notificationDeliveries, bucketName: bucketName
        ) { $0.bucketName == bucketName }
        await OutboxMailbox.purgeBucketAcrossCluster(
            ReplicationTask.self, app: req.application, collection: OutboxCollections.replicationTasks,
            bucketName: bucketName
        ) { $0.bucketName == bucketName }
        await OutboxMailbox.purgeBucketAcrossCluster(
            ClusterReplicationTask.self, app: req.application,
            collection: OutboxCollections.clusterReplicationTasks, bucketName: bucketName
        ) { $0.bucketName == bucketName }

        try BucketHandler.delete(name: bucketName, force: force)
    }

    /// Enumerates every current object, historical version, and delete marker across the *whole
    /// cluster* (not just this node's local disk) and deletes each one via the same cluster-aware
    /// per-key routing/replication `S3Controller.handleDeleteObjects` and the admin console's
    /// folder delete already use - so a force-deleted bucket's data is actually gone everywhere,
    /// not just on whichever node happened to field the delete request.
    private static func deleteAllObjectsClusterWide(req: Request, bucketName: String) async throws {
        var keyMarker: String?
        var versionIdMarker: String?
        repeat {
            let (versions, deleteMarkers, _, isTruncated, nextKeyMarker, nextVersionIdMarker) =
                try await ClusterListingService.listAllVersions(
                    req: req, bucketName: bucketName, prefix: "", delimiter: nil,
                    keyMarker: keyMarker, versionIdMarker: versionIdMarker, maxKeys: 1000)

            for entry in versions + deleteMarkers {
                // versioningStatus: .disabled unconditionally - regardless of the bucket's real
                // status, so this hard-deletes rather than creating a delete marker. Irrelevant
                // whenever entry.versionId is non-nil anyway (a specific version/marker id always
                // takes the hard-delete path), and correct for the nil case too: a bucket-wide
                // force delete is a permanent prune, not a versioning-aware operation.
                _ = try await ClusterReplicationService.deleteObjectClusterWide(
                    req: req, bucketName: bucketName, key: entry.key, versionId: entry.versionId,
                    versioningStatus: .disabled)
            }

            keyMarker = isTruncated ? nextKeyMarker : nil
            versionIdMarker = isTruncated ? nextVersionIdMarker : nil
        } while keyMarker != nil
    }
}
