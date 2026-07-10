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
import XMLCoder

struct BucketService {
    static func create(
        on database: any Database,
        bucketName: String,
        userId: UUID,
        versioningEnabled: Bool = false
    )
        async throws
    {
        let bucket: Bucket = Bucket(
            name: bucketName, userId: userId,
            versioningStatus: versioningEnabled ? .enabled : .disabled)

        // Deliberately outside the do/catch below: if this fails (e.g. the name already
        // exists - `name` has a unique DB constraint), nothing has been created yet, so there
        // is nothing to roll back. Rolling back here would otherwise delete an *existing*
        // bucket's directory that this call never created, just because it shares the
        // requested name.
        try await bucket.save(on: database)

        do {
            try BucketHandler.create(name: bucketName)

            // Get all access keys for this user
            let userAccessKeys = await AccessKeyUserMapCache.shared.accessKeys(for: userId)

            // Map the bucket to ALL of the user's access keys - each key's set gets its own
            // notify (not one per bucket), since each iteration here touches a different key.
            for accessKey in userAccessKeys {
                await AccessKeyBucketMapCache.shared.add(
                    accessKey: accessKey,
                    bucketName: bucketName
                )
                CacheInvalidationService.notify(
                    on: database, cache: "accessKeyBucket", op: .upsert, key: accessKey)
            }

            await BucketVersioningCache.shared.addBucket(
                bucketName, versioningStatus: versioningEnabled ? .enabled : .disabled)
            CacheInvalidationService.notify(
                on: database, cache: "bucketVersioning", op: .upsert, key: bucketName)
        } catch {
            // Best-effort: each step rolls back independently, so a failure in one (e.g. the
            // directory was never created) doesn't prevent the others from running, and the
            // original error - not a secondary rollback failure - is always what's thrown.
            try? await bucket.delete(on: database)
            try? BucketHandler.delete(name: bucketName, force: true)
            await BucketVersioningCache.shared.removeBucket(bucketName)
            CacheInvalidationService.notify(
                on: database, cache: "bucketVersioning", op: .remove, key: bucketName)
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
        let database = req.db

        // A force delete may be removing a genuinely non-empty bucket (unlike the S3-protocol
        // DeleteBucket path, which requires cluster-wide emptiness *before* ever reaching this
        // function) - every object, across every version and delete marker, on every node, has
        // to be explicitly deleted first. Skipping this and only wiping this node's own local
        // directory (the previous behavior) left every other node's physical copies completely
        // orphaned: invisible while no Bucket row existed to reach them through the API, but
        // immediately visible again the moment a bucket with the same name was recreated, since
        // the on-disk path is derived purely from the bucket name, not any unique bucket id.
        // Propagates (doesn't swallow) a per-object failure - if any object can't be confirmed
        // deleted everywhere, the Bucket row below must not be dropped, or whatever's left would
        // become unreachably orphaned exactly like before. Safe to retry: already-deleted
        // objects simply won't reappear in the next listing.
        if force {
            try await Self.deleteAllObjectsClusterWide(req: req, bucketName: bucketName)
        }

        try await Bucket.query(on: database)
            .filter(\.$name == bucketName)
            .filter(\.$user.$id == userId)
            .delete()

        await AccessKeyBucketMapCache.shared.removeAll(for: bucketName)
        CacheInvalidationService.notify(
            on: database, cache: "accessKeyBucket", op: .removeBucket, key: bucketName)
        await BucketVersioningCache.shared.removeBucket(bucketName)
        CacheInvalidationService.notify(
            on: database, cache: "bucketVersioning", op: .remove, key: bucketName)
        await BucketPolicyCache.shared.removePolicy(for: bucketName)
        CacheInvalidationService.notify(on: database, cache: "bucketPolicy", op: .remove, key: bucketName)
        await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)
        CacheInvalidationService.notify(
            on: database, cache: "bucketPublicAccessBlock", op: .remove, key: bucketName)
        await NotificationConfigCache.shared.removeBucket(bucketName)
        CacheInvalidationService.notify(
            on: database, cache: "notificationConfig", op: .remove, key: bucketName)
        await ReplicationConfigCache.shared.removeBucket(bucketName)
        CacheInvalidationService.notify(
            on: database, cache: "replicationConfig", op: .remove, key: bucketName)

        // Drop any queued webhook deliveries / replication tasks for the deleted bucket -
        // retrying them would announce or push objects for a bucket that no longer exists
        try await NotificationDelivery.query(on: database)
            .filter(\.$bucketName == bucketName)
            .delete()
        try await ReplicationTask.query(on: database)
            .filter(\.$bucketName == bucketName)
            .delete()
        try await ClusterReplicationTask.query(on: database)
            .filter(\.$bucketName == bucketName)
            .delete()

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
