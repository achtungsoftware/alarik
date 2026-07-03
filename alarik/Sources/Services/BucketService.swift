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

            // Map the bucket to ALL of the user's access keys
            for accessKey in userAccessKeys {
                await AccessKeyBucketMapCache.shared.add(
                    accessKey: accessKey,
                    bucketName: bucketName
                )
            }

            await BucketVersioningCache.shared.addBucket(
                bucketName, versioningStatus: versioningEnabled ? .enabled : .disabled)
        } catch {
            // Best-effort: each step rolls back independently, so a failure in one (e.g. the
            // directory was never created) doesn't prevent the others from running, and the
            // original error - not a secondary rollback failure - is always what's thrown.
            try? await bucket.delete(on: database)
            try? BucketHandler.delete(name: bucketName, force: true)
            await BucketVersioningCache.shared.removeBucket(bucketName)
            throw error
        }
    }

    static func delete(
        on database: any Database,
        bucketName: String,
        userId: UUID,
        force: Bool = false
    )
        async throws
    {
        try await Bucket.query(on: database)
            .filter(\.$name == bucketName)
            .filter(\.$user.$id == userId)
            .delete()

        await AccessKeyBucketMapCache.shared.removeAll(for: bucketName)
        await BucketVersioningCache.shared.removeBucket(bucketName)
        await BucketPolicyCache.shared.removePolicy(for: bucketName)
        await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)
        await NotificationConfigCache.shared.removeBucket(bucketName)
        await ReplicationConfigCache.shared.removeBucket(bucketName)

        // Drop any queued webhook deliveries / replication tasks for the deleted bucket -
        // retrying them would announce or push objects for a bucket that no longer exists
        try await NotificationDelivery.query(on: database)
            .filter(\.$bucketName == bucketName)
            .delete()
        try await ReplicationTask.query(on: database)
            .filter(\.$bucketName == bucketName)
            .delete()

        try BucketHandler.delete(name: bucketName, force: force)
    }
}
