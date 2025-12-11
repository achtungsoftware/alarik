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

        do {
            try await bucket.save(on: database)
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
            try await bucket.delete(on: database)
            try BucketHandler.delete(name: bucketName)
            throw error
        }
    }

    static func delete(
        on database: any Database,
        bucketName: String,
        userId: UUID
    )
        async throws
    {
        try await Bucket.query(on: database)
            .filter(\.$name == bucketName)
            .filter(\.$user.$id == userId)
            .delete()

        await AccessKeyBucketMapCache.shared.removeAll(for: bucketName)
        await BucketVersioningCache.shared.removeBucket(bucketName)
        try BucketHandler.delete(name: bucketName)
    }
}
