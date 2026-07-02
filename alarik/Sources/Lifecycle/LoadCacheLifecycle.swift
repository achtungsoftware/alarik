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
                guard let rawPolicy = bucket.policy else { return nil }
                guard
                    let policy = try? BucketPolicy.parseAndValidate(
                        rawJSON: rawPolicy, bucketName: bucket.name, requestId: "boot")
                else {
                    app.logger.error(
                        "Failed to load stored policy for bucket '\(bucket.name)' - skipping")
                    return nil
                }
                return (bucketName: bucket.name, policy: policy)
            }
            await BucketPolicyCache.shared.load(initialData: policyData)

            // Load public access block cache - only buckets with at least one flag set are
            // worth caching, an all-false bucket behaves identically to "not in the map".
            let publicAccessBlockData:
                [(bucketName: String, configuration: PublicAccessBlockConfiguration)] =
                    allBuckets.compactMap { bucket in
                        let config = bucket.publicAccessBlock
                        guard
                            config.blockPublicAcls || config.ignorePublicAcls
                                || config.blockPublicPolicy || config.restrictPublicBuckets
                        else { return nil }
                        return (bucketName: bucket.name, configuration: config)
                    }
            await BucketPolicyCache.shared.loadPublicAccessBlocks(
                initialData: publicAccessBlockData)

            // Load notification (webhook) configuration cache
            let notificationData: [(bucketName: String, config: NotificationConfiguration)] =
                allBuckets.compactMap { bucket in
                    guard let raw = bucket.notificationConfig else { return nil }
                    let config = NotificationConfiguration.fromJSON(raw)
                    guard !config.rules.isEmpty else { return nil }
                    return (bucketName: bucket.name, config: config)
                }
            await NotificationConfigCache.shared.load(initialData: notificationData)

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
}
