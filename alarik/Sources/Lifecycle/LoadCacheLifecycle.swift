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

        } catch {
            app.logger.error("Failed to load access key cache: \(error)")
        }

        /*print(await AccessKeyUserMapCache.shared.getMap())
        print(await AccessKeyBucketMapCache.shared.getMap())
        print(await AccessKeySecretKeyMapCache.shared.getMap())*/
    }
}
