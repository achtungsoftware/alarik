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

struct AccessKeyService {
    /// Pass `id` when the caller already knows it (skips a read); with `nil` the by-id pointer
    /// is cleaned up best-effort via reading the primary record first. A pointer that survives
    /// a missed cleanup is self-healed on next use by `AccessKey.findIdPointer`.
    static func delete(
        app: Application,
        accessKey: String,
        id: UUID? = nil
    )
        async throws
    {
        var pointerId = id
        if pointerId == nil {
            pointerId = (try? await MetadataStore.get(
                AccessKey.self, app: app, collection: MetadataCollections.accessKeys,
                id: accessKey))?.id
        }

        try await MetadataStore.delete(
            app: app, collection: MetadataCollections.accessKeys, id: accessKey)
        if let pointerId {
            try? await MetadataStore.delete(
                app: app, collection: MetadataCollections.accessKeysById, id: pointerId.uuidString)
        }

        // Remove from all caches. Security-sensitive: a revoked key must stop authenticating
        // on every node, not just this one - notify immediately after each removal, not
        // batched, so the signal goes out as soon as this node itself has forgotten the key.
        await AccessKeySecretKeyMapCache.shared.remove(accessKey: accessKey)
        CacheInvalidationService.notify(app: app, cache: "accessKeySecret", op: .remove, key: accessKey)
        await AccessKeyUserMapCache.shared.remove(accessKey: accessKey)
        CacheInvalidationService.notify(app: app, cache: "accessKeyUser", op: .remove, key: accessKey)
        await AccessKeyBucketMapCache.shared.removeAccessKey(accessKey)
        CacheInvalidationService.notify(app: app, cache: "accessKeyBucket", op: .remove, key: accessKey)
    }
}
