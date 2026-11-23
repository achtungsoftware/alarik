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

/// Per-bucket CORS rules, consulted by `AlarikCORSMiddleware` on every S3 request that carries
/// an `Origin`. A bucket known to have no rules is cached as an empty configuration, so the
/// common browser-less case never re-reads the store.
final actor BucketCORSCache: StoreBackedCache {
    public static let shared = BucketCORSCache()

    private var map: [String: CORSConfiguration] = [:]

    func config(for bucketName: String) -> CORSConfiguration? {
        map[bucketName]
    }

    func setConfig(for bucketName: String, config: CORSConfiguration) {
        map[bucketName] = config
        missLedger.clear(bucketName)
    }

    func removeBucket(_ bucketName: String) {
        map.removeValue(forKey: bucketName)
    }

    func getMap() -> [String: CORSConfiguration] {
        map
    }

    // MARK: - StoreBackedCache

    var missLedger = CacheMissLedger<String>()

    func cachedValue(for key: String) -> CORSConfiguration? { map[key] }

    func absorb(_ value: CORSConfiguration, for key: String) { map[key] = value }

    /// nil only when the bucket itself is gone; a bucket without rules is a real answer.
    func loadFromStore(app: Application, key: String) async throws -> CORSConfiguration? {
        guard let bucket = try await Bucket.find(app: app, name: key) else { return nil }
        await LoadCacheLifecycle.seedBucketCaches(from: bucket, logger: app.logger)
        return LoadCacheLifecycle.corsConfig(for: bucket)
    }

    /// The bucket's rules, or nil when it has none (or does not exist).
    func resolvedConfig(app: Application, bucket: String) async -> CORSConfiguration? {
        guard let config = await resolve(app: app, key: bucket), !config.rules.isEmpty else {
            return nil
        }
        return config
    }
}
