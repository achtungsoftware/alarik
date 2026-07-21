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

/// In-memory mirror of every bucket's replication configuration, loaded at boot and kept in
/// sync on writes - so the object write/delete hot paths can check "does this bucket replicate
/// anywhere?" with a single actor dictionary lookup and zero database access. Mirrors
/// `NotificationConfigCache` exactly.
final actor ReplicationConfigCache: StoreBackedCache {
    public static let shared = ReplicationConfigCache()

    private var map: [String: ReplicationConfiguration] = [:]

    /// Full replace, not merge - see `AccessKeySecretKeyMapCache.load` for why this matters on a
    /// LISTEN-outage reload, not just at boot.
    func load(initialData: [(bucketName: String, config: ReplicationConfiguration)]) {
        map = Dictionary(uniqueKeysWithValues: initialData.map { ($0.bucketName, $0.config) })
    }

    /// Returns the bucket's configuration, or nil when the bucket has no (enabled) rules -
    /// nil is the fast path taken by every request on buckets without replication.
    func config(for bucketName: String) -> ReplicationConfiguration? {
        guard let config = map[bucketName], config.rules.contains(where: \.enabled) else {
            return nil
        }
        return config
    }

    func setConfig(for bucketName: String, config: ReplicationConfiguration) {
        if config.rules.isEmpty && config.targets.isEmpty {
            map.removeValue(forKey: bucketName)
        } else {
            map[bucketName] = config
        }
    }

    func removeBucket(_ bucketName: String) {
        map.removeValue(forKey: bucketName)
    }

    func getMap() -> [String: ReplicationConfiguration] {
        map
    }

    // MARK: - StoreBackedCache

    var missLedger = CacheMissLedger<String>()

    func cachedValue(for key: String) -> ReplicationConfiguration? { map[key] }

    func absorb(_ value: ReplicationConfiguration, for key: String) { map[key] = value }

    func loadFromStore(app: Application, key: String) async throws -> ReplicationConfiguration? {
        guard let bucket = try await Bucket.find(app: app, name: key) else { return nil }
        return LoadCacheLifecycle.replicationConfig(for: bucket)
    }

    /// The bucket's replication configuration, consulting the store on a miss - a miss here means
    /// writes silently never replicate, which is a durability problem, not just a stale read.
    func resolvedConfig(app: Application, bucket: String) async -> ReplicationConfiguration? {
        await resolve(app: app, key: bucket)
    }
}
