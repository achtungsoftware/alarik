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

final actor AccessKeyUserMapCache: StoreBackedCache {
    public static let shared = AccessKeyUserMapCache()

    private var map: [String: UUID] = [:]

    /// Full replace, not merge - see `AccessKeySecretKeyMapCache.load` for why this matters on a
    /// LISTEN-outage reload, not just at boot.
    func load(initialData: [(accessKey: String, userId: UUID)]) {
        map = Dictionary(uniqueKeysWithValues: initialData.map { ($0.accessKey, $0.userId) })
    }

    func add(accessKey: String, userId: UUID) {
        map[accessKey] = userId
        missLedger.clear(accessKey)
    }

    func remove(accessKey: String) {
        map.removeValue(forKey: accessKey)
    }

    func userId(for accessKey: String) -> UUID? {
        map[accessKey]
    }

    func accessKeys(for userId: UUID) -> [String] {
        map.filter { $0.value == userId }.map { $0.key }
    }

    func getMap() -> [String: UUID] {
        map
    }

    // MARK: - StoreBackedCache

    var missLedger = CacheMissLedger<String>()

    func cachedValue(for key: String) -> UUID? { map[key] }

    func absorb(_ value: UUID, for key: String) { map[key] = value }

    func loadFromStore(app: Application, key: String) async throws -> UUID? {
        guard let stored = try await AccessKey.find(app: app, accessKey: key) else { return nil }
        // Seed the secret from the same record - but only while the key is still valid.
        // `AccessKeySecretKeyMapCache` deliberately refuses an expired key, and seeding past that
        // check would hand SigV4 a working secret for a credential that has already expired.
        if stored.expirationDate.map({ $0 > Date() }) ?? true {
            await AccessKeySecretKeyMapCache.shared.add(
                accessKey: key, secretKey: stored.secretKey)
        }
        return stored.userId
    }

    /// The owning user, consulting the store when this node has no cached entry.
    func resolvedUserId(app: Application, accessKey: String) async -> UUID? {
        await resolve(app: app, key: accessKey)
    }
}
