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

final actor AccessKeySecretKeyMapCache: StoreBackedCache {

    public static let shared = AccessKeySecretKeyMapCache()

    private var map: [String: String] = [:]

    var missLedger = CacheMissLedger<String>()

    /// Full replace, not merge. Nothing in the running system calls this - `reloadAll` is
    /// deliberately upsert-only, because a cluster-wide listing that came back incomplete would
    /// otherwise evict live credentials. Kept only for tests that exercise replace semantics
    /// directly; do not wire it into a reload path.
    func load(initialData: [(accessKey: String, secretKey: String)]) {
        map = Dictionary(uniqueKeysWithValues: initialData.map { ($0.accessKey, $0.secretKey) })
    }

    func add(accessKey: String, secretKey: String) {
        map[accessKey] = secretKey
        missLedger.clear(accessKey)
    }

    func remove(accessKey: String) {
        map.removeValue(forKey: accessKey)
    }

    func secretKey(for accessKey: String) -> String? {
        map[accessKey]
    }

    func exists(accessKey: String) -> Bool {
        map[accessKey] != nil
    }

    func keys(for secretKey: String) -> [String] {
        map.filter { $0.value == secretKey }.map { $0.key }
    }

    func getMap() -> [String: String] {
        map
    }

    // MARK: - StoreBackedCache

    func cachedValue(for key: String) -> String? { map[key] }

    func absorb(_ value: String, for key: String) { map[key] = value }

    func loadFromStore(app: Application, key: String) async throws -> String? {
        guard let stored = try await AccessKey.find(app: app, accessKey: key) else { return nil }
        // An expired key is genuinely unusable, matching what a cache reload would have filtered.
        if let expiry = stored.expirationDate, expiry <= Date() { return nil }
        // Seed the owner mapping from this same record. The authorization check that follows on
        // the auth path resolves the owner next, and would otherwise gather the identical record
        // a second time - two cluster-wide reads per request until the caches warm.
        await AccessKeyUserMapCache.shared.add(accessKey: key, userId: stored.userId)
        return stored.secretKey
    }
}
