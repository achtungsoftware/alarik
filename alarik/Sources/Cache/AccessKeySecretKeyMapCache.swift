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

/// A cached credential: the secret SigV4 signs with, plus the instant it stops being usable.
///
/// The expiry travels WITH the secret rather than being checked only when the record is loaded
/// from the store. Caching a bare secret meant expiry was enforced solely by the background sweep
/// that deletes expired records - so a key stayed fully usable for up to a sweep interval past its
/// own expiry, and longer on any node that missed the removal broadcast. A time-limited credential
/// has to stop working because time passed, not because a sweep got around to it.
struct AccessKeyCredential: Sendable, Equatable {
    let secretKey: String
    /// `nil` means the key never expires.
    let expiresAt: Date?

    func isValid(now: Date = Date()) -> Bool {
        guard let expiresAt else { return true }
        return expiresAt > now
    }
}

final actor AccessKeySecretKeyMapCache: StoreBackedCache {

    public static let shared = AccessKeySecretKeyMapCache()

    private var map: [String: AccessKeyCredential] = [:]

    var missLedger = CacheMissLedger<String>()

    /// Full replace, not merge. Nothing in the running system calls this - `reloadAll` is
    /// deliberately upsert-only, because a cluster-wide listing that came back incomplete would
    /// otherwise evict live credentials. Kept only for tests that exercise replace semantics
    /// directly; do not wire it into a reload path.
    func load(initialData: [(accessKey: String, secretKey: String)]) {
        map = Dictionary(
            uniqueKeysWithValues: initialData.map {
                ($0.accessKey, AccessKeyCredential(secretKey: $0.secretKey, expiresAt: nil))
            })
    }

    /// `expiresAt` defaults to "never expires" - callers that HAVE the key's expiration date must
    /// pass it, or the cached copy outlives the credential it represents.
    func add(accessKey: String, secretKey: String, expiresAt: Date? = nil) {
        map[accessKey] = AccessKeyCredential(secretKey: secretKey, expiresAt: expiresAt)
        missLedger.clear(accessKey)
    }

    func remove(accessKey: String) {
        map.removeValue(forKey: accessKey)
    }

    /// The secret, or nil if the key is unknown OR has expired. Expiry is enforced here, not just
    /// at load time, so a key that expires while cached stops signing immediately.
    func secretKey(for accessKey: String) -> String? {
        guard let credential = map[accessKey], credential.isValid() else { return nil }
        return credential.secretKey
    }

    /// Whether a still-valid credential is cached for this key. An expired one reads as absent.
    func exists(accessKey: String) -> Bool {
        map[accessKey]?.isValid() == true
    }

    func keys(for secretKey: String) -> [String] {
        map.filter { $0.value.secretKey == secretKey }.map { $0.key }
    }

    /// Access key -> secret, for the reconcile pass and debug dumps. Expired entries are included
    /// deliberately: the only caller that reads the values reconciles cache membership against a
    /// cluster listing, and an expired-but-not-yet-deleted key is still a record that exists.
    func getMap() -> [String: String] {
        map.mapValues(\.secretKey)
    }

    // MARK: - StoreBackedCache

    func cachedValue(for key: String) -> AccessKeyCredential? {
        guard let credential = map[key], credential.isValid() else { return nil }
        return credential
    }

    func absorb(_ value: AccessKeyCredential, for key: String) { map[key] = value }

    func loadFromStore(app: Application, key: String) async throws -> AccessKeyCredential? {
        guard let stored = try await AccessKey.find(app: app, accessKey: key) else { return nil }
        let credential = AccessKeyCredential(
            secretKey: stored.secretKey, expiresAt: stored.expirationDate)
        // An expired key is genuinely unusable, matching what a cache reload would have filtered.
        guard credential.isValid() else { return nil }
        // Seed the owner mapping from this same record. The authorization check that follows on
        // the auth path resolves the owner next, and would otherwise gather the identical record
        // a second time - two cluster-wide reads per request until the caches warm.
        await AccessKeyUserMapCache.shared.add(accessKey: key, userId: stored.userId)
        return credential
    }
}
