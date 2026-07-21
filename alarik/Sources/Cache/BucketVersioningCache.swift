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

final actor BucketVersioningCache: StoreBackedCache {
    public static let shared = BucketVersioningCache()

    private var map: [String: VersioningStatus] = [:]

    /// Full replace, not merge - see `AccessKeySecretKeyMapCache.load` for why this matters on a
    /// LISTEN-outage reload, not just at boot.
    func load(initialData: [(bucketName: String, versioningStatus: String)]) {
        map = Dictionary(
            uniqueKeysWithValues: initialData.map {
                ($0.bucketName, VersioningStatus(rawValue: $0.versioningStatus) ?? .disabled)
            })
    }

    /// Get versioning status for a bucket
    /// Prefer `resolvedStatus(app:bucket:)` over this: a miss here is
    /// indistinguishable from genuinely-disabled versioning, and answering `.disabled` for a
    /// bucket this node simply hasn't cached yet makes a write overwrite instead of version.
    func getStatus(for bucketName: String) -> VersioningStatus {
        map[bucketName] ?? .disabled
    }

    var missLedger = CacheMissLedger<String>()

    // MARK: - StoreBackedCache

    /// The cached status, or `nil` when this node has no entry - lets a caller tell "versioning
    /// is off" apart from "this node doesn't know yet".
    func cachedValue(for key: String) -> VersioningStatus? { map[key] }

    func absorb(_ value: VersioningStatus, for key: String) { map[key] = value }

    /// Versioning status with the store as a fallback - the accessor every caller should use.
    /// Falls back to `.disabled` only when the bucket genuinely cannot be resolved at all, which
    /// matches the old behaviour for a bucket that really is unversioned or gone.
    func resolvedStatus(app: Application, bucket: String) async -> VersioningStatus {
        await resolve(app: app, key: bucket) ?? .disabled
    }

    /// Whether `bucket` exists at all, keeping "no such bucket" apart from "couldn't check".
    ///
    /// Bucket existence is answered here because this cache is this node's projection of the
    /// ENTIRE `buckets` collection: every bucket is present in it regardless of who owns the
    /// bucket or whether any access key maps to it, and a miss falls through to the store.
    ///
    /// It must never be answered from `AccessKeyBucketMapCache`, which only knows buckets
    /// reachable by some access key. A bucket whose owner has no access key - the normal state
    /// for one created and managed entirely through the web console - is absent from that map
    /// entirely, so existence checks reported `NoSuchBucket` and anonymous public-read was
    /// impossible until the owner happened to create a key (issue #16).
    func existence(app: Application, bucket: String) async -> StoreBackedResolution<VersioningStatus> {
        await resolveDistinguishing(app: app, key: bucket)
    }

    func loadFromStore(app: Application, key: String) async throws -> VersioningStatus? {
        guard let stored = try await Bucket.find(app: app, name: key) else { return nil }
        return VersioningStatus(rawValue: stored.versioningStatus) ?? .disabled
    }

    /// Update versioning status for a bucket
    func setStatus(for bucketName: String, status: VersioningStatus) {
        map[bucketName] = status
    }

    /// Add a new bucket with default disabled status
    func addBucket(_ bucketName: String, versioningStatus: VersioningStatus = .disabled) {
        map[bucketName] = versioningStatus
    }

    /// Remove a bucket from cache
    func removeBucket(_ bucketName: String) {
        map.removeValue(forKey: bucketName)
    }

    /// Check if versioning is enabled for a bucket
    func isVersioningEnabled(for bucketName: String) -> Bool {
        map[bucketName] == .enabled
    }

    /// Check if versioning is suspended for a bucket
    func isVersioningSuspended(for bucketName: String) -> Bool {
        map[bucketName] == .suspended
    }

    func getMap() -> [String: VersioningStatus] {
        map
    }
}
