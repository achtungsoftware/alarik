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

final actor BucketVersioningCache {
    public static let shared = BucketVersioningCache()

    private var map: [String: VersioningStatus] = [:]

    func load(initialData: [(bucketName: String, versioningStatus: String)]) {
        for entry in initialData {
            let status = VersioningStatus(rawValue: entry.versioningStatus) ?? .disabled
            map[entry.bucketName] = status
        }
    }

    /// Get versioning status for a bucket
    func getStatus(for bucketName: String) -> VersioningStatus {
        map[bucketName] ?? .disabled
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
