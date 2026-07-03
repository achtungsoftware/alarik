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

/// In-memory mirror of every bucket's replication configuration, loaded at boot and kept in
/// sync on writes - so the object write/delete hot paths can check "does this bucket replicate
/// anywhere?" with a single actor dictionary lookup and zero database access. Mirrors
/// `NotificationConfigCache` exactly.
final actor ReplicationConfigCache {
    public static let shared = ReplicationConfigCache()

    private var map: [String: ReplicationConfiguration] = [:]

    func load(initialData: [(bucketName: String, config: ReplicationConfiguration)]) {
        for entry in initialData {
            map[entry.bucketName] = entry.config
        }
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
}
