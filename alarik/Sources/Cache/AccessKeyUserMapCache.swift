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

final actor AccessKeyUserMapCache {
    public static let shared = AccessKeyUserMapCache()

    private var map: [String: UUID] = [:]

    /// Full replace, not merge - see `AccessKeySecretKeyMapCache.load` for why this matters on a
    /// LISTEN-outage reload, not just at boot.
    func load(initialData: [(accessKey: String, userId: UUID)]) {
        map = Dictionary(uniqueKeysWithValues: initialData.map { ($0.accessKey, $0.userId) })
    }

    func add(accessKey: String, userId: UUID) {
        map[accessKey] = userId
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
}
