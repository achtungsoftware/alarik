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

final actor AccessKeySecretKeyMapCache {

    public static let shared = AccessKeySecretKeyMapCache()

    private var map: [String: String] = [:]

    func load(initialData: [(accessKey: String, secretKey: String)]) {
        for entry: (accessKey: String, secretKey: String) in initialData {
            map[entry.accessKey] = entry.secretKey
        }
    }

    func add(accessKey: String, secretKey: String) {
        map[accessKey] = secretKey
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
}