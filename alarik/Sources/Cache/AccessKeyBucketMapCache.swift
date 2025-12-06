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

final actor AccessKeyBucketMapCache {
    public static let shared = AccessKeyBucketMapCache()

    private var map: [String: Set<String>] = [:]

    func load(initialData: [(accessKey: String, bucketName: String)]) {
        for entry in initialData {
            if map[entry.accessKey] != nil {
                map[entry.accessKey]?.insert(entry.bucketName)
            } else {
                map[entry.accessKey] = [entry.bucketName]
            }
        }
    }

    func add(accessKey: String, bucketName: String) {
        if map[accessKey] != nil {
            map[accessKey]?.insert(bucketName)
        } else {
            map[accessKey] = [bucketName]
        }
    }

    func remove(accessKey: String, bucketName: String) {
        map[accessKey]?.remove(bucketName)
        if map[accessKey]?.isEmpty == true {
            map.removeValue(forKey: accessKey)
        }
    }

    // Check if a bucket exists for this access key
    func bucket(for bucketName: String) -> String? {
        // Check if ANY access key has this bucket
        for (_, buckets) in map {
            if buckets.contains(bucketName) {
                return bucketName
            }
        }
        return nil
    }

    // Check if this access key can access this bucket
    func canAccess(accessKey: String, bucket: String) -> Bool {
        map[accessKey]?.contains(bucket) ?? false
    }

    func exists(accessKey: String) -> Bool {
        map[accessKey] != nil
    }

    func buckets(for accessKey: String) -> Set<String>? {
        map[accessKey]
    }

    func removeAccessKey(_ accessKey: String) {
        map.removeValue(forKey: accessKey)
    }

    func keys(for bucketName: String) -> [String] {
        map.filter { $0.value.contains(bucketName) }.map { $0.key }
    }

    func removeAll(for bucketName: String) {
        for key in map.keys {
            map[key]?.remove(bucketName)
            if map[key]?.isEmpty == true {
                map.removeValue(forKey: key)
            }
        }
    }

    func getMap() -> [String: Set<String>] {
        map
    }
}
