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

/// Caches already-parsed bucket policies so the request hot path (every anonymous request)
/// never touches the database or re-parses JSON. Mirrors BucketVersioningCache.
final actor BucketPolicyCache {
    public static let shared = BucketPolicyCache()

    private var map: [String: BucketPolicy] = [:]

    func load(initialData: [(bucketName: String, policy: BucketPolicy)]) {
        for entry in initialData {
            map[entry.bucketName] = entry.policy
        }
    }

    /// Get the parsed policy for a bucket, or nil if none has been set
    func policy(for bucketName: String) -> BucketPolicy? {
        map[bucketName]
    }

    /// Set/replace the policy for a bucket
    func setPolicy(for bucketName: String, policy: BucketPolicy) {
        map[bucketName] = policy
    }

    /// Remove a bucket's policy (DeleteBucketPolicy, or bucket deletion)
    func removePolicy(for bucketName: String) {
        map.removeValue(forKey: bucketName)
    }

    func getMap() -> [String: BucketPolicy] {
        map
    }
}
