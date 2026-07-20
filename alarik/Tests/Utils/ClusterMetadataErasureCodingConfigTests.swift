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
import Testing

@testable import Alarik

/// `CLUSTER_METADATA_EC_DATA_SHARDS`/`CLUSTER_METADATA_EC_PARITY_SHARDS` are process-wide
/// environment variables, so these tests mutate and restore them around each assertion -
/// `.serialized` mirrors `ClusterErasureCodingConfigTests`.
@Suite("ClusterMetadataErasureCodingConfig tests", .serialized)
struct ClusterMetadataErasureCodingConfigTests {
    private func withEnv(_ vars: [String: String?], _ body: () throws -> Void) rethrows {
        let originals = vars.keys.reduce(into: [String: String?]()) {
            $0[$1] = ProcessInfo.processInfo.environment[$1]
        }
        defer {
            for (key, original) in originals {
                if let original { setenv(key, original, 1) } else { unsetenv(key) }
            }
        }
        for (key, value) in vars {
            if let value { setenv(key, value, 1) } else { unsetenv(key) }
        }
        try body()
    }

    @Test("resolves to the default k=2/m=1 when both env vars are unset")
    func resolvesToDefaultWhenUnset() throws {
        try withEnv(["CLUSTER_METADATA_EC_DATA_SHARDS": nil, "CLUSTER_METADATA_EC_PARITY_SHARDS": nil]) {
            let config = try ClusterMetadataErasureCodingConfig.resolve()
            #expect(config == ClusterMetadataErasureCodingConfig.default)
            #expect(config.dataShards == 2)
            #expect(config.parityShards == 1)
            #expect(config.totalShards == 3)
        }
    }

    @Test("resolves to the configured values when both env vars are set")
    func resolvesToConfiguredValues() throws {
        try withEnv(["CLUSTER_METADATA_EC_DATA_SHARDS": "4", "CLUSTER_METADATA_EC_PARITY_SHARDS": "2"]) {
            let config = try ClusterMetadataErasureCodingConfig.resolve()
            #expect(config.dataShards == 4)
            #expect(config.parityShards == 2)
        }
    }

    @Test("rejects CLUSTER_METADATA_EC_DATA_SHARDS below the minimum of 1")
    func rejectsTooFewDataShards() throws {
        withEnv(["CLUSTER_METADATA_EC_DATA_SHARDS": "0", "CLUSTER_METADATA_EC_PARITY_SHARDS": nil]) {
            #expect(throws: ClusterErasureCodingConfigError.self) {
                try ClusterMetadataErasureCodingConfig.resolve()
            }
        }
    }

    @Test("allows CLUSTER_METADATA_EC_PARITY_SHARDS of 0 (no redundancy, still valid)")
    func allowsZeroParityShards() throws {
        try withEnv(["CLUSTER_METADATA_EC_DATA_SHARDS": nil, "CLUSTER_METADATA_EC_PARITY_SHARDS": "0"]) {
            let config = try ClusterMetadataErasureCodingConfig.resolve()
            #expect(config.parityShards == 0)
        }
    }

    @Test("rejects a non-integer value")
    func rejectsNonIntegerValue() throws {
        withEnv(["CLUSTER_METADATA_EC_DATA_SHARDS": "two", "CLUSTER_METADATA_EC_PARITY_SHARDS": nil]) {
            #expect(throws: ClusterErasureCodingConfigError.self) {
                try ClusterMetadataErasureCodingConfig.resolve()
            }
        }
    }

    @Test("rejects k+m beyond the GF(256) Reed-Solomon limit of 255 total shards")
    func rejectsBeyondGaloisFieldLimit() throws {
        withEnv(["CLUSTER_METADATA_EC_DATA_SHARDS": "200", "CLUSTER_METADATA_EC_PARITY_SHARDS": "100"]) {
            #expect(throws: ClusterErasureCodingConfigError.self) {
                try ClusterMetadataErasureCodingConfig.resolve()
            }
        }
    }

    // MARK: - Graceful auto-cap (`effective(activeNodeCount:)`)

    @Test("a fully-sized cluster gets the configured k/m unchanged")
    func fullClusterUsesConfiguredShards() {
        let config = ClusterMetadataErasureCodingConfig(dataShards: 2, parityShards: 1)
        let effective = config.effective(activeNodeCount: 10)
        #expect(effective.dataShards == 2)
        #expect(effective.parityShards == 1)
    }

    @Test("exactly k+m nodes gets the configured k/m unchanged")
    func exactSizeClusterUsesConfiguredShards() {
        let config = ClusterMetadataErasureCodingConfig(dataShards: 2, parityShards: 1)
        let effective = config.effective(activeNodeCount: 3)
        #expect(effective.dataShards == 2)
        #expect(effective.parityShards == 1)
    }

    @Test("a single node auto-caps to k=1/m=0 - no redundancy, but never refuses")
    func singleNodeDegeneratesToOneZero() {
        let config = ClusterMetadataErasureCodingConfig(dataShards: 2, parityShards: 1)
        let effective = config.effective(activeNodeCount: 1)
        #expect(effective.dataShards == 1)
        #expect(effective.parityShards == 0)
    }

    @Test("zero active nodes still returns a usable (1, 0) rather than crashing/negative values")
    func zeroActiveNodesStillUsable() {
        let config = ClusterMetadataErasureCodingConfig(dataShards: 2, parityShards: 1)
        let effective = config.effective(activeNodeCount: 0)
        #expect(effective.dataShards == 1)
        #expect(effective.parityShards == 0)
    }

    @Test("two nodes caps parity down to 1, keeping dataShards intact")
    func twoNodesCapsParityOnly() {
        let config = ClusterMetadataErasureCodingConfig(dataShards: 2, parityShards: 1)
        let effective = config.effective(activeNodeCount: 2)
        #expect(effective.dataShards == 2)
        #expect(effective.parityShards == 0)
    }

    @Test("a wide configured k also gets capped down when the cluster is small")
    func wideDataShardsCapDownToClusterSize() {
        let config = ClusterMetadataErasureCodingConfig(dataShards: 8, parityShards: 4)
        let effective = config.effective(activeNodeCount: 3)
        #expect(effective.dataShards == 3)
        #expect(effective.parityShards == 0)
    }
}
