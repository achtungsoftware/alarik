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

/// `CLUSTER_EC_DATA_SHARDS`/`CLUSTER_EC_PARITY_SHARDS` are process-wide environment variables,
/// so these tests mutate and restore them around each assertion - `.serialized` mirrors every
/// other suite that touches shared process state in this test target.
@Suite("ClusterErasureCodingConfig tests", .serialized)
struct ClusterErasureCodingConfigTests {
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

    @Test("resolves to the default k=4/m=2 when both env vars are unset")
    func resolvesToDefaultWhenUnset() throws {
        try withEnv(["CLUSTER_EC_DATA_SHARDS": nil, "CLUSTER_EC_PARITY_SHARDS": nil]) {
            let config = try ClusterErasureCodingConfig.resolve()
            #expect(config == ClusterErasureCodingConfig.default)
            #expect(config.dataShards == 4)
            #expect(config.parityShards == 2)
            #expect(config.totalShards == 6)
        }
    }

    @Test("resolves to the configured values when both env vars are set")
    func resolvesToConfiguredValues() throws {
        try withEnv(["CLUSTER_EC_DATA_SHARDS": "6", "CLUSTER_EC_PARITY_SHARDS": "3"]) {
            let config = try ClusterErasureCodingConfig.resolve()
            #expect(config.dataShards == 6)
            #expect(config.parityShards == 3)
            #expect(config.totalShards == 9)
        }
    }

    @Test("one env var set, the other defaults independently")
    func resolvesMixedExplicitAndDefault() throws {
        try withEnv(["CLUSTER_EC_DATA_SHARDS": "8", "CLUSTER_EC_PARITY_SHARDS": nil]) {
            let config = try ClusterErasureCodingConfig.resolve()
            #expect(config.dataShards == 8)
            #expect(config.parityShards == ClusterErasureCodingConfig.default.parityShards)
        }
    }

    @Test("rejects CLUSTER_EC_DATA_SHARDS below the minimum of 2")
    func rejectsTooFewDataShards() throws {
        withEnv(["CLUSTER_EC_DATA_SHARDS": "1", "CLUSTER_EC_PARITY_SHARDS": nil]) {
            #expect(throws: ClusterErasureCodingConfigError.self) {
                try ClusterErasureCodingConfig.resolve()
            }
        }
    }

    @Test("rejects CLUSTER_EC_PARITY_SHARDS below the minimum of 1")
    func rejectsTooFewParityShards() throws {
        withEnv(["CLUSTER_EC_DATA_SHARDS": nil, "CLUSTER_EC_PARITY_SHARDS": "0"]) {
            #expect(throws: ClusterErasureCodingConfigError.self) {
                try ClusterErasureCodingConfig.resolve()
            }
        }
    }

    @Test("rejects a non-integer value")
    func rejectsNonIntegerValue() throws {
        withEnv(["CLUSTER_EC_DATA_SHARDS": "four", "CLUSTER_EC_PARITY_SHARDS": nil]) {
            #expect(throws: ClusterErasureCodingConfigError.self) {
                try ClusterErasureCodingConfig.resolve()
            }
        }
    }

    // MARK: - Scrub interval

    @Test("scrub interval defaults to weekly (168h) and reports scrubbing enabled")
    func scrubIntervalDefaultsToWeekly() throws {
        try withEnv(["CLUSTER_EC_SCRUB_INTERVAL_HOURS": nil]) {
            let config = try ClusterErasureCodingConfig.resolve()
            #expect(config.scrubIntervalHours == 168)
            #expect(config.scrubbingEnabled)
        }
    }

    @Test("an explicit scrub interval is honored")
    func scrubIntervalHonored() throws {
        try withEnv(["CLUSTER_EC_SCRUB_INTERVAL_HOURS": "24"]) {
            let config = try ClusterErasureCodingConfig.resolve()
            #expect(config.scrubIntervalHours == 24)
            #expect(config.scrubbingEnabled)
        }
    }

    @Test("a scrub interval of 0 disables scrubbing (the one shard-count minimum that allows 0)")
    func scrubIntervalZeroDisables() throws {
        try withEnv(["CLUSTER_EC_SCRUB_INTERVAL_HOURS": "0"]) {
            let config = try ClusterErasureCodingConfig.resolve()
            #expect(config.scrubIntervalHours == 0)
            #expect(!config.scrubbingEnabled)
        }
    }

    @Test("rejects a negative scrub interval")
    func rejectsNegativeScrubInterval() throws {
        withEnv(["CLUSTER_EC_SCRUB_INTERVAL_HOURS": "-5"]) {
            #expect(throws: ClusterErasureCodingConfigError.self) {
                try ClusterErasureCodingConfig.resolve()
            }
        }
    }
}
