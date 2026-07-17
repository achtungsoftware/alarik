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

import Vapor

/// Cluster-wide Reed-Solomon shard counts - a deployment setting, not a per-bucket choice.
/// Only meaningful in cluster mode (`CLUSTER_NODE_ADDRESS`/`CLUSTER_SECRET`); once active, every
/// write is erasure-coded across `dataShards + parityShards` nodes, and the cluster needs at
/// least that many active nodes or writes fail outright.
struct ClusterErasureCodingConfig: Sendable, Equatable {
    let dataShards: Int
    let parityShards: Int
    /// How often the background bit-rot scrubber re-verifies every local shard's checksums and
    /// heals any it finds corrupt. `0` disables it. Defaults to weekly.
    let scrubIntervalHours: Int

    /// `k=4/m=2`, scaled to a realistic self-hosted 6-node
    /// baseline cluster.
    static let `default` = ClusterErasureCodingConfig(
        dataShards: 4, parityShards: 2, scrubIntervalHours: defaultScrubIntervalHours)

    static let defaultScrubIntervalHours = 168  // weekly

    var totalShards: Int { dataShards + parityShards }
    var scrubbingEnabled: Bool { scrubIntervalHours > 0 }

    /// Reads `CLUSTER_EC_DATA_SHARDS`/`CLUSTER_EC_PARITY_SHARDS`/`CLUSTER_EC_SCRUB_INTERVAL_HOURS`,
    /// defaulting whichever is unset. Throws (fails boot) on a nonsensical explicit value - same
    /// loud-not-silent treatment `CLUSTER_SECRET` misconfiguration already gets in `configure.swift`.
    static func resolve() throws -> ClusterErasureCodingConfig {
        let dataShards = try parseShardCount(
            envVar: "CLUSTER_EC_DATA_SHARDS", minimum: 2, defaultValue: `default`.dataShards)
        let parityShards = try parseShardCount(
            envVar: "CLUSTER_EC_PARITY_SHARDS", minimum: 1, defaultValue: `default`.parityShards)
        // Scrub interval may be 0 (disabled), so its floor is 0, not the >= 1 the shard counts need.
        let scrubIntervalHours = try parseShardCount(
            envVar: "CLUSTER_EC_SCRUB_INTERVAL_HOURS", minimum: 0,
            defaultValue: defaultScrubIntervalHours)
        return ClusterErasureCodingConfig(
            dataShards: dataShards, parityShards: parityShards, scrubIntervalHours: scrubIntervalHours)
    }

    private static func parseShardCount(envVar: String, minimum: Int, defaultValue: Int) throws -> Int {
        guard let raw = Environment.sanitizedGet(envVar) else { return defaultValue }
        guard let value = Int(raw), value >= minimum else {
            throw ClusterErasureCodingConfigError(
                description: "\(envVar)=\"\(raw)\" is invalid - must be an integer >= \(minimum).")
        }
        return value
    }
}

struct ClusterErasureCodingConfigError: Error, CustomStringConvertible {
    let description: String
}

struct ClusterErasureCodingConfigKey: StorageKey {
    typealias Value = ClusterErasureCodingConfig
}
