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

/// Shard counts for Alarik's own control-plane metadata (users, buckets, access keys, cluster
/// membership, ...) - deliberately separate from `ClusterErasureCodingConfig` (bulk object data):
/// metadata availability must never depend on the object-data `k+m` fitting the cluster's current
/// size, and an undersized cluster must never simply refuse metadata writes outright -
/// `effective(activeNodeCount:)` auto-caps down instead of hard-refusing like
/// `PlacementService.ensureErasureCodingAdmission`.
///
/// **Metadata is REPLICATED, not striped: `dataShards` is 1 and every extra shard is a full,
/// independently-usable copy.** This is the single most important property of the control plane,
/// and it is not an optimization - it is what makes the control plane behave like the database it
/// replaced.
///
/// With `k > 1`, a record is *split*: no node holds a whole answer, so "does this bucket exist",
/// "who owns this access key" and "is this credential valid" each become a distributed gather
/// that needs `k` nodes to cooperate, over a responsible-node set recomputed from live membership
/// on every call. Any restart, drain, or briefly-slow peer then turns routine control-plane reads
/// into `NoSuchBucket`/`AccessDenied`/`NoSuchKey` - intermittently, and differently each time.
/// It also makes the record's width a function of cluster size at write time, so the same key
/// could be k=1 when the cluster was small and k=2 later, which every reader then has to
/// rediscover from shard headers.
///
/// With `k = 1` none of that exists: each responsible node holds a complete copy, ANY single
/// reachable holder answers a read locally (verified for parity-only survivors in
/// `ReedSolomonEngineTests`), and the width can never drift. Metadata
/// is kilobytes, so the extra copies cost nothing that matters.
struct ClusterMetadataErasureCodingConfig: Sendable, Equatable {
    let dataShards: Int
    let parityShards: Int

    /// `k=1/m=2` - three full copies, matching `PlacementService.replicationFactor`. Read the
    /// type's doc comment before changing `dataShards` away from 1: a value above 1 reintroduces
    /// the "no node can answer alone" failure mode this layout exists to remove.
    static let `default` = ClusterMetadataErasureCodingConfig(dataShards: 1, parityShards: 2)

    var totalShards: Int { dataShards + parityShards }

    /// Same GF(256) ceiling `ClusterErasureCodingConfig` enforces - shared field, shared bound.
    static let maxTotalShards = ClusterErasureCodingConfig.maxTotalShards

    /// Reads `CLUSTER_METADATA_EC_DATA_SHARDS`/`CLUSTER_METADATA_EC_PARITY_SHARDS`, defaulting
    /// whichever is unset. Throws (fails boot) on a nonsensical explicit value, same loud-not-
    /// silent treatment `ClusterErasureCodingConfig.resolve()` already gets.
    static func resolve() throws -> ClusterMetadataErasureCodingConfig {
        let dataShards = try parseShardCount(
            envVar: "CLUSTER_METADATA_EC_DATA_SHARDS", minimum: 1,
            defaultValue: `default`.dataShards)
        let parityShards = try parseShardCount(
            envVar: "CLUSTER_METADATA_EC_PARITY_SHARDS", minimum: 0,
            defaultValue: `default`.parityShards)
        guard dataShards + parityShards <= maxTotalShards else {
            throw ClusterErasureCodingConfigError(
                description:
                    "CLUSTER_METADATA_EC_DATA_SHARDS + CLUSTER_METADATA_EC_PARITY_SHARDS = \(dataShards + parityShards) exceeds the Reed-Solomon GF(256) limit of \(maxTotalShards) total shards."
            )
        }
        return ClusterMetadataErasureCodingConfig(dataShards: dataShards, parityShards: parityShards)
    }

    /// Auto-caps `k`/`m` down to whatever the cluster actually has right now, rather than
    /// refusing the write the way object-data admission control does. Always leaves
    /// `dataShards >= 1`; `parityShards` only uses whatever's left over after `dataShards`. A
    /// standalone node (`activeNodeCount <= 1`) degenerates to `(1, 0)` - no redundancy possible
    /// with nowhere else to place a shard, matching a single-node deployment's existing lack of
    /// object-data replication too.
    func effective(activeNodeCount: Int) -> (dataShards: Int, parityShards: Int) {
        guard activeNodeCount > 0 else { return (1, 0) }
        let effectiveDataShards = max(1, min(dataShards, activeNodeCount))
        let effectiveParityShards = max(0, min(parityShards, activeNodeCount - effectiveDataShards))
        return (effectiveDataShards, effectiveParityShards)
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

struct ClusterMetadataErasureCodingConfigKey: StorageKey {
    typealias Value = ClusterMetadataErasureCodingConfig
}
