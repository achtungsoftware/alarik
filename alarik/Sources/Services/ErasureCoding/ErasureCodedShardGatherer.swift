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

enum ErasureCodedGatherError: Error, CustomStringConvertible {
    /// No reachable responsible node holds any shard for this (key, version), and none were
    /// unreachable - the object is positively absent. Callers map this to a 404.
    case notFound
    /// Shards exist somewhere, but fewer than `needed` could be gathered right now (a holder was
    /// unreachable, or a fetch failed). The object exists but is currently degraded - callers map
    /// this to a 503, never a 404 (a 404 would tell clients/replication the object was deleted).
    case degraded(found: Int, needed: Int)
    /// The gathered shards disagree on object identity (etag) - a read raced a non-versioned
    /// overwrite and picked up a mix of generations. Decoding a mix would produce garbage, so the
    /// gatherer refuses; the read path retries a fresh gather (the overwrite is brief).
    case inconsistent

    var description: String {
        switch self {
        case .notFound:
            "No shard exists for this object on any reachable responsible node"
        case .degraded(let found, let needed):
            "Only \(found) of \(needed) required shards could be gathered (object is degraded)"
        case .inconsistent:
            "Gathered shards disagree on object identity (concurrent overwrite)"
        }
    }
}

/// One gathered set of shards, keyed by true shard index, plus the object metadata they all agree
/// on. `paths` are local file paths (this node's own shard, or a fetched temp copy); callers that
/// received temp copies (`isTemp`) must unlink them once done.
struct GatheredShards {
    var shards: [Int: (path: String, isTemp: Bool)]
    var meta: ObjectMeta
    /// Every shard index any *reachable* responsible node reported holding (from the discovery
    /// phase, across the whole responsible set - not just the shards actually fetched). Lets the
    /// read path spot indices missing cluster-wide and trigger read-repair, without a second probe.
    var heldIndices: Set<Int>

    func cleanup() {
        for entry in shards.values where entry.isTemp {
            _ = POSIXFile.unlink(entry.path)
        }
    }
}

/// Location-independent shard gathering, shared by the read coordinator and the reconstruction/
/// rebalance path. The invariant that makes this necessary: a shard's index is fixed at encode
/// time (stored in its filename/header), but which node holds a given index drifts as HRW ranks
/// shift on every membership change - so gathering must *discover* what each node actually holds
/// (`/held`) and fetch shards by their true index, never assume "node at rank r holds shard r".
/// The old rank==index assumption silently broke every read and every repair the moment a node
/// was drained, joined, or lost.
enum ErasureCodedShardGatherer {
    /// Discovers held indices across `responsible` (in parallel), plans a covering set of `needed`
    /// (plus one spare when a healthy extra exists) distinct indices - optionally excluding one
    /// index the caller is about to reconstruct - fetches them (local reads never hit the network),
    /// and verifies the gathered shards agree on identity before returning. Throws
    /// `ErasureCodedGatherError` distinguishing genuine absence (404) from degraded availability
    /// (503) from a concurrent-overwrite race (retryable).
    static func gather(
        app: Application, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo], selfNodeId: UUID, needed: Int, wantSpare: Bool,
        excludingIndex: Int?, requestId: String
    ) async throws -> GatheredShards {
        // Phase 1: discover which shard indices each responsible node actually holds. `nil` means
        // unreachable (kept distinct from "reachable, holds none" so absence and degradation don't
        // get conflated).
        let discovered: [(node: ClusterNodeInfo, indices: [Int]?)] = await withTaskGroup(
            of: (node: ClusterNodeInfo, indices: [Int]?).self
        ) { group in
            for node in responsible {
                group.addTask {
                    if node.id == selfNodeId {
                        return (
                            node,
                            ErasureCodedObjectHandler.locallyHeldShardIndices(
                                bucketName: bucketName, key: key, versionId: versionId)
                        )
                    }
                    let held = await ClusterReplicationClient.heldShards(
                        app: app, node: node, bucketName: bucketName, key: key, versionId: versionId)
                    return (node, held)
                }
            }
            var results: [(node: ClusterNodeInfo, indices: [Int]?)] = []
            for await outcome in group { results.append(outcome) }
            return results
        }

        var anyUnreachable = false
        var reportedIndices: Set<Int> = []
        for entry in discovered {
            if let indices = entry.indices {
                reportedIndices.formUnion(indices)
            } else {
                anyUnreachable = true
            }
        }
        let allHeldIndices = reportedIndices
        if let excludingIndex { reportedIndices.remove(excludingIndex) }

        // Nothing anywhere and everyone answered -> the object genuinely doesn't exist here.
        if reportedIndices.isEmpty {
            if anyUnreachable { throw ErasureCodedGatherError.degraded(found: 0, needed: needed) }
            throw ErasureCodedGatherError.notFound
        }

        // Phase 2: assign each distinct needed index to one holder (first holder wins; two nodes
        // can transiently hold the same index mid-reindex, so dedup by index).
        let target = needed + (wantSpare ? 1 : 0)
        var plan: [(index: Int, node: ClusterNodeInfo)] = []
        var claimed: Set<Int> = []
        if let excludingIndex { claimed.insert(excludingIndex) }
        outer: for entry in discovered {
            guard let indices = entry.indices else { continue }
            for index in indices where !claimed.contains(index) {
                claimed.insert(index)
                plan.append((index, entry.node))
                if plan.count >= target { break outer }
            }
        }

        // Phase 3: fetch the planned (index, node) pairs in parallel - local ranks read from disk,
        // remote ranks fetch to a temp file.
        var gathered: [Int: (path: String, isTemp: Bool)] = [:]
        let requestIdCopy = requestId
        let fetched: [(index: Int, result: (path: String, isTemp: Bool)?)] = await withTaskGroup(
            of: (index: Int, result: (path: String, isTemp: Bool)?).self
        ) { group in
            for item in plan {
                group.addTask {
                    if item.node.id == selfNodeId {
                        let localPath = ErasureCodedObjectHandler.shardPath(
                            bucketName: bucketName, key: key, versionId: versionId,
                            shardIndex: item.index)
                        guard FileManager.default.fileExists(atPath: localPath) else {
                            return (item.index, nil)
                        }
                        return (item.index, (localPath, false))
                    }
                    guard
                        let tempPath = try? await ClusterReplicationClient.fetchShard(
                            app: app, candidates: [item.node], bucketName: bucketName, key: key,
                            versionId: versionId, shardIndex: item.index, requestId: requestIdCopy)
                    else { return (item.index, nil) }
                    return (item.index, (tempPath, true))
                }
            }
            var results: [(index: Int, result: (path: String, isTemp: Bool)?)] = []
            for await outcome in group { results.append(outcome) }
            return results
        }
        for entry in fetched {
            if let result = entry.result { gathered[entry.index] = result }
        }

        guard gathered.count >= needed else {
            for entry in gathered.values where entry.isTemp { _ = POSIXFile.unlink(entry.path) }
            // Shards were reported to exist (reportedIndices non-empty) but too few could be
            // pulled together - degraded, never "not found".
            throw ErasureCodedGatherError.degraded(found: gathered.count, needed: needed)
        }

        // Phase 4: verify identity agreement. Immutable versioned objects always agree; only a
        // non-versioned overwrite can produce a cross-generation mix, which decodes to garbage if
        // accepted - reject it so the read path re-gathers a settled generation.
        let paths = gathered.values.map(\.path)
        let headers = try? await S3Service.offloadBlockingIO(app) {
            try paths.map { path -> ErasureCodedShardHeader in
                let reader = try ErasureCodedShardReader(path: path)
                defer { reader.close() }
                return reader.header
            }
        }
        guard let headers, let reference = headers.first?.objectMeta else {
            for entry in gathered.values where entry.isTemp { _ = POSIXFile.unlink(entry.path) }
            throw ErasureCodedGatherError.degraded(found: 0, needed: needed)
        }
        let consistent = headers.allSatisfy {
            $0.objectMeta.etag == reference.etag && $0.objectMeta.size == reference.size
        }
        guard consistent else {
            for entry in gathered.values where entry.isTemp { _ = POSIXFile.unlink(entry.path) }
            throw ErasureCodedGatherError.inconsistent
        }

        return GatheredShards(shards: gathered, meta: reference, heldIndices: allHeldIndices)
    }
}
