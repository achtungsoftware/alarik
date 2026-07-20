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
import NIOCore
import Vapor

/// Gathers an erasure-coded object's shards from local disk + network peers and streams the
/// reconstructed payload - the read counterpart of `ErasureCodedWriteCoordinator`. Unlike
/// writes, reads need no rank-0 pinning: gather-and-decode is naturally idempotent from any node,
/// so any of the `k+m` responsible nodes can coordinate a GET independently.
enum ErasureCodedReadCoordinator {
    /// Returns the object's metadata immediately (read from whichever shard's header was fetched
    /// during gathering), so the caller can set response headers before the body starts
    /// streaming - the body itself reconstructs and streams stripe by stripe in the background,
    /// bounded memory regardless of object size, same as every other streaming GET path.
    /// A concurrent non-versioned overwrite can briefly expose a mix of shard generations; the
    /// gatherer rejects that mix rather than decode garbage. Retry a small, bounded number of
    /// times - the overwrite window is short (each shard lands atomically), so a settled
    /// generation appears quickly - before surfacing a 503.
    private static let maxInconsistentRetries = 5

    /// A gathered-but-not-yet-streamed read: the object metadata is known (so headers, the
    /// delete-marker check, and range parsing can happen before any body streams), while the shard
    /// set stays held open for `streamBody` to decode. Callers that take a `PreparedRead` MUST call
    /// `streamBody` exactly once (it owns cleanup of any fetched temp shards); on an early return
    /// before streaming, call `discard()`.
    struct PreparedRead {
        let meta: ObjectMeta
        fileprivate let gathered: GatheredShards
        fileprivate let bucketName: String
        fileprivate let key: String
        fileprivate let versionId: String?
        fileprivate let responsible: [ClusterNodeInfo]
        fileprivate let missingIndices: Set<Int>

        func discard() { gathered.cleanup() }
    }

    /// Gathers the object's shards and resolves its metadata, without streaming. Split out from the
    /// body stream so a ranged GET can read `meta.size` (to validate/parse the `Range`) and reject
    /// a delete marker with a 404 *before* committing to decode.
    /// `shardCounts` lets a caller supply its own `(dataShards, totalShards)` instead of reading
    /// the object-data `ClusterErasureCodingConfigKey` from storage - needed by `MetadataStore`,
    /// whose control-plane records are erasure-coded under their own, independently-configured
    /// `ClusterMetadataErasureCodingConfig` (which can, and often does, differ from the bulk
    /// object-data `k+m`). `nil` (every existing object-data call site) preserves today's
    /// behavior exactly.
    static func prepare(
        app: Application, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo], selfNodeId: UUID, requestId: String,
        shardCounts: (dataShards: Int, totalShards: Int)? = nil
    ) async throws -> PreparedRead {
        let dataShards: Int
        let totalShards: Int
        if let shardCounts {
            dataShards = shardCounts.dataShards
            totalShards = shardCounts.totalShards
        } else {
            guard let ecConfig = app.storage[ClusterErasureCodingConfigKey.self] else {
                throw ErasureCodedRebalanceError.notConfigured
            }
            dataShards = ecConfig.dataShards
            totalShards = ecConfig.totalShards
        }

        var gathered: GatheredShards?
        var attempt = 0
        while gathered == nil {
            do {
                gathered = try await ErasureCodedShardGatherer.gather(
                    app: app, bucketName: bucketName, key: key, versionId: versionId,
                    responsible: responsible, selfNodeId: selfNodeId, needed: dataShards,
                    wantSpare: true, excludingIndex: nil, requestId: requestId)
            } catch ErasureCodedGatherError.inconsistent where attempt < maxInconsistentRetries {
                attempt += 1
                try? await Task.sleep(for: .milliseconds(100))
            } catch {
                throw mapGatherError(error, requestId: requestId)
            }
        }
        let result = gathered!

        // Indices genuinely missing - not held anywhere AND whose rank-holder was reachable during
        // discovery (so we know it's actually gone, not merely on a briefly-unreachable node whose
        // shard is very likely fine). Detected for free from the gather's own discovery; drives
        // missing-shard read-repair once the read completes. (Shards the decode finds corrupt are
        // handled separately, via per-holder verify-heal.)
        let missingIndices = Set(0..<totalShards)
            .subtracting(result.heldIndices)
            .intersection(result.reachableRanks)

        return PreparedRead(
            meta: result.meta, gathered: result, bucketName: bucketName, key: key,
            versionId: versionId, responsible: responsible, missingIndices: missingIndices)
    }

    /// Streams a prepared read's reconstructed payload (optionally a byte `range`), then fires
    /// read-repair off the response path for any shard that was missing or checksum-failed.
    static func streamBody(
        app: Application, prepared: PreparedRead, range: (start: Int, end: Int)? = nil
    ) -> AsyncThrowingStream<ByteBuffer, any Error> {
        let gathered = prepared.gathered
        let bucketName = prepared.bucketName
        let key = prepared.key
        let versionId = prepared.versionId
        let responsible = prepared.responsible
        let missingIndices = prepared.missingIndices

        return AsyncThrowingStream<ByteBuffer, any Error> { continuation in
            Task {
                do {
                    let decodeResult = try await S3Service.offloadBlockingIO(app) {
                        try StripeDecoder.decode(
                            shardPaths: gathered.shards.mapValues(\.path), range: range
                        ) { chunk in
                            continuation.yield(ByteBuffer(data: chunk))
                        }
                    }
                    gathered.cleanup()
                    continuation.finish()
                    // Read-repair, best-effort and off the response path, so silently-rotted or lost
                    // copies self-heal on access rather than waiting for the next rebalance or scrub.
                    // Two distinct paths, deliberately:
                    //  - MISSING shards (held by no responsible node) are reconstructed directly -
                    //    definitively gone, safe to rebuild onto their rank-holders.
                    //  - CORRUPT shards (a checksum failed during decode) are NOT deleted from here:
                    //    the failure could be transit damage on a fetched copy, not on-disk rot.
                    //    Instead each responsible node is asked to verify its OWN copy and heal only
                    //    if it's genuinely corrupt locally - so a healthy peer shard is never
                    //    destroyed on the strength of a bad transfer.
                    let corrupt = decodeResult.corruptShardIndices
                    if !missingIndices.isEmpty {
                        Task.detached {
                            await ErasureCodedRebalanceService.healObject(
                                app: app, bucketName: bucketName, key: key, versionId: versionId,
                                responsible: responsible, missingIndices: missingIndices)
                        }
                    }
                    if !corrupt.isEmpty {
                        Task.detached {
                            await withTaskGroup(of: Void.self) { group in
                                for node in responsible {
                                    group.addTask {
                                        await ClusterReplicationClient.requestVerifyHeal(
                                            app: app, to: node, bucketName: bucketName, key: key,
                                            versionId: versionId)
                                    }
                                }
                            }
                        }
                    }
                } catch {
                    gathered.cleanup()
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Convenience full-object read - `prepare` + `streamBody(range: nil)` - for the callers that
    /// don't need range handling or a pre-stream metadata check (shared links, copy sources, admin
    /// download, cross-cluster replication).
    static func read(
        app: Application, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo], selfNodeId: UUID, requestId: String,
        shardCounts: (dataShards: Int, totalShards: Int)? = nil
    ) async throws -> (meta: ObjectMeta, body: AsyncThrowingStream<ByteBuffer, any Error>) {
        let prepared = try await prepare(
            app: app, bucketName: bucketName, key: key, versionId: versionId,
            responsible: responsible, selfNodeId: selfNodeId, requestId: requestId,
            shardCounts: shardCounts)
        return (prepared.meta, streamBody(app: app, prepared: prepared))
    }

    /// Translates a gather failure into the correct S3 status: genuine absence is a 404, but a
    /// degraded/racy object is a 503 - never a 404, which would tell an S3 client (or bucket
    /// replication) the object was deleted when it merely couldn't be assembled right now.
    private static func mapGatherError(_ error: any Error, requestId: String) -> any Error {
        switch error {
        case ErasureCodedGatherError.notFound:
            return S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: requestId)
        case ErasureCodedGatherError.degraded(let found, let needed):
            return S3Error(
                status: .serviceUnavailable, code: "ServiceUnavailable",
                message:
                    "Not enough healthy shards to reconstruct this object (\(found)/\(needed) available).",
                requestId: requestId)
        case ErasureCodedGatherError.inconsistent:
            return S3Error(
                status: .serviceUnavailable, code: "ServiceUnavailable",
                message: "Object is being overwritten; please retry.", requestId: requestId)
        default:
            return error
        }
    }
}
