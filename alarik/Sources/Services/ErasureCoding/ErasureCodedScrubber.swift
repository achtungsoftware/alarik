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

/// Background bit-rot scrubber - the at-rest-integrity defense that read-repair and rebalancing
/// can't provide on their own, since a shard that's never read and never moved would otherwise rot
/// undetected. Each node periodically re-verifies the per-stripe SHA-256 checksums of the shards
/// it physically holds; any corrupt or unreadable shard is deleted and rebuilt from healthy
/// survivors. Deliberately gentle: shards are verified one at a time with a small pause between
/// them, so a full scrub trickles rather than saturating disk I/O.
enum ErasureCodedScrubber {
    /// Coalesces overlapping scrub requests (a periodic tick landing on top of an operator-
    /// triggered one) into a single in-flight pass, the same shape as the rebalance debouncer.
    private static let running = ScrubGuard()

    private static let interShardPause: Duration = .milliseconds(25)

    private actor ScrubGuard {
        private var isRunning = false
        func begin() -> Bool {
            if isRunning { return false }
            isRunning = true
            return true
        }
        func end() { isRunning = false }
    }

    /// Verifies every locally-held shard and heals any that fail. Safe to call from the periodic
    /// tick and the on-demand admin trigger concurrently - a second call while one is in flight is
    /// a no-op. Never throws: a scrub is best-effort maintenance, and any per-shard failure just
    /// leaves that shard for the next pass.
    static func scrub(app: Application) async {
        guard app.storage[ClusterConfigurationKey.self] != nil else { return }
        guard await running.begin() else { return }
        defer { Task { await running.end() } }

        let localShards: [(path: String, header: ErasureCodedShardHeader)]
        do {
            localShards = try await S3Service.offloadBlockingIO(app) {
                ErasureCodedObjectHandler.listAllLocalShards()
            }
        } catch {
            app.logger.warning("EC scrub could not enumerate local shards: \(error)")
            return
        }
        guard !localShards.isEmpty else { return }

        var corruptCount = 0
        for (path, header) in localShards {
            // `?? false`, never `?? true`: an offload failure (thread pool saturated, shutdown
            // in progress) is an OPERATIONAL error, not evidence of corruption. Treating it as
            // corrupt would delete a healthy shard on a busy node - corruption must be decided
            // only by a checksum that actually ran and actually failed.
            let isCorrupt =
                (try? await S3Service.offloadBlockingIO(app) { verifyShard(path: path) }) ?? false
            if isCorrupt {
                corruptCount += 1
                await healLocalShard(
                    app: app, bucketName: header.objectMeta.bucketName, key: header.objectMeta.key,
                    versionId: header.objectMeta.versionId == "null" ? nil : header.objectMeta.versionId,
                    shardIndex: header.shardIndex)
            }
            try? await Task.sleep(for: interShardPause)
        }

        if corruptCount > 0 {
            app.logger.warning(
                "EC scrub found and queued repair for \(corruptCount) corrupt shard(s) across \(localShards.count) checked."
            )
        }
    }

    /// Verifies the shard(s) THIS node holds for one specific (bucket, key, version) and heals any
    /// that are actually corrupt on local disk - the targeted, one-object counterpart of the full
    /// `scrub`. This is what makes read-repair safe for corruption: a checksum failure a reader saw
    /// while decoding a *fetched* copy might be transit damage, so rather than the reader deleting a
    /// peer's shard, the peer runs this against its own on-disk copy - authoritative, no transit
    /// ambiguity. Returns whether anything was healed. Never throws.
    @discardableResult
    static func verifyAndHealObjectShards(
        app: Application, bucketName: String, key: String, versionId: String?
    ) async -> Bool {
        let indices = ErasureCodedObjectHandler.locallyHeldShardIndices(
            bucketName: bucketName, key: key, versionId: versionId)
        var healedAny = false
        for index in indices {
            let path = ErasureCodedObjectHandler.shardPath(
                bucketName: bucketName, key: key, versionId: versionId, shardIndex: index)
            // Same `?? false` reasoning as the full scrub above: only a checksum that ran and
            // failed is corruption; an offload error must never condemn a healthy shard.
            let isCorrupt =
                (try? await S3Service.offloadBlockingIO(app) { verifyShard(path: path) }) ?? false
            guard isCorrupt else { continue }
            healedAny = true
            await healLocalShard(
                app: app, bucketName: bucketName, key: key, versionId: versionId,
                shardIndex: index)
        }
        return healedAny
    }

    /// Reads every stripe of the shard at `path`, verifying its checksum. Returns `true` if the
    /// shard is corrupt or otherwise unreadable (a bad header, a truncated file) - all of which
    /// mean "this copy can't be trusted, rebuild it".
    private static func verifyShard(path: String) -> Bool {
        guard let reader = try? ErasureCodedShardReader(path: path) else { return true }
        defer { reader.close() }
        for stripeIndex in 0..<reader.header.stripeCount {
            do {
                _ = try reader.readStripe(stripeIndex)
            } catch {
                return true
            }
        }
        return false
    }

    /// Heals a confirmed-corrupt local shard WITHOUT ever making things worse: the corrupt file
    /// is quarantined (renamed aside), a replacement is obtained - a same-index copy fetched from
    /// a peer, or a reconstruction from the other indices - and only a successful replacement
    /// discards the quarantined original. If no replacement is obtainable right now, the original
    /// is restored: a corrupt copy still pins the record's existence (its header/filename are
    /// evidence) and a later pass may find peers this one couldn't - deleting it first, as this
    /// used to do, destroyed the only copy whenever the rebuild then failed. Heals the shard
    /// index this node ACTUALLY held (from the filename/header), never its current HRW rank -
    /// rank drifts with membership, the index on disk is ground truth.
    private static func healLocalShard(
        app: Application, bucketName: String, key: String, versionId: String?, shardIndex: Int
    ) async {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return }
        let finalPath = ErasureCodedObjectHandler.shardPath(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex)
        let quarantinePath = finalPath + ".quarantine"
        do {
            try FileManager.default.moveItem(atPath: finalPath, toPath: quarantinePath)
        } catch {
            app.logger.warning(
                "EC scrub could not quarantine corrupt shard \(shardIndex) of '\(bucketName)/\(key)' (skipped this pass): \(error)"
            )
            return
        }

        var healed = false

        // Attempt 1: fetch a healthy same-index copy from any peer. This is the ONLY heal that
        // can work for a k=1/m=0 record (there are no other indices to reconstruct from), and
        // it's cheaper than reconstruction whenever a duplicate copy exists.
        let peers = await ClusterNodeCache.shared.all().filter { $0.id != config.nodeId }
        if !peers.isEmpty,
            let tempPath = try? await ClusterReplicationClient.fetchShard(
                app: app, candidates: peers, bucketName: bucketName, key: key,
                versionId: versionId, shardIndex: shardIndex, requestId: UUID().uuidString)
        {
            let fetchedOk =
                (try? await S3Service.offloadBlockingIO(app) { verifyShard(path: tempPath) }).map {
                    !$0
                } ?? false
            if fetchedOk,
                (try? await S3Service.offloadBlockingIO(app) {
                    try ErasureCodedObjectHandler.commitShardFile(
                        sourcePath: tempPath, finalPath: finalPath, bucketName: bucketName,
                        key: key)
                }) != nil
            {
                healed = true
            } else {
                _ = POSIXFile.unlink(tempPath)
            }
        }

        // Attempt 2: reconstruct this index from the other indices (k>1 objects). The quarantined
        // corrupt copy is invisible to the gather, so it can't poison the reconstruction.
        if !healed {
            do {
                try await ErasureCodedRebalanceService.reconstructAndPlaceShard(
                    app: app, bucketName: bucketName, key: key, versionId: versionId,
                    shardIndex: shardIndex, targetNodeId: config.nodeId)
                healed = FileManager.default.fileExists(atPath: finalPath)
            } catch {
                app.logger.debug(
                    "EC scrub reconstruction of shard \(shardIndex) for '\(bucketName)/\(key)' failed: \(error)"
                )
            }
        }

        if healed {
            _ = POSIXFile.unlink(quarantinePath)
            app.logger.info(
                "EC scrub healed corrupt shard \(shardIndex) of '\(bucketName)/\(key)'.")
        } else {
            // Put the corrupt original back rather than leaving a gap - it may be the record's
            // only remaining physical trace, and the next pass retries with fresh peers.
            try? FileManager.default.moveItem(atPath: quarantinePath, toPath: finalPath)
            app.logger.warning(
                "EC scrub could not obtain a replacement for corrupt shard \(shardIndex) of '\(bucketName)/\(key)' - kept the damaged copy for a later attempt."
            )
        }
    }
}
