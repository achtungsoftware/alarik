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
/// it physically holds; any shard with a corrupt (or unreadable) stripe is deleted and rebuilt
/// from healthy survivors, exactly like read-repair. Deliberately gentle: shards are verified one
/// at a time with a small pause between them, so a full scrub of a large store trickles rather than
/// saturating disk I/O.
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
        guard app.storage[ClusterConfigurationKey.self] != nil,
            let ecConfig = app.storage[ClusterErasureCodingConfigKey.self]
        else { return }
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
            let isCorrupt =
                (try? await S3Service.offloadBlockingIO(app) { verifyShard(path: path) }) ?? true
            if isCorrupt {
                corruptCount += 1
                await heal(app: app, header: header, ecConfig: ecConfig)
            }
            try? await Task.sleep(for: interShardPause)
        }

        if corruptCount > 0 {
            app.logger.warning(
                "EC scrub found and queued repair for \(corruptCount) corrupt shard(s) across \(localShards.count) checked."
            )
        }
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

    /// Deletes the corrupt local shard and queues a reconstruction of this node's correct-rank
    /// shard from healthy survivors. Reuses the read-repair path (`healObject`) so the outbox dedup
    /// and reconstruction machinery are shared.
    private static func heal(
        app: Application, header: ErasureCodedShardHeader, ecConfig: ClusterErasureCodingConfig
    ) async {
        let bucketName = header.objectMeta.bucketName
        let key = header.objectMeta.key
        let versionId = header.objectMeta.versionId == "null" ? nil : header.objectMeta.versionId

        // Remove the damaged copy up front so the rebuild has a genuine gap to fill.
        ErasureCodedObjectHandler.removeLocalShard(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: header.shardIndex)

        let active = await ClusterNodeCache.shared.activeNodes()
        guard let config = app.storage[ClusterConfigurationKey.self], !active.isEmpty else { return }
        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: active, count: ecConfig.totalShards)
        guard let selfRank = responsible.firstIndex(where: { $0.id == config.nodeId }) else {
            // No longer responsible for this key at all - the deleted corrupt shard was stale;
            // rebalance/reclaim will settle placement, nothing to rebuild here.
            return
        }
        await ErasureCodedRebalanceService.healObject(
            app: app, bucketName: bucketName, key: key, versionId: versionId,
            responsible: responsible, missingIndices: [selfRank], corruptIndices: [])
    }
}
