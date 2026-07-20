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

import AsyncHTTPClient
import Foundation
import NIOCore
import Vapor

/// A cache-invalidation signal. Deliberately carries no value - only which cache and which key
/// changed. The receiving node always re-reads its own state (`CacheReloadDispatch`) rather than
/// trusting a value carried over the wire: one source of truth, no duplicate serialization
/// logic, and it's the only design that also works for `AccessKeyBucketMapCache` (a *derived*
/// cache - a cross-product via `user_id`, not a 1:1 table mirror - "the new value" isn't even a
/// well-defined thing to send for it).
struct CacheInvalidationMessage: Codable, Sendable {
    enum Op: String, Codable {
        /// Reload this one key's value from its backing store.
        case upsert
        /// The backing record for this key is gone - just drop it locally, nothing to reload.
        case remove
        /// `accessKeyBucket` only: a bucket was deleted - strip it out of every access key's
        /// cached set. The only op where "reload from the store" doesn't apply (the bucket
        /// record is already gone by the time this arrives, so there'd be nothing to read).
        case removeBucket
    }

    let cache: String
    let op: Op
    let key: String
    /// `cache == "clusterNode"` only: the node's info, carried directly rather than left for the
    /// receiver to re-read - see `CacheInvalidationService.notify`'s doc comment on `nodeInfo`
    /// for why this one cache is a deliberate exception to "always re-read your own state".
    let nodeInfo: InternalClusterMetadataController.ClusterMemberWire?

    init(
        cache: String, op: Op, key: String,
        nodeInfo: InternalClusterMetadataController.ClusterMemberWire? = nil
    ) {
        self.cache = cache
        self.op = op
        self.key = key
        self.nodeInfo = nodeInfo
    }
}

/// Sends cache-invalidation signals to every other active node over Alarik's own authenticated
/// inter-node HTTP protocol (`POST /internal/cluster/cache-invalidate`). A true no-op in
/// single-node mode (no `ClusterConfigurationKey`, or no other active nodes yet): the caller's
/// own in-process cache mutation, already done immediately before this is called, is the whole
/// story.
enum CacheInvalidationService {
    /// Fire-and-forget: never throws to the caller. A dropped broadcast only costs the rest of
    /// the cluster staleness until the next periodic full reload - it must never fail the write
    /// path that triggered it. `nodeInfo` (`clusterNode` only) is passed directly rather than
    /// left for the receiver to re-derive, since re-reading a node's own placement is circular
    /// when the receiver doesn't have that node in its active set yet.
    static func notify(
        app: Application, cache: String, op: CacheInvalidationMessage.Op, key: String,
        nodeInfo: InternalClusterMetadataController.ClusterMemberWire? = nil
    ) {
        Task {
            await broadcastToPeers(app: app, cache: cache, op: op, key: key, nodeInfo: nodeInfo)
        }
    }

    /// Same broadcast, but awaited instead of fire-and-forget - for call sites where the very
    /// next request is expected to reach any node immediately (bucket creation: a client can
    /// upload to any node right after `CreateBucket` returns, with no settling delay). Trades a
    /// bit of latency for actually closing that race rather than just narrowing it.
    static func notifyAndWait(
        app: Application, cache: String, op: CacheInvalidationMessage.Op, key: String,
        nodeInfo: InternalClusterMetadataController.ClusterMemberWire? = nil
    ) async {
        await broadcastToPeers(app: app, cache: cache, op: op, key: key, nodeInfo: nodeInfo)
    }

    private static func broadcastToPeers(
        app: Application, cache: String, op: CacheInvalidationMessage.Op, key: String,
        nodeInfo: InternalClusterMetadataController.ClusterMemberWire?
    ) async {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return }
        let message = CacheInvalidationMessage(cache: cache, op: op, key: key, nodeInfo: nodeInfo)
        var peers = await ClusterNodeCache.shared.activeNodes().filter { $0.id != config.nodeId }

        // A `clusterNode` broadcast's subject (e.g. a drain target) is already filtered out of
        // `activeNodes()` above by the time this runs - always include it explicitly, sourced
        // from the full membership, or the one node the status change is actually FOR would
        // never receive it.
        if cache == "clusterNode", let subjectId = UUID(uuidString: key), subjectId != config.nodeId,
            !peers.contains(where: { $0.id == subjectId }),
            let subject = await ClusterNodeCache.shared.get(id: subjectId)
        {
            peers.append(subject)
        }

        guard !peers.isEmpty else { return }
        await withTaskGroup(of: Void.self) { group in
            for peer in peers {
                group.addTask {
                    await broadcast(app: app, node: peer, secret: config.secret, message: message)
                }
            }
        }
    }

    /// Up to 3 attempts total, with escalating delays: a peer can stay too busy to answer a
    /// trivial POST for several seconds while running its own reactive rebalance walk after a
    /// sibling's restart. A peer still unreachable after all 3 attempts (worst case ~8s) is one
    /// the periodic reload is the right mechanism for.
    private static let retryDelays: [Duration] = [.milliseconds(750), .seconds(2)]

    private static func broadcast(
        app: Application, node: ClusterNodeInfo, secret: String, message: CacheInvalidationMessage
    ) async {
        var lastError: any Error
        do {
            try await send(app: app, node: node, secret: secret, message: message)
            return
        } catch {
            lastError = error
        }
        for delay in retryDelays {
            try? await Task.sleep(for: delay)
            do {
                try await send(app: app, node: node, secret: secret, message: message)
                return
            } catch {
                lastError = error
            }
        }
        app.logger.warning(
            "Failed to send cache invalidation broadcast to \(node.address) (cache=\(message.cache), op=\(message.op), key=\(message.key)): \(lastError)"
        )
    }

    private static func send(
        app: Application, node: ClusterNodeInfo, secret: String, message: CacheInvalidationMessage
    ) async throws {
        var outbound = HTTPClientRequest(url: node.address + "/internal/cluster/cache-invalidate")
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: secret)
        outbound.headers.replaceOrAdd(name: .contentType, value: "application/json")
        outbound.body = .bytes(ByteBuffer(data: try JSONEncoder().encode(message)))
        // `LightweightClusterControlClient.shared`, not `app.http.client.shared` - see its own
        // doc comment for why cluster control-plane traffic gets a dedicated client.
        _ = try await LightweightClusterControlClient.shared.execute(
            outbound, timeout: .seconds(2), logger: app.logger)
    }
}
