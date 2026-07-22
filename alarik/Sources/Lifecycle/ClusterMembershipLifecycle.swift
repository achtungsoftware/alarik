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

/// Self-registers this node into the `cluster-nodes` metadata collection at boot and keeps it
/// alive with a heartbeat - a true no-op when cluster mode is off. Registered as a
/// `LifecycleHandler` *before* `LoadCacheLifecycle`, since that handler's cluster-wide listing
/// fan-out needs `ClusterNodeCache.shared.activeNodes()` already populated or it queries nobody.
///
/// **Bootstrap circularity**: placing any metadata record, including this node's own, needs
/// `activeNodes()` to know who else is in the cluster - but a joining node's cache starts empty.
/// `didBootAsync` seeds the local cache from `CLUSTER_SEED_NODES` *before* ever calling
/// `registerSelf`, so a brand-new cluster's founding node (no seeds reachable) just proceeds with
/// an empty cache and takes the standalone (k=1,m=0) path.
///
/// Two independent periodic ticks: heartbeat (own record's liveness only, no broadcast - would
/// otherwise stampede a rebalance walk on every tick) and a full membership refresh (propagates
/// peers' heartbeats to this node and backstops a missed broadcast).
final actor ClusterMembershipLifecycle: LifecycleHandler {
    static let shared = ClusterMembershipLifecycle()

    /// How often this node refreshes its own heartbeat.
    static let heartbeatInterval: Int64 = 10
    /// How often this node pulls the full membership collection to refresh peer liveness/status.
    /// Longer than the heartbeat interval - a safety-net poll, not the primary propagation path
    /// (that's `CacheReloadDispatch`'s broadcast handling) - bounding how long a single dropped
    /// `registerSelf` broadcast can leave a peer believing a restarted node is still absent.
    static let membershipRefreshInterval: Int64 = 5

    private var heartbeatTask: Task<Void, Never>?
    private var refreshTask: Task<Void, Never>?

    /// This node's own `joinedAt`/status, tracked locally so the heartbeat loop never *depends*
    /// on successfully reading its own record back before it can write - see `heartbeatLoop`'s
    /// doc comment for why that dependency is dangerous. Updated from `registerSelf` and from
    /// every heartbeat tick that does manage a successful read.
    private var lastKnownJoinedAt: Date?
    private var lastKnownStatus: ClusterNode.Status = .active

    func didBootAsync(_ app: Application) async throws {
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            return  // Cluster mode off - nothing to register.
        }
        guard heartbeatTask == nil, refreshTask == nil else { return }

        await bootstrapMembership(app: app, config: config)
        // Never let a transient registration failure crash the whole node: `didBootAsync`
        // throwing is fatal to the process. `registerSelf` can throw `coordinatorUnreachable` if
        // this node isn't rank-0 for its own record and the peer that is happens to be slow right
        // now - a missed first registration isn't fatal, the heartbeat loop below retries it.
        do {
            try await registerSelf(app: app, config: config)
        } catch {
            app.logger.warning(
                "Initial cluster self-registration failed - will retry via the heartbeat loop: \(error)"
            )
        }

        // `registerSelf`'s own broadcast is fire-and-forget with no retry - a single dropped
        // delivery leaves that peer believing this node is still absent until the next
        // `membershipRefreshInterval` tick. A cheap, short-delay repeat closes that gap in
        // seconds; `registerSelf` is a plain idempotent upsert-and-broadcast, safe to call twice.
        Task { [weak self] in
            try? await Task.sleep(for: .seconds(2))
            guard let self else { return }
            try? await self.registerSelf(app: app, config: config)
        }

        // A node coming back up never otherwise triggers its own rebalance walk - it only runs
        // one reactively when it receives a peer's `clusterNode` broadcast, and that broadcast
        // excludes the broadcaster itself. Left as-is, a restarted node passively waits to be
        // noticed instead of reclaiming stale shards or picking up newly-owed ones itself.
        await ClusterRebalanceService.scheduleRebalance(app: app, reason: .membershipChange)
        await ErasureCodedRebalanceService.scheduleRebalance(app: app, reason: .membershipChange)

        heartbeatTask = Task { [weak self] in
            await self?.heartbeatLoop(app: app, config: config)
        }
        refreshTask = Task { [weak self] in
            await self?.refreshLoop(app: app, config: config)
        }
    }

    func shutdownAsync(_ app: Application) async {
        heartbeatTask?.cancel()
        heartbeatTask = nil
        refreshTask?.cancel()
        refreshTask = nil
    }

    /// How many times `bootstrapMembership` retries an incomplete seed picture before giving up
    /// and falling through to the periodic `refreshLoop` reseed instead - tuned to stay well
    /// under typical startup budgets while giving a cold multi-node start a few chances to
    /// catch peers as they come up.
    private static let bootstrapRetryAttempts = 8
    private static let bootstrapRetryDelay: Duration = .milliseconds(300)
    /// Deliberately much shorter than `ClusterReplicationClient.probeTimeout`: this call is
    /// boot-blocking and retried, and a closed port refuses a connection in milliseconds at the
    /// OS level - there's nothing legitimate for this query to wait 5 seconds for.
    private static let seedQueryTimeout: TimeAmount = .milliseconds(800)

    /// Seeds this node's local `ClusterNodeCache` from every configured seed before this node
    /// ever places any metadata record
    private func bootstrapMembership(app: Application, config: ClusterConfiguration) async {
        guard !config.seeds.isEmpty else { return }
        var anySucceeded = false
        for attempt in 0..<Self.bootstrapRetryAttempts {
            if await reseedFromConfiguredSeeds(
                app: app, config: config, triggerCacheReloadOnGrowth: false)
            {
                anySucceeded = true
                break
            }
            if attempt < Self.bootstrapRetryAttempts - 1 {
                try? await Task.sleep(for: Self.bootstrapRetryDelay)
            }
        }
        guard anySucceeded else {
            app.logger.warning(
                "Could not reach any CLUSTER_SEED_NODES peer at boot (\(config.seeds.count) configured) - proceeding with only the locally cached membership view."
            )
            return
        }
    }

    /// Last time `refreshNow` actually queried the seeds, used to debounce it.
    private var lastForcedRefreshAt: Date?
    /// A forced refresh is only worth doing this often - concurrent requests all discovering the
    /// same too-small view must not each fire their own seed fan-out.
    private static let forcedRefreshInterval: TimeInterval = 1

    /// Re-seeds membership *right now*, for a caller about to fail an operation because its own
    /// view of the cluster looks too small to proceed.
    ///
    /// A node that boots while its seeds happen to be down (several nodes restarting together)
    /// ends up with a membership cache containing only itself, and starts serving traffic that
    /// way. The periodic `refreshLoop` repairs this within seconds, but until it does, every
    /// write is rejected with a confident, wrong error ("requires at least 4 active cluster
    /// nodes, only 1 are active") while the cluster is in fact perfectly healthy. Refreshing
    /// before rejecting turns that lie into a brief delay. Debounced, and only ever reached on a
    /// path that is already failing, so it costs nothing in the normal case.
    func refreshNow(app: Application) async {
        guard let config = app.storage[ClusterConfigurationKey.self], !config.seeds.isEmpty else {
            return
        }
        let now = Date()
        if let last = lastForcedRefreshAt,
            now.timeIntervalSince(last) < Self.forcedRefreshInterval
        {
            return
        }
        lastForcedRefreshAt = now
        await reseedFromConfiguredSeeds(app: app, config: config, triggerCacheReloadOnGrowth: true)
    }

    /// Queries every statically-configured seed in parallel and merges their snapshots into the
    /// local cache via `reconcile` (additive, never drops, freshest entry wins) - reusable so
    /// `refreshLoop` can repeat it periodically. This lets two independently-booted subsets of a cluster eventually merge,
    /// since re-querying the fixed seed list sidesteps `ClusterNode.all()`'s fan-out only ever
    /// discovering nodes whose records sit on an already-known peer. Returns whether at least
    /// one seed answered. `triggerCacheReloadOnGrowth`: `false` from `bootstrapMembership` (a
    /// full reload runs moments later anyway), `true` from `refreshLoop`.
    @discardableResult
    private func reseedFromConfiguredSeeds(
        app: Application, config: ClusterConfiguration, triggerCacheReloadOnGrowth: Bool
    ) async -> Bool {
        let knownBefore = Set(await ClusterNodeCache.shared.all().map(\.id))
        var anySucceeded = false
        await withTaskGroup(of: [ClusterNodeInfo]?.self) { group in
            for seedAddress in config.seeds {
                group.addTask {
                    await self.querySeed(app: app, address: seedAddress, config: config)
                }
            }
            for await result in group {
                guard let snapshot = result else { continue }
                anySucceeded = true
                // `reconcile`, never raw upsert: a seed's snapshot is its own possibly-stale
                // cache. Blindly overwriting lets one lagging peer clobber this node's fresher
                // entry for a live, heartbeating node - which then silently drops out of
                // `activeNodes()` (heartbeat looks >60s old) and out of placement, while
                // health checks (which re-read via listing fan-out) still pass.
                await ClusterNodeCache.shared.reconcile(snapshot: snapshot)
            }
        }

        // Discovering a genuinely new peer here (rather than via its join broadcast) means that
        // broadcast was dropped - so this node never ran the join's reactive work. Do it now:
        //
        //  - Reload caches, so a record the new peer alone holds (a just-seeded admin access key,
        //    say) becomes visible immediately instead of waiting for the 60s periodic reload.
        //  - Schedule a rebalance, so this node migrates the keys the new peer is now responsible
        //    for onto it. Without this the join broadcast is the ONLY thing that triggers that
        //    migration, and a single dropped packet would leave a node permanently un-rebalanced
        //    (an availability-safe convergence lag - reads still widen to all known nodes - but it
        //    must self-heal without a manual resync).
        //
        // Only from `refreshLoop` (`triggerCacheReloadOnGrowth`): `bootstrapMembership` already
        // schedules its own rebalance and full reload in `didBootAsync`.
        if triggerCacheReloadOnGrowth {
            let knownAfter = Set(await ClusterNodeCache.shared.all().map(\.id))
            if !knownAfter.isSubset(of: knownBefore) {
                do {
                    try await LoadCacheLifecycle.reloadAll(app: app)
                } catch {
                    app.logger.warning("Cache reload after discovering a new peer failed: \(error)")
                }
                await ClusterRebalanceService.scheduleRebalance(app: app, reason: .membershipChange)
                await ErasureCodedRebalanceService.scheduleRebalance(
                    app: app, reason: .membershipChange)
            }
        }
        return anySucceeded
    }

    private func querySeed(app: Application, address: String, config: ClusterConfiguration) async
        -> [ClusterNodeInfo]?
    {
        var outbound = HTTPClientRequest(url: address + "/internal/cluster/members")
        outbound.method = .GET
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        do {
            // `LightweightClusterControlClient.shared`, not `app.http.client.shared` - see its
            // doc comment: this boot-blocking, retried probe must never queue behind unrelated
            // shard-transfer/rebalance traffic on the general-purpose client's connection pool.
            // `seedQueryTimeout`, not `ClusterReplicationClient.probeTimeout` - see its own doc
            // comment for why this specific probe needs a much shorter ceiling than a normal
            // peer-liveness check.
            let response = try await LightweightClusterControlClient.shared.execute(
                outbound, timeout: Self.seedQueryTimeout, logger: app.logger)
            guard response.status == .ok else { return nil }
            let body = try await response.body.collect(upTo: 4 * 1024 * 1024)
            let decoded = try JSONDecoder().decode(
                [InternalClusterMetadataController.ClusterMemberWire].self, from: Data(buffer: body))
            return decoded.compactMap { wire -> ClusterNodeInfo? in
                return ClusterNodeInfo(
                    id: wire.id, address: wire.address, status: wire.status,
                    lastHeartbeatAt: wire.lastHeartbeatAt, totalBytes: wire.totalBytes,
                    availableBytes: wire.availableBytes)
            }
        } catch {
            return nil
        }
    }

    /// Upserts this node's own record (creating it on first-ever boot, refreshing address/
    /// heartbeat on every restart), updates the local cache immediately (not waiting on the
    /// broadcast round-trip - same "update local state first, then notify" order), then notifies
    /// the cluster. A restart always re-activates a previously-`draining` node - if an operator
    /// restarts the process, that's a clear signal they want it back in service, not a state the
    /// node should second-guess.
    private func registerSelf(app: Application, config: ClusterConfiguration) async throws {
        let now = Date()
        let (totalBytes, availableBytes) = DiskSpace.availableAndTotal(for: BucketHandler.rootURL)
        let node: ClusterNode
        if let existing = try? await ClusterNode.find(app: app, id: config.nodeId) {
            node = existing
            node.address = config.address
            node.status = .active
            node.lastHeartbeatAt = now
            node.totalBytes = totalBytes
            node.availableBytes = availableBytes
        } else {
            node = ClusterNode(
                id: config.nodeId, address: config.address, status: .active, joinedAt: now,
                lastHeartbeatAt: now, totalBytes: totalBytes, availableBytes: availableBytes)
        }
        // Best-effort, not `try await`: `node.save` forwards to whichever node currently ranks
        // rank-0 for this node's own record, not necessarily reachable right now. A node's
        // belief about its own liveness must never depend on reaching some other peer first -
        // the local cache upsert below still runs, and the durable write retries on the next tick.
        do {
            try await node.save(app: app)
        } catch {
            app.logger.warning(
                "Cluster self-registration write failed - continuing with the in-memory update, the next heartbeat tick will retry the durable write: \(error)"
            )
        }
        lastKnownJoinedAt = node.joinedAt
        lastKnownStatus = .active

        await ClusterNodeCache.shared.upsert(
            ClusterNodeInfo(
                id: config.nodeId, address: config.address, status: .active, lastHeartbeatAt: now,
                totalBytes: totalBytes, availableBytes: availableBytes)
        )
        CacheInvalidationService.notify(
            app: app, cache: "clusterNode", op: .upsert, key: config.nodeId.uuidString,
            nodeInfo: InternalClusterMetadataController.ClusterMemberWire(
                id: config.nodeId, address: config.address, status: .active,
                lastHeartbeatAt: now, totalBytes: totalBytes, availableBytes: availableBytes))
    }

    /// A heartbeat tick must never depend on first successfully *reading* this node's own record:
    /// metadata routing is recomputed fresh on every call, so a read attempted before this node's
    /// view has converged can legitimately fail even though the record is durable. Requiring a
    /// successful read first would permanently wedge the heartbeat on a single boot-time hiccup.
    /// Instead the read is best-effort (only used to preserve fields this loop doesn't own, like
    /// an admin-initiated `draining` status); the write always goes through, self-correcting once
    /// every node's view converges.
    private func heartbeatLoop(app: Application, config: ClusterConfiguration) async {
        while !Task.isCancelled {
            try? await Task.sleep(for: .seconds(Self.heartbeatInterval))
            guard !Task.isCancelled else { return }

            let now = Date()
            let (totalBytes, availableBytes) = DiskSpace.availableAndTotal(for: BucketHandler.rootURL)

            let node: ClusterNode
            if let existing = try? await ClusterNode.find(app: app, id: config.nodeId) {
                node = existing
                lastKnownJoinedAt = existing.joinedAt
                lastKnownStatus = ClusterNode.Status(rawValue: existing.status.rawValue) ?? lastKnownStatus
            } else if let cached = await ClusterNodeCache.shared.get(id: config.nodeId) {
                // The direct read failed (a convergence hiccup), but this node's in-memory belief
                // about ITSELF is usually still fresh: an admin-initiated drain reaches this node
                // directly via `CacheInvalidationService.notify`'s broadcast, independent of
                // whether this node's own read succeeds. Preferring the cache over
                // `lastKnownStatus` here closes the "heartbeat silently reverts a drain" gap.
                lastKnownStatus = cached.status
                lastKnownJoinedAt = lastKnownJoinedAt ?? now
                node = ClusterNode(
                    id: config.nodeId, address: config.address, status: cached.status,
                    joinedAt: lastKnownJoinedAt ?? now)
            } else {
                app.logger.warning(
                    "Cluster heartbeat could not read this node's own record - re-registering from last-known state rather than skipping (see ClusterMembershipLifecycle.heartbeatLoop)."
                )
                node = ClusterNode(
                    id: config.nodeId, address: config.address, status: lastKnownStatus,
                    joinedAt: lastKnownJoinedAt ?? now)
            }
            node.lastHeartbeatAt = now
            node.totalBytes = totalBytes
            node.availableBytes = availableBytes

            // Best-effort, not gating the cache update below: `node.save` forwards to whichever
            // node currently ranks rank-0 for this node's own record, not necessarily reachable.
            // Skipping the local cache upsert on a save failure would let a coordinator that's
            // merely temporarily down leave this perfectly healthy node excluding ITSELF from its
            // own `activeNodes()`. The durable write is simply retried on the next tick.
            do {
                try await node.save(app: app)
            } catch {
                app.logger.warning(
                    "Cluster heartbeat durable write failed - the local cache still updates below, and the next tick retries: \(error)"
                )
            }

            // Always a valid rawValue here: `node` is either a fresh read-back (whose status was
            // already parsed successfully above) or built from `lastKnownStatus`, itself always a
            // real `ClusterNode.Status`.
            let statusForCache = ClusterNode.Status(rawValue: node.status.rawValue) ?? .active
            await ClusterNodeCache.shared.upsert(
                ClusterNodeInfo(
                    id: config.nodeId, address: config.address, status: statusForCache,
                    lastHeartbeatAt: now, totalBytes: totalBytes, availableBytes: availableBytes)
            )
        }
    }

    private func refreshLoop(app: Application, config: ClusterConfiguration) async {
        while !Task.isCancelled {
            try? await Task.sleep(for: .seconds(Self.membershipRefreshInterval))
            guard !Task.isCancelled else { return }

            // `all` degrades to a partial listing rather than throwing, and `status` is a typed
            // enum - a malformed record fails at decode, which is where it belongs.
            let snapshot = await ClusterNode.all(app: app).map { row in
                ClusterNodeInfo(
                    id: row.id, address: row.address, status: row.status,
                    lastHeartbeatAt: row.lastHeartbeatAt,
                    totalBytes: row.totalBytes, availableBytes: row.availableBytes)
            }
            await ClusterNodeCache.shared.reconcile(snapshot: snapshot)

            // Re-query the static seed list too, not just the listing fan-out above - see
            // `reseedFromConfiguredSeeds`'s doc comment for why the fan-out alone can leave two
            // independently-booted subsets of a cluster permanently unaware of each other.
            // Ordered after the listing-based reconcile (which can only ever shrink the cache) so
            // any node it dropped that's still genuinely reachable via a seed is restored within
            // the same tick, rather than staying dropped until the next cycle.
            await reseedFromConfiguredSeeds(
                app: app, config: config, triggerCacheReloadOnGrowth: true)
        }
    }
}
