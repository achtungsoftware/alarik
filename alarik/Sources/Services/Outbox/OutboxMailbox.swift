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

/// Target-affine outbox storage, backing `GenericOutboxDispatcher`'s `fetchDue`/persist/remove
/// closures.
///
/// Outbox rows are node-affine ("give me *my* due work"), not key-affine, so HRW-hashing a task's
/// own identity would scatter it onto the wrong node. Each task instead lives as a plain file
/// (`OutboxMailboxFileHandler`) at `Storage/outbox/<collection>/<ownerNodeId>/<taskId>.task`, so
/// discovery is a cheap local `readdir`, never a cluster-wide scan. Durability without full k+m
/// striping comes from best-effort mirroring to a handful of other nodes, needed only for the two
/// collections with no independent ground truth (`NotificationDelivery`/`ReplicationTask`).
enum OutboxMailbox {
    /// Synthetic bucket name used only to feed HRW when electing an owner for a task. Not a real
    /// bucket - it just namespaces the hash so task placement can't collide with object placement.
    static let outboxPlacementBucket = "outbox-promotion"

    /// How many total copies (owner + mirrors) a non-backstopped task is kept at. `1` disables
    /// mirroring outright (single-node deployments have no peers to mirror to anyway).
    static let defaultReplicaCount = 2

    private static func replicaCount(app: Application) -> Int {
        guard let raw = Environment.sanitizedGet("CLUSTER_METADATA_REPLICA_COUNT"),
            let value = Int(raw), value >= 1
        else { return defaultReplicaCount }
        return value
    }

    /// This node's own stable identity for mailbox-ownership purposes. Prefers
    /// `ClusterConfigurationKey.nodeId` whenever clustered, the same identity every other
    /// cluster subsystem already agrees on. Falls back to the restart-stable
    /// `Storage/cluster_node_id` file only when not clustered at all.
    static func selfNodeId(app: Application) -> UUID {
        if let configured = app.storage[ClusterConfigurationKey.self]?.nodeId {
            return configured
        }
        return (try? ClusterNodeIdentity.loadOrCreate()) ?? UUID()
    }

    // MARK: - Dispatcher integration (fetchDue / persist / remove)

    /// This node's own due work in `collection`, sorted by `nextAttemptAt`, capped at `limit`.
    /// Cost is proportional to this node's own backlog in this one collection only.
    static func dueTasks<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String, limit: Int
    ) -> [Row] {
        let selfId = selfNodeId(app: app)
        let now = Date()
        let ids = OutboxMailboxFileHandler.listTaskIds(
            root: OutboxMailboxFileHandler.rootPath, collection: collection, ownerNodeId: selfId)

        // Skip not-yet-due work with a `stat` rather than a read-and-decode. A backed-up queue is
        // mostly tasks sitting on an exponential backoff, so decoding all of them on every tick to
        // discover they aren't due yet is the dominant cost. A file with no usable hint is read as
        // before, so this can only ever save work, never hide a due task.
        var candidates: [(id: UUID, hint: Date)] = []
        candidates.reserveCapacity(ids.count)
        for id in ids {
            let path = OutboxMailboxFileHandler.taskPath(
                root: OutboxMailboxFileHandler.rootPath, collection: collection,
                ownerNodeId: selfId, taskId: id)
            guard let hint = OutboxMailboxFileHandler.dueHint(path: path) else {
                candidates.append((id, .distantPast))  // unknown - always inspect
                continue
            }
            guard hint <= now else { continue }
            candidates.append((id, hint))
        }

        // Oldest-due first, so a bounded tick drains the most overdue work rather than an
        // arbitrary slice of it.
        candidates.sort { $0.hint < $1.hint }

        var rows: [Row] = []
        rows.reserveCapacity(min(candidates.count, limit))
        for candidate in candidates {
            guard
                let row = readOwned(
                    Row.self, collection: collection, ownerNodeId: selfId, taskId: candidate.id)
            else { continue }
            // The decoded values stay authoritative - the hint only decided what to read.
            guard row.state == OutboxRowState.pending, row.nextAttemptAt <= now else { continue }
            rows.append(row)
            if rows.count == limit { break }
        }
        return rows
    }

    /// Rewrites an existing task in place (bump attempts, back off, mark failed) - a local
    /// atomic file rewrite, no network round-trip, the overwhelmingly common dispatcher
    /// operation.
    static func update<Row: OutboxMailboxRow>(_ row: Row, collection: String) throws {
        let path = OutboxMailboxFileHandler.taskPath(
            root: OutboxMailboxFileHandler.rootPath, collection: collection,
            ownerNodeId: row.ownerNodeId, taskId: row.id)
        try OutboxMailboxFileHandler.write(
            path: path, data: try JSONEncoder().encode(row), dueAt: row.nextAttemptAt)
    }

    /// Deletes a completed task - both the owner's copy and any backup mirror, since a done task
    /// needs no further protecting.
    static func remove<Row: OutboxMailboxRow>(_ row: Row, collection: String) {
        OutboxMailboxFileHandler.remove(
            path: OutboxMailboxFileHandler.taskPath(
                root: OutboxMailboxFileHandler.rootPath, collection: collection,
                ownerNodeId: row.ownerNodeId, taskId: row.id))
        OutboxMailboxFileHandler.remove(
            path: OutboxMailboxFileHandler.taskPath(
                root: OutboxMailboxFileHandler.backupRootPath, collection: collection,
                ownerNodeId: row.ownerNodeId, taskId: row.id))
    }

    private static func readOwned<Row: OutboxMailboxRow>(
        _ type: Row.Type, collection: String, ownerNodeId: UUID, taskId: UUID
    ) -> Row? {
        let path = OutboxMailboxFileHandler.taskPath(
            root: OutboxMailboxFileHandler.rootPath, collection: collection, ownerNodeId: ownerNodeId,
            taskId: taskId)
        guard let data = OutboxMailboxFileHandler.read(path: path) else { return nil }
        return try? JSONDecoder().decode(Row.self, from: data)
    }

    /// This node's own owned tasks in `collection`, unfiltered (unlike `dueTasks`, includes
    /// non-pending/not-yet-due rows too) - the building block for admin/console listing and for
    /// local-predicate operations (`removeOwned(matching:)`, `retryOwned(taskId:)`). Local-only,
    /// no network - callers needing the whole cluster's view compose this with a fan-out.
    static func allOwnedTasks<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String
    ) -> [Row] {
        let selfId = selfNodeId(app: app)
        let ids = OutboxMailboxFileHandler.listTaskIds(
            root: OutboxMailboxFileHandler.rootPath, collection: collection, ownerNodeId: selfId)
        return ids.compactMap { readOwned(Row.self, collection: collection, ownerNodeId: selfId, taskId: $0) }
    }

    /// Purges this node's own owned tasks that are `.failed` and older than `olderThan`.
    /// Purging is inherently per-node (each node only ever holds tasks it owns), so the periodic
    /// purge tick already running on every node is exactly the right cadence - no separate
    /// cluster-wide sweep needed.
    static func purgeExpiredFailures<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String,
        olderThan: TimeInterval = 7 * 24 * 3600
    ) {
        let cutoff = Date().addingTimeInterval(-olderThan)
        for row in allOwnedTasks(Row.self, app: app, collection: collection)
        where row.state == .failed && row.createdAt < cutoff {
            remove(row, collection: collection)
        }
    }

    /// Removes every one of this node's own owned tasks matching `predicate` - the local half of
    /// a "purge everywhere" operation (e.g. a deleted bucket's queued deliveries/replication
    /// tasks). Callers broadcast the same command to every node (mirroring
    /// `CacheReloadDispatch`'s `clusterRebalance`/`clusterScrub` idempotent-broadcast shape) so
    /// each node purges its own local subset; there's no single node that could purge the whole
    /// cluster's matching set by itself.
    static func removeOwned<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String, matching predicate: (Row) -> Bool
    ) {
        for row in allOwnedTasks(Row.self, app: app, collection: collection) where predicate(row) {
            remove(row, collection: collection)
        }
    }

    /// Resets `taskId`'s backoff for immediate redelivery, if this node owns it - returns the
    /// updated row on success, `nil` if this node doesn't own that id (the caller broadcasts to
    /// every node and only the actual owner will find and reset it).
    static func retryOwned<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String, taskId: UUID
    ) -> Row? {
        guard
            let row = readOwned(
                Row.self, collection: collection, ownerNodeId: selfNodeId(app: app), taskId: taskId)
        else { return nil }
        row.attempts = 0
        row.nextAttemptAt = Date()
        row.lastError = nil
        if row.state == .failed { row.state = .pending }
        try? update(row, collection: collection)
        return row
    }

    // MARK: - Enqueue (creates a brand-new task, routing it to its owner)

    /// Durably places a freshly-created task. If `row.ownerNodeId` is this node, it's a pure
    /// local file write (no network). Otherwise delivers it to the owner via
    /// `POST /internal/cluster/outbox/enqueue`, falling back to a local pending-enqueue spool
    /// (retried by `retryPendingEnqueues`) if the owner is unreachable or unknown.
    static func enqueue<Row: OutboxMailboxRow>(app: Application, collection: String, row: Row) async {
        let selfId = selfNodeId(app: app)
        if row.ownerNodeId == selfId {
            try? update(row, collection: collection)
            return
        }
        guard let config = app.storage[ClusterConfigurationKey.self],
            let owner = await ClusterNodeCache.shared.get(id: row.ownerNodeId)
        else {
            spoolPendingEnqueue(row, collection: collection)
            return
        }
        let delivered = await sendEnqueueRPC(
            app: app, node: owner, secret: config.secret, collection: collection, row: row)
        if !delivered {
            spoolPendingEnqueue(row, collection: collection)
        }
    }

    private static func spoolPendingEnqueue<Row: OutboxMailboxRow>(_ row: Row, collection: String) {
        let path = OutboxMailboxFileHandler.taskPath(
            root: OutboxMailboxFileHandler.pendingEnqueueRootPath, collection: collection,
            ownerNodeId: row.ownerNodeId, taskId: row.id)
        try? OutboxMailboxFileHandler.write(path: path, data: try JSONEncoder().encode(row))
    }

    /// Retries every task still sitting in this node's own pending-enqueue spool - called on the
    /// same drain tick as the dispatcher itself, so a stragglered enqueue catches up as soon as
    /// the owner becomes reachable/known again, without needing a dedicated timer.
    static func retryPendingEnqueues<Row: OutboxMailboxRow>(_ type: Row.Type, app: Application, collection: String)
        async
    {
        let selfId = selfNodeId(app: app)
        let entries = OutboxMailboxFileHandler.listAllOwnerTaskIds(
            root: OutboxMailboxFileHandler.pendingEnqueueRootPath, collection: collection)
        guard !entries.isEmpty else { return }

        for (ownerNodeId, taskId) in entries {
            let spoolPath = OutboxMailboxFileHandler.taskPath(
                root: OutboxMailboxFileHandler.pendingEnqueueRootPath, collection: collection,
                ownerNodeId: ownerNodeId, taskId: taskId)
            guard let data = OutboxMailboxFileHandler.read(path: spoolPath),
                let row = try? JSONDecoder().decode(Row.self, from: data)
            else {
                OutboxMailboxFileHandler.remove(path: spoolPath)
                continue
            }

            if row.ownerNodeId == selfId {
                try? update(row, collection: collection)
                OutboxMailboxFileHandler.remove(path: spoolPath)
                continue
            }
            guard let config = app.storage[ClusterConfigurationKey.self],
                let owner = await ClusterNodeCache.shared.get(id: row.ownerNodeId)
            else { continue }
            let delivered = await sendEnqueueRPC(
                app: app, node: owner, secret: config.secret, collection: collection, row: row)
            if delivered {
                OutboxMailboxFileHandler.remove(path: spoolPath)
            }
        }
    }

    private static func sendEnqueueRPC<Row: OutboxMailboxRow>(
        app: Application, node: ClusterNodeInfo, secret: String, collection: String, row: Row
    ) async -> Bool {
        do {
            var outbound = HTTPClientRequest(
                url: node.address + "/internal/cluster/outbox/enqueue"
                    + querySuffix(collection: collection))
            outbound.method = .POST
            outbound.headers.replaceOrAdd(
                name: ClusterForwardAuthenticator.secretHeaderName, value: secret)
            outbound.headers.replaceOrAdd(name: .contentType, value: "application/json")
            outbound.body = .bytes(ByteBuffer(data: try JSONEncoder().encode(row)))
            let response = try await LightweightClusterControlClient.shared.execute(
                outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
            return response.status == .ok
        } catch {
            return false
        }
    }

    /// The receiving side of `sendEnqueueRPC` - places `row` into this (the owner) node's own
    /// mailbox directory. Trusts the caller's placement decision unconditionally, the same trust
    /// model every other internal cluster RPC uses (secured by `ClusterSecretMiddleware`).
    static func receiveEnqueue<Row: OutboxMailboxRow>(_ row: Row, collection: String) throws {
        try update(row, collection: collection)
    }

    // MARK: - Backup mirroring (best-effort, non-backstopped collections only)

    /// Re-pushes every one of THIS node's own owned tasks in `collection` to `replicaCount - 1`
    /// other active peers. Driven purely by re-scanning what's already durably local - no
    /// separate "pending backup" bookkeeping - so this is simple at the cost of some redundant
    /// (but small - these are tiny JSON records) network traffic on every sweep, deliberately
    /// traded for not needing to track confirmation state at all.
    static func mirrorBackups<Row: OutboxMailboxRow>(_ type: Row.Type, app: Application, collection: String)
        async
    {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return }
        let selfId = config.nodeId
        let replicas = replicaCount(app: app)
        guard replicas > 1 else { return }

        let active = await ClusterNodeCache.shared.activeNodes().filter { $0.id != selfId }
        guard !active.isEmpty else { return }
        let targets = Array(active.prefix(replicas - 1))

        let ids = OutboxMailboxFileHandler.listTaskIds(
            root: OutboxMailboxFileHandler.rootPath, collection: collection, ownerNodeId: selfId)
        guard !ids.isEmpty else { return }

        for id in ids {
            let path = OutboxMailboxFileHandler.taskPath(
                root: OutboxMailboxFileHandler.rootPath, collection: collection, ownerNodeId: selfId,
                taskId: id)
            guard let data = OutboxMailboxFileHandler.read(path: path) else { continue }
            await withTaskGroup(of: Void.self) { group in
                for node in targets {
                    group.addTask {
                        await sendBackupRPC(
                            app: app, node: node, secret: config.secret, collection: collection,
                            ownerNodeId: selfId, taskId: id, data: data)
                    }
                }
            }
        }
    }

    private static func sendBackupRPC(
        app: Application, node: ClusterNodeInfo, secret: String, collection: String,
        ownerNodeId: UUID, taskId: UUID, data: Data
    ) async {
        do {
            var outbound = HTTPClientRequest(
                url: node.address + "/internal/cluster/outbox/backup"
                    + querySuffix(collection: collection, ownerNodeId: ownerNodeId, taskId: taskId))
            outbound.method = .POST
            outbound.headers.replaceOrAdd(
                name: ClusterForwardAuthenticator.secretHeaderName, value: secret)
            outbound.headers.replaceOrAdd(name: .contentType, value: "application/json")
            outbound.body = .bytes(ByteBuffer(data: data))
            _ = try await LightweightClusterControlClient.shared.execute(
                outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
        } catch {
            // Best-effort - the next sweep re-tries automatically.
        }
    }

    /// The receiving side of `sendBackupRPC` - stores a mirror copy under `outbox-backup/`, never
    /// the live `outbox/` tree (a backup is only ever promoted, deliberately, by
    /// `promoteOrphanedBackups`, never served directly by this node's own dispatcher tick).
    static func receiveBackup(data: Data, collection: String, ownerNodeId: UUID, taskId: UUID) throws {
        let path = OutboxMailboxFileHandler.taskPath(
            root: OutboxMailboxFileHandler.backupRootPath, collection: collection,
            ownerNodeId: ownerNodeId, taskId: taskId)
        try OutboxMailboxFileHandler.write(path: path, data: data)
    }

    // MARK: - Promotion on ungraceful owner loss

    /// Walks this node's own held backup entries in `collection` and self-promotes any whose
    /// owner has dropped out of active membership - moves the file from `outbox-backup/` into
    /// this node's own `outbox/<collection>/<selfId>/`, re-owning it. A deterministic HRW
    /// tie-break (via `PlacementService`) among the other backup-holders ensures at most one
    /// promotes any given task, even though every holder runs this sweep independently.
    static func promoteOrphanedBackups<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String
    ) async {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return }
        let selfId = config.nodeId
        // Liveness decides WHETHER to promote (is the owner gone?); stable placement decides WHO
        // promotes. Electing from the live set would let two nodes with slightly different
        // heartbeat views each elect themselves and both promote the same task - a webhook
        // delivered twice, a replication task run twice. `placementNodes()` is the same on every
        // node, so the election is unanimous; self must still be in it to be eligible.
        let activeIds = Set(await ClusterNodeCache.shared.activeNodes().map(\.id))
        let electorate = await ClusterNodeCache.shared.placementNodes()

        let backupEntries = OutboxMailboxFileHandler.listAllOwnerTaskIds(
            root: OutboxMailboxFileHandler.backupRootPath, collection: collection)
        guard !backupEntries.isEmpty else { return }

        for (ownerNodeId, taskId) in backupEntries {
            guard !activeIds.contains(ownerNodeId) else { continue }

            // Elect among nodes that are both placement members AND currently reachable, so a
            // task isn't handed to a node that is itself down; every node computes this from the
            // same two sets.
            let eligible = electorate.filter { activeIds.contains($0.id) }
            let winner = PlacementService.responsibleNodes(
                bucketName: outboxPlacementBucket,
                key: "\(collection)/\(ownerNodeId)/\(taskId)",
                activeNodes: eligible, count: 1
            ).first
            guard winner?.id == selfId else { continue }

            let backupPath = OutboxMailboxFileHandler.taskPath(
                root: OutboxMailboxFileHandler.backupRootPath, collection: collection,
                ownerNodeId: ownerNodeId, taskId: taskId)
            guard let data = OutboxMailboxFileHandler.read(path: backupPath),
                let row = try? JSONDecoder().decode(Row.self, from: data)
            else {
                OutboxMailboxFileHandler.remove(path: backupPath)
                continue
            }

            row.ownerNodeId = selfId
            try? update(row, collection: collection)
            OutboxMailboxFileHandler.remove(path: backupPath)
        }
    }

    // MARK: - Reassignment on graceful drain

    /// For the two non-backstopped collections, walks a departing node's own owner directory
    /// (while it's still reachable, i.e. this runs *before* the node transitions to `.removed`)
    /// and re-`enqueue`s each pending item under a freshly HRW-chosen owner - unlike the
    /// ground-truth-backstopped collections, simply deleting these rows and hoping a later
    /// process regenerates them isn't safe, since nothing else independently knows "this webhook
    /// still needs firing."
    static func reassignOwnedTasks<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String, departingNodeId: UUID
    ) async {
        let ids = OutboxMailboxFileHandler.listTaskIds(
            root: OutboxMailboxFileHandler.rootPath, collection: collection,
            ownerNodeId: departingNodeId)
        guard !ids.isEmpty else { return }

        let candidates = await ClusterNodeCache.shared.activeNodes().filter {
            $0.id != departingNodeId
        }
        guard !candidates.isEmpty else { return }

        for id in ids {
            let path = OutboxMailboxFileHandler.taskPath(
                root: OutboxMailboxFileHandler.rootPath, collection: collection,
                ownerNodeId: departingNodeId, taskId: id)
            guard let data = OutboxMailboxFileHandler.read(path: path),
                let row = try? JSONDecoder().decode(Row.self, from: data)
            else {
                OutboxMailboxFileHandler.remove(path: path)
                continue
            }
            // HRW per task, not one node for all of them: `activeNodes()` is backed by a
            // dictionary and has no stable order, so taking its first element both picks an
            // arbitrary node and dumps a draining node's entire backlog onto that one peer.
            guard
                let newOwner = PlacementService.responsibleNodes(
                    bucketName: outboxPlacementBucket, key: "\(collection)/\(id)",
                    activeNodes: candidates, count: 1
                ).first
            else { continue }
            row.ownerNodeId = newOwner.id
            await enqueue(app: app, collection: collection, row: row)
            OutboxMailboxFileHandler.remove(path: path)
        }
    }

    // MARK: - Cluster-wide admin/console operations

    /// Every task in `collection`, across every node - admin/console listing only (e.g. "show me
    /// this bucket's queued webhook deliveries"), never a per-request hot path. Fans out to every
    /// active peer in parallel (mirroring `MetadataListingService`'s shape) and merges with this
    /// node's own `allOwnedTasks`.
    static func listAllAcrossCluster<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String
    ) async -> [Row] {
        var results = allOwnedTasks(Row.self, app: app, collection: collection)
        guard let config = app.storage[ClusterConfigurationKey.self] else { return results }
        let peers = await ClusterNodeCache.shared.activeNodes().filter { $0.id != config.nodeId }
        guard !peers.isEmpty else { return results }

        await withTaskGroup(of: [Row].self) { group in
            for node in peers {
                group.addTask {
                    await fetchRemoteList(Row.self, app: app, node: node, secret: config.secret, collection: collection)
                }
            }
            for await remote in group { results.append(contentsOf: remote) }
        }
        return results
    }

    private static func fetchRemoteList<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, node: ClusterNodeInfo, secret: String, collection: String
    ) async -> [Row] {
        do {
            var outbound = HTTPClientRequest(
                url: node.address + "/internal/cluster/outbox/list" + querySuffix(collection: collection))
            outbound.method = .GET
            outbound.headers.replaceOrAdd(
                name: ClusterForwardAuthenticator.secretHeaderName, value: secret)
            let response = try await LightweightClusterControlClient.shared.execute(
                outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
            guard response.status == .ok else { return [] }
            let body = try await response.body.collect(upTo: 64 * 1024 * 1024)
            return try JSONDecoder().decode([Row].self, from: Data(buffer: body))
        } catch {
            return []
        }
    }

    /// Broadcasts a "retry this task if you own it" command to every active node (including
    /// self) - exactly one will actually own `taskId` and reset its backoff; the rest silently
    /// no-op. Returns `true` if any node reported success.
    static func retryAcrossCluster<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String, taskId: UUID
    ) async -> Bool {
        if retryOwned(Row.self, app: app, collection: collection, taskId: taskId)
            != nil
        {
            return true
        }
        guard let config = app.storage[ClusterConfigurationKey.self] else { return false }
        let peers = await ClusterNodeCache.shared.activeNodes().filter { $0.id != config.nodeId }
        guard !peers.isEmpty else { return false }

        return await withTaskGroup(of: Bool.self, returning: Bool.self) { group in
            for node in peers {
                group.addTask {
                    await sendRetryRPC(
                        app: app, node: node, secret: config.secret, collection: collection, taskId: taskId)
                }
            }
            for await ok in group where ok { return true }
            return false
        }
    }

    private static func sendRetryRPC(
        app: Application, node: ClusterNodeInfo, secret: String, collection: String, taskId: UUID
    ) async -> Bool {
        do {
            var outbound = HTTPClientRequest(
                url: node.address + "/internal/cluster/outbox/retry"
                    + querySuffix(collection: collection, taskId: taskId))
            outbound.method = .POST
            outbound.headers.replaceOrAdd(
                name: ClusterForwardAuthenticator.secretHeaderName, value: secret)
            let response = try await LightweightClusterControlClient.shared.execute(
                outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
            return response.status == .ok
        } catch {
            return false
        }
    }

    /// Broadcasts "purge every task matching this bucket" to every active node (including self) -
    /// each node purges its own local owned subset, mirroring `CacheReloadDispatch`'s
    /// `clusterRebalance`/`clusterScrub` idempotent-broadcast shape. Used when a bucket is
    /// deleted, so no stale task keeps trying to announce/replicate objects that no longer exist.
    static func purgeBucketAcrossCluster<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String, bucketName: String,
        matching predicate: @escaping (Row) -> Bool
    ) async {
        removeOwned(Row.self, app: app, collection: collection, matching: predicate)
        guard let config = app.storage[ClusterConfigurationKey.self] else { return }
        let peers = await ClusterNodeCache.shared.activeNodes().filter { $0.id != config.nodeId }
        guard !peers.isEmpty else { return }

        await withTaskGroup(of: Void.self) { group in
            for node in peers {
                group.addTask {
                    await sendPurgeBucketRPC(
                        app: app, node: node, secret: config.secret, collection: collection,
                        bucketName: bucketName)
                }
            }
        }
    }

    /// Best-effort - a missed purge just leaves a harmless task that fails/no-ops once drained
    /// (the bucket it referenced is already gone).
    private static func sendPurgeBucketRPC(
        app: Application, node: ClusterNodeInfo, secret: String, collection: String, bucketName: String
    ) async {
        var outbound = HTTPClientRequest(
            url: node.address + "/internal/cluster/outbox/purge-bucket"
                + querySuffix(collection: collection, bucketName: bucketName))
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: secret)
        _ = try? await LightweightClusterControlClient.shared.execute(
            outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
    }

    /// Broadcasts "purge every task whose `targetNodeId` is this" to every active node (including
    /// self) - used by `InternalClusterController.drainNode` to clean up stale copy/repair tasks
    /// aimed at a node that's leaving. Needed specifically because `targetNodeId` isn't always the
    /// mailbox owner (`ClusterReplicationTask`'s owner is the *sender*, not the target - see its
    /// doc comment), so these stale tasks can live on any node's mailbox, not just the draining
    /// one's.
    static func purgeByTargetNodeAcrossCluster<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String, targetNodeId: UUID,
        matching predicate: @escaping (Row) -> Bool
    ) async {
        removeOwned(Row.self, app: app, collection: collection, matching: predicate)
        guard let config = app.storage[ClusterConfigurationKey.self] else { return }
        let peers = await ClusterNodeCache.shared.activeNodes().filter { $0.id != config.nodeId }
        guard !peers.isEmpty else { return }

        await withTaskGroup(of: Void.self) { group in
            for node in peers {
                group.addTask {
                    await sendPurgeTargetRPC(
                        app: app, node: node, secret: config.secret, collection: collection,
                        targetNodeId: targetNodeId)
                }
            }
        }
    }

    private static func sendPurgeTargetRPC(
        app: Application, node: ClusterNodeInfo, secret: String, collection: String, targetNodeId: UUID
    ) async {
        var outbound = HTTPClientRequest(
            url: node.address + "/internal/cluster/outbox/purge-target"
                + querySuffix(collection: collection, targetNodeId: targetNodeId))
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: secret)
        _ = try? await LightweightClusterControlClient.shared.execute(
            outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
    }

    private static func querySuffix(
        collection: String, ownerNodeId: UUID? = nil, taskId: UUID? = nil, bucketName: String? = nil,
        targetNodeId: UUID? = nil
    ) -> String {
        var components = URLComponents()
        var items = [URLQueryItem(name: "collection", value: collection)]
        if let ownerNodeId { items.append(URLQueryItem(name: "ownerNodeId", value: ownerNodeId.uuidString)) }
        if let taskId { items.append(URLQueryItem(name: "taskId", value: taskId.uuidString)) }
        if let bucketName { items.append(URLQueryItem(name: "bucketName", value: bucketName)) }
        if let targetNodeId {
            items.append(URLQueryItem(name: "targetNodeId", value: targetNodeId.uuidString))
        }
        components.queryItems = items
        return "?" + (components.percentEncodedQuery ?? "")
    }
}
