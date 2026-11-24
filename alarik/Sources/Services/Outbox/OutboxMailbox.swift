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
    /// Shared, not per-row: the sweeps below decode one row per file across the whole backlog.
    private static let decoder = JSONDecoder()

    /// Decodes a task file, or discards it after saying so. Every caller here deletes the file on
    /// failure - a row nothing can decode would otherwise be retried forever - so this must never
    /// be silent: the file IS the task, and the discard is a webhook that never fires or a
    /// replication that never happens, with no other record that it was ever queued.
    private static func decodeOrDiscard<Row: OutboxMailboxRow>(
        _ type: Row.Type, data: Data, path: String, app: Application
    ) -> Row? {
        do {
            return try decoder.decode(Row.self, from: data)
        } catch {
            app.logger.error(
                "Discarding undecodable \(Row.self) task file '\(path)' - the queued work is lost, not retried: \(error)"
            )
            return nil
        }
    }
    /// Synthetic bucket name used only to feed HRW when electing an owner for a task. Not a real
    /// bucket - it just namespaces the hash so task placement can't collide with object placement.
    static let outboxPlacementBucket = "outbox-promotion"

    /// How many total copies (owner + mirrors) a non-backstopped task is kept at. `1` disables
    /// mirroring outright (single-node deployments have no peers to mirror to anyway).
    static let defaultReplicaCount = 2

    /// `CLUSTER_OUTBOX_REPLICA_COUNT`; `CLUSTER_METADATA_REPLICA_COUNT` is the name it shipped
    /// under and is still honoured.
    static func replicaCount(app: Application) -> Int {
        for name in ["CLUSTER_OUTBOX_REPLICA_COUNT", "CLUSTER_METADATA_REPLICA_COUNT"] {
            if let raw = Environment.sanitizedGet(name), let value = Int(raw), value >= 1 {
                return value
            }
        }
        return defaultReplicaCount
    }

    /// The collections that have no independent ground truth to rebuild from, and so are the
    /// ones mirrored: a lost webhook or bucket-replication row is a delivery that never happens.
    /// Cluster replication and shard repair rows are backstopped by the rebalance walks.
    static let mirroredCollections = [
        OutboxCollections.notificationDeliveries, OutboxCollections.replicationTasks,
    ]

    /// One periodic pass of the durability scheme: re-mirror this node's own rows, then adopt
    /// rows whose owner has dropped out of the cluster. Wakes the dispatchers when anything was
    /// adopted so the work resumes without waiting for their own tick.
    static func runBackupSweep(app: Application) async {
        guard app.storage[ClusterConfigurationKey.self] != nil else { return }
        await mirrorBackups(
            NotificationDelivery.self, app: app, collection: OutboxCollections.notificationDeliveries)
        await mirrorBackups(ReplicationTask.self, app: app, collection: OutboxCollections.replicationTasks)
        await pruneDeliveredBackups(
            NotificationDelivery.self, app: app, collection: OutboxCollections.notificationDeliveries)
        await pruneDeliveredBackups(ReplicationTask.self, app: app, collection: OutboxCollections.replicationTasks)
        let adoptedDeliveries = await promoteOrphanedBackups(
            NotificationDelivery.self, app: app, collection: OutboxCollections.notificationDeliveries)
        let adoptedReplications = await promoteOrphanedBackups(
            ReplicationTask.self, app: app, collection: OutboxCollections.replicationTasks)
        if adoptedDeliveries > 0 { NotificationDispatcher.shared.wake() }
        if adoptedReplications > 0 { ReplicationDispatcher.shared.wake() }
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
        return try? decoder.decode(Row.self, from: data)
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
        // A retry that could not be written back did not happen; the caller reports failure.
        guard (try? update(row, collection: collection)) != nil else { return nil }
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
            storeOrReport(row, collection: collection, app: app)
            return
        }
        guard let config = app.storage[ClusterConfigurationKey.self],
            let owner = await ClusterNodeCache.shared.get(id: row.ownerNodeId)
        else {
            spoolPendingEnqueue(row, collection: collection, app: app)
            return
        }
        let delivered = await sendEnqueueRPC(
            app: app, node: owner, secret: config.secret, collection: collection, row: row)
        if !delivered {
            spoolPendingEnqueue(row, collection: collection, app: app)
        }
    }

    /// The file IS the task: a write that fails is work that will never run, and must be said.
    private static func storeOrReport<Row: OutboxMailboxRow>(_ row: Row, collection: String, app: Application) {
        do {
            try store(row, collection: collection)
        } catch {
            app.logger.error(
                "Could not file \(Row.self) task \(row.id) in '\(collection)' - the queued work is lost: \(error)")
        }
    }

    private static func spoolPendingEnqueue<Row: OutboxMailboxRow>(_ row: Row, collection: String, app: Application) {
        let path = OutboxMailboxFileHandler.taskPath(
            root: OutboxMailboxFileHandler.pendingEnqueueRootPath, collection: collection,
            ownerNodeId: row.ownerNodeId, taskId: row.id)
        do {
            try OutboxMailboxFileHandler.write(path: path, data: try JSONEncoder().encode(row))
        } catch {
            app.logger.error(
                "Could not spool \(Row.self) task \(row.id) for node \(row.ownerNodeId) - the queued work is lost: \(error)")
        }
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
                let row = decodeOrDiscard(Row.self, data: data, path: spoolPath, app: app)
            else {
                OutboxMailboxFileHandler.remove(path: spoolPath)
                continue
            }

            if row.ownerNodeId == selfId {
                storeOrReport(row, collection: collection, app: app)
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
            var outbound = HTTPClientRequest.internalCluster(
                url: node.address + "/internal/cluster/outbox/enqueue" + querySuffix(collection: collection),
                method: .POST, secret: secret)
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
        try store(row, collection: collection)
    }

    /// Files a newly enqueued row. A row whose id is derived from its identity
    /// (`ErasureCodedReplicationTask`) may already be here: a pending one is left as it is, a
    /// dead-lettered one is revived in place, so re-requesting the same work never stacks
    /// duplicates and needs no look around the cluster first.
    static func store<Row: OutboxMailboxRow>(_ row: Row, collection: String) throws {
        if let existing = readOwned(
            Row.self, collection: collection, ownerNodeId: row.ownerNodeId, taskId: row.id)
        {
            guard existing.state == .failed else { return }
            existing.state = .pending
            existing.attempts = 0
            existing.nextAttemptAt = Date()
            existing.lastError = nil
            try update(existing, collection: collection)
            return
        }
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

        let candidates = await adoptionElectorate()
        guard candidates.contains(where: { $0.id != selfId }) else { return }

        let ids = OutboxMailboxFileHandler.listTaskIds(
            root: OutboxMailboxFileHandler.rootPath, collection: collection, ownerNodeId: selfId)
        guard !ids.isEmpty else { return }

        for id in ids {
            let path = OutboxMailboxFileHandler.taskPath(
                root: OutboxMailboxFileHandler.rootPath, collection: collection, ownerNodeId: selfId,
                taskId: id)
            guard let data = OutboxMailboxFileHandler.read(path: path) else { continue }
            let targets = mirrorTargets(
                collection: collection, ownerNodeId: selfId, taskId: id, candidates: candidates,
                replicas: replicas)
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
            var outbound = HTTPClientRequest.internalCluster(
                url: node.address + "/internal/cluster/outbox/backup" + querySuffix(collection: collection, ownerNodeId: ownerNodeId, taskId: taskId),
                method: .POST, secret: secret)
            outbound.headers.replaceOrAdd(name: .contentType, value: "application/json")
            outbound.body = .bytes(ByteBuffer(data: data))
            _ = try await LightweightClusterControlClient.shared.execute(
                outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
        } catch {
            // Best-effort - the next sweep re-tries automatically.
        }
    }

    /// The peers that hold `taskId`'s mirrors: the top `replicas - 1` of the same HRW ranking
    /// `electAdopter` uses, so whoever wins the adoption election is one of them. Any other
    /// choice (e.g. "the first few active peers") elects a non-holder in a cluster of three or
    /// more, and the row then sits on its mirrors forever.
    static func mirrorTargets(
        collection: String, ownerNodeId: UUID, taskId: UUID, candidates: [ClusterNodeInfo], replicas: Int
    ) -> [ClusterNodeInfo] {
        PlacementService.responsibleNodes(
            bucketName: outboxPlacementBucket,
            key: adoptionKey(collection: collection, ownerNodeId: ownerNodeId, taskId: taskId),
            activeNodes: candidates.filter { $0.id != ownerNodeId }, count: replicas - 1)
    }

    /// The one node that adopts `taskId` once its owner is gone. The owner is never a candidate:
    /// it is gone at election time, and ranking as if it were present would shift the winner away
    /// from the mirror holders chosen when it was still alive.
    static func electAdopter(
        collection: String, ownerNodeId: UUID, taskId: UUID, eligible: [ClusterNodeInfo]
    ) -> ClusterNodeInfo? {
        PlacementService.responsibleNodes(
            bucketName: outboxPlacementBucket,
            key: adoptionKey(collection: collection, ownerNodeId: ownerNodeId, taskId: taskId),
            activeNodes: eligible.filter { $0.id != ownerNodeId }, count: 1
        ).first
    }

    private static func adoptionKey(collection: String, ownerNodeId: UUID, taskId: UUID) -> String {
        "\(collection)/\(ownerNodeId)/\(taskId)"
    }

    /// Nodes that are both placement members and reachable right now - the set both the mirror
    /// targets and the adoption election are ranked over. Every node computes it from the same
    /// two views, so the ranking is unanimous.
    static func adoptionElectorate() async -> [ClusterNodeInfo] {
        let activeIds = Set(await ClusterNodeCache.shared.activeNodes().map(\.id))
        return await ClusterNodeCache.shared.placementNodes().filter { activeIds.contains($0.id) }
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

    /// Drops this node's mirrors of rows their owner no longer has - delivered or purged ones.
    /// `remove` only reaches the owner's own disk, so without this every mirror lived forever and
    /// was adopted wholesale the moment its owner looked gone (every webhook ever queued, again).
    /// An owner that cannot be asked right now is left alone; the next sweep asks again.
    static func pruneDeliveredBackups<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String
    ) async {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return }
        let owners = Set(
            OutboxMailboxFileHandler.listAllOwnerTaskIds(
                root: OutboxMailboxFileHandler.backupRootPath, collection: collection
            ).map(\.ownerNodeId)
        ).subtracting([config.nodeId])
        guard !owners.isEmpty else { return }
        let reachable = await ClusterNodeCache.shared.activeNodes()
        for owner in reachable where owners.contains(owner.id) {
            guard
                let live = await fetchRemoteRows(
                    Row.self, app: app, node: owner, secret: config.secret, collection: collection)
            else { continue }
            pruneBackups(collection: collection, ownerNodeId: owner.id, keeping: Set(live.map(\.id)))
        }
    }

    /// The local half of `pruneDeliveredBackups`: every mirror held for `ownerNodeId` whose id
    /// is not in `live` goes. Returns how many were removed.
    @discardableResult
    static func pruneBackups(collection: String, ownerNodeId: UUID, keeping live: Set<UUID>) -> Int {
        var removed = 0
        let held = OutboxMailboxFileHandler.listTaskIds(
            root: OutboxMailboxFileHandler.backupRootPath, collection: collection, ownerNodeId: ownerNodeId)
        for taskId in held where !live.contains(taskId) {
            OutboxMailboxFileHandler.remove(
                path: OutboxMailboxFileHandler.taskPath(
                    root: OutboxMailboxFileHandler.backupRootPath, collection: collection,
                    ownerNodeId: ownerNodeId, taskId: taskId))
            removed += 1
        }
        return removed
    }

    /// How long an owner may be silent before its mirrors are adopted. Far longer than
    /// `heartbeatStaleness`: a restart or a goodbye is routine, and the owner still holds its
    /// rows and delivers them itself once back - adopting them then runs each twice.
    static let adoptionGrace: TimeInterval = 10 * 60

    /// Whether `owner`'s mirrors should be adopted: it left membership altogether, or has been
    /// silent past `adoptionGrace`. Status never decides - a draining node is alive and hands
    /// its rows over itself (`reassignOwnedTasks`).
    static func ownerIsGone(_ owner: ClusterNodeInfo?, now: Date = Date()) -> Bool {
        guard let owner else { return true }
        return now.timeIntervalSince(owner.lastHeartbeatAt) > adoptionGrace
    }

    /// Walks this node's own held backup entries in `collection` and self-promotes any whose
    /// owner has dropped out of active membership - moves the file from `outbox-backup/` into
    /// this node's own `outbox/<collection>/<selfId>/`, re-owning it. A deterministic HRW
    /// tie-break (via `PlacementService`) among the other backup-holders ensures at most one
    /// promotes any given task, even though every holder runs this sweep independently.
    @discardableResult
    static func promoteOrphanedBackups<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, collection: String
    ) async -> Int {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return 0 }
        let selfId = config.nodeId
        // `ownerIsGone` decides WHETHER to promote; stable placement decides WHO
        // promotes. Electing from the live set would let two nodes with slightly different
        // heartbeat views each elect themselves and both promote the same task - a webhook
        // delivered twice, a replication task run twice. `placementNodes()` is the same on every
        // node, so the election is unanimous; self must still be in it to be eligible, and the
        // eligible set is also what `mirrorTargets` ranked over, so the winner holds a mirror.
        let eligible = await adoptionElectorate()

        let backupEntries = OutboxMailboxFileHandler.listAllOwnerTaskIds(
            root: OutboxMailboxFileHandler.backupRootPath, collection: collection)
        guard !backupEntries.isEmpty else { return 0 }

        var promoted = 0
        for (ownerNodeId, taskId) in backupEntries {
            guard ownerIsGone(await ClusterNodeCache.shared.get(id: ownerNodeId)) else { continue }

            let winner = electAdopter(
                collection: collection, ownerNodeId: ownerNodeId, taskId: taskId, eligible: eligible)
            guard winner?.id == selfId else { continue }

            let backupPath = OutboxMailboxFileHandler.taskPath(
                root: OutboxMailboxFileHandler.backupRootPath, collection: collection,
                ownerNodeId: ownerNodeId, taskId: taskId)
            guard let data = OutboxMailboxFileHandler.read(path: backupPath),
                let row = decodeOrDiscard(Row.self, data: data, path: backupPath, app: app)
            else {
                OutboxMailboxFileHandler.remove(path: backupPath)
                continue
            }

            row.ownerNodeId = selfId
            do {
                try update(row, collection: collection)
            } catch {
                app.logger.warning(
                    "Could not adopt outbox task \(taskId) of departed node \(ownerNodeId): \(error)")
                continue
            }
            OutboxMailboxFileHandler.remove(path: backupPath)
            promoted += 1
        }
        if promoted > 0 {
            app.logger.notice(
                "Adopted \(promoted) queued \(collection) task(s) whose owner left the cluster.")
        }
        return promoted
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
                let row = decodeOrDiscard(Row.self, data: data, path: path, app: app)
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
        await fetchRemoteRows(Row.self, app: app, node: node, secret: secret, collection: collection) ?? []
    }

    /// `node`'s own owned rows; nil when it could not be asked, which callers that act on an
    /// absence (pruning) must tell apart from "owns nothing".
    private static func fetchRemoteRows<Row: OutboxMailboxRow>(
        _ type: Row.Type, app: Application, node: ClusterNodeInfo, secret: String, collection: String
    ) async -> [Row]? {
        do {
            var outbound = HTTPClientRequest.internalCluster(
                url: node.address + "/internal/cluster/outbox/list" + querySuffix(collection: collection),
                method: .GET, secret: secret)
            let response = try await LightweightClusterControlClient.shared.execute(
                outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
            guard response.status == .ok else { return nil }
            let body = try await response.body.collect(upTo: 64 * 1024 * 1024)
            return try decoder.decode([Row].self, from: Data(buffer: body))
        } catch {
            return nil
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
            var outbound = HTTPClientRequest.internalCluster(
                url: node.address + "/internal/cluster/outbox/retry" + querySuffix(collection: collection, taskId: taskId),
                method: .POST, secret: secret)
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
        var outbound = HTTPClientRequest.internalCluster(
            url: node.address + "/internal/cluster/outbox/purge-bucket" + querySuffix(collection: collection, bucketName: bucketName),
            method: .POST, secret: secret)
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
        var outbound = HTTPClientRequest.internalCluster(
            url: node.address + "/internal/cluster/outbox/purge-target" + querySuffix(collection: collection, targetNodeId: targetNodeId),
            method: .POST, secret: secret)
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
