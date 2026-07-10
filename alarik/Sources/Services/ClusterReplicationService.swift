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

import Fluent
import Foundation
import Vapor

import struct Foundation.UUID

/// Quorum write coordination for object-data clustering. Called by the coordinator node -
/// whichever node is actually responsible for this object, whether it received the client
/// request directly or via `ObjectRoutingService`'s forward - immediately after its own local
/// write (`ObjectFileHandler`) already succeeded. Fans out to the other responsible nodes
/// concurrently, bounded by `synchronousTimeout` each, and returns as soon as a quorum of total
/// acks (local write + peer acks) is reached - the caller only tells the client "success" after
/// this returns. Every peer that didn't confirm in time (slow, or genuinely down) still gets a
/// durable outbox task, so quorum-based success never permanently strands a replica behind -
/// mirrors `ReplicationService`'s synchronous-attempt-then-async-fallback shape exactly.
enum ClusterReplicationService {
    /// Bounded shorter than `ReplicationService.synchronousTimeout` (20s, for external
    /// targets) - inter-node traffic is expected to be low-latency (same private network), so a
    /// slow peer should fall back to the async outbox sooner rather than holding a client
    /// request open.
    static let synchronousTimeout: Duration = .seconds(10)

    static func replicateWrite(
        app: Application,
        bucketName: String,
        key: String,
        versionId: String?,
        operation: ClusterReplicationTask.Operation,
        peers: [ClusterNodeInfo]
    ) async {
        guard !peers.isEmpty else { return }

        // -1: the coordinator's own local write (already done before this is called) counts as
        // one ack toward quorum, so only `quorumRemaining` more are needed from `peers`.
        let quorumRemaining = PlacementService.quorumThreshold(replicaCount: peers.count + 1) - 1
        guard quorumRemaining > 0 else {
            // The local write alone already satisfies quorum (e.g. a 1-replica-effective
            // cluster) - still fan out for eventual consistency, but never block the client on
            // it.
            await enqueueOutbox(app: app, nodes: peers, bucketName: bucketName, key: key,
                versionId: versionId, operation: operation)
            return
        }

        var delivered: Set<UUID> = []
        await withTaskGroup(of: (node: ClusterNodeInfo, delivered: Bool).self) { group in
            for node in peers {
                group.addTask {
                    let ok = await attemptImmediateDelivery(
                        app: app, node: node, bucketName: bucketName, key: key,
                        versionId: versionId, operation: operation)
                    return (node, ok)
                }
            }
            for await outcome in group {
                if outcome.delivered { delivered.insert(outcome.node.id) }
                if delivered.count >= quorumRemaining { break }
            }
            // Exiting here while some child tasks may still be in flight is safe: without an
            // explicit `group.cancelAll()`, Swift's TaskGroup does NOT cancel stragglers when
            // this closure returns - it only awaits them (structured concurrency guarantees no
            // child outlives the group), letting each straggler's push run to completion in the
            // background. Their eventual outcome doesn't matter either way - any node not
            // captured in `delivered` unconditionally gets an outbox task below, so a straggler
            // that actually succeeds after we stopped listening just means one harmless
            // redundant push later (the receiving side always writes the exact same version, so
            // this is idempotent).
        }

        let undelivered = peers.filter { !delivered.contains($0.id) }
        guard !undelivered.isEmpty else { return }
        await enqueueOutbox(
            app: app, nodes: undelivered, bucketName: bucketName, key: key, versionId: versionId,
            operation: operation)
    }

    /// Runs a local delete then replicates the outcome to `peers` - the sequence
    /// `S3Controller.handleObjectDelete` uses for a client-facing DELETE, and also what a node
    /// runs when it's asked to act as delegate coordinator for a key inside a Multi-Object-Delete
    /// batch that some other node fielded (see `InternalClusterObjectController.handleDelete`'s
    /// `coordinate` mode). Branches on whether the local delete created a delete marker or
    /// removed bytes outright: a freshly minted marker replicates as `.put`, so every peer writes
    /// the exact same marker id this node just minted rather than each minting its own; a true
    /// byte-removal replicates as `.delete`.
    static func coordinateDelete(
        app: Application, bucketName: String, key: String, versionId: String?,
        versioningStatus: VersioningStatus, peers: [ClusterNodeInfo]
    ) async throws -> S3Service.ObjectDeleteOutcome {
        let outcome = try await S3Service.offloadBlockingIO(app) {
            try S3Service.deleteObject(
                bucketName: bucketName, key: key, versionId: versionId,
                versioningStatus: versioningStatus)
        }
        if outcome.isDeleteMarker {
            await replicateWrite(
                app: app, bucketName: bucketName, key: key, versionId: outcome.versionId,
                operation: .put, peers: peers)
        } else {
            await replicateWrite(
                app: app, bucketName: bucketName, key: key, versionId: versionId,
                operation: .delete, peers: peers)
        }
        return outcome
    }

    /// Pushes an object this node just wrote locally out to every node that's actually
    /// responsible for it, for the rare write path that can't determine its destination key (and
    /// therefore routing) until *after* the request body is already consumed - the admin
    /// console's upload endpoint can't know the key until it's decoded the multipart form body
    /// looking for the filename, by which point forwarding the original request is no longer
    /// possible. Unlike `replicateWrite`, this node's local write does NOT count toward quorum
    /// (it isn't one of `responsible`'s real replicas), so every one of `responsible` needs its
    /// own ack, not `responsible.count - 1`.
    ///
    /// Returns `true` once a quorum of `responsible` nodes durably hold the exact version
    /// (synchronously delivered, with any remainder guaranteed by the outbox) - the signal the
    /// caller uses to decide it's now safe to reclaim its own stray local copy, since the object
    /// is durable on the nodes that actually own it. Returns `false` when quorum couldn't be
    /// reached synchronously (peers slow/down); the caller must then keep its local copy as the
    /// durability backstop until the outbox catches the responsible nodes up.
    @discardableResult
    static func pushToResponsibleNodes(
        app: Application, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo]
    ) async -> Bool {
        guard !responsible.isEmpty else { return false }
        let quorum = PlacementService.quorumThreshold(replicaCount: responsible.count)

        var delivered: Set<UUID> = []
        await withTaskGroup(of: (node: ClusterNodeInfo, delivered: Bool).self) { group in
            for node in responsible {
                group.addTask {
                    let ok = await attemptImmediateDelivery(
                        app: app, node: node, bucketName: bucketName, key: key,
                        versionId: versionId, operation: .put)
                    return (node, ok)
                }
            }
            for await outcome in group {
                if outcome.delivered { delivered.insert(outcome.node.id) }
                if delivered.count >= quorum { break }
            }
        }

        let undelivered = responsible.filter { !delivered.contains($0.id) }
        if !undelivered.isEmpty {
            await enqueueOutbox(
                app: app, nodes: undelivered, bucketName: bucketName, key: key,
                versionId: versionId, operation: .put)
        }
        return delivered.count >= quorum
    }

    /// Deletes `key` correctly regardless of whether this node is one of its responsible nodes -
    /// coordinates locally via `coordinateDelete` when it is, otherwise delegates to one of the
    /// actual responsible peers over the internal, secret-only replication protocol (trying each
    /// in turn until one succeeds). This is the shared logic behind both
    /// `S3Controller.handleDeleteObjects`'s per-key Multi-Object-Delete routing and the admin
    /// console's folder/prefix delete - any handler that already authenticated the caller once
    /// for a whole batch/request and needs to delete a key it may not itself be responsible for,
    /// without requiring a second per-key client signature.
    static func deleteObjectClusterWide(
        req: Request, bucketName: String, key: String, versionId: String?,
        versioningStatus: VersioningStatus
    ) async throws -> S3Service.ObjectDeleteOutcome {
        let (isLocal, peers, responsible) = await ObjectRoutingService.coordinationTarget(
            req: req, bucketName: bucketName, key: key)

        if isLocal {
            return try await coordinateDelete(
                app: req.application, bucketName: bucketName, key: key, versionId: versionId,
                versioningStatus: versioningStatus, peers: peers)
        }

        var lastError: any Error = ClusterProxyError.objectNotFound
        for node in responsible {
            do {
                return try await ClusterReplicationClient.deleteObject(
                    app: req.application, to: node, bucketName: bucketName, key: key,
                    versionId: versionId, coordinate: true)
            } catch {
                lastError = error
                continue
            }
        }
        throw lastError
    }

    private static func enqueueOutbox(
        app: Application, nodes: [ClusterNodeInfo], bucketName: String, key: String,
        versionId: String?, operation: ClusterReplicationTask.Operation
    ) async {
        for node in nodes {
            let task = ClusterReplicationTask(
                bucketName: bucketName, key: key, versionId: versionId, operation: operation,
                targetNodeId: node.id, reason: .write)
            do {
                try await task.save(on: app.db)
            } catch {
                app.logger.error(
                    "Failed to enqueue cluster replication task for '\(key)' -> \(node.id): \(error)"
                )
            }
        }
        ClusterReplicationDispatcher.shared.wake()
    }

    private static func attemptImmediateDelivery(
        app: Application, node: ClusterNodeInfo, bucketName: String, key: String,
        versionId: String?, operation: ClusterReplicationTask.Operation
    ) async -> Bool {
        do {
            try await withTimeout(synchronousTimeout) {
                switch operation {
                case .put:
                    try await ClusterReplicationClient.pushObject(
                        app: app, to: node, bucketName: bucketName, key: key, versionId: versionId
                    )
                case .delete:
                    try await ClusterReplicationClient.deleteObject(
                        app: app, to: node, bucketName: bucketName, key: key,
                        versionId: versionId)
                }
            }
            return true
        } catch {
            app.logger.warning(
                "Synchronous cluster replication of '\(key)' to node \(node.id) failed or timed out - falling back to async retry: \(error)"
            )
            return false
        }
    }

    private static func withTimeout<T: Sendable>(
        _ timeout: Duration,
        operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask { try await operation() }
            group.addTask {
                try await Task.sleep(for: timeout)
                throw ClusterReplicationTimeoutError.timedOut
            }
            defer { group.cancelAll() }
            guard let result = try await group.next() else {
                throw ClusterReplicationTimeoutError.timedOut
            }
            return result
        }
    }
}

enum ClusterReplicationTimeoutError: Error, CustomStringConvertible {
    case timedOut
    var description: String { "Synchronous cluster replication attempt timed out" }
}
