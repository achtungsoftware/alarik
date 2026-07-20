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

/// The internal-only receiving side of every `OutboxMailbox` cross-node operation - never
/// reachable by S3 clients, guarded entirely by `ClusterSecretMiddleware`. `collection` is a
/// runtime string (one of `OutboxCollections`'s 4 constants), so every handler here switches on
/// it to know which concrete `OutboxMailboxRow` type to decode into - the one place in the
/// mailbox subsystem that has to know about all 4 row types by name.
struct InternalClusterOutboxController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        let cluster = routes.grouped("internal", "cluster", "outbox").grouped(ClusterSecretMiddleware())
        cluster.on(.POST, "enqueue", body: .collect(maxSize: "1mb"), use: handleEnqueue)
        cluster.on(.POST, "backup", body: .collect(maxSize: "1mb"), use: handleBackup)
        cluster.get("list", use: handleList)
        cluster.on(.POST, "retry", use: handleRetry)
        cluster.on(.POST, "purge-bucket", use: handlePurgeBucket)
        cluster.on(.POST, "purge-target", use: handlePurgeTarget)
    }

    private func collection(req: Request) throws -> String {
        guard let collection = req.query[String.self, at: "collection"] else {
            throw Abort(.badRequest, reason: "Missing collection query parameter")
        }
        return collection
    }

    @Sendable
    func handleEnqueue(req: Request) async throws -> HTTPStatus {
        let collection = try collection(req: req)
        guard let bodyBuffer = req.body.data else {
            throw Abort(.badRequest, reason: "Missing request body")
        }
        let data = Data(buffer: bodyBuffer)

        switch collection {
        case OutboxCollections.notificationDeliveries:
            try OutboxMailbox.receiveEnqueue(
                try JSONDecoder().decode(NotificationDelivery.self, from: data), collection: collection)
        case OutboxCollections.replicationTasks:
            try OutboxMailbox.receiveEnqueue(
                try JSONDecoder().decode(ReplicationTask.self, from: data), collection: collection)
        case OutboxCollections.clusterReplicationTasks:
            try OutboxMailbox.receiveEnqueue(
                try JSONDecoder().decode(ClusterReplicationTask.self, from: data), collection: collection)
        case OutboxCollections.erasureCodedReplicationTasks:
            try OutboxMailbox.receiveEnqueue(
                try JSONDecoder().decode(ErasureCodedReplicationTask.self, from: data),
                collection: collection)
        default:
            throw Abort(.badRequest, reason: "Unknown outbox collection '\(collection)'")
        }
        NotificationDispatcher.shared.wake()
        ReplicationDispatcher.shared.wake()
        ClusterReplicationDispatcher.shared.wake()
        ErasureCodedDispatcher.shared.wake()
        return .ok
    }

    @Sendable
    func handleBackup(req: Request) async throws -> HTTPStatus {
        let collection = try collection(req: req)
        guard
            let ownerNodeId = req.query[String.self, at: "ownerNodeId"].flatMap({ UUID(uuidString: $0) }),
            let taskId = req.query[String.self, at: "taskId"].flatMap({ UUID(uuidString: $0) })
        else {
            throw Abort(.badRequest, reason: "Missing/invalid ownerNodeId or taskId")
        }
        guard let bodyBuffer = req.body.data else {
            throw Abort(.badRequest, reason: "Missing request body")
        }
        try OutboxMailbox.receiveBackup(
            data: Data(buffer: bodyBuffer), collection: collection, ownerNodeId: ownerNodeId,
            taskId: taskId)
        return .ok
    }

    @Sendable
    func handleList(req: Request) async throws -> Response {
        let collection = try collection(req: req)
        let data: Data
        switch collection {
        case OutboxCollections.notificationDeliveries:
            data = try JSONEncoder().encode(
                OutboxMailbox.allOwnedTasks(NotificationDelivery.self, app: req.application, collection: collection))
        case OutboxCollections.replicationTasks:
            data = try JSONEncoder().encode(
                OutboxMailbox.allOwnedTasks(ReplicationTask.self, app: req.application, collection: collection))
        case OutboxCollections.clusterReplicationTasks:
            data = try JSONEncoder().encode(
                OutboxMailbox.allOwnedTasks(ClusterReplicationTask.self, app: req.application, collection: collection))
        case OutboxCollections.erasureCodedReplicationTasks:
            data = try JSONEncoder().encode(
                OutboxMailbox.allOwnedTasks(ErasureCodedReplicationTask.self, app: req.application, collection: collection))
        default:
            throw Abort(.badRequest, reason: "Unknown outbox collection '\(collection)'")
        }
        let response = Response(status: .ok)
        response.headers.replaceOrAdd(name: .contentType, value: "application/json")
        response.body = Response.Body(data: data)
        return response
    }

    @Sendable
    func handleRetry(req: Request) async throws -> HTTPStatus {
        let collection = try collection(req: req)
        guard let taskId = req.query[String.self, at: "taskId"].flatMap({ UUID(uuidString: $0) }) else {
            throw Abort(.badRequest, reason: "Missing/invalid taskId")
        }

        let retried: Bool
        switch collection {
        case OutboxCollections.notificationDeliveries:
            retried =
                OutboxMailbox.retryOwned(
                    NotificationDelivery.self, app: req.application, collection: collection,
                    taskId: taskId, failedStateValue: NotificationDelivery.State.failed.rawValue) != nil
            if retried { NotificationDispatcher.shared.wake() }
        case OutboxCollections.replicationTasks:
            retried =
                OutboxMailbox.retryOwned(
                    ReplicationTask.self, app: req.application, collection: collection, taskId: taskId,
                    failedStateValue: ReplicationTask.State.failed.rawValue) != nil
            if retried { ReplicationDispatcher.shared.wake() }
        case OutboxCollections.clusterReplicationTasks:
            retried =
                OutboxMailbox.retryOwned(
                    ClusterReplicationTask.self, app: req.application, collection: collection,
                    taskId: taskId, failedStateValue: ClusterReplicationTask.State.failed.rawValue) != nil
            if retried { ClusterReplicationDispatcher.shared.wake() }
        case OutboxCollections.erasureCodedReplicationTasks:
            retried =
                OutboxMailbox.retryOwned(
                    ErasureCodedReplicationTask.self, app: req.application, collection: collection,
                    taskId: taskId,
                    failedStateValue: ErasureCodedReplicationTask.State.failed.rawValue) != nil
            if retried { ErasureCodedDispatcher.shared.wake() }
        default:
            throw Abort(.badRequest, reason: "Unknown outbox collection '\(collection)'")
        }
        return retried ? .ok : .notFound
    }

    @Sendable
    func handlePurgeBucket(req: Request) async throws -> HTTPStatus {
        let collection = try collection(req: req)
        guard let bucketName = req.query[String.self, at: "bucketName"] else {
            throw Abort(.badRequest, reason: "Missing bucketName query parameter")
        }

        switch collection {
        case OutboxCollections.notificationDeliveries:
            OutboxMailbox.removeOwned(NotificationDelivery.self, app: req.application, collection: collection) {
                $0.bucketName == bucketName
            }
        case OutboxCollections.replicationTasks:
            OutboxMailbox.removeOwned(ReplicationTask.self, app: req.application, collection: collection) {
                $0.bucketName == bucketName
            }
        case OutboxCollections.clusterReplicationTasks:
            OutboxMailbox.removeOwned(ClusterReplicationTask.self, app: req.application, collection: collection) {
                $0.bucketName == bucketName
            }
        case OutboxCollections.erasureCodedReplicationTasks:
            OutboxMailbox.removeOwned(ErasureCodedReplicationTask.self, app: req.application, collection: collection) {
                $0.bucketName == bucketName
            }
        default:
            throw Abort(.badRequest, reason: "Unknown outbox collection '\(collection)'")
        }
        return .ok
    }

    /// The receiving side of `OutboxMailbox.purgeByTargetNodeAcrossCluster` - used by
    /// `InternalClusterController.drainNode` to clean up stale copy/repair tasks aimed at a node
    /// that's leaving. Only meaningful for the two collections with a `targetNodeId` distinct
    /// from the mailbox owner; the predicate matches `drainNode`'s original semantics exactly
    /// (cluster-replication reclaim tasks are deliberately preserved - a draining node still runs
    /// its own process and still needs to clean up its own now-unowned copies).
    @Sendable
    func handlePurgeTarget(req: Request) async throws -> HTTPStatus {
        let collection = try collection(req: req)
        guard
            let targetNodeId = req.query[String.self, at: "targetNodeId"].flatMap({
                UUID(uuidString: $0)
            })
        else {
            throw Abort(.badRequest, reason: "Missing/invalid targetNodeId query parameter")
        }

        switch collection {
        case OutboxCollections.clusterReplicationTasks:
            OutboxMailbox.removeOwned(ClusterReplicationTask.self, app: req.application, collection: collection) {
                $0.targetNodeId == targetNodeId
                    && $0.reason != ClusterReplicationTask.Reason.reclaim.rawValue
            }
        case OutboxCollections.erasureCodedReplicationTasks:
            OutboxMailbox.removeOwned(ErasureCodedReplicationTask.self, app: req.application, collection: collection) {
                $0.targetNodeId == targetNodeId
            }
        default:
            throw Abort(.badRequest, reason: "Unsupported outbox collection '\(collection)' for purge-target")
        }
        return .ok
    }
}
