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

#if canImport(Darwin)
    import Darwin
#elseif canImport(Glibc)
    import Glibc
#elseif canImport(Musl)
    import Musl
#endif

import Fluent
import Foundation
import Vapor

/// The internal-only receiving side of `ClusterReplicationClient.pushObject`/`deleteObject`
/// never reachable by S3 clients, guarded entirely by
/// `ClusterSecretMiddleware`. Every request here is a peer node replicating one object write or
/// delete, triggered by `ClusterReplicationService`'s quorum fan-out or
/// `ClusterReplicationDispatcher`'s outbox drain.
struct InternalClusterObjectController: RouteCollection {
    private static let objectMetaHeaderName = "X-Alarik-Object-Meta"

    func boot(routes: any RoutesBuilder) throws {
        let cluster = routes.grouped("internal", "cluster", "objects").grouped(
            ClusterSecretMiddleware())
        cluster.on(.POST, "push", body: .stream, use: handlePush)
        cluster.delete(use: handleDelete)
        cluster.get("fetch", use: handleFetch)
        cluster.get("exists", use: handleExists)
    }

    /// Header-only existence probe (the read-side counterpart to `handleFetch`) - resolves the
    /// object's on-disk path and answers `200`/`404` without opening or streaming any payload.
    /// A live delete marker counts as "not found", matching what a client GET would see.
    @Sendable
    func handleExists(req: Request) async throws -> HTTPStatus {
        guard
            let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing bucket/key query parameters")
        }
        let versionId = req.query[String.self, at: "versionId"]

        guard
            let path = try ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: versionId),
            let (meta, _, _) = try? ObjectFileHandler.payloadLocation(path: path),
            !meta.isDeleteMarker
        else {
            return .notFound
        }
        return .ok
    }

    /// The read-mirror of `handlePush` - streams this node's local copy of an object back to a
    /// peer that isn't itself responsible for it. Used by CopyObject/UploadPartCopy when the
    /// copy *source* lives on a different node than the one handling the request (which is
    /// always a node responsible for the *destination* key - source and destination can be
    /// arbitrary buckets/keys with no placement relationship to each other). The full
    /// `ObjectMeta` travels as the same base64-encoded JSON header `handlePush` reads, so the
    /// caller gets everything it needs (tags, isLatest, isDeleteMarker included) in one round
    /// trip.
    @Sendable
    func handleFetch(req: Request) async throws -> Response {
        guard
            let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing bucket/key query parameters")
        }
        let versionId = req.query[String.self, at: "versionId"]

        guard
            let path = try ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: versionId)
        else {
            throw Abort(.notFound, reason: "Object not found")
        }
        let (meta, _, payloadSize) = try ObjectFileHandler.payloadLocation(path: path)

        let metaJSON = try JSONEncoder().encode(meta)
        let response = Response(status: .ok)
        response.headers.replaceOrAdd(
            name: Self.objectMetaHeaderName, value: Data(metaJSON).base64EncodedString())
        response.headers.replaceOrAdd(name: .contentType, value: "application/octet-stream")

        let threadPool = req.application.threadPool
        response.body = Response.Body(
            managedAsyncStream: { writer in
                let snapshot = try await threadPool.runIfActive {
                    try ObjectFileHandler.openPayloadSnapshot(path: path)
                }
                let fd = snapshot.fd
                do {
                    try await StreamingIOLoops.readWindowed(
                        threadPool: threadPool, fd: fd, offset: snapshot.payloadOffset,
                        length: snapshot.payloadSize, chunkSize: Constants.streamingReadChunkSize
                    ) { chunk in
                        try await writer.writeBuffer(chunk)
                    }
                    _ = POSIXFile.close(fd)
                } catch is IOLoopError {
                    _ = POSIXFile.close(fd)
                    throw Abort(.internalServerError, reason: "Object payload ended early")
                } catch {
                    _ = POSIXFile.close(fd)
                    throw error
                }
            }, count: payloadSize)

        return response
    }

    /// Writes the exact version a peer sends - never mints a new version id, since replicas
    /// must agree on the id for the same write. The bucket's versioning status is read from
    /// this node's own `BucketVersioningCache`, not re-derived from the pushed metadata - that
    /// cache is already guaranteed consistent cluster-wide, so both nodes compute the identical
    /// on-disk path convention independently.
    @Sendable
    func handlePush(req: Request) async throws -> HTTPStatus {
        guard
            let metaHeader = req.headers.first(name: Self.objectMetaHeaderName),
            let metaData = Data(base64Encoded: metaHeader),
            let meta = try? JSONDecoder().decode(ObjectMeta.self, from: metaData)
        else {
            throw Abort(.badRequest, reason: "Missing or invalid \(Self.objectMetaHeaderName) header")
        }

        let threadPool = req.application.threadPool
        let spoolPath = Constants.spoolDirectory + ".cluster-push-" + UUID().uuidString

        let fd = try await threadPool.runIfActive { () -> Int32 in
            var fd = POSIXFile.openWrite(spoolPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            if fd < 0 && errno == ENOENT {
                try FileManager.default.createDirectory(
                    atPath: Constants.spoolDirectory, withIntermediateDirectories: true)
                fd = POSIXFile.openWrite(spoolPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            }
            guard fd >= 0 else {
                throw Abort(.internalServerError, reason: "Failed to open cluster push spool file")
            }
            return fd
        }

        var size = 0
        do {
            for try await buffer in req.body {
                let chunk = buffer
                size += chunk.readableBytes
                try await threadPool.runIfActive {
                    try chunk.withUnsafeReadableBytes { raw in
                        do {
                            try StreamingIOLoops.writeFully(fd: fd, raw)
                        } catch {
                            throw Abort(
                                .internalServerError,
                                reason: "Failed writing cluster push spool file")
                        }
                    }
                }
            }
            _ = try await threadPool.runIfActive { POSIXFile.close(fd) }
        } catch {
            _ = try? await threadPool.runIfActive { POSIXFile.close(fd) }
            _ = POSIXFile.unlink(spoolPath)
            throw error
        }

        do {
            let versioningStatus = await BucketVersioningCache.shared.getStatus(
                for: meta.bucketName)
            let path: String
            switch versioningStatus {
            case .disabled:
                path = ObjectFileHandler.storagePath(for: meta.bucketName, key: meta.key)
            case .enabled, .suspended:
                let versionId = meta.versionId ?? "null"
                path = ObjectFileHandler.versionedPath(
                    for: meta.bucketName, key: meta.key, versionId: versionId)
                if meta.isLatest {
                    try ObjectFileHandler.markAllVersionsNotLatest(
                        bucketName: meta.bucketName, key: meta.key)
                }
            }

            let payloadSize = size
            try await threadPool.runIfActive {
                try ObjectFileHandler.writeStreamed(
                    metadata: meta, payloadFile: spoolPath, payloadOffset: 0,
                    payloadSize: payloadSize, to: path)
            }

            if versioningStatus != .disabled, meta.isLatest, let versionId = meta.versionId {
                try ObjectFileHandler.updateLatestPointer(
                    bucketName: meta.bucketName, key: meta.key, versionId: versionId)
            }
        } catch {
            _ = POSIXFile.unlink(spoolPath)
            throw error
        }

        _ = POSIXFile.unlink(spoolPath)
        return .ok
    }

    /// Two modes, both returning the resulting `ObjectDeleteOutcome` as a JSON body:
    ///
    /// - Default: a dumb, single-copy delete via `S3Service.deleteObject` directly - used for the
    ///   outbox dispatcher's plain per-peer catch-up delivery and for pruning one specific
    ///   historical version. Never mints a new version id, so there's nothing to keep in sync
    ///   across replicas, and never recurses into further replication - a newly-*created* delete
    ///   marker never reaches this mode, it's replicated as a `.put` instead (see
    ///   `ClusterReplicationService.coordinateDelete`) so every peer gets the exact same marker id
    ///   rather than each minting its own.
    /// - `coordinate=true`: this node acts as delegate coordinator for a key it's responsible for
    ///   but that some other node's request landed on (a Multi-Object-Delete batch key routed via
    ///   `ObjectRoutingService.coordinationTarget`) - runs the full local-delete-then-replicate-
    ///   to-peers sequence, exactly as if the client's request had reached it directly.
    @Sendable
    func handleDelete(req: Request) async throws -> Response {
        guard
            let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing bucket/key query parameters")
        }
        let versionId = req.query[String.self, at: "versionId"]
        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        let outcome: S3Service.ObjectDeleteOutcome
        if req.query[Bool.self, at: "coordinate"] == true {
            let (_, peers, _) = await ObjectRoutingService.coordinationTarget(
                req: req, bucketName: bucketName, key: key)
            outcome = try await ClusterReplicationService.coordinateDelete(
                app: req.application, bucketName: bucketName, key: key, versionId: versionId,
                versioningStatus: versioningStatus, peers: peers)
        } else {
            outcome = try S3Service.deleteObject(
                bucketName: bucketName, key: key, versionId: versionId,
                versioningStatus: versioningStatus)
        }

        let response = Response(status: .ok)
        response.headers.replaceOrAdd(name: .contentType, value: "application/json")
        response.body = try Response.Body(data: JSONEncoder().encode(outcome))
        return response
    }
}
