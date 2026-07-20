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

import Foundation
import Vapor

/// The internal-only receiving side of `ClusterReplicationClient.pushShard`/`fetchShard`/
/// `shardExists`/`deleteShard` - the `.ecshard` sibling of `InternalClusterObjectController`,
/// never reachable by S3 clients, guarded entirely by `ClusterSecretMiddleware`. `.ecshard` files
/// are fully self-describing (header + per-stripe checksums), so push/fetch here are raw byte
/// copies - no metadata header dance like `handlePush`/`handleFetch` need for `.obj` files.
struct InternalClusterErasureCodedController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        let cluster = routes.grouped("internal", "cluster", "ecshards").grouped(
            ClusterSecretMiddleware())
        cluster.on(.POST, "push", body: .stream, use: handlePush)
        cluster.get("fetch", use: handleFetch)
        cluster.get("exists", use: handleExists)
        cluster.get("held", use: handleHeld)
        cluster.get("meta", use: handleMeta)
        cluster.get("encoding", use: handleEncoding)
        cluster.delete(use: handleDelete)
        cluster.on(.PATCH, "metadata", use: handleMetadataPatch)
        cluster.on(.POST, "restore-latest", use: handleRestoreLatest)
        cluster.on(.POST, "verify-heal", use: handleVerifyHeal)
    }

    /// Verifies the shard(s) THIS node holds for (bucket, key, version) against their on-disk
    /// checksums and heals any that are genuinely corrupt - the safe, authoritative side of
    /// read-repair for corruption. A reader that hit a checksum failure while decoding a fetched
    /// copy asks the holder to check its own copy here, so a peer's healthy shard is never deleted
    /// on the strength of what might have been transit damage.
    ///
    /// Responds 202 and verifies detached: reading every stripe of a large shard can outlast the
    /// caller's request timeout, and the caller is fire-and-forget anyway (`requestVerifyHeal`
    /// ignores the response body) - tying the verify to the request's lifetime would only risk it
    /// being cancelled halfway through for no one's benefit.
    @Sendable
    func handleVerifyHeal(req: Request) async throws -> HTTPStatus {
        guard
            let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing bucket/key query parameters")
        }
        let versionId = req.query[String.self, at: "versionId"]
        let app = req.application
        Task.detached {
            await ErasureCodedScrubber.verifyAndHealObjectShards(
                app: app, bucketName: bucketName, key: key, versionId: versionId)
        }
        return .accepted
    }

    private func bucketKey(req: Request) throws -> (bucketName: String, key: String, versionId: String?) {
        guard
            let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing bucket/key query parameters")
        }
        return (bucketName, key, req.query[String.self, at: "versionId"])
    }

    /// The shard indices this node physically holds for (bucket, key[, versionId]) - the network
    /// face of `ErasureCodedObjectHandler.locallyHeldShardIndices`. Callers (read gather,
    /// reconstruction) use it to discover where each shard *actually* lives, since a shard's index
    /// no longer implies which node holds it once HRW ranks have drifted on a membership change.
    @Sendable
    func handleHeld(req: Request) async throws -> [Int] {
        let (bucketName, key, versionId) = try bucketKey(req: req)
        return ErasureCodedObjectHandler.locallyHeldShardIndices(
            bucketName: bucketName, key: key, versionId: versionId)
    }

    /// The `ObjectMeta` from any shard this node holds for (bucket, key[, versionId]) - a
    /// header-only read, no stripe data touched. Lets a HEAD-shaped handler on a node that hasn't
    /// received its own shard yet (the fresh-write straggler window) resolve the object's metadata
    /// from a peer without downloading a whole shard. A nil versionId resolves to this node's own
    /// view of the latest version, mirroring how local metadata resolution works. 404 when this
    /// node holds nothing for it.
    @Sendable
    func handleMeta(req: Request) async throws -> Response {
        let (bucketName, key, versionId) = try bucketKey(req: req)

        let effectiveVersionId: String?
        if let versionId {
            effectiveVersionId = versionId
        } else if ObjectFileHandler.isVersioned(bucketName: bucketName, key: key) {
            effectiveVersionId = try? ObjectFileHandler.getLatestVersionId(
                bucketName: bucketName, key: key)
        } else {
            effectiveVersionId = nil
        }

        let held = ErasureCodedObjectHandler.locallyHeldShardIndices(
            bucketName: bucketName, key: key, versionId: effectiveVersionId)
        guard let index = held.first else {
            throw Abort(.notFound, reason: "No shard held for this object")
        }
        let path = ErasureCodedObjectHandler.shardPath(
            bucketName: bucketName, key: key, versionId: effectiveVersionId, shardIndex: index)
        let meta = try await req.application.threadPool.runIfActive {
            let reader = try ErasureCodedShardReader(path: path)
            defer { reader.close() }
            return reader.header.objectMeta
        }

        let response = Response(status: .ok)
        response.headers.replaceOrAdd(name: .contentType, value: "application/json")
        response.body = try Response.Body(data: JSONEncoder().encode(meta))
        return response
    }

    /// The `(dataShards, parityShards)` this node's held shard was actually encoded with, read
    /// straight from its on-disk header - never recomputed from live cluster state. Metadata
    /// records (`MetadataStore`) can be written under a smaller effective k/m than a later read
    /// would otherwise assume (e.g. the very first write on a cluster's founding node, before any
    /// peers are known), so a read must discover the encoding actually used rather than guess from
    /// current membership size. 404 when this node holds nothing for it.
    @Sendable
    func handleEncoding(req: Request) async throws -> EncodingDTO {
        let (bucketName, key, versionId) = try bucketKey(req: req)
        let held = ErasureCodedObjectHandler.locallyHeldShardIndices(
            bucketName: bucketName, key: key, versionId: versionId)
        guard let index = held.first else {
            throw Abort(.notFound, reason: "No shard held for this object")
        }
        let path = ErasureCodedObjectHandler.shardPath(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: index)
        return try await req.application.threadPool.runIfActive {
            let reader = try ErasureCodedShardReader(path: path)
            defer { reader.close() }
            return EncodingDTO(
                dataShards: reader.header.dataShards, parityShards: reader.header.parityShards)
        }
    }

    struct EncodingDTO: Content {
        let dataShards: Int
        let parityShards: Int
    }

    /// Repoints this node's `.latest` pointer for (bucket, key) back to `priorVersionId` and
    /// re-promotes that version's local shard to `isLatest = true` - the peer side of a
    /// coordinator's failed-quorum rollback. A missing `priorVersionId` means the key had no prior
    /// version at all (a first-ever write that failed), so the pointer is removed outright. Idempotent
    /// and safe to broadcast to every responsible node, whether or not it actually received the
    /// rolled-back shard.
    @Sendable
    func handleRestoreLatest(req: Request) async throws -> HTTPStatus {
        guard
            let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing bucket/key query parameters")
        }
        let priorVersionId = req.query[String.self, at: "priorVersionId"]
        try await req.application.threadPool.runIfActive {
            ErasureCodedObjectHandler.restoreLatest(
                bucketName: bucketName, key: key, priorVersionId: priorVersionId)
        }
        return .ok
    }

    private func shardPath(req: Request) throws -> (path: String, bucketName: String, key: String) {
        guard
            let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"],
            let shardIndex = req.query[Int.self, at: "shardIndex"]
        else {
            throw Abort(.badRequest, reason: "Missing bucket/key/shardIndex query parameters")
        }
        let versionId = req.query[String.self, at: "versionId"]
        let path = ErasureCodedObjectHandler.shardPath(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex)
        return (path, bucketName, key)
    }

    /// Header-only existence probe - the shard counterpart of `handleExists`.
    @Sendable
    func handleExists(req: Request) async throws -> HTTPStatus {
        let (path, _, _) = try shardPath(req: req)
        return FileManager.default.fileExists(atPath: path) ? .ok : .notFound
    }

    /// Streams this node's raw local `.ecshard` file back to the requester - the shard
    /// counterpart of `handleFetch`. No metadata header: the file's own header already carries
    /// everything (`ErasureCodedShardHeader`, including the full `ObjectMeta`).
    @Sendable
    func handleFetch(req: Request) async throws -> Response {
        let (path, _, _) = try shardPath(req: req)
        guard FileManager.default.fileExists(atPath: path) else {
            throw Abort(.notFound, reason: "Shard not found")
        }

        let fd = POSIXFile.open(path, O_RDONLY)
        guard fd >= 0 else {
            throw Abort(.notFound, reason: "Shard not found")
        }
        var statInfo = stat()
        guard POSIXFile.fstat(fd, &statInfo) == 0 else {
            _ = POSIXFile.close(fd)
            throw Abort(.internalServerError, reason: "Could not stat shard file")
        }
        let fileSize = Int(statInfo.st_size)

        let response = Response(status: .ok)
        response.headers.replaceOrAdd(name: .contentType, value: "application/octet-stream")

        let threadPool = req.application.threadPool
        response.body = Response.Body(
            managedAsyncStream: { writer in
                do {
                    try await StreamingIOLoops.readWindowed(
                        threadPool: threadPool, fd: fd, offset: 0, length: fileSize,
                        chunkSize: Constants.streamingReadChunkSize
                    ) { chunk in
                        try await writer.writeBuffer(chunk)
                    }
                    _ = POSIXFile.close(fd)
                } catch is IOLoopError {
                    _ = POSIXFile.close(fd)
                    throw Abort(.internalServerError, reason: "Shard payload ended early")
                } catch {
                    _ = POSIXFile.close(fd)
                    throw error
                }
            }, count: fileSize)

        return response
    }

    /// Spools the incoming raw `.ecshard` bytes to a temp file, then window-copies them into
    /// their final destination via `AtomicObjectWriter` in one blocking call - the sender already
    /// produced a complete, self-describing shard file (`StripeEncoder`'s scratch output), so
    /// this is a streamed copy, never re-parsed or re-encoded on the receiving end. Two steps
    /// (spool, then assemble) rather than writing straight to the final-path writer, mirroring
    /// `InternalClusterObjectController.handlePush` - a `var` `AtomicObjectWriter` can't be
    /// mutated from inside a `threadPool.runIfActive` closure per request body chunk (Swift 6
    /// flags that as a captured-var data race), so the writer is only ever touched from a single
    /// blocking call, same as the existing `.obj` push path.
    @Sendable
    func handlePush(req: Request) async throws -> HTTPStatus {
        let (path, bucketName, key) = try shardPath(req: req)

        let threadPool = req.application.threadPool
        let spoolPath = Constants.spoolDirectory + ".ecshard-push-" + UUID().uuidString

        let fd = try await threadPool.runIfActive { () -> Int32 in
            var fd = POSIXFile.openWrite(spoolPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            if fd < 0 && errno == ENOENT {
                try FileManager.default.createDirectory(
                    atPath: Constants.spoolDirectory, withIntermediateDirectories: true)
                fd = POSIXFile.openWrite(spoolPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            }
            guard fd >= 0 else {
                throw Abort(.internalServerError, reason: "Failed to open shard push spool file")
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
                                .internalServerError, reason: "Failed writing shard push spool file")
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

        // Peek the spooled shard's header (cheap - no stripe data touched) to learn whether this
        // is a new "latest" version before committing it - demoting any prior local latest must
        // happen BEFORE the new file lands, mirroring InternalClusterObjectController.handlePush's
        // exact ordering for `.obj` (never demote the file we're about to write).
        let incomingMeta = try? await threadPool.runIfActive {
            try ErasureCodedShardReader(path: spoolPath).header.objectMeta
        }
        if incomingMeta?.isLatest == true {
            try? await threadPool.runIfActive {
                ErasureCodedObjectHandler.markAllLocalShardsNotLatest(bucketName: bucketName, key: key)
            }
        }

        let payloadSize = size
        do {
            try await threadPool.runIfActive {
                var writer = try AtomicObjectWriter(finalPath: path)
                do {
                    let sourceFd = POSIXFile.open(spoolPath, O_RDONLY)
                    guard sourceFd >= 0 else {
                        throw Abort(.internalServerError, reason: "Could not reopen shard spool file")
                    }
                    defer { _ = POSIXFile.close(sourceFd) }

                    let windowSize = Constants.fileCopyWindowSize
                    var window = [UInt8](repeating: 0, count: windowSize)
                    var remaining = payloadSize
                    while remaining > 0 {
                        let toRead = Swift.min(windowSize, remaining)
                        let bytesRead = POSIXFile.read(sourceFd, &window, toRead)
                        guard bytesRead > 0 else {
                            throw Abort(.internalServerError, reason: "Shard spool file ended early")
                        }
                        try window.withUnsafeBytes { raw in
                            try writer.writeRaw(UnsafeRawBufferPointer(rebasing: raw.prefix(bytesRead)))
                        }
                        remaining -= bytesRead
                    }
                    try writer.finish()
                } catch {
                    writer.abort()
                    throw error
                }
            }
        } catch {
            _ = POSIXFile.unlink(spoolPath)
            throw error
        }

        _ = POSIXFile.unlink(spoolPath)

        if let incomingMeta, incomingMeta.isLatest, let versionId = incomingMeta.versionId {
            try? await threadPool.runIfActive {
                try ObjectFileHandler.updateLatestPointer(
                    bucketName: bucketName, key: key, versionId: versionId)
            }
        }
        return .ok
    }

    /// Deletes whatever shard(s) this node locally holds for (bucketName, key, versionId) - the
    /// whole `.ecshards`/`{versionId}.ecshards` directory, since a node only ever holds the one
    /// shard index it's currently responsible for.
    @Sendable
    func handleDelete(req: Request) async throws -> HTTPStatus {
        guard
            let bucketName = req.query[String.self, at: "bucket"],
            let key = req.query[String.self, at: "key"]
        else {
            throw Abort(.badRequest, reason: "Missing bucket/key query parameters")
        }
        let versionId = req.query[String.self, at: "versionId"]
        try? FileManager.default.removeItem(
            atPath: ErasureCodedObjectHandler.shardBasePath(
                bucketName: bucketName, key: key, versionId: versionId))
        return .ok
    }

    /// Overwrites the `ObjectMeta` in this node's local shard `shardIndex` of (bucketName, key,
    /// versionId), leaving payload bytes untouched - the shard counterpart of a plain object's
    /// in-place metadata push (tagging, admin metadata edit).
    @Sendable
    func handleMetadataPatch(req: Request) async throws -> HTTPStatus {
        let (path, _, _) = try shardPath(req: req)
        guard FileManager.default.fileExists(atPath: path) else {
            throw Abort(.notFound, reason: "Shard not found")
        }

        guard let bodyBuffer = try await req.body.collect(max: 1024 * 1024).get(),
            let newMeta = try? JSONDecoder().decode(ObjectMeta.self, from: Data(buffer: bodyBuffer))
        else {
            throw Abort(.badRequest, reason: "Invalid ObjectMeta body")
        }

        _ = try await req.application.threadPool.runIfActive {
            try ErasureCodedObjectHandler.rewriteShardMetadata(at: path) { $0 = newMeta }
        }
        return .ok
    }
}
