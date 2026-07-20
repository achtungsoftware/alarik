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

import AsyncHTTPClient
import Foundation
import NIOCore
import NIOHTTP1
import Vapor

/// The internal-only push/fetch/delete protocol `ClusterReplicationService`/
/// `ClusterReplicationDispatcher` use to actually copy object bytes between nodes - guarded by
/// the cluster secret alone, never SigV4 (see `ClusterSecretMiddleware`). Distinct from
/// `ClusterForwardingClient`, which replays an already-authenticated *client* request to a peer's
/// public S3 route instead of speaking this internal protocol.
///
/// Uses raw `AsyncHTTPClient` (`app.http.client.shared`) rather than Vapor's `Client` wrapper -
/// Vapor's `ClientRequest`/`ClientResponse` only expose a fully-buffered `ByteBuffer?` body
/// (confirmed against the vendored Vapor source), which would defeat the bounded-memory
/// streaming every other object-IO path in this codebase deliberately maintains for
/// multi-gigabyte objects.
enum ClusterReplicationClient {
    /// Whole-request deadline for a single object push/delete/fetch. Generous - large object
    /// transfers must have room to complete, not just connect.
    static let requestTimeout: TimeAmount = .minutes(10)

    /// Deadline for a small, metadata-only probe (does this node hold a shard, what does its
    /// header say) - deliberately much shorter than `requestTimeout`. Several of these calls sit
    /// directly on a client-facing GET/HEAD's synchronous path (shard discovery fans out to
    /// every responsible node and waits for all of them), so a peer that's merely hanging - not
    /// erroring, which would return quickly, but genuinely unresponsive - must not be able to
    /// stall every read of a key it's responsible for for `requestTimeout`'s full 10 minutes.
    static let probeTimeout: TimeAmount = .seconds(5)

    private static let objectMetaHeaderName = "X-Alarik-Object-Meta"

    /// Streams the local object at `bucketName`/`key`/`versionId` to `node`'s internal push
    /// endpoint, bounded-memory (windowed `pread`, same technique
    /// `S3Service.buildStreamingObjectResponse` uses for client GETs). The object's `ObjectMeta`
    /// - crucially including its already-assigned `versionId`/`isLatest` - travels as a
    /// base64-encoded JSON header, so the receiving node writes the *exact same* version rather
    /// than minting a new one (replicas must agree on version ids for the same write).
    static func pushObject(
        app: Application, to node: ClusterNodeInfo, bucketName: String, key: String,
        versionId: String?
    ) async throws {
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            throw ClusterConfigurationError(description: "This node is not part of a cluster.")
        }
        guard let path = try ObjectFileHandler.resolvePath(bucketName: bucketName, key: key, versionId: versionId)
        else {
            throw ClusterProxyError.objectNotFound
        }

        let snapshot = try await app.threadPool.runIfActive {
            try ObjectFileHandler.openPayloadSnapshot(path: path)
        }

        let metaJSON = try JSONEncoder().encode(snapshot.meta)
        let metaHeaderValue = Data(metaJSON).base64EncodedString()

        var outbound = HTTPClientRequest(url: node.address + "/internal/cluster/objects/push")
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        outbound.headers.replaceOrAdd(name: objectMetaHeaderName, value: metaHeaderValue)
        outbound.headers.replaceOrAdd(name: .contentType, value: "application/octet-stream")
        outbound.body = .stream(
            fileByteStream(
                threadPool: app.threadPool, fd: snapshot.fd, offset: snapshot.payloadOffset,
                length: snapshot.payloadSize),
            length: .known(Int64(snapshot.payloadSize)))

        let response = try await app.http.client.shared.execute(
            outbound, timeout: requestTimeout, logger: app.logger)
        guard (200..<300).contains(response.status.code) else {
            throw ClusterProxyError.pushFailed(status: Int(response.status.code))
        }
    }

    /// Best-effort "does any of `candidates` physically hold this object" probe - a header-only
    /// GET against the internal existence endpoint, no payload transferred. For read-side
    /// callers (e.g. creating a share link) that need to confirm an object exists cluster-wide
    /// but must run on *this* node rather than forward (so the answer, or a generated URL, stays
    /// local). An unreachable peer is treated as "not found here" and the next candidate is
    /// tried; `false` only after every candidate has been asked.
    static func objectExists(
        app: Application, candidates: [ClusterNodeInfo], bucketName: String, key: String,
        versionId: String?
    ) async -> Bool {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return false }

        let allowed = CharacterSet.urlQueryAllowed
        let encodedBucket = bucketName.addingPercentEncoding(withAllowedCharacters: allowed) ?? bucketName
        let encodedKey = key.addingPercentEncoding(withAllowedCharacters: allowed) ?? key
        var urlSuffix = "?bucket=\(encodedBucket)&key=\(encodedKey)"
        if let versionId {
            let encodedVersionId =
                versionId.addingPercentEncoding(withAllowedCharacters: allowed) ?? versionId
            urlSuffix += "&versionId=\(encodedVersionId)"
        }

        for node in candidates {
            var outbound = HTTPClientRequest(
                url: node.address + "/internal/cluster/objects/exists" + urlSuffix)
            outbound.method = .GET
            outbound.headers.replaceOrAdd(
                name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
            do {
                let response = try await app.http.client.shared.execute(
                    outbound, timeout: requestTimeout, logger: app.logger)
                if response.status == .ok { return true }
            } catch {
                continue
            }
        }
        return false
    }

    /// Asks `candidates` (tried in order) to resolve "what's the latest version id of this key"
    /// from their own local `.latest` pointer - format-agnostic, works whether the key turns
    /// out to be `.obj`- or `.ecshard`-backed. `nil` means either no candidate could answer, or
    /// the key genuinely has no versions (the caller can't tell which from this alone, matching
    /// how a plain local `getLatestVersionId` also can't distinguish those cases).
    static func resolveLatestVersionId(
        app: Application, candidates: [ClusterNodeInfo], bucketName: String, key: String
    ) async -> String? {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return nil }
        let allowed = CharacterSet.urlQueryAllowed
        let encodedBucket = bucketName.addingPercentEncoding(withAllowedCharacters: allowed) ?? bucketName
        let encodedKey = key.addingPercentEncoding(withAllowedCharacters: allowed) ?? key
        let urlSuffix = "?bucket=\(encodedBucket)&key=\(encodedKey)"

        for node in candidates {
            var outbound = HTTPClientRequest(
                url: node.address + "/internal/cluster/objects/latest-version" + urlSuffix)
            outbound.method = .GET
            outbound.headers.replaceOrAdd(
                name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
            do {
                let response = try await app.http.client.shared.execute(
                    outbound, timeout: requestTimeout, logger: app.logger)
                guard response.status == .ok else { continue }
                let body = try await response.body.collect(upTo: 1024)
                guard let versionId = body.getString(at: 0, length: body.readableBytes) else {
                    continue
                }
                return versionId
            } catch {
                continue
            }
        }
        return nil
    }

    /// Tells `node` to delete its copy of `bucketName`/`key`/`versionId` (or the current object,
    /// if `versionId` is nil) - used both for genuine deletes and for reclaim tasks after a
    /// rebalance confirms the new owner(s) have a full copy. `coordinate: true` asks `node` to
    /// act as delegate coordinator instead of a dumb single-copy delete - it runs the local
    /// delete then replicates to its own peers, exactly as if the client's request had landed on
    /// it directly. Used when this node fields a Multi-Object-Delete batch key it isn't itself
    /// responsible for; never set for the outbox dispatcher's plain per-peer catch-up delivery,
    /// which must stay a single, non-recursing copy.
    @discardableResult
    static func deleteObject(
        app: Application, to node: ClusterNodeInfo, bucketName: String, key: String,
        versionId: String?, coordinate: Bool = false
    ) async throws -> S3Service.ObjectDeleteOutcome {
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            throw ClusterConfigurationError(description: "This node is not part of a cluster.")
        }

        let allowed = CharacterSet.urlQueryAllowed
        let encodedBucket = bucketName.addingPercentEncoding(withAllowedCharacters: allowed) ?? bucketName
        let encodedKey = key.addingPercentEncoding(withAllowedCharacters: allowed) ?? key
        var url = "\(node.address)/internal/cluster/objects?bucket=\(encodedBucket)&key=\(encodedKey)"
        if let versionId {
            let encodedVersionId =
                versionId.addingPercentEncoding(withAllowedCharacters: allowed) ?? versionId
            url += "&versionId=\(encodedVersionId)"
        }
        if coordinate {
            url += "&coordinate=true"
        }
        var outbound = HTTPClientRequest(url: url)
        outbound.method = .DELETE
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)

        let response = try await app.http.client.shared.execute(
            outbound, timeout: requestTimeout, logger: app.logger)
        guard (200..<300).contains(response.status.code) else {
            throw ClusterProxyError.pushFailed(status: Int(response.status.code))
        }
        let body = try await response.body.collect(upTo: 1024 * 1024)
        guard
            let outcome = try? JSONDecoder().decode(
                S3Service.ObjectDeleteOutcome.self, from: body)
        else {
            throw ClusterProxyError.invalidDeleteResponse
        }
        return outcome
    }

    /// The read-mirror of `pushObject`: fetches `bucketName`/`key`/`versionId` from one of
    /// `candidates` (tried in order, same read-fallback semantics as `ClusterForwardingClient
    /// .forward`'s bodiless-request path) into a local temp file, returning it alongside the
    /// full `ObjectMeta` the peer sent. Used by CopyObject/UploadPartCopy when the copy *source*
    /// isn't held by this node - the source and destination of a copy can be arbitrary,
    /// unrelated buckets/keys, so the source may need fetching even though this node is already
    /// correctly responsible for the destination. Callers own the returned temp file and must
    /// unlink it themselves once done.
    static func fetchObjectToTempFile(
        app: Application, candidates: [ClusterNodeInfo], bucketName: String, key: String,
        versionId: String?, requestId: String
    ) async throws -> (path: String, meta: ObjectMeta) {
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            throw ClusterConfigurationError(description: "This node is not part of a cluster.")
        }
        guard !candidates.isEmpty else {
            throw S3Error(
                status: .serviceUnavailable, code: "ServiceUnavailable",
                message: "No cluster peer is currently available for the copy source.",
                requestId: requestId)
        }

        let allowed = CharacterSet.urlQueryAllowed
        let encodedBucket = bucketName.addingPercentEncoding(withAllowedCharacters: allowed) ?? bucketName
        let encodedKey = key.addingPercentEncoding(withAllowedCharacters: allowed) ?? key
        var urlSuffix = "?bucket=\(encodedBucket)&key=\(encodedKey)"
        if let versionId {
            let encodedVersionId =
                versionId.addingPercentEncoding(withAllowedCharacters: allowed) ?? versionId
            urlSuffix += "&versionId=\(encodedVersionId)"
        }

        var lastError: any Error = S3Error(
            status: .serviceUnavailable, code: "ServiceUnavailable",
            message: "No cluster peer could serve the copy source.", requestId: requestId)
        for node in candidates {
            do {
                return try await fetchOnce(
                    app: app, secret: config.secret,
                    url: node.address + "/internal/cluster/objects/fetch" + urlSuffix,
                    requestId: requestId)
            } catch {
                lastError = error
                continue
            }
        }
        throw lastError
    }

    private static func fetchOnce(
        app: Application, secret: String, url: String, requestId: String
    ) async throws -> (path: String, meta: ObjectMeta) {
        var outbound = HTTPClientRequest(url: url)
        outbound.method = .GET
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: secret)

        let response = try await app.http.client.shared.execute(
            outbound, timeout: requestTimeout, logger: app.logger)
        guard response.status == .ok else {
            throw ClusterProxyError.pushFailed(status: Int(response.status.code))
        }
        guard
            let metaHeader = response.headers.first(name: objectMetaHeaderName),
            let metaData = Data(base64Encoded: metaHeader),
            let meta = try? JSONDecoder().decode(ObjectMeta.self, from: metaData)
        else {
            throw ClusterProxyError.missingObjectMeta
        }

        let threadPool = app.threadPool
        let tempPath = Constants.spoolDirectory + ".cluster-fetch-" + UUID().uuidString
        let fd = try await threadPool.runIfActive { () -> Int32 in
            var fd = POSIXFile.openWrite(tempPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            if fd < 0 && errno == ENOENT {
                try FileManager.default.createDirectory(
                    atPath: Constants.spoolDirectory, withIntermediateDirectories: true)
                fd = POSIXFile.openWrite(tempPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            }
            guard fd >= 0 else {
                throw S3Error(
                    status: .internalServerError, code: "InternalError",
                    message: "Failed to open cluster fetch temp file", requestId: requestId)
            }
            return fd
        }

        do {
            for try await buffer in response.body {
                let chunk = buffer
                try await threadPool.runIfActive {
                    try chunk.withUnsafeReadableBytes { raw in
                        do {
                            try StreamingIOLoops.writeFully(fd: fd, raw)
                        } catch {
                            throw S3Error(
                                status: .internalServerError, code: "InternalError",
                                message: "Failed writing cluster fetch temp file",
                                requestId: requestId)
                        }
                    }
                }
            }
            _ = try await threadPool.runIfActive { POSIXFile.close(fd) }
        } catch {
            _ = try? await threadPool.runIfActive { POSIXFile.close(fd) }
            _ = POSIXFile.unlink(tempPath)
            throw error
        }

        return (tempPath, meta)
    }

    /// Adapts a windowed `pread` loop over an open file descriptor into an `AsyncSequence` the
    /// streaming `HTTPClientRequest.Body` can consume - the send-side mirror of
    /// `S3Service.buildStreamingObjectResponse`'s receive-side read loop. Closes `fd` when done
    /// (success or failure) - callers never close it themselves.
    private static func fileByteStream(
        threadPool: NIOThreadPool, fd: Int32, offset: Int, length: Int
    ) -> AsyncThrowingStream<ByteBuffer, any Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    try await StreamingIOLoops.readWindowed(
                        threadPool: threadPool, fd: fd, offset: offset, length: length,
                        chunkSize: Constants.streamingReadChunkSize
                    ) { chunk in
                        continuation.yield(chunk)
                    }
                    _ = POSIXFile.close(fd)
                    continuation.finish()
                } catch is IOLoopError {
                    _ = POSIXFile.close(fd)
                    continuation.finish(throwing: ClusterProxyError.objectNotFound)
                } catch {
                    _ = POSIXFile.close(fd)
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    // MARK: - Erasure-coded shards

    /// Streams `sourcePath` (a local `.ecshard` file - typically fresh scratch output from
    /// `StripeEncoder`) to `node`'s internal shard-push endpoint. The `.ecshard` format is
    /// already fully self-describing (header + per-stripe checksums), so this is a raw byte
    /// copy - no repacking needed, unlike `pushObject`'s JSON-meta-header dance.
    static func pushShard(
        app: Application, to node: ClusterNodeInfo, sourcePath: String, bucketName: String,
        key: String, versionId: String?, shardIndex: Int
    ) async throws {
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            throw ClusterConfigurationError(description: "This node is not part of a cluster.")
        }
        let fd = POSIXFile.open(sourcePath, O_RDONLY)
        guard fd >= 0 else { throw ClusterProxyError.objectNotFound }
        var statInfo = stat()
        guard POSIXFile.fstat(fd, &statInfo) == 0 else {
            _ = POSIXFile.close(fd)
            throw ClusterProxyError.objectNotFound
        }
        let fileSize = Int(statInfo.st_size)
        
        let suffix = shardQuerySuffix(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex)
        var outbound = HTTPClientRequest(url: node.address + "/internal/cluster/ecshards/push" + suffix)
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        outbound.headers.replaceOrAdd(name: .contentType, value: "application/octet-stream")
        outbound.body = .stream(
            fileByteStream(threadPool: app.threadPool, fd: fd, offset: 0, length: fileSize),
            length: .known(Int64(fileSize)))

        let response = try await app.http.client.shared.execute(
            outbound, timeout: requestTimeout, logger: app.logger)
        guard (200..<300).contains(response.status.code) else {
            throw ClusterProxyError.pushFailed(status: Int(response.status.code))
        }
    }

    /// Best-effort "does `node` already hold shard `shardIndex`" probe - mirrors `objectExists`.
    static func shardExists(
        app: Application, node: ClusterNodeInfo, bucketName: String, key: String,
        versionId: String?, shardIndex: Int
    ) async -> Bool {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return false }
        let suffix = shardQuerySuffix(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex)
        var outbound = HTTPClientRequest(url: node.address + "/internal/cluster/ecshards/exists" + suffix)
        outbound.method = .GET
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        do {
            let response = try await app.http.client.shared.execute(
                outbound, timeout: requestTimeout, logger: app.logger)
            return response.status == .ok
        } catch {
            return false
        }
    }

    /// Discovers which shard indices `node` physically holds for (bucketName, key, versionId).
    /// Returns `nil` when the node is unreachable/errored (distinct from `[]` = reachable but holds
    /// no shard), so the gatherer can tell "object genuinely absent" from "can't currently reach a
    /// holder" - the difference between a correct 404 and a correct 503.
    static func heldShards(
        app: Application, node: ClusterNodeInfo, bucketName: String, key: String, versionId: String?
    ) async -> [Int]? {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return nil }
        let suffix = shardVersionQuerySuffix(bucketName: bucketName, key: key, versionId: versionId)
        var outbound = HTTPClientRequest(url: node.address + "/internal/cluster/ecshards/held" + suffix)
        outbound.method = .GET
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        do {
            let response = try await app.http.client.shared.execute(
                outbound, timeout: probeTimeout, logger: app.logger)
            guard response.status == .ok else { return nil }
            let body = try await response.body.collect(upTo: 64 * 1024)
            return try JSONDecoder().decode([Int].self, from: body)
        } catch {
            return nil
        }
    }

    /// Tells `node` to repoint its `.latest` for (bucketName, key) back to `priorVersionId` (or
    /// remove it when nil) - the peer side of a coordinator's failed-quorum rollback. Best-effort:
    /// a peer that can't be reached simply keeps whatever pointer it had; the failing PUT already
    /// returns an error to the client regardless.
    static func restoreShardLatest(
        app: Application, to node: ClusterNodeInfo, bucketName: String, key: String,
        priorVersionId: String?
    ) async {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return }
        let allowed = CharacterSet.urlQueryAllowed
        let encodedBucket = bucketName.addingPercentEncoding(withAllowedCharacters: allowed) ?? bucketName
        let encodedKey = key.addingPercentEncoding(withAllowedCharacters: allowed) ?? key
        var url =
            "\(node.address)/internal/cluster/ecshards/restore-latest?bucket=\(encodedBucket)&key=\(encodedKey)"
        if let priorVersionId {
            let encoded = priorVersionId.addingPercentEncoding(withAllowedCharacters: allowed) ?? priorVersionId
            url += "&priorVersionId=\(encoded)"
        }
        var outbound = HTTPClientRequest(url: url)
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        _ = try? await app.http.client.shared.execute(
            outbound, timeout: probeTimeout, logger: app.logger)
    }

    /// Fetches the `(dataShards, parityShards)` whatever shard `node` holds for (bucketName, key,
    /// versionId) was actually encoded with, read straight from its on-disk header - the
    /// network-visible counterpart of `handleEncoding`'s local read. `nil` when the node is
    /// unreachable or holds nothing for it. See `MetadataStore.discoverShardCounts`.
    static func fetchShardEncoding(
        app: Application, node: ClusterNodeInfo, bucketName: String, key: String,
        versionId: String?
    ) async -> (dataShards: Int, parityShards: Int)? {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return nil }
        let suffix = shardVersionQuerySuffix(bucketName: bucketName, key: key, versionId: versionId)
        var outbound = HTTPClientRequest(url: node.address + "/internal/cluster/ecshards/encoding" + suffix)
        outbound.method = .GET
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        do {
            let response = try await app.http.client.shared.execute(
                outbound, timeout: probeTimeout, logger: app.logger)
            guard response.status == .ok else { return nil }
            let body = try await response.body.collect(upTo: 64 * 1024)
            let dto = try JSONDecoder().decode(
                InternalClusterErasureCodedController.EncodingDTO.self, from: body)
            return (dto.dataShards, dto.parityShards)
        } catch {
            return nil
        }
    }

    /// Fetches the `ObjectMeta` of whatever shard `node` holds for (bucketName, key, versionId) -
    /// a header-only probe (no stripe data crosses the wire), for HEAD-shaped metadata resolution
    /// on a node that hasn't received its own shard yet. `nil` when the node is unreachable or
    /// holds nothing for it.
    static func fetchShardMeta(
        app: Application, node: ClusterNodeInfo, bucketName: String, key: String,
        versionId: String?
    ) async -> ObjectMeta? {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return nil }
        let suffix = shardVersionQuerySuffix(bucketName: bucketName, key: key, versionId: versionId)
        var outbound = HTTPClientRequest(url: node.address + "/internal/cluster/ecshards/meta" + suffix)
        outbound.method = .GET
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        do {
            let response = try await app.http.client.shared.execute(
                outbound, timeout: probeTimeout, logger: app.logger)
            guard response.status == .ok else { return nil }
            let body = try await response.body.collect(upTo: 4 * 1024 * 1024)
            return try JSONDecoder().decode(ObjectMeta.self, from: body)
        } catch {
            return nil
        }
    }

    /// Asks `node` to verify the shard(s) it holds for (bucketName, key, versionId) against their
    /// on-disk checksums and heal any genuinely corrupt - the safe corruption side of read-repair.
    /// Best-effort: a node that can't be reached simply doesn't verify this pass; the next read,
    /// scrub, or rebalance covers it.
    static func requestVerifyHeal(
        app: Application, to node: ClusterNodeInfo, bucketName: String, key: String,
        versionId: String?
    ) async {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return }
        let suffix = shardVersionQuerySuffix(bucketName: bucketName, key: key, versionId: versionId)
        var outbound = HTTPClientRequest(
            url: node.address + "/internal/cluster/ecshards/verify-heal" + suffix)
        outbound.method = .POST
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        _ = try? await app.http.client.shared.execute(
            outbound, timeout: probeTimeout, logger: app.logger)
    }

    /// Fetches shard `shardIndex` of (bucketName, key, versionId) from one of `candidates` into a
    /// local temp file - tried in order, same fallback semantics as `fetchObjectToTempFile`.
    /// Callers own the returned temp file and must unlink it themselves once done.
    static func fetchShard(
        app: Application, candidates: [ClusterNodeInfo], bucketName: String, key: String,
        versionId: String?, shardIndex: Int, requestId: String
    ) async throws -> String {
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            throw ClusterConfigurationError(description: "This node is not part of a cluster.")
        }
        guard !candidates.isEmpty else {
            throw S3Error(
                status: .serviceUnavailable, code: "ServiceUnavailable",
                message: "No cluster peer is currently available for this shard.",
                requestId: requestId)
        }
        let suffix = shardQuerySuffix(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex)

        var lastError: any Error = S3Error(
            status: .serviceUnavailable, code: "ServiceUnavailable",
            message: "No cluster peer could serve this shard.", requestId: requestId)
        for node in candidates {
            do {
                return try await fetchShardOnce(
                    app: app, secret: config.secret,
                    url: node.address + "/internal/cluster/ecshards/fetch" + suffix,
                    requestId: requestId)
            } catch {
                lastError = error
                continue
            }
        }
        throw lastError
    }

    private static func fetchShardOnce(
        app: Application, secret: String, url: String, requestId: String
    ) async throws -> String {
        var outbound = HTTPClientRequest(url: url)
        outbound.method = .GET
        outbound.headers.replaceOrAdd(name: ClusterForwardAuthenticator.secretHeaderName, value: secret)

        let response = try await app.http.client.shared.execute(
            outbound, timeout: requestTimeout, logger: app.logger)
        guard response.status == .ok else {
            throw ClusterProxyError.pushFailed(status: Int(response.status.code))
        }

        let threadPool = app.threadPool
        let tempPath = Constants.spoolDirectory + ".ecshard-fetch-" + UUID().uuidString
        let fd = try await threadPool.runIfActive { () -> Int32 in
            var fd = POSIXFile.openWrite(tempPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            if fd < 0 && errno == ENOENT {
                try FileManager.default.createDirectory(
                    atPath: Constants.spoolDirectory, withIntermediateDirectories: true)
                fd = POSIXFile.openWrite(tempPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            }
            guard fd >= 0 else {
                throw S3Error(
                    status: .internalServerError, code: "InternalError",
                    message: "Failed to open shard fetch temp file", requestId: requestId)
            }
            return fd
        }

        do {
            for try await buffer in response.body {
                let chunk = buffer
                try await threadPool.runIfActive {
                    try chunk.withUnsafeReadableBytes { raw in
                        do {
                            try StreamingIOLoops.writeFully(fd: fd, raw)
                        } catch {
                            throw S3Error(
                                status: .internalServerError, code: "InternalError",
                                message: "Failed writing shard fetch temp file", requestId: requestId)
                        }
                    }
                }
            }
            _ = try await threadPool.runIfActive { POSIXFile.close(fd) }
        } catch {
            _ = try? await threadPool.runIfActive { POSIXFile.close(fd) }
            _ = POSIXFile.unlink(tempPath)
            throw error
        }

        return tempPath
    }

    /// Tells `node` to delete whatever shard(s) it locally holds for (bucketName, key,
    /// versionId) - no `shardIndex` needed: a node only ever holds the one shard index it's
    /// currently responsible for, so "delete my shard for this version" is unambiguous. Used for
    /// genuine object deletes and for rebalance reclaim (see `ClusterRebalanceService`'s
    /// `.reclaim` pattern, which this mirrors exactly, including targeting `node == self`).
    static func deleteShard(
        app: Application, to node: ClusterNodeInfo, bucketName: String, key: String,
        versionId: String?
    ) async throws {
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            throw ClusterConfigurationError(description: "This node is not part of a cluster.")
        }
        let allowed = CharacterSet.urlQueryAllowed
        let encodedBucket = bucketName.addingPercentEncoding(withAllowedCharacters: allowed) ?? bucketName
        let encodedKey = key.addingPercentEncoding(withAllowedCharacters: allowed) ?? key
        var url = "\(node.address)/internal/cluster/ecshards?bucket=\(encodedBucket)&key=\(encodedKey)"
        if let versionId {
            let encodedVersionId =
                versionId.addingPercentEncoding(withAllowedCharacters: allowed) ?? versionId
            url += "&versionId=\(encodedVersionId)"
        }
        var outbound = HTTPClientRequest(url: url)
        outbound.method = .DELETE
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)

        let response = try await app.http.client.shared.execute(
            outbound, timeout: requestTimeout, logger: app.logger)
        guard (200..<300).contains(response.status.code) else {
            throw ClusterProxyError.pushFailed(status: Int(response.status.code))
        }
    }

    /// Tells `node` to overwrite the `ObjectMeta` in whatever local shard it holds for
    /// (bucketName, key, versionId), leaving payload bytes untouched - the shard counterpart of
    /// pushing a plain object's updated metadata. Best-effort: callers already treat this
    /// fan-out as non-durable, matching `ClusterReplicationService.replicateWrite`'s handling of
    /// the equivalent plain-object metadata push.
    static func patchShardMetadata(
        app: Application, to node: ClusterNodeInfo, bucketName: String, key: String,
        versionId: String?, shardIndex: Int, meta: ObjectMeta
    ) async throws {
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            throw ClusterConfigurationError(description: "This node is not part of a cluster.")
        }
        let suffix = shardQuerySuffix(
            bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex)
        var outbound = HTTPClientRequest(url: node.address + "/internal/cluster/ecshards/metadata" + suffix)
        outbound.method = .PATCH
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        outbound.headers.replaceOrAdd(name: .contentType, value: "application/json")
        outbound.body = .bytes(try JSONEncoder().encode(meta))

        let response = try await app.http.client.shared.execute(
            outbound, timeout: requestTimeout, logger: app.logger)
        guard (200..<300).contains(response.status.code) else {
            throw ClusterProxyError.pushFailed(status: Int(response.status.code))
        }
    }

    private static func shardQuerySuffix(
        bucketName: String, key: String, versionId: String?, shardIndex: Int
    ) -> String {
        let allowed = CharacterSet.urlQueryAllowed
        let encodedBucket = bucketName.addingPercentEncoding(withAllowedCharacters: allowed) ?? bucketName
        let encodedKey = key.addingPercentEncoding(withAllowedCharacters: allowed) ?? key
        var suffix = "?bucket=\(encodedBucket)&key=\(encodedKey)&shardIndex=\(shardIndex)"
        if let versionId {
            let encodedVersionId =
                versionId.addingPercentEncoding(withAllowedCharacters: allowed) ?? versionId
            suffix += "&versionId=\(encodedVersionId)"
        }
        return suffix
    }

    /// Same as `shardQuerySuffix` without a `shardIndex` - for endpoints scoped to a whole
    /// (bucket, key, version) rather than one shard (`held`).
    private static func shardVersionQuerySuffix(
        bucketName: String, key: String, versionId: String?
    ) -> String {
        let allowed = CharacterSet.urlQueryAllowed
        let encodedBucket = bucketName.addingPercentEncoding(withAllowedCharacters: allowed) ?? bucketName
        let encodedKey = key.addingPercentEncoding(withAllowedCharacters: allowed) ?? key
        var suffix = "?bucket=\(encodedBucket)&key=\(encodedKey)"
        if let versionId {
            let encodedVersionId =
                versionId.addingPercentEncoding(withAllowedCharacters: allowed) ?? versionId
            suffix += "&versionId=\(encodedVersionId)"
        }
        return suffix
    }
}

enum ClusterProxyError: Error, CustomStringConvertible {
    case objectNotFound
    case pushFailed(status: Int)
    case missingObjectMeta
    case invalidDeleteResponse

    var description: String {
        switch self {
        case .missingObjectMeta: "Cluster fetch response was missing its object metadata header"
        case .objectNotFound: "Object not found locally"
        case .pushFailed(let status): "Cluster object push/delete failed with status \(status)"
        case .invalidDeleteResponse: "Cluster delete response body was missing or invalid"
        }
    }
}
