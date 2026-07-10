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
