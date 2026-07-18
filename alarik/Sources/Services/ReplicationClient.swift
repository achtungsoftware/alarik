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
import SotoS3
import Vapor

/// Performs the actual outbound PUT/DELETE against a replication target, using SotoS3 - a full
/// SigV4-signing AWS S3 client This is deliberate: hand-writing outbound request signing would duplicate work Soto
/// already does correctly for "arbitrary S3-compatible endpoint" (it defaults to path-style
/// addressing whenever a custom `endpoint` is set, which is exactly what a non-AWS target like
/// another Alarik instance needs).
///
/// Each target has its own credentials, and Soto's `AWSClient` binds its credential provider at
/// construction (there is no per-request credential override) - so unlike a typical
/// single-shared-client setup, a fresh, short-lived `AWSClient` scoped to one target's static
/// credentials is created per operation. This is still cheap: `AWSClient` defaults to the
/// process-wide `HTTPClient.shared` connection pool (confirmed in SotoCore source - `shutdown()`
/// only tears down the credential provider, never the shared `httpClient` it was given), so
/// creating/discarding `AWSClient` wrappers doesn't create or destroy real network connections.
enum ReplicationClient {

    /// Objects at or below this size are sent as a single PUT with the full body buffered in
    /// memory. Above it, replication switches to multipart, reading the source object in
    /// fixed-size windows via `ObjectFileHandler`'s existing ranged-read primitive - never
    /// buffering the whole object, so a multi-GB object doesn't blow up memory on a
    /// self-hosted box.
    static let multipartThreshold = 8 * 1024 * 1024
    static let partSize = 8 * 1024 * 1024

    /// Encodes an object's tag-set into the URL-query-string form S3's `x-amz-tagging`/
    /// `tagging` request field expects (e.g. `"key1=value1&key2=value2"` - verified against the
    /// PutObject API reference, same format `Tagging.parseHeaderValue` already decodes on the
    /// inbound side). Percent-encodes both key and value so a tag value containing `&`/`=`
    /// can't corrupt the query string or get merged into a neighboring tag.
    private static func taggingQuery(from tags: [String: String]?) -> String? {
        guard let tags, !tags.isEmpty else { return nil }
        return tags.map { key, value in
            let encodedKey = key.addingPercentEncoding(withAllowedCharacters: .taggingValueAllowed) ?? key
            let encodedValue =
                value.addingPercentEncoding(withAllowedCharacters: .taggingValueAllowed) ?? value
            return "\(encodedKey)=\(encodedValue)"
        }.joined(separator: "&")
    }

    /// Builds a target-scoped AWSClient + S3 service client pair. Callers must `defer { try?
    /// await awsClient.shutdown() }` immediately after calling this (the `deinit` on
    /// `AWSClient` asserts it was shut down).
    private static func makeClient(for target: any ReplicationTaskConnection) -> (
        awsClient: AWSClient, s3: S3
    ) {
        let awsClient = AWSClient(
            credentialProvider: .static(
                accessKeyId: target.accessKeyId, secretAccessKey: target.secretAccessKey),
            httpClient: HTTPClient.shared
        )
        let s3 = S3(client: awsClient, region: .init(rawValue: target.region), endpoint: target.endpoint)
        return (awsClient, s3)
    }

    /// Replicates one object version to `target`. Reads the object's current size to decide
    /// single-PUT vs. multipart, then transfers it. EC-aware: the source object may be
    /// erasure-coded (every write is, by default, once a cluster has EC configured) - checked
    /// via a local shard probe first, falling through to the plain `.obj` read unchanged when
    /// it isn't. `app` is only needed for the EC path (gather-and-decode needs the thread pool
    /// and cluster storage keys); the plain path remains pure disk I/O, matching how every other
    /// EC-aware read in this codebase layers on top of the existing plain path rather than
    /// replacing it.
    static func replicatePut(
        app: Application,
        target: any ReplicationTaskConnection,
        bucketName: String,
        key: String,
        versionId: String?
    ) async throws {
        if let ecPlacement = await erasureCodedSourcePlacement(
            app: app, bucketName: bucketName, key: key, versionId: versionId)
        {
            try await replicatePutErasureCoded(
                app: app, target: target, bucketName: bucketName, key: key, versionId: versionId,
                responsible: ecPlacement.responsible, selfNodeId: ecPlacement.selfNodeId)
            return
        }

        let (meta, _) = try ObjectFileHandler.readVersion(
            bucketName: bucketName, key: key, versionId: versionId, loadData: false)

        let (awsClient, s3) = makeClient(for: target)
        do {
            if meta.size <= multipartThreshold {
                try await putSmall(
                    s3: s3, target: target, bucketName: bucketName, key: key, meta: meta,
                    versionId: versionId)
            } else {
                try await putLarge(
                    s3: s3, target: target, bucketName: bucketName, key: key, meta: meta,
                    versionId: versionId)
            }
        } catch {
            try? await awsClient.shutdown()
            throw error
        }
        try? await awsClient.shutdown()
    }

    /// Resolves EC placement and confirms this node actually holds a local shard for
    /// (bucketName, key, versionId) - `nil` when EC isn't configured, membership hasn't
    /// populated, or (most commonly) the object is simply plain-format. This dispatcher always
    /// runs on the node that originally coordinated the write (the request path that calls
    /// `ReplicationService.enqueuePut`/`attemptImmediateDelivery` only ever runs on the node
    /// that just wrote locally), which for an EC write is always rank-0 - but resolving full
    /// placement rather than assuming "self is shard 0" also keeps this correct for the async
    /// outbox case, where membership may have shifted since the write.
    private static func erasureCodedSourcePlacement(
        app: Application, bucketName: String, key: String, versionId: String?
    ) async -> (responsible: [ClusterNodeInfo], selfNodeId: UUID)? {
        guard let clusterConfig = app.storage[ClusterConfigurationKey.self],
            let ecConfig = app.storage[ClusterErasureCodingConfigKey.self]
        else { return nil }
        let active = await ClusterNodeCache.shared.activeNodes()
        guard !active.isEmpty else { return nil }

        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: active, count: ecConfig.totalShards)
        guard let selfRank = responsible.firstIndex(where: { $0.id == clusterConfig.nodeId })
        else { return nil }

        let shardPath =
            versionId != nil
            ? ErasureCodedObjectHandler.versionedShardPath(
                bucketName: bucketName, key: key, versionId: versionId!, shardIndex: selfRank)
            : ErasureCodedObjectHandler.shardPath(
                bucketName: bucketName, key: key, shardIndex: selfRank)
        guard FileManager.default.fileExists(atPath: shardPath) else { return nil }

        return (responsible, clusterConfig.nodeId)
    }

    /// Gathers and decodes the EC source into a local scratch file (reusing the same drain
    /// primitive CopyObject's cross-node source resolution uses), then uploads from that file -
    /// letting `putSmall`/`putLarge`'s exact windowed-range shape stay unchanged by handing them
    /// a plain temp-file read closure instead of `ObjectFileHandler.readVersion`.
    private static func replicatePutErasureCoded(
        app: Application, target: any ReplicationTaskConnection, bucketName: String, key: String,
        versionId: String?, responsible: [ClusterNodeInfo], selfNodeId: UUID
    ) async throws {
        let (meta, stream) = try await ErasureCodedReadCoordinator.read(
            app: app, bucketName: bucketName, key: key, versionId: versionId,
            responsible: responsible, selfNodeId: selfNodeId, requestId: UUID().uuidString)
        let tempPath = try await S3Controller.drainToTempFile(stream: stream, app: app)
        defer { _ = POSIXFile.unlink(tempPath) }

        let (awsClient, s3) = makeClient(for: target)
        do {
            if meta.size <= multipartThreshold {
                let data = try Data(contentsOf: URL(fileURLWithPath: tempPath))
                _ = try await s3.putObject(
                    .init(
                        body: AWSHTTPBody(buffer: ByteBuffer(data: data)),
                        bucket: target.targetBucket,
                        contentType: meta.contentType,
                        key: key,
                        metadata: meta.metadata,
                        tagging: taggingQuery(from: meta.tags)
                    ))
            } else {
                try await putLargeFromFile(
                    s3: s3, target: target, path: tempPath, key: key, meta: meta)
            }
        } catch {
            try? await awsClient.shutdown()
            throw error
        }
        try? await awsClient.shutdown()
    }

    private static func putLargeFromFile(
        s3: S3, target: any ReplicationTaskConnection, path: String, key: String, meta: ObjectMeta
    ) async throws {
        let created = try await s3.createMultipartUpload(
            .init(
                bucket: target.targetBucket,
                contentType: meta.contentType,
                key: key,
                metadata: meta.metadata,
                tagging: taggingQuery(from: meta.tags)
            ))
        guard let uploadId = created.uploadId else {
            throw ReplicationError.multipartInitFailed
        }

        do {
            let handle = try FileHandle(forReadingFrom: URL(fileURLWithPath: path))
            defer { try? handle.close() }

            var completedParts: [S3.CompletedPart] = []
            var offset = 0
            var partNumber = 1

            while offset < meta.size {
                let length = min(partSize, meta.size - offset)
                try handle.seek(toOffset: UInt64(offset))
                guard let chunk = try handle.read(upToCount: length), !chunk.isEmpty else {
                    throw ReplicationError.objectUnreadable
                }

                let uploaded = try await s3.uploadPart(
                    .init(
                        body: AWSHTTPBody(buffer: ByteBuffer(data: chunk)),
                        bucket: target.targetBucket,
                        key: key,
                        partNumber: partNumber,
                        uploadId: uploadId
                    ))
                guard let eTag = uploaded.eTag else {
                    throw ReplicationError.multipartPartFailed(partNumber: partNumber)
                }
                completedParts.append(.init(eTag: eTag, partNumber: partNumber))

                offset += chunk.count
                partNumber += 1
            }

            _ = try await s3.completeMultipartUpload(
                .init(
                    bucket: target.targetBucket,
                    key: key,
                    multipartUpload: .init(parts: completedParts),
                    uploadId: uploadId
                ))
        } catch {
            _ = try? await s3.abortMultipartUpload(
                .init(bucket: target.targetBucket, key: key, uploadId: uploadId))
            throw error
        }
    }

    private static func putSmall(
        s3: S3, target: any ReplicationTaskConnection, bucketName: String, key: String,
        meta: ObjectMeta, versionId: String?
    ) async throws {
        let (_, data) = try ObjectFileHandler.readVersion(
            bucketName: bucketName, key: key, versionId: versionId, loadData: true)
        guard let data else {
            throw ReplicationError.objectUnreadable
        }

        _ = try await s3.putObject(
            .init(
                body: AWSHTTPBody(buffer: ByteBuffer(data: data)),
                bucket: target.targetBucket,
                contentType: meta.contentType,
                key: key,
                metadata: meta.metadata,
                tagging: taggingQuery(from: meta.tags)
            ))
    }

    private static func putLarge(
        s3: S3, target: any ReplicationTaskConnection, bucketName: String, key: String,
        meta: ObjectMeta, versionId: String?
    ) async throws {
        let created = try await s3.createMultipartUpload(
            .init(
                bucket: target.targetBucket,
                contentType: meta.contentType,
                key: key,
                metadata: meta.metadata,
                tagging: taggingQuery(from: meta.tags)
            ))
        guard let uploadId = created.uploadId else {
            throw ReplicationError.multipartInitFailed
        }

        // On any failure past this point, best-effort abort the remote upload so it doesn't
        // linger as an orphaned incomplete upload on the target.
        do {
            var completedParts: [S3.CompletedPart] = []
            var offset = 0
            var partNumber = 1

            while offset < meta.size {
                let end = min(offset + partSize, meta.size) - 1
                let (_, chunk) = try ObjectFileHandler.readVersion(
                    bucketName: bucketName, key: key, versionId: versionId, loadData: true,
                    range: (offset, end))
                guard let chunk else { throw ReplicationError.objectUnreadable }

                let uploaded = try await s3.uploadPart(
                    .init(
                        body: AWSHTTPBody(buffer: ByteBuffer(data: chunk)),
                        bucket: target.targetBucket,
                        key: key,
                        partNumber: partNumber,
                        uploadId: uploadId
                    ))
                guard let eTag = uploaded.eTag else {
                    throw ReplicationError.multipartPartFailed(partNumber: partNumber)
                }
                completedParts.append(.init(eTag: eTag, partNumber: partNumber))

                offset = end + 1
                partNumber += 1
            }

            _ = try await s3.completeMultipartUpload(
                .init(
                    bucket: target.targetBucket,
                    key: key,
                    multipartUpload: .init(parts: completedParts),
                    uploadId: uploadId
                ))
        } catch {
            _ = try? await s3.abortMultipartUpload(
                .init(bucket: target.targetBucket, key: key, uploadId: uploadId))
            throw error
        }
    }

    /// Replicates a delete to `target` as a plain (no-versionId) DELETE, letting the target's
    /// own versioning status decide the outcome - creating its own delete marker if the target
    /// is versioned, or permanently removing the object otherwise. This deliberately never
    /// forwards the source's versionId: version ids are assigned independently by each S3
    /// endpoint (Alarik has no way to write a chosen version id via the ordinary PutObject/
    /// DeleteObject API, unlike AWS's internal replication protocol), so a source version id
    /// can never identify anything meaningful on the target. Only "the current object" is a
    /// concept replication can faithfully mirror - see the call sites in `ReplicationService`,
    /// which never enqueue a delete task for a client-specified historical-version delete.
    static func replicateDelete(
        target: any ReplicationTaskConnection,
        key: String
    ) async throws {
        let (awsClient, s3) = makeClient(for: target)
        do {
            _ = try await s3.deleteObject(.init(bucket: target.targetBucket, key: key))
        } catch {
            try? await awsClient.shutdown()
            throw error
        }
        try? await awsClient.shutdown()
    }
}

/// The subset of a `ReplicationTarget`'s connection details a replicate operation needs -
/// satisfied by both `ReplicationTarget` itself (used when sending a resync/test request
/// immediately) and `ReplicationTask` (used when the dispatcher replays a queued task from its
/// own snapshotted columns).
protocol ReplicationTaskConnection {
    var endpoint: String { get }
    var targetBucket: String { get }
    var accessKeyId: String { get }
    var secretAccessKey: String { get }
    var region: String { get }
}

extension ReplicationTarget: ReplicationTaskConnection {}

extension ReplicationTask: ReplicationTaskConnection {}

enum ReplicationError: Error, CustomStringConvertible {
    case objectUnreadable
    case multipartInitFailed
    case multipartPartFailed(partNumber: Int)

    var description: String {
        switch self {
        case .objectUnreadable: "Could not read the source object from disk"
        case .multipartInitFailed: "Remote target did not return an upload id"
        case .multipartPartFailed(let n): "Remote target did not return an ETag for part \(n)"
        }
    }
}

extension CharacterSet {
    /// Characters left unencoded in a replicated tag's key/value - everything else (including
    /// `&` and `=`, which are otherwise valid query characters) is percent-encoded so a tag
    /// value can never be misread as a second tag or corrupt the query string.
    fileprivate static let taggingValueAllowed: CharacterSet = {
        var set = CharacterSet.alphanumerics
        set.insert(charactersIn: "-_.:/@")
        return set
    }()
}
