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

import Vapor

/// Serves files shared via `POST /api/v1/objects/share` (InternalBucketController). Registered
/// directly on the bare /api/v1 group in routes.swift (not wrapped in InternalAuthenticator like
/// most of that group is) - this route must work for completely unauthenticated callers, since
/// that's the entire point of a shared link. Resulting path: GET /api/v1/shared/:token.
struct SharedLinkController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("shared").get(":token", use: self.serve)
    }

    @Sendable
    func serve(req: Request) async throws -> Response {
        guard
            let tokenString = req.parameters.get("token"),
            let token = UUID(uuidString: tokenString)
        else {
            throw Abort(.notFound)
        }

        // A nil expiresAt means the link never expires - only an elapsed explicit expiry
        // rejects.
        guard
            let link = try await SharedLink.find(app: req.application, id: token),
            link.expiresAt.map({ $0 > Date() }) ?? true
        else {
            throw Abort(.notFound)
        }

        let (isLocal, candidates, responsible) =
            await ObjectRoutingService.erasureCodedReadPlacement(
                req: req, bucketName: link.bucketName, key: link.key)

        if isLocal {
            // EC-aware: check this node's own local shard first, falling through to the plain
            // `.obj` path unchanged when the target isn't erasure-coded.
            if let config = req.application.storage[ClusterConfigurationKey.self],
                req.application.storage[ClusterErasureCodingConfigKey.self] != nil,
                let selfRank = responsible.firstIndex(where: { $0.id == config.nodeId }),
                ErasureCodedDeleteCoordinator.localShardExists(
                    bucketName: link.bucketName, key: link.key, versionId: nil, selfRank: selfRank)
            {
                let (meta, body) = try await ErasureCodedReadCoordinator.read(
                    app: req.application, bucketName: link.bucketName, key: link.key,
                    versionId: nil, responsible: responsible, selfNodeId: config.nodeId,
                    requestId: req.id)
                guard !meta.isDeleteMarker else { throw Abort(.notFound) }

                let response = S3Service.buildObjectMetadataResponse(
                    meta: meta, includeBody: false, data: nil, range: nil)
                response.headers.replaceOrAdd(name: .contentLength, value: String(meta.size))
                S3Service.addVersionHeaders(to: response, meta: meta)
                response.body = Response.Body(
                    managedAsyncStream: { writer in
                        for try await chunk in body {
                            try await writer.writeBuffer(chunk)
                        }
                    }, count: meta.size)
                return attachDisposition(to: response, key: link.key)
            }

            // Resolve the object's on-disk path so the body can stream straight from the file -
            // a shared link to a multi-GB object must not buffer it in memory per download.
            // resolvePath also nils delete markers (object deleted from a versioned bucket after
            // the link was created), matching every other GetObject-style read in this codebase.
            if let path = try? ObjectFileHandler.resolvePath(
                bucketName: link.bucketName, key: link.key, versionId: nil),
                let (meta, payloadOffset, payloadSize) = try? ObjectFileHandler.payloadLocation(
                    path: path)
            {
                let response: Response
                if payloadSize > Constants.streamingThreshold {
                    response = S3Service.buildStreamingObjectResponse(
                        req: req, meta: meta, path: path, payloadOffset: payloadOffset)
                } else {
                    // Headers and body from the same read, so they always describe one snapshot of
                    // the file (the streaming branch gets the same guarantee from its ETag check)
                    guard
                        let (freshMeta, data) = try? ObjectFileHandler.read(from: path, loadData: true),
                        let data
                    else {
                        throw Abort(.notFound)
                    }
                    response = S3Service.buildVersionedObjectMetadataResponse(
                        meta: freshMeta, includeBody: true, data: data)
                }
                return attachDisposition(to: response, key: link.key)
            }
        }

        // Not on this node - a shared link is public and can land on any node, so forward to
        // whichever node holds the object; a deleted object resolves nowhere and the responsible
        // node returns 404 same as a local miss.
        //
        // `candidates` is empty whenever `isLocal` was true, but once k+m > 3 being in the wider
        // EC set doesn't mean this node is also in the legacy top-3 a plain object replicated to -
        // fall back to forwarding to the legacy top-3 unless this node genuinely is one of them.
        if let config = req.application.storage[ClusterConfigurationKey.self],
            candidates.isEmpty, !responsible.isEmpty,
            !ObjectRoutingService.isLegacyReplica(responsible: responsible, selfNodeId: config.nodeId)
        {
            return try await ClusterForwardingClient.forward(
                req: req, candidates: Array(responsible.prefix(PlacementService.replicationFactor)))
        }

        guard !candidates.isEmpty else {
            throw Abort(.notFound)
        }
        return try await ClusterForwardingClient.forward(req: req, candidates: candidates)
    }

    /// The share URL itself is an opaque token with no filename in it (by design, so it doesn't
    /// leak the bucket/key) - without this, browsers fall back to naming the download after the
    /// token, with no extension at all.
    private func attachDisposition(to response: Response, key: String) -> Response {
        let fileName = String(key.split(separator: "/").last ?? "download")
        response.headers.replaceOrAdd(
            name: "Content-Disposition",
            value: "attachment; filename=\"\(fileName.contentDispositionFilenameEscaped)\""
        )
        return response
    }
}
