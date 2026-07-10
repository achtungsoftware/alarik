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
            let link = try await SharedLink.find(token, on: req.db),
            link.expiresAt.map({ $0 > Date() }) ?? true
        else {
            throw Abort(.notFound)
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

        // Not on this node - a shared link is public and lands on whichever node the load
        // balancer picked, which needn't be one that holds the object. The token lookup above
        // works from the shared DB on any node, but the bytes only live on the responsible
        // nodes, so forward there (the object never being local, unlike an authed GET this can't
        // fall through to a local read). A deleted object resolves nowhere and the responsible
        // node returns 404 the same as a local miss would.
        let (isLocal, candidates) = await ObjectRoutingService.isResponsible(
            req: req, bucketName: link.bucketName, key: link.key)
        guard !isLocal, !candidates.isEmpty else {
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
