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

        guard
            let link = try await SharedLink.find(token, on: req.db),
            link.expiresAt > Date()
        else {
            throw Abort(.notFound)
        }

        let result: (meta: ObjectMeta, data: Data?)
        do {
            result = try ObjectFileHandler.readVersion(
                bucketName: link.bucketName, key: link.key, versionId: nil, loadData: true)
        } catch {
            // Object was deleted (or its bucket was) after the link was created
            throw Abort(.notFound)
        }

        // The latest version may be a delete marker (object deleted from a versioned bucket
        // after the link was created) - it reads successfully with empty data, so this must be
        // checked explicitly, matching every other GetObject-style read in this codebase.
        guard !result.meta.isDeleteMarker, let data = result.data else {
            throw Abort(.notFound)
        }

        let response = S3Service.buildVersionedObjectMetadataResponse(
            meta: result.meta, includeBody: true, data: data)

        // The share URL itself is an opaque token with no filename in it (by design, so it
        // doesn't leak the bucket/key) - without this, browsers fall back to naming the download
        // after the token, with no extension at all.
        let fileName = String(link.key.split(separator: "/").last ?? "download")
        response.headers.replaceOrAdd(
            name: "Content-Disposition",
            value: "attachment; filename=\"\(fileName.contentDispositionFilenameEscaped)\""
        )

        return response
    }
}
