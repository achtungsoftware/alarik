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

/// Public share links for single objects, created and revoked by the bucket owner.
struct InternalSharedLinkController: RouteCollection, ConsoleBucketRoutes {
    struct ShareRequestDTO: Content {
        let bucket: String
        let key: String
        /// Omit (or send null) for a link that never expires - it then works until explicitly
        /// revoked.
        let expiresInSeconds: Int?
    }

    struct ShareResponseDTO: Content {
        let url: String
        /// Nil for a link that never expires.
        let expiresAt: Date?
    }

    /// Cap on how long a shared link can stay valid. Kept at the same 7 days as a S3
    /// presigned URL would allow, for familiarity - but this is an Alarik-specific limit, not
    /// a SigV4 constraint, since shared links don't use SigV4 at all.
    static let maxShareExpirySeconds = 604_800

    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("objects", "share").post(use: self.shareObject)
        routes.grouped("objects", "share").get(use: self.listSharedLinks)
        routes.grouped("objects", "share").grouped(":sharedLinkId").delete(
            use: self.deleteSharedLink)
    }

    /// Creates a public link to an object you own - time-limited when `expiresInSeconds` is
    /// given, or non-expiring when it's omitted (working until explicitly revoked). Nothing
    /// about your account or credentials is exposed - the link is just an opaque, unguessable
    /// token (the new row's own id) that `SharedLinkController` looks up. Revoking access just
    /// means deleting the row, which happens automatically for expired links (see the cleanup
    /// task in configure.swift); non-expiring links are never auto-deleted.
    @Sendable
    func shareObject(req: Request) async throws -> ShareResponseDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        let input = try req.content.decode(ShareRequestDTO.self)

        try await requireOwnedBucketExists(
            req: req, bucketName: input.bucket, userId: auth.userId)

        // The object may live on any node, but this handler must NOT forward - the share URL it
        // returns is built from this node's own base address, so it has to be the one that
        // answers. Check locally first (either format), then (only if not local) confirm
        // existence on a responsible peer with a header-only probe rather than pulling the bytes.
        let localResult = try? await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.readCurrentObject(
                bucketName: input.bucket, key: input.key, loadData: false)
        }
        var existsLocally = (localResult ?? nil) != nil
        let (isLocal, _, responsible) = await ObjectRoutingService.erasureCodedReadPlacement(
            req: req, bucketName: input.bucket, key: input.key)
        if !existsLocally, isLocal, let config = req.application.storage[ClusterConfigurationKey.self],
            responsible.contains(where: { $0.id == config.nodeId })
        {
            existsLocally = ErasureCodedObjectHandler.holdsAnyLocalShard(
                bucketName: input.bucket, key: input.key, versionId: nil)
        }
        if !existsLocally {
            var existsRemotely = false
            // Deliberately not gated on `!isLocal`: that only means self is within the *wider*
            // top-(k+m) set, not that a plain (non-EC) object actually replicated here - once
            // k+m > 3 those diverge, and `existsLocally` above already checked disk directly
            // (both formats), so it's the accurate signal for whether a remote probe is needed.
            if !responsible.isEmpty {
                existsRemotely = await ClusterReplicationClient.objectExists(
                    app: req.application, candidates: responsible, bucketName: input.bucket,
                    key: input.key, versionId: nil)
                if !existsRemotely {
                    // Ask each node what it actually holds, rather than whether it holds the
                    // shard matching its *current* rank. A shard's index is fixed when the object
                    // is written, but ranks move with membership, so after any membership change
                    // the two stop lining up - and probing by rank then reports a perfectly
                    // healthy object as missing. This is the same reason
                    // `ErasureCodedShardGatherer` discovers held indices instead of assuming them.
                    for node in responsible {
                        let held = await ClusterReplicationClient.heldShards(
                            app: req.application, node: node, bucketName: input.bucket,
                            key: input.key, versionId: nil)
                        if let held, !held.isEmpty {
                            existsRemotely = true
                            break
                        }
                    }
                }
            }
            guard existsRemotely else {
                throw Abort(.notFound, reason: "Object not found")
            }
        }

        let expiresAt: Date?
        if let expiresInSeconds = input.expiresInSeconds {
            guard expiresInSeconds > 0, expiresInSeconds <= Self.maxShareExpirySeconds else {
                throw Abort(
                    .badRequest,
                    reason:
                        "expiresInSeconds must be between 1 and \(Self.maxShareExpirySeconds) (7 days), or omitted for a link that never expires"
                )
            }
            expiresAt = Date().addingTimeInterval(TimeInterval(expiresInSeconds))
        } else {
            expiresAt = nil
        }

        let link = SharedLink(
            userId: auth.userId, bucketName: input.bucket, key: input.key, expiresAt: expiresAt)
        try await link.save(app: req.application)

        let url = "\(apiBaseURL)/api/v1/shared/\(link.id.uuidString)"
        return ShareResponseDTO(url: url, expiresAt: expiresAt)
    }

    /// Lists shared links created by the authenticated user, across all of their buckets.
    @Sendable
    func listSharedLinks(req: Request) async throws -> Page<SharedLink.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)

        let links = await SharedLink.all(app: req.application)
            .filter { $0.userId == auth.userId }
            .sorted { $0.createdAt > $1.createdAt }

        return try links.paginated(for: req).map { $0.toResponseDTO() }
    }

    /// Revokes a shared link early, before it would otherwise expire.
    @Sendable
    func deleteSharedLink(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let sharedLinkId = req.parameters.get("sharedLinkId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid shared link ID.")
        }

        guard
            let link = try await SharedLink.find(app: req.application, id: sharedLinkId),
            link.userId == auth.userId
        else {
            throw Abort(.notFound, reason: "Shared link not found.")
        }

        try await link.delete(app: req.application)

        return .noContent
    }
}
