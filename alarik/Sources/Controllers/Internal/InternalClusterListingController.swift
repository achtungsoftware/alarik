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

import struct Foundation.UUID

/// The internal-only receiving side of `ClusterListingClient` - never reachable by S3 clients,
/// guarded entirely by `ClusterSecretMiddleware`. Every request here answers "what do you have
/// locally," purely from this node's own disk - no recursion, no further fan-out.
/// `ClusterListingService` is what actually merges these per-node answers into a cluster-wide
/// result; this controller only ever serves one node's own contribution.
struct InternalClusterListingController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        let cluster = routes.grouped("internal", "cluster", "listing").grouped(
            ClusterSecretMiddleware())
        cluster.get("objects", use: handleObjects)
        cluster.get("versions", use: handleVersions)
        cluster.get("uploads", use: handleUploads)
        cluster.get("has-objects", use: handleHasObjects)
        cluster.get("owned-stats", use: handleOwnedStats)
        cluster.get("owned-stats-all", use: handleOwnedStatsAll)
    }

    @Sendable
    func handleObjects(req: Request) async throws -> Response {
        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing bucket query parameter")
        }
        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let delimiter = req.query[String.self, at: "delimiter"]
        let maxKeys = req.query[Int.self, at: "maxKeys"] ?? 1000
        let marker = req.query[String.self, at: "marker"]

        let (objects, commonPrefixes, isTruncated, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName, prefix: prefix, delimiter: delimiter, maxKeys: maxKeys,
            marker: marker)

        return try jsonResponse(
            ClusterListingClient.ObjectsPageResponse(
                objects: objects, commonPrefixes: commonPrefixes, isTruncated: isTruncated))
    }

    @Sendable
    func handleVersions(req: Request) async throws -> Response {
        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing bucket query parameter")
        }
        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let delimiter = req.query[String.self, at: "delimiter"]
        let maxKeys = req.query[Int.self, at: "maxKeys"] ?? 1000
        let keyMarker = req.query[String.self, at: "keyMarker"]
        let versionIdMarker = req.query[String.self, at: "versionIdMarker"]

        let (versions, deleteMarkers, commonPrefixes, isTruncated, _, _) =
            try ObjectFileHandler.listAllVersions(
                bucketName: bucketName, prefix: prefix, delimiter: delimiter,
                keyMarker: keyMarker, versionIdMarker: versionIdMarker, maxKeys: maxKeys)

        return try jsonResponse(
            ClusterListingClient.VersionsPageResponse(
                versions: versions, deleteMarkers: deleteMarkers, commonPrefixes: commonPrefixes,
                isTruncated: isTruncated))
    }

    @Sendable
    func handleUploads(req: Request) async throws -> Response {
        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing bucket query parameter")
        }
        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let maxUploads = req.query[Int.self, at: "maxUploads"] ?? 1000
        let keyMarker = req.query[String.self, at: "keyMarker"]
        let uploadIdMarker = req.query[String.self, at: "uploadIdMarker"]

        let (uploads, isTruncated, _, _) = try MultipartFileHandler.listUploads(
            bucketName: bucketName, prefix: prefix, keyMarker: keyMarker,
            uploadIdMarker: uploadIdMarker, maxUploads: maxUploads)

        return try jsonResponse(
            ClusterListingClient.UploadsPageResponse(uploads: uploads, isTruncated: isTruncated))
    }

    @Sendable
    func handleHasObjects(req: Request) async throws -> Response {
        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing bucket query parameter")
        }
        let hasObjects = ObjectFileHandler.hasBucketObjects(bucketName: bucketName)
        return try jsonResponse(ClusterListingClient.HasObjectsResponse(hasObjects: hasObjects))
    }

    @Sendable
    func handleOwnedStats(req: Request) async throws -> Response {
        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing bucket query parameter")
        }
        let prefix = req.query[String.self, at: "prefix"] ?? ""
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            throw Abort(.serviceUnavailable, reason: "This node is not part of a cluster.")
        }
        let active = await ClusterNodeCache.shared.activeNodes()
        let stats = try ClusterListingService.ownedStats(
            bucketName: bucketName, prefix: prefix, activeNodes: active,
            selfNodeId: config.nodeId)

        return try jsonResponse(
            ClusterListingClient.OwnedStatsResponse(
                sizeBytes: stats.sizeBytes, objectCount: stats.objectCount))
    }

    /// Cluster-wide (all-buckets) counterpart of `handleOwnedStats` - this node's own storage
    /// share across every bucket, for the admin console's storage-distribution view.
    @Sendable
    func handleOwnedStatsAll(req: Request) async throws -> Response {
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            throw Abort(.serviceUnavailable, reason: "This node is not part of a cluster.")
        }
        let active = await ClusterNodeCache.shared.activeNodes()
        let stats = try await ClusterListingService.ownedStatsAllBuckets(
            on: req.db, activeNodes: active, selfNodeId: config.nodeId)

        return try jsonResponse(
            ClusterListingClient.OwnedStatsResponse(
                sizeBytes: stats.sizeBytes, objectCount: stats.objectCount))
    }

    private func jsonResponse<T: Encodable>(_ value: T) throws -> Response {
        let response = Response(status: .ok)
        response.headers.replaceOrAdd(name: .contentType, value: "application/json")
        response.body = try Response.Body(data: JSONEncoder().encode(value))
        return response
    }
}
