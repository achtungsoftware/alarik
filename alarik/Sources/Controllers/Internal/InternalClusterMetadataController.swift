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

/// The internal-only receiving side of `MetadataStore`'s rank-0 write forwarding
/// (`forwardPut`/`forwardPutIfAbsent`/`forwardDelete`/`forwardConsumeIfPresent`) - never
/// reachable by S3 clients, guarded entirely by `ClusterSecretMiddleware`. A request landing here
/// is trusted as "the caller already resolved this node as rank-0 for this key", secured by the
/// shared cluster secret rather than a receiving-side re-verification. Reads (`GET`) have no
/// route here: `MetadataStore.get` never forwards - gather-and-decode is idempotent from any node.
struct InternalClusterMetadataController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        let cluster = routes.grouped("internal", "cluster", "metadata").grouped(
            ClusterSecretMiddleware())
        cluster.on(.POST, "put", body: .collect(maxSize: "16mb"), use: handlePut)
        cluster.on(.POST, "put-if-absent", body: .collect(maxSize: "16mb"), use: handlePutIfAbsent)
        cluster.on(.POST, "delete", use: handleDelete)
        cluster.on(.POST, "consume-if-present", use: handleConsumeIfPresent)
        cluster.get("list", use: handleList)

        // Cache-invalidation broadcast - kept top-level (not under .../metadata) since it isn't
        // itself a metadata-record operation.
        routes.grouped("internal", "cluster", "cache-invalidate").grouped(ClusterSecretMiddleware())
            .on(.POST, "", body: .collect(maxSize: "64kb"), use: handleCacheInvalidate)

        // Membership bootstrap - see `ClusterMembershipLifecycle`'s doc comment for why a joining
        // node needs this before it can place any metadata record at all, including its own.
        routes.grouped("internal", "cluster", "members").grouped(ClusterSecretMiddleware())
            .get(use: handleMembers)
    }

    struct ClusterMemberWire: Codable, Sendable {
        let id: UUID
        let address: String
        let status: String
        let lastHeartbeatAt: Date
        let totalBytes: Int64?
        let availableBytes: Int64?
    }

    /// Reports this node's own local `ClusterNodeCache` snapshot - not a fan-out (a joining node
    /// intentionally only asks ONE seed, taking whatever view that seed currently has; it isn't
    /// trying to assemble an authoritative merge, just enough of a starting point to correctly
    /// place its own record, which then propagates the rest via the normal upsert broadcast).
    @Sendable
    func handleMembers(req: Request) async throws -> Response {
        let nodes = await ClusterNodeCache.shared.all()
        let wire = nodes.map {
            ClusterMemberWire(
                id: $0.id, address: $0.address, status: $0.status.rawValue,
                lastHeartbeatAt: $0.lastHeartbeatAt, totalBytes: $0.totalBytes,
                availableBytes: $0.availableBytes)
        }
        let response = Response(status: .ok)
        response.headers.replaceOrAdd(name: .contentType, value: "application/json")
        response.body = try Response.Body(data: JSONEncoder().encode(wire))
        return response
    }

    /// The receiving side of `CacheInvalidationService.notify`'s broadcast - decodes the message
    /// and applies it via `CacheReloadDispatch.apply`.
    @Sendable
    func handleCacheInvalidate(req: Request) async throws -> HTTPStatus {
        guard let bodyBuffer = req.body.data,
            let message = try? JSONDecoder().decode(
                CacheInvalidationMessage.self, from: Data(buffer: bodyBuffer))
        else {
            throw Abort(.badRequest, reason: "Invalid cache invalidation payload")
        }
        try await CacheReloadDispatch.apply(message: message, app: req.application)
        return .ok
    }

    /// The receiving side of `MetadataListingService`'s cluster-wide fan-out - reports only this
    /// node's own local contribution, never recursing into fanning out again itself, or every
    /// `list` call would storm the whole cluster.
    ///
    /// Transmits whole envelopes: the caller merges replicas by `updatedAtMillis` and needs to
    /// see tombstones, so stripping either here would let a stale copy beat a newer one and make
    /// deletes invisible cluster-wide.
    @Sendable
    func handleList(req: Request) async throws -> Response {
        guard let collection = req.query[String.self, at: "collection"] else {
            throw Abort(.badRequest, reason: "Missing collection query parameter")
        }
        let local = await MetadataListingService.localEnvelopeEntriesVerified(
            app: req.application, collection: collection)
        let wire = try local.entries.map {
            MetadataListingService.WireEntry(
                id: $0.id, value: try $0.envelope.encoded().base64EncodedString())
        }
        let response = Response(status: .ok)
        response.headers.replaceOrAdd(name: .contentType, value: "application/json")
        // Self-reported completeness: a skipped-unreadable record here must taint the CALLER's
        // completeness too, or "answered but partial" would be indistinguishable from complete.
        response.headers.replaceOrAdd(
            name: MetadataListingService.listingCompleteHeader,
            value: local.allReadable ? "true" : "false")
        response.body = try Response.Body(data: JSONEncoder().encode(wire))
        return response
    }

    private func collectionAndId(req: Request) throws -> (collection: String, id: String) {
        guard
            let collection = req.query[String.self, at: "collection"],
            let id = req.query[String.self, at: "id"]
        else {
            throw Abort(.badRequest, reason: "Missing collection/id query parameters")
        }
        return (collection, id)
    }

    @Sendable
    func handlePut(req: Request) async throws -> HTTPStatus {
        let (collection, id) = try collectionAndId(req: req)
        guard let bodyBuffer = req.body.data else {
            throw Abort(.badRequest, reason: "Missing request body")
        }
        try await MetadataStore.executeLocalPut(
            app: req.application, collection: collection, id: id, value: Data(buffer: bodyBuffer))
        return .ok
    }

    @Sendable
    func handlePutIfAbsent(req: Request) async throws -> Response {
        let (collection, id) = try collectionAndId(req: req)
        guard let bodyBuffer = req.body.data else {
            throw Abort(.badRequest, reason: "Missing request body")
        }
        let created = try await MetadataStore.executeLocalPutIfAbsent(
            app: req.application, collection: collection, id: id, value: Data(buffer: bodyBuffer))
        let response = Response(status: .ok)
        response.headers.replaceOrAdd(name: "X-Alarik-Created", value: created ? "true" : "false")
        return response
    }

    @Sendable
    func handleDelete(req: Request) async throws -> HTTPStatus {
        let (collection, id) = try collectionAndId(req: req)
        try await MetadataStore.executeLocalDelete(
            app: req.application, collection: collection, id: id)
        return .ok
    }

    @Sendable
    func handleConsumeIfPresent(req: Request) async throws -> Response {
        let (collection, id) = try collectionAndId(req: req)
        guard
            let value = try await MetadataStore.executeLocalConsumeIfPresent(
                app: req.application, collection: collection, id: id)
        else {
            throw Abort(.notFound, reason: "No metadata record for this collection/id")
        }
        let response = Response(status: .ok)
        response.headers.replaceOrAdd(name: .contentType, value: "application/octet-stream")
        response.body = Response.Body(data: value)
        return response
    }
}
