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
import NIOHTTP1
import Vapor

/// The internal-only wire protocol for fanning a bucket-wide scan out to one peer's own local
/// disk - `ClusterListingService` uses this to gather every node's local page before merging.
/// Distinct from `ClusterReplicationClient` (object bytes push/fetch/delete) and
/// `ClusterForwardingClient` (whole-request forwarding) - only ever asks "what do you have
/// locally," never moves object bytes. Responses are small and bounded (`<= maxKeys` entries),
/// so a plain buffered JSON body is used throughout, unlike replication's streaming push/fetch.
enum ClusterListingClient {
    /// Short relative to replication's 10-minute streaming deadline - listing responses are
    /// small bounded JSON, and a hung peer here is stalling an interactive LIST or a
    /// DeleteBucket safety check, not a large background transfer. Matches
    /// `ClusterReplicationService.synchronousTimeout`'s same-private-network assumption.
    static let requestTimeout: TimeAmount = .seconds(10)

    struct ObjectsPageResponse: Codable {
        let objects: [ObjectMeta]
        let commonPrefixes: [String]
        let isTruncated: Bool
    }

    struct VersionsPageResponse: Codable {
        let versions: [ObjectMeta]
        let deleteMarkers: [ObjectMeta]
        let commonPrefixes: [String]
        let isTruncated: Bool
    }

    struct UploadsPageResponse: Codable {
        let uploads: [MultipartUploadMeta]
        let isTruncated: Bool
    }

    struct HasObjectsResponse: Codable {
        let hasObjects: Bool
    }

    struct OwnedStatsResponse: Codable {
        let sizeBytes: Int64
        let objectCount: Int
    }

    static func fetchObjectsPage(
        app: Application, from node: ClusterNodeInfo, bucketName: String, prefix: String,
        delimiter: String?, maxKeys: Int, marker: String?
    ) async throws -> ObjectsPageResponse {
        var query = [("bucket", bucketName), ("prefix", prefix), ("maxKeys", String(maxKeys))]
        if let delimiter { query.append(("delimiter", delimiter)) }
        if let marker { query.append(("marker", marker)) }
        return try await get(
            app: app, node: node, path: "/internal/cluster/listing/objects", query: query)
    }

    static func fetchVersionsPage(
        app: Application, from node: ClusterNodeInfo, bucketName: String, prefix: String,
        delimiter: String?, maxKeys: Int, keyMarker: String?, versionIdMarker: String?
    ) async throws -> VersionsPageResponse {
        var query = [("bucket", bucketName), ("prefix", prefix), ("maxKeys", String(maxKeys))]
        if let delimiter { query.append(("delimiter", delimiter)) }
        if let keyMarker { query.append(("keyMarker", keyMarker)) }
        if let versionIdMarker { query.append(("versionIdMarker", versionIdMarker)) }
        return try await get(
            app: app, node: node, path: "/internal/cluster/listing/versions", query: query)
    }

    static func fetchUploadsPage(
        app: Application, from node: ClusterNodeInfo, bucketName: String, prefix: String,
        maxUploads: Int, keyMarker: String?, uploadIdMarker: String?
    ) async throws -> UploadsPageResponse {
        var query = [("bucket", bucketName), ("prefix", prefix), ("maxUploads", String(maxUploads))]
        if let keyMarker { query.append(("keyMarker", keyMarker)) }
        if let uploadIdMarker { query.append(("uploadIdMarker", uploadIdMarker)) }
        return try await get(
            app: app, node: node, path: "/internal/cluster/listing/uploads", query: query)
    }

    static func fetchHasObjects(
        app: Application, from node: ClusterNodeInfo, bucketName: String
    ) async throws -> Bool {
        let response: HasObjectsResponse = try await get(
            app: app, node: node, path: "/internal/cluster/listing/has-objects",
            query: [("bucket", bucketName)])
        return response.hasObjects
    }

    static func fetchOwnedStats(
        app: Application, from node: ClusterNodeInfo, bucketName: String, prefix: String
    ) async throws -> (sizeBytes: Int64, objectCount: Int) {
        let response: OwnedStatsResponse = try await get(
            app: app, node: node, path: "/internal/cluster/listing/owned-stats",
            query: [("bucket", bucketName), ("prefix", prefix)])
        return (response.sizeBytes, response.objectCount)
    }

    /// Cluster-wide (all-buckets) counterpart of `fetchOwnedStats` - a peer's own storage share
    /// across every bucket it holds anything in, for the admin console's storage-distribution
    /// view.
    static func fetchOwnedStatsAll(
        app: Application, from node: ClusterNodeInfo
    ) async throws -> (sizeBytes: Int64, objectCount: Int) {
        let response: OwnedStatsResponse = try await get(
            app: app, node: node, path: "/internal/cluster/listing/owned-stats-all", query: [])
        return (response.sizeBytes, response.objectCount)
    }

    private static func get<T: Decodable>(
        app: Application, node: ClusterNodeInfo, path: String, query: [(String, String)]
    ) async throws -> T {
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            throw ClusterConfigurationError(description: "This node is not part of a cluster.")
        }

        let allowed = CharacterSet.urlQueryAllowed
        let queryString = query.map { key, value in
            let encodedValue = value.addingPercentEncoding(withAllowedCharacters: allowed) ?? value
            return key + "=" + encodedValue
        }.joined(separator: "&")

        var outbound = HTTPClientRequest(url: "\(node.address)\(path)?\(queryString)")
        outbound.method = .GET
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)

        let response = try await LightweightClusterControlClient.shared.execute(
            outbound, timeout: requestTimeout, logger: app.logger)
        guard (200..<300).contains(response.status.code) else {
            throw ClusterProxyError.pushFailed(status: Int(response.status.code))
        }
        let body = try await response.body.collect(upTo: 16 * 1024 * 1024)
        return try JSONDecoder().decode(T.self, from: body)
    }
}
