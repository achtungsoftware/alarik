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
import Vapor

/// "List every record in a `MetadataStore` collection" - a bounded cluster-wide fan-out, not a
/// hot path (only ever called from admin/console/auth-cache-reload paths, never per-S3-request).
///
/// Placement is per-key HRW hashing, so a single node's local walk only sees the fraction of a
/// collection it's rank-0 for. `list`/`count` therefore fan out to every active node in parallel,
/// merging and deduping by id. Best-effort throughout: an unreachable node just contributes
/// nothing to a given call, rather than failing the whole listing.
enum MetadataListingService {
    struct Entry: Sendable {
        let id: String
        let value: Data
    }

    static func list(app: Application, collection: String) async -> [Entry] {
        var merged: [String: Data] = [:]
        for entry in await localEntries(app: app, collection: collection) {
            merged[entry.id] = entry.value
        }

        guard let config = app.storage[ClusterConfigurationKey.self] else {
            return merged.map { Entry(id: $0.key, value: $0.value) }
        }
        // Every KNOWN peer, not `activeNodes()` - a `.draining` peer is excluded from
        // `activeNodes()` for placement, but can still physically hold records that haven't
        // finished migrating off it yet. Excluding it here would make those records vanish from
        // every other node's listing until the drain finishes, even though the data isn't gone.
        let peers = await ClusterNodeCache.shared.all().filter { $0.id != config.nodeId }
        guard !peers.isEmpty else {
            return merged.map { Entry(id: $0.key, value: $0.value) }
        }

        await withTaskGroup(of: [Entry].self) { group in
            for node in peers {
                group.addTask {
                    await fetchRemote(app: app, node: node, collection: collection)
                }
            }
            for await entries in group {
                for entry in entries where merged[entry.id] == nil {
                    merged[entry.id] = entry.value
                }
            }
        }

        return merged.map { Entry(id: $0.key, value: $0.value) }
    }

    static func count(app: Application, collection: String) async -> Int {
        await list(app: app, collection: collection).count
    }

    /// This node's own contribution only - no network fan-out. Used both by `list`'s local
    /// portion and directly by `InternalClusterMetadataController.handleList` when serving a
    /// peer's fan-out request (a peer must only ever report what it itself holds, never recurse
    /// into fanning out again, or every `list` call would storm the whole cluster).
    static func localEntries(app: Application, collection: String) async -> [Entry] {
        let prefix = "\(collection)/"
        let discovered = ErasureCodedObjectHandler.listLocalShardZeroEntries(
            bucketName: MetadataNamespace.bucketName, keyPrefix: prefix)

        var entries: [Entry] = []
        entries.reserveCapacity(discovered.count)
        for meta in discovered {
            let id = String(meta.key.dropFirst(prefix.count))
            // Must be a real gather (`get`), not a naive "read local shard 0 alone" shortcut:
            // Reed-Solomon reconstruction needs at least `dataShards` distinct shards, so a
            // record with dataShards > 1 can't be decoded from one shard alone - a single-shard
            // read would silently fail and drop the entry from the listing.
            guard let value = try? await MetadataStore.get(app: app, collection: collection, id: id)
            else { continue }
            entries.append(Entry(id: id, value: value))
        }
        return entries
    }

    private static func fetchRemote(
        app: Application, node: ClusterNodeInfo, collection: String
    ) async -> [Entry] {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return [] }
        var outbound = HTTPClientRequest(
            url: node.address + "/internal/cluster/metadata/list"
                + querySuffix(collection: collection))
        outbound.method = .GET
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: config.secret)
        do {
            // `probeTimeout` (5s), not `requestTimeout` (10 minutes, sized for large shard
            // transfers) - this is a bounded control-plane listing call on the boot-time
            // cache-load path, where a long timeout lets one slow/unreachable peer stall boot.
            let response = try await LightweightClusterControlClient.shared.execute(
                outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
            guard response.status == .ok else { return [] }
            let body = try await response.body.collect(upTo: 256 * 1024 * 1024)
            let decoded = try JSONDecoder().decode([WireEntry].self, from: Data(buffer: body))
            return decoded.compactMap { entry in
                guard let data = Data(base64Encoded: entry.value) else { return nil }
                return Entry(id: entry.id, value: data)
            }
        } catch {
            return []
        }
    }

    private static func querySuffix(collection: String) -> String {
        var components = URLComponents()
        components.queryItems = [URLQueryItem(name: "collection", value: collection)]
        return "?" + (components.percentEncodedQuery ?? "")
    }

    struct WireEntry: Codable {
        let id: String
        let value: String
    }
}
