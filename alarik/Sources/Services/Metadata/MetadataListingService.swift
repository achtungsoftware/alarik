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
    /// A record as callers consume it: `value` is the record's own bytes, already unwrapped from
    /// its envelope and migrated to the current schema. Tombstoned ids never appear.
    struct Entry: Sendable {
        let id: String
        let value: Data
    }

    /// A record as it is stored and transmitted between nodes - envelope intact, tombstones
    /// included. The merge needs these; callers of `list` do not.
    struct EnvelopeEntry: Sendable {
        let id: String
        let envelope: MetadataEnvelope
    }

    static func list(app: Application, collection: String) async -> [Entry] {
        var merged: [String: MetadataEnvelope] = [:]

        // Replicas can legitimately disagree (a write or delete that hasn't reached everyone
        // yet), so the winner is decided by `supersedes` rather than by whoever answered first -
        // otherwise the result would depend on gather order, and a stale copy could mask a newer
        // one, including masking a delete.
        func merge(_ entries: [EnvelopeEntry]) {
            for entry in entries {
                if let existing = merged[entry.id], !entry.envelope.supersedes(existing) { continue }
                merged[entry.id] = entry.envelope
            }
        }

        merge(await localEnvelopeEntries(app: app, collection: collection))

        if let config = app.storage[ClusterConfigurationKey.self] {
            // Every KNOWN peer, not `activeNodes()` - a `.draining` peer is excluded from
            // `activeNodes()` for placement, but can still physically hold records that haven't
            // finished migrating off it yet. Excluding it here would make those records vanish
            // from every other node's listing until the drain finishes.
            let peers = await ClusterNodeCache.shared.all().filter { $0.id != config.nodeId }
            if !peers.isEmpty {
                await withTaskGroup(of: [EnvelopeEntry].self) { group in
                    for node in peers {
                        group.addTask {
                            await fetchRemote(app: app, node: node, collection: collection)
                        }
                    }
                    for await entries in group { merge(entries) }
                }
            }
        }

        return merged.compactMap { id, envelope in
            guard !envelope.isTombstone, let payload = envelope.payload else { return nil }
            return Entry(
                id: id,
                value: MetadataMigrations.upgrade(
                    payload: payload, collection: collection,
                    storedVersion: envelope.schemaVersion, logger: app.logger))
        }
    }

    static func count(app: Application, collection: String) async -> Int {
        await list(app: app, collection: collection).count
    }

    /// `list`, decoded into `T`, reporting anything that fails to decode instead of discarding it.
    ///
    /// The distinction matters more than it looks: a record that won't decode is indistinguishable
    /// from a record that doesn't exist to every caller above this line, so silently skipping one
    /// can drop a live access key out of authentication or a bucket out of a listing with no
    /// symptom anywhere. That is exactly what a bad schema change would cause, so it is logged at
    /// error with the collection and id needed to find it.
    static func list<T: Decodable>(
        _ type: T.Type, app: Application, collection: String
    ) async -> [T] {
        let entries = await list(app: app, collection: collection)
        var decoded: [T] = []
        decoded.reserveCapacity(entries.count)
        for entry in entries {
            do {
                decoded.append(try JSONDecoder().decode(T.self, from: entry.value))
            } catch {
                app.logger.error(
                    "Undecodable \(T.self) record '\(collection)/\(entry.id)' excluded from this listing - it is stored but unreadable by this binary: \(error)"
                )
            }
        }
        return decoded
    }

    /// This node's own contribution only - no network fan-out. Used both by `list`'s local
    /// portion and directly by `InternalClusterMetadataController.handleList` when serving a
    /// peer's fan-out request (a peer must only ever report what it itself holds, never recurse
    /// into fanning out again, or every `list` call would storm the whole cluster).
    ///
    /// Envelopes, not payloads: the receiving node needs `updatedAtMillis` to pick a winner and
    /// the tombstone flag to know a record was deleted. Handing it bare payloads would strip
    /// exactly the information the merge runs on, and would make deletes invisible to peers.
    static func localEnvelopeEntries(app: Application, collection: String) async -> [EnvelopeEntry] {
        let prefix = "\(collection)/"
        let discovered = ErasureCodedObjectHandler.listLocalShardZeroEntries(
            bucketName: MetadataNamespace.bucketName, keyPrefix: prefix)

        var entries: [EnvelopeEntry] = []
        entries.reserveCapacity(discovered.count)
        for meta in discovered {
            let id = String(meta.key.dropFirst(prefix.count))
            // Must be a real gather, not a naive "read local shard 0 alone" shortcut:
            // Reed-Solomon reconstruction needs at least `dataShards` distinct shards, so a
            // record with dataShards > 1 can't be decoded from one shard alone - a single-shard
            // read would silently fail and drop the entry from the listing.
            guard
                let envelope = try? await MetadataStore.getEnvelope(
                    app: app, collection: collection, id: id)
            else { continue }
            entries.append(EnvelopeEntry(id: id, envelope: envelope))
        }
        return entries
    }

    /// One retry after a short delay - without it, a peer that's transiently busy (most notably
    /// right after its own restart, still running boot-time catch-up or a reactive rebalance
    /// walk) silently contributes nothing to this collection for the entire call, which for
    /// `LoadCacheLifecycle.reloadAll` on the *caller's* boot path means missing access keys or
    /// buckets right when a freshly-restarted node starts serving real traffic.
    private static let retryDelay: Duration = .milliseconds(750)

    private static func fetchRemote(
        app: Application, node: ClusterNodeInfo, collection: String
    ) async -> [EnvelopeEntry] {
        if let entries = await fetchRemoteOnce(app: app, node: node, collection: collection) {
            return entries
        }
        try? await Task.sleep(for: retryDelay)
        return await fetchRemoteOnce(app: app, node: node, collection: collection) ?? []
    }

    private static func fetchRemoteOnce(
        app: Application, node: ClusterNodeInfo, collection: String
    ) async -> [EnvelopeEntry]? {
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
            guard response.status == .ok else { return nil }
            let body = try await response.body.collect(upTo: 256 * 1024 * 1024)
            let decoded = try JSONDecoder().decode([WireEntry].self, from: Data(buffer: body))
            return decoded.compactMap { entry -> EnvelopeEntry? in
                guard let data = Data(base64Encoded: entry.value) else { return nil }
                return EnvelopeEntry(id: entry.id, envelope: MetadataEnvelope.decode(data))
            }
        } catch {
            return nil
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
