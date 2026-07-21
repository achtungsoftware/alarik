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

    /// A merged listing plus the honesty flag callers need before treating it as authoritative:
    /// `complete` is true only when this node's own local walk read every record it holds AND
    /// every known peer's contribution arrived (including that peer's own self-reported
    /// completeness). A partial listing is still useful for additive work (cache warm-up), but
    /// only a complete one may ever justify a REMOVAL - "absent from a partial snapshot" and
    /// "deleted" are indistinguishable otherwise.
    struct VerifiedListing {
        let entries: [Entry]
        /// Ids of every live (non-tombstoned) record in the merged view - including records whose
        /// payload a typed decode later drops, so reconcile-style callers never mistake
        /// "stored but undecodable" for "gone".
        let presentIds: Set<String>
        let complete: Bool
    }

    static func list(app: Application, collection: String) async -> [Entry] {
        await listVerified(app: app, collection: collection).entries
    }

    static func listVerified(app: Application, collection: String) async -> VerifiedListing {
        var merged: [String: MetadataEnvelope] = [:]
        var complete = true

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

        let local = await localEnvelopeEntriesVerified(app: app, collection: collection)
        merge(local.entries)
        complete = complete && local.allReadable

        if let config = app.storage[ClusterConfigurationKey.self] {
            // Every KNOWN peer, not `activeNodes()` - a `.draining` peer is excluded from
            // `activeNodes()` for placement, but can still physically hold records that haven't
            // finished migrating off it yet. Excluding it here would make those records vanish
            // from every other node's listing until the drain finishes.
            let peers = await ClusterNodeCache.shared.all().filter { $0.id != config.nodeId }
            if !peers.isEmpty {
                await withTaskGroup(of: (entries: [EnvelopeEntry], ok: Bool).self) { group in
                    for node in peers {
                        group.addTask {
                            await fetchRemote(app: app, node: node, collection: collection)
                        }
                    }
                    for await outcome in group {
                        merge(outcome.entries)
                        complete = complete && outcome.ok
                    }
                }
            }
        }

        let entries = merged.compactMap { id, envelope -> Entry? in
            guard !envelope.isTombstone, let payload = envelope.payload else { return nil }
            return Entry(
                id: id,
                value: MetadataMigrations.upgrade(
                    payload: payload, collection: collection,
                    storedVersion: envelope.schemaVersion, logger: app.logger))
        }
        return VerifiedListing(
            entries: entries, presentIds: Set(entries.map(\.id)), complete: complete)
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
        await listVerified(type, app: app, collection: collection).records
    }

    /// Typed variant of `listVerified`: `presentIds` still covers undecodable records (they exist,
    /// they just can't be read by this binary), so reconciliation callers won't purge them.
    static func listVerified<T: Decodable>(
        _ type: T.Type, app: Application, collection: String
    ) async -> (records: [T], presentIds: Set<String>, complete: Bool) {
        let listing = await listVerified(app: app, collection: collection)
        var decoded: [T] = []
        decoded.reserveCapacity(listing.entries.count)
        for entry in listing.entries {
            do {
                decoded.append(try JSONDecoder().decode(T.self, from: entry.value))
            } catch {
                app.logger.error(
                    "Undecodable \(T.self) record '\(collection)/\(entry.id)' excluded from this listing - it is stored but unreadable by this binary: \(error)"
                )
            }
        }
        return (decoded, listing.presentIds, listing.complete)
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
        await localEnvelopeEntriesVerified(app: app, collection: collection).entries
    }

    /// `localEnvelopeEntries` plus `allReadable`: false when at least one record discovered on
    /// local disk could not be read right now - the caller's view is then partial, and must not
    /// be treated as authoritative for absence.
    static func localEnvelopeEntriesVerified(
        app: Application, collection: String
    ) async -> (entries: [EnvelopeEntry], allReadable: Bool) {
        let prefix = "\(collection)/"
        // Any shard index, not just shard 0: a record must not vanish from cluster-wide listings
        // merely because the one node holding its shard 0 is down, when the remaining shards can
        // still reconstruct it. See `listLocalShardEntries`.
        let discovered = ErasureCodedObjectHandler.listLocalShardEntries(
            bucketName: MetadataNamespace.bucketName, keyPrefix: prefix)

        // Parallel, not sequential: each entry's gather is an independent network round trip,
        // and this whole method must finish inside a peer's `probeTimeout` (5s) when serving
        // a fan-out request - a sequential walk of N records during membership churn can blow
        // that budget and make this node contribute NOTHING to the caller's listing.
        return await withTaskGroup(of: (entry: EnvelopeEntry?, failed: Bool).self) { group in
            for meta in discovered {
                let id = String(meta.key.dropFirst(prefix.count))
                group.addTask {
                    // Replicated metadata (the default `k=1`) means this node's own copy IS the
                    // whole record, so its contribution is a local decode - no per-record cluster
                    // gather at all. That is the difference between a listing costing N local
                    // reads and N gathers each probing every responsible node; the latter
                    // routinely overran the caller's 5s budget, at which point this node
                    // contributed nothing and the listing came back partial.
                    if let envelope = await MetadataStore.localEnvelopeIfWholeCopy(
                        app: app, collection: collection, id: id)
                    {
                        return (entry: EnvelopeEntry(id: id, envelope: envelope), failed: false)
                    }

                    // Older striped (`k > 1`) records genuinely need a gather - Reed-Solomon
                    // needs `dataShards` distinct shards, so one shard alone can't decode them.
                    do {
                        guard
                            let envelope = try await MetadataStore.getEnvelope(
                                app: app, collection: collection, id: id)
                        else {
                            // `nil` from getEnvelope is genuine absence (deleted out from under
                            // the walk) - not a read failure, so it doesn't taint completeness.
                            return (entry: nil, failed: false)
                        }
                        return (entry: EnvelopeEntry(id: id, envelope: envelope), failed: false)
                    } catch {
                        // Never silent: a record that is physically held here but unreadable
                        // right now vanishes from the caller's merged listing, which upstream
                        // turns into "does not exist" - for an access key that means a revoke
                        // 404s or a live key drops out of auth with no symptom anywhere.
                        app.logger.warning(
                            "Local listing skipped '\(collection)/\(id)' - held on disk but not readable right now: \(error)"
                        )
                        return (entry: nil, failed: true)
                    }
                }
            }
            var entries: [EnvelopeEntry] = []
            var allReadable = true
            entries.reserveCapacity(discovered.count)
            for await outcome in group {
                if let entry = outcome.entry { entries.append(entry) }
                allReadable = allReadable && !outcome.failed
            }
            return (entries, allReadable)
        }
    }

    /// One retry after a short delay - without it, a peer that's transiently busy (most notably
    /// right after its own restart, still running boot-time catch-up or a reactive rebalance
    /// walk) silently contributes nothing to this collection for the entire call, which for
    /// `LoadCacheLifecycle.reloadAll` on the *caller's* boot path means missing access keys or
    /// buckets right when a freshly-restarted node starts serving real traffic.
    private static let retryDelay: Duration = .milliseconds(750)

    /// Header the peer's `handleList` uses to self-report whether ITS local walk read every
    /// record it holds - without this, a peer that answered but silently skipped an unreadable
    /// record would look complete to the caller. Absent header (older binary) reads as complete,
    /// matching the old behavior during a rolling upgrade.
    static let listingCompleteHeader = "x-alarik-listing-complete"

    private static func fetchRemote(
        app: Application, node: ClusterNodeInfo, collection: String
    ) async -> (entries: [EnvelopeEntry], ok: Bool) {
        if let outcome = await fetchRemoteOnce(app: app, node: node, collection: collection) {
            return outcome
        }
        try? await Task.sleep(for: retryDelay)
        if let outcome = await fetchRemoteOnce(app: app, node: node, collection: collection) {
            return outcome
        }
        // Never silent: everything this peer holds exclusively is now missing from the merged
        // result, and callers can't tell a partial listing from a complete one.
        app.logger.warning(
            "Peer \(node.address) contributed nothing to the '\(collection)' listing (unreachable or timed out twice) - records only it holds are missing from this result."
        )
        return ([], false)
    }

    private static func fetchRemoteOnce(
        app: Application, node: ClusterNodeInfo, collection: String
    ) async -> (entries: [EnvelopeEntry], ok: Bool)? {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return ([], true) }
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
            let peerComplete = response.headers.first(name: listingCompleteHeader) != "false"
            let body = try await response.body.collect(upTo: 256 * 1024 * 1024)
            let decoded = try JSONDecoder().decode([WireEntry].self, from: Data(buffer: body))
            let entries = decoded.compactMap { entry -> EnvelopeEntry? in
                guard let data = Data(base64Encoded: entry.value) else { return nil }
                return EnvelopeEntry(id: entry.id, envelope: MetadataEnvelope.decode(data))
            }
            return (entries, peerComplete)
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
