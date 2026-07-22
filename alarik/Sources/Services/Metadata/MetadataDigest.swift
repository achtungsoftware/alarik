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
import Crypto
import Foundation
import NIOCore
import Vapor

/// A fingerprint of what one node holds for a collection: enough to answer "has anything changed
/// here since I last looked?" without transferring a single record.
///
/// The periodic full reload exists as a safety net for a dropped cache-invalidation broadcast, and
/// in the overwhelmingly common case nothing has changed at all - yet it used to re-pull every
/// access key and bucket from every peer, once a minute, on every node. That is the same work
/// whether the cluster is idle or not, and it scales with (nodes x records). Comparing a 32-byte
/// digest first turns the idle case into one tiny request per peer.
struct MetadataDigest: Codable, Sendable, Equatable {
    /// Hex-encoded XOR of every record's individual hash. XOR is commutative and associative, so
    /// this is independent of the order records happen to be walked in - two nodes holding the
    /// same records at the same versions always produce the same value.
    let digest: String
    /// Records covered. Carried alongside the digest because XOR alone cannot distinguish an
    /// empty set from a set whose hashes happen to cancel out.
    let count: Int

    static let empty = MetadataDigest(digest: String(repeating: "0", count: 64), count: 0)
}

/// Computes and caches this node's own per-collection digests, and answers the digest endpoint.
actor MetadataDigestService {
    static let shared = MetadataDigestService()

    /// How long a computed digest is reused before being recomputed. Computing one walks this
    /// node's local records for that collection, and every peer probes every other peer each
    /// reload cycle - without a memo that is N scans per node per cycle purely to answer other
    /// people's questions. A few seconds of staleness is irrelevant here: the digest gates a
    /// safety-net reload, never a correctness decision.
    private static let memoTTL: TimeInterval = 5

    private var memo: [String: (digest: MetadataDigest, computedAt: Date)] = [:]

    /// This node's digest for `collection`, recomputed at most once per `memoTTL`.
    func localDigest(app: Application, collection: String, now: Date = Date()) async -> MetadataDigest {
        if let cached = memo[collection], now.timeIntervalSince(cached.computedAt) < Self.memoTTL {
            return cached.digest
        }
        let entries = await MetadataListingService.localEnvelopeEntries(
            app: app, collection: collection)

        var accumulator = [UInt8](repeating: 0, count: 32)
        for entry in entries {
            // Identity + version + liveness. A tombstone must hash differently from the record it
            // replaced, or a delete would look like no change at all.
            let material =
                "\(entry.id)\u{0}\(entry.envelope.updatedAtMillis)\u{0}\(entry.envelope.deleted)"
            let hash = SHA256.hash(data: Data(material.utf8))
            for (index, byte) in hash.enumerated() {
                accumulator[index] ^= byte
            }
        }

        let digest = MetadataDigest(
            digest: accumulator.map { String(format: "%02x", $0) }.joined(),
            count: entries.count)
        memo[collection] = (digest, now)
        return digest
    }
}

/// Decides whether the periodic full cache reload actually has anything to do.
///
/// Tracks the last digest seen from this node and from each peer, per collection. A reload is only
/// worth running when at least one of them has moved - which, in a cluster where nothing is being
/// created or deleted, is never.
///
/// **Scope of the improvement, stated honestly:** this removes the *payload* from the idle case,
/// not the request count - every node still probes every peer once per cycle, so the probe traffic
/// is still O(N^2) per cycle, just with a ~100-byte response instead of the entire contents of
/// several collections. Making the request count itself sub-quadratic means gossip-replicating the
/// control plane rather than polling it, which is a much larger design change than this.
actor MetadataReloadGate {
    static let shared = MetadataReloadGate()

    /// Keyed by peer address (or `localKey` for this node), then collection.
    private var lastSeen: [String: [String: MetadataDigest]] = [:]
    private static let localKey = "\u{0}local"

    /// Forgets everything, so the next check reports "changed". Used by tests, and by any path
    /// that needs to force a genuine reload.
    func reset() {
        lastSeen = [:]
    }

    /// Whether anything in `collections` has changed on this node or any peer since the last call.
    ///
    /// Fails OPEN: a peer that can't be reached, or any probe error at all, counts as "changed" so
    /// the reload still runs. Skipping work is only ever safe on positive evidence that there is
    /// none to do - the cost of a needless reload is a little traffic, while wrongly skipping one
    /// keeps a revoked credential cached.
    func changedSinceLastCheck(app: Application, collections: [String]) async -> Bool {
        var changed = false

        for collection in collections {
            let local = await MetadataDigestService.shared.localDigest(app: app, collection: collection)
            if lastSeen[Self.localKey]?[collection] != local {
                changed = true
            }
            lastSeen[Self.localKey, default: [:]][collection] = local
        }

        guard let config = app.storage[ClusterConfigurationKey.self] else { return changed }
        let peers = await ClusterNodeCache.shared.activeNodes().filter { $0.id != config.nodeId }
        guard !peers.isEmpty else { return changed }

        let probes = await withTaskGroup(
            of: (address: String, digests: [String: MetadataDigest]?).self
        ) { group in
            for peer in peers {
                group.addTask {
                    (
                        peer.address,
                        await Self.probe(
                            app: app, node: peer, collections: collections, secret: config.secret)
                    )
                }
            }
            var results: [(address: String, digests: [String: MetadataDigest]?)] = []
            for await outcome in group { results.append(outcome) }
            return results
        }

        for probe in probes {
            guard let digests = probe.digests else {
                // Unreachable or malformed - assume it changed rather than assume it didn't, and
                // drop the stale baseline so the next successful probe is compared honestly.
                changed = true
                lastSeen.removeValue(forKey: probe.address)
                continue
            }
            for (collection, digest) in digests
            where lastSeen[probe.address]?[collection] != digest {
                changed = true
            }
            lastSeen[probe.address] = digests
        }

        return changed
    }

    private static func probe(
        app: Application, node: ClusterNodeInfo, collections: [String], secret: String
    ) async -> [String: MetadataDigest]? {
        var digests: [String: MetadataDigest] = [:]
        for collection in collections {
            var components = URLComponents()
            components.queryItems = [URLQueryItem(name: "collection", value: collection)]
            let query = "?" + (components.percentEncodedQuery ?? "")
            var outbound = HTTPClientRequest(
                url: node.address + "/internal/cluster/metadata/digest" + query)
            outbound.method = .GET
            outbound.headers.replaceOrAdd(
                name: ClusterForwardAuthenticator.secretHeaderName, value: secret)
            do {
                let response = try await LightweightClusterControlClient.shared.execute(
                    outbound, timeout: ClusterReplicationClient.probeTimeout, logger: app.logger)
                guard response.status == .ok else { return nil }
                let body = try await response.body.collect(upTo: 64 * 1024)
                digests[collection] = try JSONDecoder().decode(
                    MetadataDigest.self, from: Data(buffer: body))
            } catch {
                return nil
            }
        }
        return digests
    }
}
