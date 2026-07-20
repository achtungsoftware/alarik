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

/// This node's cluster identity/config, parsed once in `configure(app:)` from
/// `CLUSTER_NODE_ADDRESS`/`CLUSTER_SECRET` and stashed in `app.storage` under
/// `ClusterConfigurationKey` for every cluster subsystem to read without each re-parsing env vars
/// or re-touching the identity file. Absent entirely on a non-clustered (single-node) node -
/// every cluster subsystem treats a missing value here as "cluster mode is off."
struct ClusterConfiguration: Sendable {
    /// This node's identity, persisted across restarts in `Storage/cluster_node_id`. Must never
    /// change on restart - rendezvous hashing would otherwise treat every restart as a brand new
    /// node joining, and the cluster would believe most objects need to move.
    let nodeId: UUID
    /// This node's internally-reachable base URL (`CLUSTER_NODE_ADDRESS`) - registered in
    /// `cluster_nodes` and used by peers to forward client requests / push replicated objects to
    /// this node.
    let address: String
    /// Shared bearer secret (`CLUSTER_SECRET`), verified on every inter-node request via
    /// `ClusterForwardAuthenticator` - separate from client-facing SigV4/JWT auth, since by the
    /// time a request reaches a peer it was already authenticated once at the entry node.
    let secret: String
    /// Comma-separated peer base URLs from `CLUSTER_SEED_NODES`, used only once, at boot, to
    /// bootstrap this node's initial view of cluster membership before it can place its own
    /// `ClusterNode` metadata record - see `ClusterMembershipLifecycle`'s doc comment. Empty for
    /// the first node of a brand-new cluster (nothing to seed from yet).
    let seeds: [String]

    init(nodeId: UUID, address: String, secret: String, seeds: [String] = []) {
        self.nodeId = nodeId
        self.address = address
        self.secret = secret
        self.seeds = seeds
    }
}

struct ClusterConfigurationKey: StorageKey {
    typealias Value = ClusterConfiguration
}

/// Thrown from `configure(app:)` on a genuinely inconsistent cluster env var combination -
/// letting the app boot half-configured (e.g. an address but no secret) risks a node either
/// silently refusing all inter-node traffic or, worse, accepting it unauthenticated, so this is
/// a hard startup failure rather than a logged warning.
struct ClusterConfigurationError: Error, CustomStringConvertible {
    let description: String
}

enum ClusterNodeIdentity {
    private static let filePath = "Storage/cluster_node_id"

    /// Loads this node's persisted identity, generating and persisting one on first boot. Safe
    /// to call every boot - a second call after the file already exists just reads it back.
    static func loadOrCreate() throws -> UUID {
        if let existing = try? String(contentsOfFile: filePath, encoding: .utf8),
            let uuid = UUID(uuidString: existing.trimmingCharacters(in: .whitespacesAndNewlines))
        {
            return uuid
        }
        let uuid = UUID()
        try FileManager.default.createDirectory(
            atPath: "Storage", withIntermediateDirectories: true)
        try uuid.uuidString.write(toFile: filePath, atomically: true, encoding: .utf8)
        return uuid
    }
}
