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

import Vapor

import struct Foundation.Date
import struct Foundation.UUID

/// One member of the object-data cluster. Backed by `MetadataStore`, not Fluent - primary record
/// at `cluster-nodes/<id>`, itself placed/erasure-coded the same as every other metadata record.
/// `ClusterNodeCache` is every node's in-memory, invalidation-synced view of this collection.
/// This is also the one collection with a genuine bootstrap circularity: placing *any* metadata
/// record needs `ClusterNodeCache.shared.activeNodes()`, but membership itself lives here -
/// `ClusterMembershipLifecycle` seeds the local cache from `CLUSTER_SEED_NODES` first to break it.
final class ClusterNode: @unchecked Sendable, MetadataRecord {
    enum Status: String, Codable, Sendable {
        /// Serving traffic normally - eligible to be a placement candidate.
        case active
        /// Being decommissioned: excluded from new placement immediately, but its existing data
        /// is still being migrated off by the rebalance walk before it's safe to stop the process.
        case draining
        /// Fully decommissioned - the rebalance walk confirmed no object is responsible-on this
        /// node anymore. Kept as a record (not deleted) for operational history/traceability.
        case removed
    }

    let id: UUID

    /// This node's internally-reachable base URL (`CLUSTER_NODE_ADDRESS`) - where peers send
    /// forwarded client requests and cluster-replication pushes.
    var address: String

    var status: Status
    var joinedAt: Date

    /// Updated on every heartbeat tick. A node is treated as unavailable by every other node once
    /// this exceeds the staleness window (`ClusterMembershipLifecycle.heartbeatStaleness`) - no
    /// separate failure-detector state, this field *is* the failure detector.
    var lastHeartbeatAt: Date

    /// Self-reported disk capacity, refreshed on every heartbeat tick. `nil` until this node's
    /// first post-upgrade heartbeat - always treated as "unknown" (fail open), never as "full".
    var totalBytes: Int64?
    var availableBytes: Int64?

    init(
        id: UUID,
        address: String,
        status: Status = .active,
        joinedAt: Date = Date(),
        lastHeartbeatAt: Date = Date(),
        totalBytes: Int64? = nil,
        availableBytes: Int64? = nil
    ) {
        self.id = id
        self.address = address
        self.status = status
        self.joinedAt = joinedAt
        self.lastHeartbeatAt = lastHeartbeatAt
        self.totalBytes = totalBytes
        self.availableBytes = availableBytes
    }
}

// MARK: - MetadataStore access

extension ClusterNode {
    static var metadataCollection: String { MetadataCollections.clusterNodes }
    var metadataId: String { id.uuidString }

    static func find(app: Application, id: UUID) async throws -> ClusterNode? {
        try await find(app: app, key: id.uuidString)
    }
}
