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

import struct Foundation.Date
import struct Foundation.UUID

/// One member of the object-data cluster. Rows in this table are
/// the durable source of truth for membership; `ClusterNodeCache` is every node's in-memory,
/// invalidation-synced view of it - same relationship as every other `{Thing}Cache` to its
/// backing table.
final class ClusterNode: Model, @unchecked Sendable {
    static let schema = "cluster_nodes"

    enum Status: String {
        /// Serving traffic normally - eligible to be a placement candidate.
        case active
        /// Being decommissioned: excluded from new placement immediately, but its existing data
        /// is still being migrated off by the rebalance walk before it's safe to stop the process.
        case draining
        /// Fully decommissioned - the rebalance walk confirmed no object is responsible-on this
        /// node anymore. Kept as a row (not deleted) for operational history/traceability.
        case removed
    }

    @ID(key: .id)
    var id: UUID?

    /// This node's internally-reachable base URL (`CLUSTER_NODE_ADDRESS`) - where peers send
    /// forwarded client requests and cluster-replication pushes.
    @Field(key: "address")
    var address: String

    @Field(key: "status")
    var status: String

    @Field(key: "joined_at")
    var joinedAt: Date

    /// Updated on every heartbeat tick. A node is treated as unavailable by every other node once
    /// this exceeds the staleness window (`ClusterMembershipLifecycle.heartbeatStaleness`) - no
    /// separate failure-detector state, this field *is* the failure detector.
    @Field(key: "last_heartbeat_at")
    var lastHeartbeatAt: Date

    /// Self-reported disk capacity, refreshed on every heartbeat tick. `nil` until this node's
    /// first post-upgrade heartbeat - always treated as "unknown" (fail open), never as "full".
    @OptionalField(key: "total_bytes")
    var totalBytes: Int64?

    @OptionalField(key: "available_bytes")
    var availableBytes: Int64?

    init() {}

    init(
        id: UUID? = nil,
        address: String,
        status: Status = .active,
        joinedAt: Date = Date(),
        lastHeartbeatAt: Date = Date(),
        totalBytes: Int64? = nil,
        availableBytes: Int64? = nil
    ) {
        self.id = id
        self.address = address
        self.status = status.rawValue
        self.joinedAt = joinedAt
        self.lastHeartbeatAt = lastHeartbeatAt
        self.totalBytes = totalBytes
        self.availableBytes = availableBytes
    }
}
