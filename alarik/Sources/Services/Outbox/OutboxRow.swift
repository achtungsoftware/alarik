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

/// Common retry-bookkeeping fields every outbox row (`ReplicationTask`,
/// `ClusterReplicationTask`, `NotificationDelivery`, ...) already has.
///
/// Deliberately not `Model`-constrained: `GenericOutboxDispatcher` is storage-agnostic (fetch/
/// persist/remove are injected closures), so any row type can conform as long as it's a
/// reference type - mutations inside `GenericOutboxDispatcher.deliver` must stay visible to the
/// `persist` closure called right after, without threading a `var` binding through.
/// Delivery state shared by every outbox row. Previously each task type declared its own
/// identical `pending`/`failed` enum and then stored it as a `String`, so every call site compared
/// against `.rawValue`. One `Codable` enum on the protocol makes invalid states unrepresentable and
/// lets call sites read `row.state == .pending`.
enum OutboxRowState: String, Codable, Sendable {
    /// Awaiting its next attempt.
    case pending
    /// Dead-lettered after exhausting its attempts - retained for inspection, never auto-retried.
    case failed
}

protocol OutboxRow: AnyObject, Sendable {
    var state: OutboxRowState { get set }
    var nextAttemptAt: Date { get set }
    var attempts: Int { get set }
    var lastError: String? { get set }
}

/// A row backed by `OutboxMailbox` - every current outbox row type. `ownerNodeId` is the node
/// whose local mailbox directory the task's file lives in (see `OutboxMailbox`'s doc comment for
/// why ownership, not HRW key placement, is the right affinity for outbox records). For
/// `ErasureCodedReplicationTask`/`ClusterReplicationTask` it's a computed alias of `targetNodeId`;
/// for `NotificationDelivery`/`ReplicationTask` it's a real stored field set to whichever node
/// received the triggering write.
protocol OutboxMailboxRow: OutboxRow, Codable {
    var id: UUID { get }
    var ownerNodeId: UUID { get set }
    var createdAt: Date { get }
}
