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
import Foundation

/// Common retry-bookkeeping fields every outbox row (`ReplicationTask`,
/// `ClusterReplicationTask`, `NotificationDelivery`, ...) already has.
///
/// No state-value strings or `createdAt` here - fetch/purge queries need real Fluent
/// `KeyPath`s (`\.$state == ...`), which stays per-dispatcher. This protocol only covers
/// post-fetch bookkeeping on an already-loaded row.
protocol OutboxRow: Model, Sendable {
    var state: String { get set }
    var nextAttemptAt: Date { get set }
    var attempts: Int { get set }
    var lastError: String? { get set }
}
