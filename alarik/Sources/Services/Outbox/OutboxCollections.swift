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

/// The 4 `OutboxMailbox` collection names - used consistently by the dispatchers, the internal
/// cluster outbox RPCs, and every admin/console call site so a typo can't silently create a 5th,
/// orphaned mailbox directory.
enum OutboxCollections {
    static let notificationDeliveries = "notification-deliveries"
    static let replicationTasks = "replication-tasks"
    static let clusterReplicationTasks = "cluster-replication-tasks"
    static let erasureCodedReplicationTasks = "erasure-coded-replication-tasks"
}
