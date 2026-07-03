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
import Vapor

/// Enqueues replication work into the persistent outbox (`replication_tasks`). Delivery itself
/// happens asynchronously in `ReplicationDispatcher` - the request path only ever pays for a
/// cache lookup, and, when rules match, one SQLite insert per matching rule. Mirrors
/// `NotificationService.emit`'s call shape exactly.
struct ReplicationService {

    /// Enqueues a `put` replication task for every enabled rule (with a resolvable, enabled
    /// target) whose prefix matches `key`. Never throws: a replication enqueue failure must
    /// not fail the object write that triggered it.
    ///
    /// Replication requires versioning, so a nil `versionId` (the bucket has versioning
    /// disabled) is a no-op - a replication rule can't be enabled on an unversioned bucket in
    /// the first place (enforced at rule-save time).
    static func enqueuePut(
        bucketName: String,
        key: String,
        versionId: String?,
        on db: any Database
    ) async {
        guard let versionId else { return }
        await enqueue(
            operation: .put, bucketName: bucketName, key: key, versionId: versionId, on: db)
    }

    /// Enqueues a `delete` replication task for every enabled rule that opted into
    /// `replicateDeletes` and whose prefix matches `key`. Only call this for a delete of the
    /// *current* object (delete-marker creation, or a permanent delete because the bucket
    /// isn't versioned) - never for a client-specified historical-version delete, which has no
    /// meaningful equivalent on the target (see `ReplicationClient.replicateDelete`: version
    /// ids are assigned independently by each S3 endpoint, so a source version id can't
    /// identify anything on the target). `versionId` is kept only for display/traceability on
    /// the task row - it is never sent to the target.
    static func enqueueDelete(
        bucketName: String,
        key: String,
        versionId: String?,
        on db: any Database
    ) async {
        await enqueue(
            operation: .delete, bucketName: bucketName, key: key, versionId: versionId, on: db)
    }

    /// Walks every current object under `rule.prefix` and enqueues a `put` replication task
    /// for each, batching inserts one page at a time (Fluent's `Collection.create(on:)`, a
    /// single statement per page rather than one awaited insert per object) and waking the
    /// dispatcher after every page so replication starts flowing before the whole walk
    /// finishes. Callers must run this off the request path (see `resyncReplicationRule`) - a
    /// bucket with hundreds of thousands of objects can take a long time to enumerate, and nothing
    /// here is bounded by request/HTTP timeouts.
    static func resync(
        bucketName: String,
        rule: ReplicationRule,
        target: ReplicationTarget,
        on db: any Database
    ) async throws -> Int {
        var enqueued = 0
        var marker: String?
        repeat {
            let (objects, _, isTruncated, nextMarker) = try ObjectFileHandler.listObjects(
                bucketName: bucketName, prefix: rule.prefix ?? "", delimiter: nil, maxKeys: 1000,
                marker: marker)

            let tasks = objects.map { object in
                ReplicationTask(
                    bucketName: bucketName, ruleId: rule.id, target: target, key: object.key,
                    versionId: object.versionId, operation: .put)
            }
            if !tasks.isEmpty {
                try await tasks.create(on: db)
                enqueued += tasks.count
                ReplicationDispatcher.shared.wake()
            }

            marker = isTruncated ? nextMarker : nil
        } while marker != nil

        return enqueued
    }

    /// How long a synchronous rule's inline delivery attempt is allowed to run before this
    /// falls back to the normal async outbox. Bounded deliberately: this blocks a live
    /// client-facing request (unlike the background dispatcher, where an unbounded wait only
    /// delays other queued work), so one unreachable target must never be able to hang a
    /// client's connection indefinitely. 20s leaves headroom under most reverse-proxy/client
    /// timeouts while still being long enough for a real multipart transfer of a large object.
    static let synchronousTimeout: Duration = .seconds(20)

    private static func enqueue(
        operation: ReplicationTask.Operation,
        bucketName: String,
        key: String,
        versionId: String?,
        on db: any Database
    ) async {
        guard let config = await ReplicationConfigCache.shared.config(for: bucketName) else {
            return
        }

        let matching = config.rules.filter { rule in
            guard rule.matches(key: key) else { return false }
            if operation == .delete && !rule.replicateDeletes { return false }
            return true
        }
        guard !matching.isEmpty else { return }

        let resolved: [(rule: ReplicationRule, target: ReplicationTarget)] = matching.compactMap {
            rule in
            guard let target = config.target(for: rule.targetId), target.enabled else {
                return nil
            }
            return (rule, target)
        }
        guard !resolved.isEmpty else { return }

        let synchronousRules = resolved.filter { $0.rule.synchronous }
        var needsOutbox = resolved.filter { !$0.rule.synchronous }

        // Synchronous rules are attempted inline, concurrently, before this function returns -
        // every caller already awaits `enqueuePut`/`enqueueDelete` within the request path, so
        // holding the response here for the duration of this task group is exactly what makes
        // a rule "synchronous". A rule that fails or times out falls back to the same async
        // outbox path as any other rule below - the write that triggered this already
        // succeeded unconditionally before this function was ever called, so a slow or
        // unreachable target only ever costs latency here, never correctness.
        if !synchronousRules.isEmpty {
            let deliveries = await withTaskGroup(
                of: (rule: ReplicationRule, target: ReplicationTarget, delivered: Bool).self
            ) { group in
                for (rule, target) in synchronousRules {
                    group.addTask {
                        let delivered = await attemptImmediateDelivery(
                            operation: operation, target: target, bucketName: bucketName,
                            key: key, versionId: versionId, logger: db.logger)
                        return (rule, target, delivered)
                    }
                }
                var results: [(rule: ReplicationRule, target: ReplicationTarget, delivered: Bool)] =
                    []
                for await outcome in group { results.append(outcome) }
                return results
            }
            needsOutbox += deliveries.filter { !$0.delivered }.map { ($0.rule, $0.target) }
        }

        guard !needsOutbox.isEmpty else { return }

        for (rule, target) in needsOutbox {
            let task = ReplicationTask(
                bucketName: bucketName,
                ruleId: rule.id,
                target: target,
                key: key,
                versionId: versionId,
                operation: operation
            )
            do {
                try await task.save(on: db)
            } catch {
                db.logger.error(
                    "Failed to enqueue replication task for bucket '\(bucketName)': \(error)")
            }
        }

        ReplicationDispatcher.shared.wake()
    }

    /// Attempts one delivery directly against `target` (bypassing the outbox entirely), bounded
    /// by `synchronousTimeout`. Returns whether it succeeded; never throws - a failure here just
    /// means the caller falls back to the normal async outbox.
    private static func attemptImmediateDelivery(
        operation: ReplicationTask.Operation,
        target: ReplicationTarget,
        bucketName: String,
        key: String,
        versionId: String?,
        logger: Logger
    ) async -> Bool {
        do {
            try await withTimeout(synchronousTimeout) {
                switch operation {
                case .put:
                    try await ReplicationClient.replicatePut(
                        target: target, bucketName: bucketName, key: key, versionId: versionId)
                case .delete:
                    try await ReplicationClient.replicateDelete(target: target, key: key)
                }
            }
            return true
        } catch {
            logger.warning(
                "Synchronous replication of '\(key)' to \(target.endpoint) failed or timed out - falling back to async retry: \(error)"
            )
            return false
        }
    }

    /// Races `operation` against a `timeout`-second sleep, cancelling whichever loses.
    private static func withTimeout<T: Sendable>(
        _ timeout: Duration,
        operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask { try await operation() }
            group.addTask {
                try await Task.sleep(for: timeout)
                throw ReplicationTimeoutError.timedOut
            }
            defer { group.cancelAll() }
            guard let result = try await group.next() else {
                throw ReplicationTimeoutError.timedOut
            }
            return result
        }
    }
}

enum ReplicationTimeoutError: Error, CustomStringConvertible {
    case timedOut

    var description: String { "Synchronous replication attempt timed out" }
}
