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

/// Bucket replication: targets, rules, resync, and the task queue behind them.
struct InternalBucketReplicationController: RouteCollection, ConsoleBucketRoutes {
    struct ReplicationTargetsDTO: Content {
        let targets: [ReplicationTarget]
    }

    struct ReplicationRulesDTO: Content {
        let rules: [ReplicationRule]
    }

    struct ReplicationTaskDTO: Content {
        let id: UUID
        let ruleId: UUID
        let targetId: UUID
        let endpoint: String
        let key: String
        let versionId: String?
        let operation: String
        let state: String
        let attempts: Int
        let nextAttemptAt: Date
        let lastError: String?
        let createdAt: Date

        init(from task: ReplicationTask) {
            self.id = task.id
            self.ruleId = task.ruleId
            self.targetId = task.targetId
            self.endpoint = task.endpoint
            self.key = task.key
            self.versionId = task.versionId
            self.operation = task.operation.rawValue
            self.state = task.state.rawValue
            self.attempts = task.attempts
            self.nextAttemptAt = task.nextAttemptAt
            self.lastError = task.lastError
            self.createdAt = task.createdAt
        }
    }

    struct ReplicationTasksDTO: Content {
        let tasks: [ReplicationTaskDTO]
    }

    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("targets").get(use: self.getReplicationTargets)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("targets")
            .on(
                .PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.setReplicationTargets)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("rules").get(use: self.getReplicationRules)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("rules")
            .on(
                .PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.setReplicationRules)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("rules").grouped(":ruleId").grouped("resync")
            .post(use: self.resyncReplicationRule)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("tasks").get(use: self.listReplicationTasks)
        routes.grouped("buckets").grouped(":bucketName").grouped("replication")
            .grouped("tasks").grouped(":taskId").grouped("retry")
            .post(use: self.retryReplicationTask)
    }

    @Sendable
    func getReplicationTargets(req: Request) async throws -> ReplicationTargetsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return ReplicationTargetsDTO(
            targets: Self.maskedReplicationTargets(bucket.replicationConfig?.targets ?? []))
    }

    /// Replaces the bucket's replication targets wholesale. Validates every target's endpoint
    /// and gates private-address targets to admins (SSRF), same as webhook URLs. Removing a
    /// target that's still referenced by a rule auto-disables that rule rather than leaving it
    /// pointing at a target that no longer exists.
    @Sendable
    func setReplicationTargets(req: Request) async throws -> ReplicationTargetsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(ReplicationTargetsDTO.self)

        guard input.targets.count <= ReplicationConfiguration.maxTargetCount else {
            throw Abort(
                .badRequest,
                reason:
                    "A bucket may have at most \(ReplicationConfiguration.maxTargetCount) replication targets."
            )
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)
        let existingConfig = bucket.replicationConfig ?? .empty
        let existingTargetsById = Dictionary(
            uniqueKeysWithValues: existingConfig.targets.map { ($0.id, $0) })

        let normalizedTargets = try input.targets.map { target -> ReplicationTarget in
            try WebhookURLValidator.validateStructure(target.endpoint)
            if WebhookURLValidator.isInternalHost(target.endpoint) && !auth.isAdmin {
                throw Abort(
                    .forbidden,
                    reason:
                        "Only administrators can configure replication targets pointing at private or loopback addresses."
                )
            }
            guard !target.targetBucket.isEmpty else {
                throw Abort(
                    .badRequest, reason: "Each replication target requires a destination bucket.")
            }

            var normalized = target
            if normalized.id == ReplicationTarget.zeroUUID {
                normalized.id = UUID()
            } else if normalized.secretAccessKey == Self.secretMaskPlaceholder {
                // The client never sees the real secret (see `secretMaskPlaceholder`) - an
                // untouched edit echoes the placeholder straight back, which means "keep
                // whatever's already stored," not "literally set the secret to this placeholder."
                normalized.secretAccessKey = existingTargetsById[normalized.id]?.secretAccessKey ?? ""
            }

            guard !normalized.accessKeyId.isEmpty, !normalized.secretAccessKey.isEmpty else {
                throw Abort(.badRequest, reason: "Each replication target requires credentials.")
            }
            return normalized
        }

        let existingRules = existingConfig.rules
        let keptTargetIds = Set(normalizedTargets.map(\.id))
        // A rule referencing a target that no longer exists is disabled, never left dangling -
        // same "clean up, don't leave a dangling reference" precedent used when a webhook rule
        // is removed.
        let adjustedRules = existingRules.map { rule -> ReplicationRule in
            guard keptTargetIds.contains(rule.targetId) else {
                var disabled = rule
                disabled.enabled = false
                return disabled
            }
            return rule
        }

        let config = ReplicationConfiguration(targets: normalizedTargets, rules: adjustedRules)
        bucket.replicationConfig =
            (normalizedTargets.isEmpty && adjustedRules.isEmpty) ? nil : config
        try await bucket.save(app: req.application)

        await ReplicationConfigCache.shared.setConfig(for: bucketName, config: config)
        CacheInvalidationService.notify(app: req.application, cache: "replicationConfig", op: .upsert, key: bucketName)

        return ReplicationTargetsDTO(targets: Self.maskedReplicationTargets(normalizedTargets))
    }

    @Sendable
    func getReplicationRules(req: Request) async throws -> ReplicationRulesDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return ReplicationRulesDTO(rules: bucket.replicationConfig?.rules ?? [])
    }

    /// Replaces the bucket's replication rules wholesale. A non-empty rule set requires the
    /// bucket's versioning to be `Enabled` (without it, `versionId` can't unambiguously
    /// identify what to replicate), and every rule's `targetId` must resolve to a target
    /// already configured on this bucket.
    @Sendable
    func setReplicationRules(req: Request) async throws -> ReplicationRulesDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(ReplicationRulesDTO.self)

        guard input.rules.count <= ReplicationConfiguration.maxRuleCount else {
            throw Abort(
                .badRequest,
                reason:
                    "A bucket may have at most \(ReplicationConfiguration.maxRuleCount) replication rules."
            )
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        guard input.rules.isEmpty || bucket.isVersioningEnabled else {
            throw Abort(
                .badRequest,
                reason: "Replication rules require the bucket's versioning to be Enabled.")
        }

        let existingTargets = bucket.replicationConfig?.targets ?? []
        let targetIds = Set(existingTargets.map(\.id))

        let normalizedRules = try input.rules.map { rule -> ReplicationRule in
            guard targetIds.contains(rule.targetId) else {
                throw Abort(.badRequest, reason: "Rule references an unknown replication target.")
            }

            var normalized = rule
            if normalized.id == ReplicationRule.zeroUUID {
                normalized.id = UUID()
            }
            return normalized
        }

        let config = ReplicationConfiguration(targets: existingTargets, rules: normalizedRules)
        bucket.replicationConfig =
            (existingTargets.isEmpty && normalizedRules.isEmpty) ? nil : config
        try await bucket.save(app: req.application)

        await ReplicationConfigCache.shared.setConfig(for: bucketName, config: config)
        CacheInvalidationService.notify(app: req.application, cache: "replicationConfig", op: .upsert, key: bucketName)

        // Stop delivering already-queued tasks for rules that were just removed - same
        // reasoning as the equivalent webhook-delivery purge above.
        let keptIds = Set(normalizedRules.map(\.id))
        await OutboxMailbox.purgeBucketAcrossCluster(
            ReplicationTask.self, app: req.application, collection: OutboxCollections.replicationTasks,
            bucketName: bucketName
        ) { $0.bucketName == bucketName && !keptIds.contains($0.ruleId) }

        return ReplicationRulesDTO(rules: normalizedRules)
    }

    /// Triggers a walk of the bucket's current objects under `rule.prefix`, enqueueing a `put`
    /// replication task for each - the explicit "do it now" trigger for a rule that already
    /// opted into `replicateExisting`, a manual action, never automatic.
    ///
    /// The walk itself runs in the background (`ReplicationService.resync`), not inline in this
    /// request: a bucket with hundreds of thousands of objects could otherwise hold the HTTP
    /// request open for minutes, well past any reasonable client/proxy timeout. This returns
    /// `202 Accepted` as soon as the rule/target have been validated - progress is only
    /// observable via the replication tasks list, same as any other asynchronously-delivered
    /// replication work.
    @Sendable
    func resyncReplicationRule(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }
        guard let ruleId = req.parameters.get("ruleId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid rule id")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        guard let config = bucket.replicationConfig else {
            throw Abort(.notFound, reason: "Replication rule not found")
        }
        guard let rule = config.rules.first(where: { $0.id == ruleId }) else {
            throw Abort(.notFound, reason: "Replication rule not found")
        }
        guard rule.enabled else {
            throw Abort(.badRequest, reason: "Cannot resync a disabled replication rule.")
        }
        guard rule.replicateExisting else {
            throw Abort(
                .badRequest,
                reason:
                    "This rule has not opted into existing-object replication (replicateExisting)."
            )
        }
        guard let target = config.target(for: rule.targetId), target.enabled else {
            throw Abort(
                .badRequest, reason: "This rule's replication target is missing or disabled.")
        }

        // Scoped to the Application, not the Request - this must keep running after the
        // response below has been sent and `req` may have gone away.
        let app = req.application
        let logger = req.application.logger
        Task {
            do {
                let enqueued = try await ReplicationService.resync(
                    app: app, bucketName: bucketName, rule: rule, target: target)
                logger.info(
                    "Replication resync for bucket '\(bucketName)' rule '\(rule.id)' enqueued \(enqueued) object(s)"
                )
            } catch {
                logger.error(
                    "Replication resync failed for bucket '\(bucketName)' rule '\(rule.id)': \(error)"
                )
            }
        }

        return .accepted
    }

    /// Lists the bucket's most recent replication outbox rows (pending and dead-lettered),
    /// most-recent first - the replication equivalent of `listBucketNotificationDeliveries`.
    @Sendable
    func listReplicationTasks(req: Request) async throws -> ReplicationTasksDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let tasks = await OutboxMailbox.listAllAcrossCluster(
            ReplicationTask.self, app: req.application, collection: OutboxCollections.replicationTasks
        )
        .filter { $0.bucketName == bucketName }
        .sorted { $0.createdAt > $1.createdAt }
        .prefix(100)

        return ReplicationTasksDTO(tasks: tasks.map(ReplicationTaskDTO.init))
    }

    /// Requeues a dead-lettered (or still-pending) replication task for immediate retry -
    /// resets the retry backoff and attempt count, then wakes the dispatcher.
    @Sendable
    func retryReplicationTask(req: Request) async throws -> ReplicationTaskDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }
        guard let taskId = req.parameters.get("taskId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid task id")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        // Scope the lookup to this bucket too, not just the id - a task id alone doesn't prove
        // the caller owns the bucket it belongs to.
        let allTasks = await OutboxMailbox.listAllAcrossCluster(
            ReplicationTask.self, app: req.application, collection: OutboxCollections.replicationTasks)
        guard let task = allTasks.first(where: { $0.id == taskId && $0.bucketName == bucketName }) else {
            throw Abort(.notFound, reason: "Replication task not found")
        }

        let retried = await OutboxMailbox.retryAcrossCluster(
            ReplicationTask.self, app: req.application, collection: OutboxCollections.replicationTasks,
            taskId: taskId)
        guard retried else {
            throw Abort(.notFound, reason: "Replication task not found")
        }

        // Mirrors exactly what `OutboxMailbox.retryOwned` just reset on whichever node actually
        // owns this task - avoids a second cluster-wide fetch just to read back what's already
        // known.
        task.state = .pending
        task.attempts = 0
        task.nextAttemptAt = Date()
        task.lastError = nil

        ReplicationDispatcher.shared.wake()

        return ReplicationTaskDTO(from: task)
    }
}
