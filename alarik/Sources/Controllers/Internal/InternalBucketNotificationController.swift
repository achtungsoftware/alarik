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

/// Webhook (event notification) configuration and delivery history for a bucket.
struct InternalBucketNotificationController: RouteCollection, ConsoleBucketRoutes {
    struct NotificationConfigDTO: Content {
        let rules: [NotificationRule]
    }

    struct DeliveryDTO: Content {
        let id: UUID
        let ruleId: UUID
        let url: String
        let state: String
        let attempts: Int
        let nextAttemptAt: Date
        let lastError: String?
        let createdAt: Date

        init(from delivery: NotificationDelivery) {
            self.id = delivery.id
            self.ruleId = delivery.ruleId
            self.url = delivery.url
            self.state = delivery.state.rawValue
            self.attempts = delivery.attempts
            self.nextAttemptAt = delivery.nextAttemptAt
            self.lastError = delivery.lastError
            self.createdAt = delivery.createdAt
        }
    }

    struct DeliveriesDTO: Content {
        let deliveries: [DeliveryDTO]
    }

    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications").get(
            use: self.getBucketNotifications)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications")
            .on(
                .PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit),
                use: self.setBucketNotifications)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications")
            .grouped(":ruleId").grouped("test").post(use: self.testBucketNotification)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications")
            .grouped("deliveries").get(use: self.listBucketNotificationDeliveries)
        routes.grouped("buckets").grouped(":bucketName").grouped("notifications")
            .grouped("deliveries").grouped(":deliveryId").grouped("retry")
            .post(use: self.retryBucketNotificationDelivery)
    }

    @Sendable
    func getBucketNotifications(req: Request) async throws -> NotificationConfigDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return NotificationConfigDTO(
            rules: Self.maskedNotificationRules(bucket.notificationConfig?.rules ?? []))
    }

    /// Replaces the bucket's webhook rules wholesale. Validates every rule, assigns ids to
    /// new rules, and gates private-address targets to admins (SSRF).
    @Sendable
    func setBucketNotifications(req: Request) async throws -> NotificationConfigDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(NotificationConfigDTO.self)

        guard input.rules.count <= NotificationConfiguration.maxRuleCount else {
            throw Abort(
                .badRequest,
                reason:
                    "A bucket may have at most \(NotificationConfiguration.maxRuleCount) notification rules."
            )
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)
        let existingRulesById = Dictionary(
            uniqueKeysWithValues: (bucket.notificationConfig?.rules ?? []).map { ($0.id, $0) })

        // Validate + normalize each rule (fresh id for new rules so ids are always server-owned)
        let normalizedRules = try input.rules.map { rule -> NotificationRule in
            try WebhookURLValidator.validateStructure(rule.url)
            if WebhookURLValidator.isInternalHost(rule.url) && !auth.isAdmin {
                throw Abort(
                    .forbidden,
                    reason:
                        "Only administrators can configure webhooks pointing at private or loopback addresses."
                )
            }

            let unknownEvents = rule.events.filter { !NotificationRule.supportedEvents.contains($0) }
            guard unknownEvents.isEmpty else {
                throw Abort(
                    .badRequest, reason: "Unsupported event type(s): \(unknownEvents.joined(separator: ", "))"
                )
            }
            guard !rule.events.isEmpty else {
                throw Abort(.badRequest, reason: "Each notification rule must subscribe to at least one event.")
            }

            var normalized = rule
            if normalized.id == NotificationRule.zeroUUID {
                normalized.id = UUID()
            } else if normalized.secret == Self.secretMaskPlaceholder {
                // The client never sees the real secret (see `secretMaskPlaceholder`) - an
                // untouched edit echoes the placeholder straight back, which means "keep
                // whatever's already stored," not "literally set the secret to this placeholder."
                normalized.secret = existingRulesById[normalized.id]?.secret
            }
            return normalized
        }

        let config = NotificationConfiguration(rules: normalizedRules)
        bucket.notificationConfig = normalizedRules.isEmpty ? nil : config
        try await bucket.save(app: req.application)

        await NotificationConfigCache.shared.setConfig(for: bucketName, config: config)
        CacheInvalidationService.notify(app: req.application, cache: "notificationConfig", op: .upsert, key: bucketName)

        // Stop delivering already-queued events for rules that were just removed - if a rule
        // is deleted because its endpoint is wrong or compromised, in-flight deliveries to the
        // old URL must not keep firing until they exhaust their retries.
        let keptIds = Set(normalizedRules.map(\.id))
        await OutboxMailbox.purgeBucketAcrossCluster(
            NotificationDelivery.self, app: req.application,
            collection: OutboxCollections.notificationDeliveries, bucketName: bucketName
        ) { $0.bucketName == bucketName && !keptIds.contains($0.ruleId) }

        return NotificationConfigDTO(rules: Self.maskedNotificationRules(normalizedRules))
    }

    /// Enqueues an `s3:TestEvent` for one rule, proving end-to-end delivery (incl. signature)
    /// without needing a real object operation.
    @Sendable
    func testBucketNotification(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }
        guard let ruleId = req.parameters.get("ruleId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid rule id")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        guard let rule = bucket.notificationConfig?.rules.first(where: { $0.id == ruleId })
        else {
            throw Abort(.notFound, reason: "Notification rule not found")
        }

        try await NotificationService.emitTestEvent(
            rule: rule, bucketName: bucketName, requestId: req.id, app: req.application)

        return .accepted
    }

    /// Lists the bucket's most recent outbox rows (pending and dead-lettered), most-recent
    /// first - lets an owner see whether their webhooks are actually being delivered, and why
    /// a delivery is stuck or has failed.
    @Sendable
    func listBucketNotificationDeliveries(req: Request) async throws -> DeliveriesDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        let deliveries = await OutboxMailbox.listAllAcrossCluster(
            NotificationDelivery.self, app: req.application,
            collection: OutboxCollections.notificationDeliveries
        )
        .filter { $0.bucketName == bucketName }
        .sorted { $0.createdAt > $1.createdAt }
        .prefix(100)

        return DeliveriesDTO(deliveries: deliveries.map(DeliveryDTO.init))
    }

    /// Requeues a dead-lettered (or still-pending) delivery for immediate redelivery - resets
    /// the retry backoff and attempt count so it gets a fresh run of `maxAttempts`, and wakes
    /// the dispatcher so it doesn't wait for the next background tick.
    @Sendable
    func retryBucketNotificationDelivery(req: Request) async throws -> DeliveryDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }
        guard let deliveryId = req.parameters.get("deliveryId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid delivery id")
        }

        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        // Scope the lookup to this bucket too, not just the id - a delivery id alone doesn't
        // prove the caller owns the bucket it belongs to.
        let deliveries = await OutboxMailbox.listAllAcrossCluster(
            NotificationDelivery.self, app: req.application,
            collection: OutboxCollections.notificationDeliveries)
        guard let delivery = deliveries.first(where: { $0.id == deliveryId && $0.bucketName == bucketName })
        else {
            throw Abort(.notFound, reason: "Delivery not found")
        }

        let retried = await OutboxMailbox.retryAcrossCluster(
            NotificationDelivery.self, app: req.application,
            collection: OutboxCollections.notificationDeliveries, taskId: deliveryId)
        guard retried else {
            throw Abort(.notFound, reason: "Delivery not found")
        }

        // Mirrors exactly what `OutboxMailbox.retryOwned` just reset on whichever node actually
        // owns this delivery - avoids a second cluster-wide fetch just to read back what's
        // already known.
        delivery.state = .pending
        delivery.attempts = 0
        delivery.nextAttemptAt = Date()
        delivery.lastError = nil

        NotificationDispatcher.shared.wake()

        return DeliveryDTO(from: delivery)
    }

    // MARK: - Bucket replication
}
