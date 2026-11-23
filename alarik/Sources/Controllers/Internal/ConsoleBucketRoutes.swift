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

/// What the console's bucket-scoped controllers share: the self-service ownership check, the
/// body cap for small JSON subresources, and the masking of stored secrets on the way out.
protocol ConsoleBucketRoutes: RouteCollection {}

extension ConsoleBucketRoutes {
    /// Fetches a bucket owned by `userId` or throws the standard "Bucket not found" 404 - the
    /// same self-service ownership check needed by nearly every console bucket endpoint.
    func requireOwnedBucket(req: Request, bucketName: String, userId: UUID) async throws
        -> Bucket
    {
        guard let bucket = try await Bucket.find(app: req.application, name: bucketName),
            bucket.userId == userId
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }
        return bucket
    }

    /// Same ownership check as `requireOwnedBucket`, for endpoints that only need to confirm
    /// the bucket exists and is owned by `userId`, without using the bucket object itself.
    func requireOwnedBucketExists(req: Request, bucketName: String, userId: UUID)
        async throws
    {
        _ = try await requireOwnedBucket(req: req, bucketName: bucketName, userId: userId)
    }

    /// The global `app.routes.defaultMaxBodySize` (`configure.swift`) is set to 5TB - it has to
    /// be, to fit an actual S3 object upload. Every one of these routes is a small JSON
    /// subresource body (a bucket policy, a handful of tags, a few webhook rules) with no
    /// business ever buffering anywhere close to that: registered with the default `body:
    /// .collect` behavior (no explicit `maxSize`), Vapor would happily buffer up to 5TB in
    /// memory before the handler even runs and gets a chance to reject anything. Every route
    /// below that decodes a JSON body (not a file upload) is registered with this override
    /// instead of the `defaultMaxBodySize`-inheriting `.put(use:)`/`.post(use:)` convenience
    /// methods, to actually bound that worst case.
    static var subresourceBodySizeLimit: ByteCount { "1mb" }

    /// Fixed placeholder every already-configured webhook `secret`/replication
    /// `secretAccessKey` is replaced with before being sent to the client - GET and PUT
    /// responses never echo the real value back over the wire, matching the write-only pattern
    /// `AccessKey.ResponseDTO` already uses for access-key secrets. Unlike a full omission,
    /// this stays a non-empty, truthy string so the console can still show "a secret is
    /// configured" (e.g. the webhooks table's "Signed (HMAC)" badge) without ever revealing it,
    /// and it doubles as the "unchanged" sentinel on the way back in: `setBucketNotifications`/
    /// `setReplicationTargets` substitute the real stored secret whenever an existing rule's/
    /// target's incoming value is exactly this placeholder, so the console's existing
    /// edit-prefills-then-echoes-back flow round-trips correctly without ever needing to know
    /// the real secret.
    static var secretMaskPlaceholder: String { "••••••••" }

    static func maskedNotificationRules(_ rules: [NotificationRule]) -> [NotificationRule] {
        rules.map { rule in
            var masked = rule
            if let secret = rule.secret, !secret.isEmpty {
                masked.secret = secretMaskPlaceholder
            }
            return masked
        }
    }

    static func maskedReplicationTargets(_ targets: [ReplicationTarget]) -> [ReplicationTarget] {
        targets.map { target in
            var masked = target
            if !target.secretAccessKey.isEmpty {
                masked.secretAccessKey = secretMaskPlaceholder
            }
            return masked
        }
    }
}
