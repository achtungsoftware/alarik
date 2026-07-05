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
import FluentSQLiteDriver
import JWTKit
import NIOSSL
import Vapor

public func configure(_ app: Application) async throws {

    // TODO: make this configurable ?
    app.routes.defaultMaxBodySize = "5tb"
    app.http.server.configuration.supportPipelining = true

    let consoleBaseUrl = ConsoleBaseURL.resolve()

    #if DEBUG
        // In debug, test & profiling - store the db relative to the work dir
        app.databases.use(.sqlite(.file("db.sqlite")), as: .sqlite)
    #else
        try FileManager.default.createDirectory(
            atPath: "Storage/buckets",
            withIntermediateDirectories: true
        )

        try FileManager.default.createDirectory(
            atPath: "Storage/multipart",
            withIntermediateDirectories: true
        )

        app.databases.use(
            DatabaseConfigurationFactory.sqlite(.file("Storage/db.sqlite")), as: .sqlite)
    #endif

    app.migrations.add(CreateUser())
    app.migrations.add(CreateAccessKey())
    app.migrations.add(CreateBucket())
    app.migrations.add(AddBucketPolicy())
    app.migrations.add(CreateSharedLink())
    app.migrations.add(AddBucketPublicAccessBlock())
    app.migrations.add(AddBucketTags())
    app.migrations.add(AddBucketLifecycleRules())
    app.migrations.add(CreateOIDCProvider())
    app.migrations.add(AddUserOIDCFields())
    app.migrations.add(AddBucketNotificationConfig())
    app.migrations.add(CreateNotificationDelivery())
    app.migrations.add(AddNotificationDeliveryLastError())
    app.migrations.add(AddBucketReplicationConfig())
    app.migrations.add(CreateReplicationTask())
    app.migrations.add(MakeSharedLinkExpiryOptional())

    app.migrations.add(CreateDefaultUser())

    let cors: CORSMiddleware = CORSMiddleware(
        configuration: .init(
            allowedOrigin: .any(
                [consoleBaseUrl] + additionalCorsOrigins(consoleBaseUrl: consoleBaseUrl)),
            allowedMethods: [.GET, .POST, .PUT, .OPTIONS, .DELETE, .PATCH],
            allowedHeaders: [
                .accept,
                .authorization,
                .contentType,
                .origin,
                .xRequestedWith,
                .init(stringLiteral: "amz-sdk-invocation-id"),
            ]
        )
    )

    // Outermost, so it sees final responses (including errors already converted by
    // S3ErrorMiddleware) and counts their status and size correctly.
    app.middleware.use(MetricsMiddleware())
    app.middleware.use(cors)
    app.middleware.use(S3ErrorMiddleware())

    if let jwt = Environment.get("JWT") {
        await app.jwt.keys.add(hmac: HMACKey(from: jwt), digestAlgorithm: .sha256)
    } else {
        app.logger.error(
            "No JWT key provided in environment variable 'JWT'. Falling back to an insecure default key. Please set a secure JWT key before deploying to production."
        )
        await app.jwt.keys.add(hmac: "super-secret-key", digestAlgorithm: .sha256)
    }

    app.lifecycle.use(LoadCacheLifecycle())

    // Outbound HTTP timeouts (webhook deliveries, OIDC fetches): a hung remote endpoint
    // must never wedge a background task or login flow indefinitely
    app.http.client.configuration.timeout = .init(
        connect: .seconds(5), read: .seconds(10))

    try await app.autoMigrate()

    // The webhook dispatcher needs the app for db/client access; configured in every
    // environment so tests can drive drains manually (only the periodic tick below is
    // gated to non-testing)
    await NotificationDispatcher.shared.configure(app: app)
    await ReplicationDispatcher.shared.configure(app: app)

    try routes(app)

    if app.environment != .testing {
        app.eventLoopGroup.next().scheduleRepeatedTask(
            initialDelay: .zero,
            delay: .minutes(1)
        ) { task in
            Task {
                do {
                    let expiredAccessKeys = try await AccessKey.query(on: app.db)
                        .filter(\.$expirationDate <= Date.now)
                        .all()

                    for accessKey in expiredAccessKeys {
                        try await AccessKeyService.delete(
                            on: app.db, accessKey: accessKey.accessKey)
                    }
                } catch {
                    app.logger.error("Failed to invalidate expired access keys: \(error)")
                }

                do {
                    let expiredSharedLinks = try await SharedLink.query(on: app.db)
                        .filter(\.$expiresAt <= Date.now)
                        .all()

                    for link in expiredSharedLinks {
                        try await link.delete(on: app.db)
                    }
                } catch {
                    app.logger.error("Failed to clean up expired shared links: \(error)")
                }

                // In-flight OIDC login attempts (state/nonce/PKCE verifier) older than 10
                // minutes - a login that never completes a round-trip should not linger.
                await OIDCStateCache.shared.removeExpired(olderThan: 600)
            }
        }

        // CPU/memory gauge sampling for the admin dashboard. Frequent but trivially cheap
        // (a couple of /proc reads); also drives the per-minute history buckets so charts
        // fill in even while nobody is watching the dashboard.
        app.eventLoopGroup.next().scheduleRepeatedTask(
            initialDelay: .seconds(5),
            delay: .seconds(5)
        ) { task in
            Task {
                await MetricsCollector.shared.sample()
            }
        }

        // Webhook outbox tick: fresh events are delivered near-instantly via the explicit
        // wake() in NotificationService - this tick exists to pick up retries whose backoff
        // has elapsed (and anything left over from before a restart). The drain query is a
        // single indexed SELECT, so a 2-second cadence costs effectively nothing when idle.
        app.eventLoopGroup.next().scheduleRepeatedTask(
            initialDelay: .seconds(2),
            delay: .seconds(2)
        ) { task in
            Task {
                await NotificationDispatcher.shared.drain()
            }
        }

        // Replication outbox tick - same reasoning as the webhook tick above: fresh writes
        // are replicated near-instantly via the explicit wake() in ReplicationService, this
        // tick exists to pick up retries whose backoff has elapsed (and anything left over
        // from before a restart).
        app.eventLoopGroup.next().scheduleRepeatedTask(
            initialDelay: .seconds(2),
            delay: .seconds(2)
        ) { task in
            Task {
                await ReplicationDispatcher.shared.drain()
            }
        }

        // Bucket lifecycle rules - a separate, much less frequent task than the minute-based
        // cleanup above, since expiring objects/versions/multipart uploads is never time-critical
        // the way short-lived access keys/share links are. Matches real S3, which evaluates
        // lifecycle rules roughly once a day.
        app.eventLoopGroup.next().scheduleRepeatedTask(
            initialDelay: .zero,
            delay: .hours(1)
        ) { task in
            Task {
                do {
                    try await LifecycleService.runSweep(app: app)
                } catch {
                    app.logger.error("Failed to run lifecycle sweep: \(error)")
                }

                do {
                    try await NotificationDispatcher.purgeExpiredFailures(on: app.db)
                } catch {
                    app.logger.error("Failed to purge expired webhook failures: \(error)")
                }

                do {
                    try await ReplicationDispatcher.purgeExpiredFailures(on: app.db)
                } catch {
                    app.logger.error("Failed to purge expired replication failures: \(error)")
                }
            }
        }
    }
}

/// This makes sure, that we always allow the console localhost in the CORS middleware.
private func additionalCorsOrigins(consoleBaseUrl: String) -> [String] {
    if consoleBaseUrl.lowercased() == "http://localhost:3000" {
        return [
            "http://0.0.0.0:3000",
            "http://127.0.0.1:3000",
        ]
    } else if consoleBaseUrl.lowercased() == "http://0.0.0.0:3000" {
        return [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
        ]
    } else if consoleBaseUrl.lowercased() == "http://127.0.0.0:3000" {
        return [
            "http://localhost:3000",
            "http://0.0.0.0:3000",
        ]
    }

    return [
        "http://localhost:3000",
        "http://0.0.0.0:3000",
        "http://127.0.0.1:3000",
    ]
}
