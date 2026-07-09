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
import FluentPostgresDriver
import FluentSQLiteDriver
import JWTKit
import NIOSSL
import PostgresKit
import Vapor

public func configure(_ app: Application) async throws {

    // TODO: make this configurable ?
    app.routes.defaultMaxBodySize = "5tb"
    app.http.server.configuration.supportPipelining = true

    let consoleBaseUrl = ConsoleBaseURL.resolve()

    #if DEBUG
    #else
        try FileManager.default.createDirectory(
            atPath: "Storage/buckets",
            withIntermediateDirectories: true
        )

        try FileManager.default.createDirectory(
            atPath: "Storage/multipart",
            withIntermediateDirectories: true
        )
    #endif

    // A single node needs nothing beyond the zero-config SQLite default. `DATABASE_URL`
    // opts into Postgres - required (not just supported) the moment more than one node shares
    // this control-plane data, since SQLite has no safe multi-writer/multi-host story. The
    // parsed config is reused for both the pooled Fluent connections and the dedicated
    // LISTEN connection below - never re-parsed from the URL string a second time.
    if let databaseURL = Environment.sanitizedGet("DATABASE_URL") {
        let postgresConfig = try SQLPostgresConfiguration(url: databaseURL)
        app.databases.use(.postgres(configuration: postgresConfig), as: .psql)
        app.storage[PostgresListenConfigurationKey.self] = postgresConfig.coreConfiguration
    } else {
        #if DEBUG
            // In debug, test & profiling - store the db relative to the work dir
            app.databases.use(.sqlite(.file("db.sqlite")), as: .sqlite)
        #else
            app.databases.use(
                DatabaseConfigurationFactory.sqlite(.file("Storage/db.sqlite")), as: .sqlite)
        #endif
    }

    // Object-data clustering is opt-in on top of the Postgres control plane above - a node only
    // joins the cluster when both `CLUSTER_NODE_ADDRESS` (its own internally-reachable base URL)
    // and `CLUSTER_SECRET` (shared inter-node auth secret) are set. Neither alone is enough, and
    // clustering requires `DATABASE_URL` (membership + cache invalidation need the shared
    // Postgres control plane) - a single Postgres-mode node without these vars stays a plain
    // single-node deployment with a shared control plane, never joining any cluster.
    let clusterNodeAddress = Environment.sanitizedGet("CLUSTER_NODE_ADDRESS")
    let clusterSecret = Environment.sanitizedGet("CLUSTER_SECRET")
    if clusterNodeAddress != nil || clusterSecret != nil {
        guard Environment.sanitizedGet("DATABASE_URL") != nil else {
            throw ClusterConfigurationError(
                description:
                    "CLUSTER_NODE_ADDRESS/CLUSTER_SECRET require DATABASE_URL to be set - object-data clustering needs the shared Postgres control plane."
            )
        }
        guard let clusterNodeAddress, let clusterSecret else {
            throw ClusterConfigurationError(
                description:
                    "Both CLUSTER_NODE_ADDRESS and CLUSTER_SECRET must be set to enable cluster mode - only one was provided."
            )
        }
        let nodeId = try ClusterNodeIdentity.loadOrCreate()
        app.storage[ClusterConfigurationKey.self] = ClusterConfiguration(
            nodeId: nodeId, address: clusterNodeAddress, secret: clusterSecret)
    }

    // Spool files are per-request transients; anything still here is an orphan from an
    // unclean shutdown mid-upload.
    StreamingBodySpooler.cleanupOrphans()

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
    app.migrations.add(CreateOIDCState())
    app.migrations.add(CreateClusterNode())
    app.migrations.add(CreateClusterReplicationTask())

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

    if let jwt = Environment.sanitizedGet("JWT") {
        await app.jwt.keys.add(hmac: HMACKey(from: jwt), digestAlgorithm: .sha256)
    } else {
        app.logger.error(
            "No JWT key provided in environment variable 'JWT'. Falling back to an insecure default key. Please set a secure JWT key before deploying to production."
        )
        await app.jwt.keys.add(hmac: "super-secret-key", digestAlgorithm: .sha256)
    }

    app.lifecycle.use(LoadCacheLifecycle())
    // Must be registered after LoadCacheLifecycle: Vapor runs didBootAsync handlers
    // sequentially in registration order, and the LISTEN loop's reconnect-safety-net reload
    // reuses the exact same bulk-load path - it must never race the initial one. No-op when
    // running SQLite (no DATABASE_URL was set above).
    app.lifecycle.use(CacheInvalidationListener.shared)
    // Self-registers this node into `cluster_nodes` and starts its heartbeat - must run after
    // the two handlers above for the same reason: it needs caches loaded and cluster-invalidation
    // NOTIFYs flowing before it announces itself. No-op when `ClusterConfigurationKey` wasn't
    // stashed above (cluster mode off).
    app.lifecycle.use(ClusterMembershipLifecycle.shared)

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
    await ClusterReplicationDispatcher.shared.configure(app: app)

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
                do {
                    try await OIDCStateCache.shared.removeExpired(on: app.db, olderThan: 600)
                } catch {
                    app.logger.error("Failed to clean up expired OIDC login states: \(error)")
                }
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

        // Cluster replication outbox tick - same reasoning as the two ticks above: fresh
        // quorum-fanout catch-up and rebalance/reclaim tasks are woken near-instantly via the
        // explicit wake() in ClusterReplicationService/ClusterRebalanceService, this tick exists
        // to pick up retries whose backoff has elapsed. Cheap single indexed SELECT against an
        // always-empty table when cluster mode is off, same as the other two outboxes when
        // their respective features are unused.
        app.eventLoopGroup.next().scheduleRepeatedTask(
            initialDelay: .seconds(2),
            delay: .seconds(2)
        ) { task in
            Task {
                await ClusterReplicationDispatcher.shared.drain()
            }
        }

        // Bucket lifecycle rules - a separate, much less frequent task than the minute-based
        // cleanup above, since expiring objects/versions/multipart uploads is never time-critical
        // the way short-lived access keys/share links are. Matches S3, which evaluates
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

                do {
                    try await ClusterReplicationDispatcher.purgeExpiredFailures(on: app.db)
                } catch {
                    app.logger.error("Failed to purge expired cluster replication failures: \(error)")
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
