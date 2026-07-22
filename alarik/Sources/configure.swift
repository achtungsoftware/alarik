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

import JWTKit
import NIOCore
import NIOSSL
import Vapor

public func configure(_ app: Application) async throws {

    // TODO: make this configurable ?
    app.routes.defaultMaxBodySize = "5tb"
    app.http.server.configuration.supportPipelining = true
    // Vapor's default (10s) can be exceeded by a busy node with many in-flight cluster-internal
    // requests (a rebalance walk's parallel probes/pushes, concurrent metadata fan-outs) at the
    // exact moment it receives a shutdown signal - when that happens, Vapor logs "Server stop
    // took too long" and proceeds with the rest of shutdown (clearing `app.storage`) regardless,
    // while those still-running requests keep executing and crash the process the moment any of
    // them next touches an `app`-scoped service ("Core not configured"). A longer budget gives
    // genuinely-busy-but-still-progressing requests a real chance to drain normally instead of
    // being abandoned mid-flight - this doesn't help a truly hung request, but that's not what's
    // happening here (the requests preceding the crash complete in normal time, there's just a
    // lot of them at once during heavy membership churn).
    app.http.server.configuration.shutdownTimeout = .seconds(30)

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

    // Object-data clustering is opt-in - a node only joins the cluster when both
    // `CLUSTER_NODE_ADDRESS` (its own internally-reachable base URL) and `CLUSTER_SECRET` (shared
    // inter-node auth secret) are set; neither alone is enough. No database of any kind is
    // required - control-plane metadata (including cluster membership itself) lives in Alarik's
    // own erasure-coded object storage (`MetadataStore`), the same engine as regular object data.
    // A brand-new node joining an existing cluster bootstraps its initial view of membership from
    // `CLUSTER_SEED_NODES` (comma-separated peer addresses, optional - empty for the first node
    // of a brand-new cluster) - see `ClusterMembershipLifecycle`.
    // Validated unconditionally (not just in cluster mode) so a typo in these vars fails boot
    // immediately, even before the operator flips on CLUSTER_NODE_ADDRESS/CLUSTER_SECRET.
    let erasureCodingConfig = try ClusterErasureCodingConfig.resolve()
    let metadataErasureCodingConfig = try ClusterMetadataErasureCodingConfig.resolve()

    let clusterNodeAddress = Environment.sanitizedGet("CLUSTER_NODE_ADDRESS")
    let clusterSecret = Environment.sanitizedGet("CLUSTER_SECRET")
    if clusterNodeAddress != nil || clusterSecret != nil {
        guard let clusterNodeAddress, let clusterSecret else {
            throw ClusterConfigurationError(
                description:
                    "Both CLUSTER_NODE_ADDRESS and CLUSTER_SECRET must be set to enable cluster mode - only one was provided."
            )
        }
        let nodeId = try ClusterNodeIdentity.loadOrCreate()
        let seeds =
            (Environment.sanitizedGet("CLUSTER_SEED_NODES") ?? "")
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
        app.storage[ClusterConfigurationKey.self] = ClusterConfiguration(
            nodeId: nodeId, address: clusterNodeAddress, secret: clusterSecret, seeds: seeds)
        app.storage[ClusterErasureCodingConfigKey.self] = erasureCodingConfig
        app.storage[ClusterMetadataErasureCodingConfigKey.self] = metadataErasureCodingConfig
    }

    // Spool files are per-request transients; anything still here is an orphan from an
    // unclean shutdown mid-upload.
    StreamingBodySpooler.cleanupOrphans()

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
    } else if app.environment == .production {
        // Refusing to boot, not warning and continuing: the fallback key below is a literal in a
        // public repository, so anyone can mint a valid admin session token against a production
        // deployment that happens to be missing this variable. A log line is not a defence when
        // the process comes up and starts serving anyway.
        throw ConfigurationError(
            description:
                "JWT is required in the production environment - no key was provided, and the insecure development fallback is never used here. Set the JWT environment variable to a strong secret."
        )
    } else {
        app.logger.error(
            "No JWT key provided in environment variable 'JWT'. Falling back to an insecure default key. Please set a secure JWT key before deploying to production."
        )
        await app.jwt.keys.add(hmac: "super-secret-key", digestAlgorithm: .sha256)
    }

    // Self-registers this node into the `cluster-nodes` metadata collection and starts its
    // heartbeat. Registered (and therefore its `didBootAsync` run) BEFORE `LoadCacheLifecycle`,
    // deliberately: `LoadCacheLifecycle.reloadAll` lists access keys/buckets cluster-wide via
    // `MetadataListingService`, which fans out to `ClusterNodeCache.shared.activeNodes()` - if
    // that ran first, every node's `ClusterNodeCache` would still be completely empty (cluster
    // bootstrap hasn't happened yet), so the fan-out would query nobody and the initial cache
    // load would come back empty on every node that doesn't happen to hold every record
    // locally itself - exactly the "InvalidAccessKeyId right after a fresh multi-node boot"
    // failure this ordering exists to prevent.
    app.lifecycle.use(ClusterMembershipLifecycle.shared)
    app.lifecycle.use(LoadCacheLifecycle())

    // Outbound HTTP timeouts (webhook deliveries, OIDC fetches): a hung remote endpoint
    // must never wedge a background task or long-running flow indefinitely
    app.http.client.configuration.timeout = .init(
        connect: .seconds(5), read: .seconds(10))
    
    app.http.client.configuration.connectionPool.concurrentHTTP1ConnectionsPerHostSoftLimit = 256

    // Idempotent (see DefaultUserSeed.swift's doc comment) - safe to run on every boot, not
    // just the first one, now that there's no migration-tracking table to gate it on. Gated to
    // a single, statically-elected node in cluster mode - see `isDesignatedSeeder`'s doc comment.
    if isDesignatedSeeder(app: app) {
        try await CreateDefaultUser.run(app: app)
    }

    // The webhook dispatcher needs the app for db/client access; configured in every
    // environment so tests can drive drains manually (only the periodic tick below is
    // gated to non-testing)
    await NotificationDispatcher.shared.configure(app: app)
    await ReplicationDispatcher.shared.configure(app: app)
    await ClusterReplicationDispatcher.shared.configure(app: app)
    await ErasureCodedDispatcher.shared.configure(app: app)
    // Stops every dispatcher's `wake()` from spawning new drain work before `app.storage` gets
    // cleared - see `GenericOutboxDispatcher.prepareForShutdown`'s doc comment for the crash this
    // closes.
    app.lifecycle.use(OutboxDispatcherShutdown())
    // Gracefully shuts down the dedicated HTTP client used for cluster control-plane traffic
    // (cache-invalidation broadcasts, membership seed queries) - see its own doc comment for why
    // that traffic gets a separate client from `app.http.client.shared`.
    app.lifecycle.use(LightweightClusterControlClient.ShutdownHandler())

    try routes(app)

    if app.environment != .testing {
        app.eventLoopGroup.next().scheduleRepeatedTask(
            initialDelay: .zero,
            delay: .minutes(1)
        ) { task in
            Task {
                // Local records, not `all(app:)`. A cluster-wide listing here meant every node
                // fanned out to every other node twice a minute purely to rediscover records it
                // already holds - O(nodes^2) of pure upkeep traffic. Sweeping what this node
                // stores keeps the cost proportional to stored data, and every holder running it
                // means the sweep never depends on one particular node being up (the deletes are
                // idempotent, so overlap is free). Same reasoning as `MetadataMaintenance`.
                do {
                    let expiredAccessKeys = await MetadataListingService.localRecords(
                        AccessKey.self, app: app, collection: MetadataCollections.accessKeys
                    ).filter {
                        guard let expirationDate = $0.expirationDate else { return false }
                        return expirationDate <= Date.now
                    }

                    for accessKey in expiredAccessKeys {
                        try await AccessKeyService.delete(
                            app: app, accessKey: accessKey.accessKey, id: accessKey.id)
                    }
                } catch {
                    app.logger.error("Failed to invalidate expired access keys: \(error)")
                }

                do {
                    let expiredSharedLinks = await MetadataListingService.localRecords(
                        SharedLink.self, app: app, collection: MetadataCollections.sharedLinks
                    ).filter {
                        guard let expiresAt = $0.expiresAt else { return false }
                        return expiresAt <= Date.now
                    }

                    for link in expiredSharedLinks {
                        try await link.delete(app: app)
                    }
                } catch {
                    app.logger.error("Failed to clean up expired shared links: \(error)")
                }

                // In-flight OIDC login attempts (state/nonce/PKCE verifier) older than 10
                // minutes - a login that never completes a round-trip should not linger.
                do {
                    try await OIDCStateCache.removeExpired(app: app, olderThan: 600)
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

        // Periodic full cache reload (upsert-only, see `LoadCacheLifecycle.reloadAll`'s doc
        // comment) - bounds staleness from a dropped cache-invalidation broadcast
        // (`CacheInvalidationService`, HTTP over the inter-node cluster protocol
        if app.storage[ClusterConfigurationKey.self] != nil {
            app.eventLoopGroup.next().scheduleRepeatedTask(
                initialDelay: .seconds(60),
                delay: .seconds(60)
            ) { task in
                Task {
                    do {
                        try await LoadCacheLifecycle.reloadAll(app: app)
                    } catch {
                        app.logger.error("Periodic full cache reload failed: \(error)")
                    }
                }
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

        // Erasure-coded shard-repair outbox tick - same reasoning as the three ticks above, and
        // just as necessary: fresh shard-fanout catch-up, rebalance, and reconstruction tasks are
        // woken near-instantly via the explicit wake() in ErasureCodedWriteCoordinator/
        // ErasureCodedRebalanceService, but a task whose delivery attempt failed (a peer down
        // during a quorum write) backs off and MUST be re-drained once that backoff elapses.
        // Without this tick that retry only ever fired if some unrelated later write happened to
        // wake the dispatcher after the backoff window - so a shard a down replica missed could
        // sit un-repaired indefinitely, purely on timing luck.
        app.eventLoopGroup.next().scheduleRepeatedTask(
            initialDelay: .seconds(2),
            delay: .seconds(2)
        ) { task in
            Task {
                await ErasureCodedDispatcher.shared.drain()
            }
        }

        // Erasure-coding bit-rot scrubber - only scheduled when this node is in cluster mode with
        // scrubbing enabled (`CLUSTER_EC_SCRUB_INTERVAL_HOURS` > 0, defaults to weekly). Each node
        // re-verifies its own shards' checksums and heals any it finds corrupt; the initial delay
        // matches the interval so a fresh boot doesn't immediately scrub. On-demand scrubs go
        // through the admin endpoint / cache NOTIFY instead of this tick.
        if app.storage[ClusterConfigurationKey.self] != nil,
            let ecConfig = app.storage[ClusterErasureCodingConfigKey.self], ecConfig.scrubbingEnabled
        {
            let interval = TimeAmount.hours(Int64(ecConfig.scrubIntervalHours))
            app.eventLoopGroup.next().scheduleRepeatedTask(
                initialDelay: interval, delay: interval
            ) { task in
                Task { await ErasureCodedScrubber.scrub(app: app) }
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
                    try await NotificationDispatcher.purgeExpiredFailures(app: app)
                } catch {
                    app.logger.error("Failed to purge expired webhook failures: \(error)")
                }

                do {
                    try await ReplicationDispatcher.purgeExpiredFailures(app: app)
                } catch {
                    app.logger.error("Failed to purge expired replication failures: \(error)")
                }

                do {
                    try await ClusterReplicationDispatcher.purgeExpiredFailures(app: app)
                } catch {
                    app.logger.error("Failed to purge expired cluster replication failures: \(error)")
                }

                do {
                    try await ErasureCodedDispatcher.purgeExpiredFailures(app: app)
                } catch {
                    app.logger.error("Failed to purge expired EC shard replication failures: \(error)")
                }

                // Metadata upkeep. Both walk only this node's own local shard-0 records, so they
                // cost the same whether the cluster has three nodes or a thousand - see
                // `MetadataMaintenance`. Neither throws; both log their own failures.
                await MetadataMaintenance.runTombstoneGC(app: app)
                await MetadataMaintenance.runMigrationSweep(app: app)
            }
        }
    }
}

/// Calls `prepareForShutdown()` on all four outbox dispatchers before `app.storage` is cleared -
/// see `GenericOutboxDispatcher.prepareForShutdown`'s doc comment.
private struct OutboxDispatcherShutdown: LifecycleHandler {
    func shutdownAsync(_ app: Application) async {
        await NotificationDispatcher.shared.prepareForShutdown()
        await ReplicationDispatcher.shared.prepareForShutdown()
        await ClusterReplicationDispatcher.shared.prepareForShutdown()
        await ErasureCodedDispatcher.shared.prepareForShutdown()
    }
}

/// Whether THIS node is the one that should attempt `CreateDefaultUser.run` at boot.
///
/// In non-clustered mode there's only ever one node, so this is trivially `true`. In cluster
/// mode, letting every node attempt the seed independently is unsafe: `ClusterMembershipLifecycle`
/// (which discovers peers via `CLUSTER_SEED_NODES`) hasn't run yet at this point in boot -
/// `configure(app:)` completes entirely before Vapor invokes any `LifecycleHandler`'s
/// `didBootAsync` - so every node's `ClusterNodeCache` is still completely empty here, on every
/// single cold boot, not just as a rare race. Each node's `MetadataStore.putIfAbsent` would then
/// see itself as the sole/local coordinator (nothing else is known yet) and independently create
/// its own distinct "default admin" record, producing N different admin accounts with N different
/// passwords/UUIDs instead of one - exactly the split-brain this function prevents.
///
/// The fix doesn't wait for discovery (still unreliable under a simultaneous cold start - peers
/// may not be listening yet regardless of when this runs) - it sidesteps discovery entirely by
/// electing the seeder from information every node already has statically, before any network
/// call: the full address set this node was configured with (itself plus every configured seed).
/// Every node in a symmetrically-configured cluster (each node lists every other as a seed, as
/// `CLUSTER_SEED_NODES` is documented and as `cluster_tests.sh` configures it) computes the exact
/// same set and therefore the exact same minimum - so exactly one node ever attempts the seed,
/// with no coordination round-trip needed.
private func isDesignatedSeeder(app: Application) -> Bool {
    guard let config = app.storage[ClusterConfigurationKey.self] else { return true }
    let allAddresses = [config.address] + config.seeds
    return config.address == allAddresses.min()
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
    } else if consoleBaseUrl.lowercased() == "http://127.0.0.1:3000" {
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

/// A fatal misconfiguration detected during `configure` - boot stops rather than starting a node
/// that would be unsafe or silently wrong.
struct ConfigurationError: Error, CustomStringConvertible {
    let description: String
}
