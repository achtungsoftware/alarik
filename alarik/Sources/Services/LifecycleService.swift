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

/// Evaluates and enforces bucket lifecycle rules - see `LifecycleConfiguration` for the
/// supported subset. Exposed as a standalone, testable function (rather than inlined in the
/// scheduled task closure in configure.swift) so tests can trigger a sweep directly instead of
/// waiting for the real once-an-hour interval.
struct LifecycleService {
    static func runSweep(app: Application) async throws {
        let buckets = try await Bucket.all(app: app)

        for bucket in buckets {
            guard let rawRules = bucket.lifecycleRules else { continue }

            let enabledRules = LifecycleConfiguration.fromJSON(rawRules).rules.filter(\.enabled)
            guard !enabledRules.isEmpty else { continue }

            let versioningStatus = await BucketVersioningCache.shared.resolvedStatus(app: app, bucket: bucket.name)

            for rule in enabledRules {
                if let expirationDays = rule.expirationDays {
                    try await expireCurrentObjects(
                        bucketName: bucket.name, prefix: rule.prefix, days: expirationDays,
                        versioningStatus: versioningStatus, app: app)
                }

                if let noncurrentDays = rule.noncurrentVersionExpirationDays {
                    try await expireNoncurrentVersions(
                        bucketName: bucket.name, prefix: rule.prefix, days: noncurrentDays,
                        app: app)
                }

                if let abortDays = rule.abortIncompleteMultipartUploadDays {
                    try await abortStaleMultipartUploads(
                        bucketName: bucket.name, prefix: rule.prefix, days: abortDays, app: app)
                }
            }
        }
    }

    /// Expiration - deletes current objects whose last-modified date is at least `days` old.
    /// In a versioned bucket this creates a delete marker; otherwise it's a permanent delete -
    /// identical to a normal DELETE request, matching S3's lifecycle Expiration behavior.
    private static func expireCurrentObjects(
        bucketName: String, prefix: String, days: Int, versioningStatus: VersioningStatus,
        app: Application
    ) async throws {
        let cutoff = Date().addingTimeInterval(-Double(days) * 86400)
        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName, prefix: prefix, delimiter: nil, maxKeys: 10000)

        // Membership is read once for the whole sweep, not per object - the active set doesn't
        // meaningfully change within a single pass, and re-reading it per key would both add
        // thousands of redundant actor hops and let placement decisions drift mid-sweep.
        let clusterContext = await clusterContext(app: app)

        for object in objects where object.updatedAt <= cutoff {
            // Every replica runs its own sweep independently and would otherwise all race to
            // expire the same object - gating on "am I the primary (rank-0) responsible node"
            // means exactly one node acts. Being primary implies holding a local replica, so
            // this node's own local listing above already covers every key it's primary for.
            let (isPrimary, peers) = primaryAuthority(
                context: clusterContext, bucketName: bucketName, key: object.key)
            guard isPrimary else { continue }

            let outcome = try await ClusterReplicationService.coordinateDelete(
                app: app, bucketName: bucketName, key: object.key, versionId: nil,
                versioningStatus: versioningStatus, peers: peers)

            await NotificationService.emit(
                event: outcome.isDeleteMarker
                    ? .lifecycleExpirationDeleteMarkerCreated : .lifecycleExpirationDelete,
                bucketName: bucketName, key: object.key, size: nil, etag: nil,
                versionId: outcome.versionId, requestId: UUID().uuidString, sourceIP: nil, app: app)
            await ReplicationService.enqueueDelete(
                app: app, bucketName: bucketName, key: object.key, versionId: outcome.versionId)
        }
    }

    /// NoncurrentVersionExpiration - permanently deletes noncurrent versions that have been
    /// noncurrent for at least `days`. Individual versions don't store when they *became*
    /// noncurrent, so this is approximated as the creation time of the next-newer version in
    /// the chain (the version that superseded them) - the closest equivalent derivable from the
    /// existing storage format.
    private static func expireNoncurrentVersions(
        bucketName: String, prefix: String, days: Int, app: Application
    ) async throws {
        let cutoff = Date().addingTimeInterval(-Double(days) * 86400)
        let (versions, deleteMarkers, _, _, _, _) = try ObjectFileHandler.listAllVersions(
            bucketName: bucketName, prefix: prefix, delimiter: nil, maxKeys: 10000)

        let keys = Set(versions.map(\.key) + deleteMarkers.map(\.key))

        // Read membership once for the whole sweep - see expireCurrentObjects.
        let clusterContext = await clusterContext(app: app)

        for key in keys {
            // See expireCurrentObjects - same primary-only gating, for the same reason.
            let (isPrimary, peers) = primaryAuthority(
                context: clusterContext, bucketName: bucketName, key: key)
            guard isPrimary else { continue }

            let chain = try ObjectFileHandler.listVersions(bucketName: bucketName, key: key)
            guard chain.count > 1 else { continue }

            for i in 1..<chain.count {
                let version = chain[i]
                guard !version.isDeleteMarker, let versionId = version.versionId else { continue }

                let becameNoncurrentAt = chain[i - 1].updatedAt
                guard becameNoncurrentAt <= cutoff else { continue }

                // Unlike external replication (ReplicationClient.replicateDelete), cluster peers
                // physically hold the exact same version files as this node, so this specific
                // historical version does need to propagate - versioningStatus is irrelevant
                // here since coordinateDelete's own deleteObject always hard-deletes an
                // explicitly-versionId'd delete.
                let outcome = try await ClusterReplicationService.coordinateDelete(
                    app: app, bucketName: bucketName, key: key, versionId: versionId,
                    versioningStatus: .disabled, peers: peers)

                await NotificationService.emit(
                    event: .lifecycleExpirationDelete, bucketName: bucketName, key: key,
                    size: nil, etag: nil, versionId: outcome.versionId,
                    requestId: UUID().uuidString, sourceIP: nil, app: app)
            }
        }
    }

    /// One-time-per-sweep membership snapshot: `nil` means not clustered (or no peers registered
    /// yet), the "act alone" case every other cluster feature treats as inert. Read once and
    /// threaded into `primaryAuthority` per object so a single sweep sees one consistent
    /// membership view.
    private static func clusterContext(app: Application) async -> (
        config: ClusterConfiguration, active: [ClusterNodeInfo]
    )? {
        guard let config = app.storage[ClusterConfigurationKey.self] else { return nil }
        let active = await ClusterNodeCache.shared.activeNodes()
        guard !active.isEmpty else { return nil }
        return (config, active)
    }

    /// Pure cluster-aware write authority for a key, for background sweeps that have no `Request`
    /// to drive `ObjectRoutingService`'s per-request routing decision. A `nil` context (not
    /// clustered) always answers "yes, alone". Returns the other responsible nodes as `peers` so
    /// a caller that gets `isPrimary: true` can replicate its write without a second placement
    /// lookup.
    private static func primaryAuthority(
        context: (config: ClusterConfiguration, active: [ClusterNodeInfo])?,
        bucketName: String, key: String
    ) -> (isPrimary: Bool, peers: [ClusterNodeInfo]) {
        guard let context else { return (true, []) }
        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: context.active)
        guard let primary = responsible.first, primary.id == context.config.nodeId else {
            return (false, [])
        }
        let peers = responsible.filter { $0.id != context.config.nodeId }
        return (true, peers)
    }

    /// AbortIncompleteMultipartUpload - aborts in-progress multipart uploads initiated at least
    /// `days` ago. Deliberately local-only, unlike the two sweeps above: an upload's part state
    /// only ever exists on the single primary node that coordinated its Create, so this node's
    /// own local listing already surfaces only uploads it genuinely owns - no peer state to
    /// replicate an abort to. An upload whose owning node is permanently lost never gets swept,
    /// the same tradeoff every other primary-pinned multipart operation already accepts.
    private static func abortStaleMultipartUploads(
        bucketName: String, prefix: String, days: Int, app: Application
    ) async throws {
        let cutoff = Date().addingTimeInterval(-Double(days) * 86400)
        let (uploads, _, _, _) = try MultipartFileHandler.listUploads(
            bucketName: bucketName, prefix: prefix, maxUploads: 10000)

        for upload in uploads where upload.initiated <= cutoff {
            try await S3Service.offloadBlockingIO(app) {
                try MultipartFileHandler.abortUpload(
                    bucketName: bucketName, uploadId: upload.uploadId)
            }
        }
    }
}
