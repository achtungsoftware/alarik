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
import Vapor

/// Evaluates and enforces bucket lifecycle rules - see `LifecycleConfiguration` for the
/// supported subset. Exposed as a standalone, testable function (rather than inlined in the
/// scheduled task closure in configure.swift) so tests can trigger a sweep directly instead of
/// waiting for the real once-an-hour interval.
struct LifecycleService {
    static func runSweep(app: Application) async throws {
        let buckets = try await Bucket.query(on: app.db).all()

        for bucket in buckets {
            guard let rawRules = bucket.lifecycleRules else { continue }

            let enabledRules = LifecycleConfiguration.fromJSON(rawRules).rules.filter(\.enabled)
            guard !enabledRules.isEmpty else { continue }

            let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucket.name)

            for rule in enabledRules {
                if let expirationDays = rule.expirationDays {
                    try await expireCurrentObjects(
                        bucketName: bucket.name, prefix: rule.prefix, days: expirationDays,
                        versioningStatus: versioningStatus, on: app.db)
                }

                if let noncurrentDays = rule.noncurrentVersionExpirationDays {
                    try await expireNoncurrentVersions(
                        bucketName: bucket.name, prefix: rule.prefix, days: noncurrentDays,
                        on: app.db)
                }

                if let abortDays = rule.abortIncompleteMultipartUploadDays {
                    try abortStaleMultipartUploads(
                        bucketName: bucket.name, prefix: rule.prefix, days: abortDays)
                }
            }
        }
    }

    /// Expiration - deletes current objects whose last-modified date is at least `days` old.
    /// In a versioned bucket this creates a delete marker; otherwise it's a permanent delete -
    /// identical to a normal DELETE request, matching S3's lifecycle Expiration behavior.
    private static func expireCurrentObjects(
        bucketName: String, prefix: String, days: Int, versioningStatus: VersioningStatus,
        on db: any Database
    ) async throws {
        let cutoff = Date().addingTimeInterval(-Double(days) * 86400)
        let (objects, _, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName, prefix: prefix, delimiter: nil, maxKeys: 10000)

        for object in objects where object.updatedAt <= cutoff {
            let outcome = try S3Service.deleteObject(
                bucketName: bucketName, key: object.key, versionId: nil,
                versioningStatus: versioningStatus)

            await NotificationService.emit(
                event: outcome.isDeleteMarker
                    ? .lifecycleExpirationDeleteMarkerCreated : .lifecycleExpirationDelete,
                bucketName: bucketName, key: object.key, size: nil, etag: nil,
                versionId: outcome.versionId, requestId: UUID().uuidString, sourceIP: nil, on: db)
            await ReplicationService.enqueueDelete(
                bucketName: bucketName, key: object.key, versionId: outcome.versionId, on: db)
        }
    }

    /// NoncurrentVersionExpiration - permanently deletes noncurrent versions that have been
    /// noncurrent for at least `days`. Individual versions don't store when they *became*
    /// noncurrent, so this is approximated as the creation time of the next-newer version in
    /// the chain (the version that superseded them) - the closest equivalent derivable from the
    /// existing storage format.
    private static func expireNoncurrentVersions(
        bucketName: String, prefix: String, days: Int, on db: any Database
    ) async throws {
        let cutoff = Date().addingTimeInterval(-Double(days) * 86400)
        let (versions, deleteMarkers, _, _, _, _) = try ObjectFileHandler.listAllVersions(
            bucketName: bucketName, prefix: prefix, delimiter: nil, maxKeys: 10000)

        let keys = Set(versions.map(\.key) + deleteMarkers.map(\.key))

        for key in keys {
            let chain = try ObjectFileHandler.listVersions(bucketName: bucketName, key: key)
            guard chain.count > 1 else { continue }

            for i in 1..<chain.count {
                let version = chain[i]
                guard !version.isDeleteMarker, let versionId = version.versionId else { continue }

                let becameNoncurrentAt = chain[i - 1].updatedAt
                guard becameNoncurrentAt <= cutoff else { continue }

                try ObjectFileHandler.deleteVersion(
                    bucketName: bucketName, key: key, versionId: versionId)

                await NotificationService.emit(
                    event: .lifecycleExpirationDelete, bucketName: bucketName, key: key,
                    size: nil, etag: nil, versionId: versionId,
                    requestId: UUID().uuidString, sourceIP: nil, on: db)
                // Not replicated: NoncurrentVersionExpiration permanently prunes one specific
                // historical version, which has no meaningful equivalent on the replication
                // target (see ReplicationClient.replicateDelete).
            }
        }
    }

    /// AbortIncompleteMultipartUpload - aborts in-progress multipart uploads initiated at least
    /// `days` ago.
    private static func abortStaleMultipartUploads(bucketName: String, prefix: String, days: Int)
        throws
    {
        let cutoff = Date().addingTimeInterval(-Double(days) * 86400)
        let (uploads, _, _, _) = try MultipartFileHandler.listUploads(
            bucketName: bucketName, prefix: prefix, maxUploads: 10000)

        for upload in uploads where upload.initiated <= cutoff {
            try MultipartFileHandler.abortUpload(bucketName: bucketName, uploadId: upload.uploadId)
        }
    }
}
