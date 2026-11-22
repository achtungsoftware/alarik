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

import struct Foundation.UUID

/// Makes bucket-wide scans (as opposed to single-key operations, which `ObjectRoutingService`
/// already handles) cluster-aware. Placement is per-key rendezvous hashing, not range-sharded,
/// so any node can hold any key - a correct bucket-wide scan therefore always fans out to every
/// active node and merges results, never just consults placement for one key.
///
/// If this node isn't clustered (or has no peers), each entry point calls the local
/// `ObjectFileHandler`/`MultipartFileHandler` function directly. Otherwise it fetches this node's
/// own local page first, fans out to peers via `ClusterListingClient`, and merges. Plain listings
/// and `calculateStats` are best-effort (an unreachable peer is logged and excluded); only
/// `hasBucketObjects` fails closed, since it gates the irreversible `DeleteBucket`.
enum ClusterListingService {
    // MARK: - Objects (ListObjectsV2 / ListObjects)

    static func listObjects(
        req: Request, bucketName: String, prefix: String, delimiter: String?, maxKeys: Int,
        marker: String?
    ) async throws -> (
        objects: [ObjectMeta], commonPrefixes: [String], isTruncated: Bool, nextMarker: String?
    ) {
        let local = {
            try ObjectFileHandler.listObjects(
                bucketName: bucketName, prefix: prefix, delimiter: delimiter, maxKeys: maxKeys,
                marker: marker)
        }
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            return try local()
        }
        let peers = await activePeers(config: config)
        guard !peers.isEmpty else { return try local() }

        let localResult = try local()
        var pages = [
            ObjectsPage(
                objects: localResult.objects, commonPrefixes: localResult.commonPrefixes,
                isTruncated: localResult.isTruncated)
        ]

        await withTaskGroup(of: ObjectsPage?.self) { group in
            for node in peers {
                group.addTask {
                    do {
                        let r = try await ClusterListingClient.fetchObjectsPage(
                            app: req.application, from: node, bucketName: bucketName,
                            prefix: prefix, delimiter: delimiter, maxKeys: maxKeys, marker: marker)
                        return ObjectsPage(
                            objects: r.objects, commonPrefixes: r.commonPrefixes,
                            isTruncated: r.isTruncated)
                    } catch {
                        req.logger.warning(
                            "Cluster ListObjects: peer \(node.id) unreachable for '\(bucketName)', proceeding best-effort: \(error)"
                        )
                        return nil
                    }
                }
            }
            for await page in group { if let page { pages.append(page) } }
        }

        return mergeObjectsPages(pages, maxKeys: maxKeys)
    }

    // MARK: - Search (console)

    struct SearchPage {
        let matches: [ObjectMeta]
        let isTruncated: Bool
        /// Last key examined (not matched) - the position to resume the scan from.
        let nextMarker: String?
    }

    /// One bounded scan step of a substring search: every node examines at most `scanLimit`
    /// keys after `marker` and returns only the matches, merged so that the result never skips
    /// a key on any node (see `mergeSearchPages`).
    static func searchObjects(
        req: Request, bucketName: String, prefix: String, term: String, scanLimit: Int,
        marker: String?
    ) async throws -> SearchPage {
        let filter = ConsoleObjectListing.keyFilter(matching: term)
        let local = { @Sendable () throws -> SearchPage in
            let (objects, _, isTruncated, nextMarker) = try ObjectFileHandler.listObjects(
                bucketName: bucketName, prefix: prefix, delimiter: nil, maxKeys: scanLimit,
                marker: marker, keyFilter: filter)
            return SearchPage(matches: objects, isTruncated: isTruncated, nextMarker: nextMarker)
        }
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            return try local()
        }
        let peers = await activePeers(config: config)
        guard !peers.isEmpty else { return try local() }

        var pages = [try local()]
        await withTaskGroup(of: SearchPage?.self) { group in
            for node in peers {
                group.addTask {
                    do {
                        let r = try await ClusterListingClient.fetchObjectsPage(
                            app: req.application, from: node, bucketName: bucketName,
                            prefix: prefix, delimiter: nil, maxKeys: scanLimit, marker: marker,
                            search: term)
                        return SearchPage(
                            matches: r.objects, isTruncated: r.isTruncated, nextMarker: r.nextMarker)
                    } catch {
                        req.logger.warning(
                            "Cluster search: peer \(node.id) unreachable for '\(bucketName)', proceeding best-effort: \(error)"
                        )
                        return nil
                    }
                }
            }
            for await page in group { if let page { pages.append(page) } }
        }
        return mergeSearchPages(pages)
    }

    /// Merges per-node scan steps. Nodes stop at different keys, so the merged position is the
    /// *earliest* stop among the nodes that still have more; matches past it are dropped and
    /// re-found by the next step, which resumes every node from that same position. Pure, so
    /// it's unit-tested directly.
    static func mergeSearchPages(_ pages: [SearchPage]) -> SearchPage {
        let bound = pages.compactMap { $0.isTruncated ? $0.nextMarker : nil }.min()
        var seen = Set<String>()
        var matches: [ObjectMeta] = []
        for page in pages {
            for match in page.matches where seen.insert(match.key).inserted {
                if let bound, match.key > bound { continue }
                matches.append(match)
            }
        }
        matches.sort { $0.key < $1.key }
        return SearchPage(matches: matches, isTruncated: bound != nil, nextMarker: bound)
    }

    // MARK: - Versions (ListObjectVersions)

    static func listAllVersions(
        req: Request, bucketName: String, prefix: String, delimiter: String?, keyMarker: String?,
        versionIdMarker: String?, maxKeys: Int
    ) async throws -> (
        versions: [ObjectMeta], deleteMarkers: [ObjectMeta], commonPrefixes: [String],
        isTruncated: Bool, nextKeyMarker: String?, nextVersionIdMarker: String?
    ) {
        let local = {
            try ObjectFileHandler.listAllVersions(
                bucketName: bucketName, prefix: prefix, delimiter: delimiter,
                keyMarker: keyMarker, versionIdMarker: versionIdMarker, maxKeys: maxKeys)
        }
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            return try local()
        }
        let peers = await activePeers(config: config)
        guard !peers.isEmpty else { return try local() }

        let localResult = try local()
        var pages = [
            VersionsPage(
                versions: localResult.versions, deleteMarkers: localResult.deleteMarkers,
                commonPrefixes: localResult.commonPrefixes, isTruncated: localResult.isTruncated)
        ]

        await withTaskGroup(of: VersionsPage?.self) { group in
            for node in peers {
                group.addTask {
                    do {
                        let r = try await ClusterListingClient.fetchVersionsPage(
                            app: req.application, from: node, bucketName: bucketName,
                            prefix: prefix, delimiter: delimiter, maxKeys: maxKeys,
                            keyMarker: keyMarker, versionIdMarker: versionIdMarker)
                        return VersionsPage(
                            versions: r.versions, deleteMarkers: r.deleteMarkers,
                            commonPrefixes: r.commonPrefixes, isTruncated: r.isTruncated)
                    } catch {
                        req.logger.warning(
                            "Cluster ListObjectVersions: peer \(node.id) unreachable for '\(bucketName)', proceeding best-effort: \(error)"
                        )
                        return nil
                    }
                }
            }
            for await page in group { if let page { pages.append(page) } }
        }

        return mergeVersionsPages(pages, maxKeys: maxKeys)
    }

    // MARK: - Multipart uploads (ListMultipartUploads)

    static func listUploads(
        req: Request, bucketName: String, prefix: String, keyMarker: String?,
        uploadIdMarker: String?, maxUploads: Int
    ) async throws -> (
        uploads: [MultipartUploadMeta], isTruncated: Bool, nextKeyMarker: String?,
        nextUploadIdMarker: String?
    ) {
        let local = {
            try MultipartFileHandler.listUploads(
                bucketName: bucketName, prefix: prefix, keyMarker: keyMarker,
                uploadIdMarker: uploadIdMarker, maxUploads: maxUploads)
        }
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            return try local()
        }
        let peers = await activePeers(config: config)
        guard !peers.isEmpty else { return try local() }

        let localResult = try local()
        var pages = [
            UploadsPage(uploads: localResult.uploads, isTruncated: localResult.isTruncated)
        ]

        await withTaskGroup(of: UploadsPage?.self) { group in
            for node in peers {
                group.addTask {
                    do {
                        let r = try await ClusterListingClient.fetchUploadsPage(
                            app: req.application, from: node, bucketName: bucketName,
                            prefix: prefix, maxUploads: maxUploads, keyMarker: keyMarker,
                            uploadIdMarker: uploadIdMarker)
                        return UploadsPage(uploads: r.uploads, isTruncated: r.isTruncated)
                    } catch {
                        req.logger.warning(
                            "Cluster ListMultipartUploads: peer \(node.id) unreachable for '\(bucketName)', proceeding best-effort: \(error)"
                        )
                        return nil
                    }
                }
            }
            for await page in group { if let page { pages.append(page) } }
        }

        return mergeUploadsPages(pages, maxUploads: maxUploads)
    }

    // MARK: - Empty-bucket check (DeleteBucket safety gate)

    /// Fails closed: any unreachable peer rethrows rather than letting `DeleteBucket` proceed on
    /// an incomplete view - see the type-level doc comment for why this one operation departs
    /// from every other listing's best-effort tolerance.
    static func hasBucketObjects(req: Request, bucketName: String) async throws -> Bool {
        if ObjectFileHandler.hasBucketObjects(bucketName: bucketName) {
            return true
        }
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            return false
        }
        let peers = await activePeers(config: config)
        guard !peers.isEmpty else { return false }

        return try await withThrowingTaskGroup(of: Bool.self) { group in
            for node in peers {
                group.addTask {
                    try await ClusterListingClient.fetchHasObjects(
                        app: req.application, from: node, bucketName: bucketName)
                }
            }
            defer { group.cancelAll() }
            for try await hasObjects in group where hasObjects {
                return true
            }
            return false
        }
    }

    // MARK: - Stats (admin console)

    /// Cluster-wide size and object count under `prefix` - cached, coalesced and walk-limited
    /// through `StorageStatsService` (issue #28), so a page of folder rows or a polling
    /// dashboard never turns into a storm of directory walks.
    static func calculateStats(
        req: Request, bucketName: String, prefix: String
    ) async throws -> StorageStatsService.Stats {
        let app = req.application
        let active = app.storage[ClusterConfigurationKey.self] == nil
            ? [] : await ClusterNodeCache.shared.activeNodes()
        let key = StorageStatsService.Key(
            scope: .clusterTotal, bucketName: bucketName, prefix: prefix, activeNodes: active)
        // With peers, the total is a sum of shares each node caches and invalidates for itself;
        // caching the sum here would hide their changes. Alone, it IS this node's own figure.
        let aggregatesPeers = active.contains { $0.id != app.storage[ClusterConfigurationKey.self]?.nodeId }
        return try await StorageStatsService.shared.stats(
            for: key, policy: aggregatesPeers ? .coalesceOnly : .cache
        ) {
            try await uncachedClusterStats(
                app: app, bucketName: bucketName, prefix: prefix, activeNodes: active)
        }
    }

    private static func uncachedClusterStats(
        app: Application, bucketName: String, prefix: String, activeNodes active: [ClusterNodeInfo]
    ) async throws -> StorageStatsService.Computation {
        let local = { @Sendable in
            let (sizeBytes, objectCount) = BucketHandler.calculateStats(bucketName: bucketName, prefix: prefix)
            return StorageStatsService.Computation(sizeBytes: sizeBytes, objectCount: objectCount)
        }
        guard let config = app.storage[ClusterConfigurationKey.self] else {
            return try await StorageStatsService.shared.localWalk(app: app, local)
        }
        let peers = active.filter { $0.id != config.nodeId }
        guard !peers.isEmpty else {
            return try await StorageStatsService.shared.localWalk(app: app, local)
        }

        var totalSize: Int64 = 0
        var totalCount = 0
        var everyPeerAnswered = true
        let own = try await ownedStats(
            app: app, bucketName: bucketName, prefix: prefix, activeNodes: active,
            selfNodeId: config.nodeId)
        totalSize += own.sizeBytes
        totalCount += own.objectCount

        await withTaskGroup(of: (sizeBytes: Int64, objectCount: Int)?.self) { group in
            for node in peers {
                group.addTask {
                    do {
                        return try await ClusterListingClient.fetchOwnedStats(
                            app: app, from: node, bucketName: bucketName,
                            prefix: prefix)
                    } catch {
                        app.logger.warning(
                            "Cluster stats: peer \(node.id) unreachable for '\(bucketName)', proceeding best-effort: \(error)"
                        )
                        return nil
                    }
                }
            }
            for await result in group {
                if let result {
                    totalSize += result.sizeBytes
                    totalCount += result.objectCount
                } else {
                    everyPeerAnswered = false
                }
            }
        }

        // A total missing a peer's share is a lower bound - served, but not cached.
        return StorageStatsService.Computation(
            sizeBytes: totalSize, objectCount: totalCount, complete: everyPeerAnswered)
    }

    /// Per-node storage breakdown across the *whole cluster* (every bucket), for the admin
    /// console's storage-distribution view - distinct from `calculateStats`, which sums every
    /// node's share into one cluster-wide total. Each node's contribution is non-overlapping by
    /// construction (the primary-owner convention `ownedStats` uses), so there's no merge/dedup
    /// step. Best-effort: an unreachable peer's entry is simply omitted.
    static func nodeStorageBreakdown(
        req: Request
    ) async throws -> [(nodeId: UUID, sizeBytes: Int64, objectCount: Int)] {
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            return []
        }
        let active = await ClusterNodeCache.shared.activeNodes()
        let peers = active.filter { $0.id != config.nodeId }

        let own = try await ownedStatsAllBuckets(
            app: req.application, activeNodes: active, selfNodeId: config.nodeId)
        var results: [(nodeId: UUID, sizeBytes: Int64, objectCount: Int)] = [
            (config.nodeId, own.sizeBytes, own.objectCount)
        ]

        await withTaskGroup(of: (nodeId: UUID, sizeBytes: Int64, objectCount: Int)?.self) { group in
            for node in peers {
                group.addTask {
                    // A short retry, not a single shot: this endpoint is polled for a
                    // point-in-time cluster-wide total, and a transiently busy peer (e.g.
                    // running its own reactive rebalance walk) would otherwise be silently
                    // omitted, undercounting the true total for this one snapshot. Bounded (2
                    // retries) so this still returns promptly for the admin dashboard.
                    for delay: Duration? in [nil, .milliseconds(750), .seconds(2)] {
                        if let delay { try? await Task.sleep(for: delay) }
                        do {
                            let r = try await ClusterListingClient.fetchOwnedStatsAll(
                                app: req.application, from: node)
                            return (node.id, r.sizeBytes, r.objectCount)
                        } catch {
                            continue
                        }
                    }
                    req.logger.warning(
                        "Cluster storage breakdown: peer \(node.id) unreachable after retries, omitting from result"
                    )
                    return nil
                }
            }
            for await result in group {
                if let result { results.append(result) }
            }
        }

        return results
    }

    /// Cluster-wide (all-buckets) generalization of `ownedStats` - same primary-owner counting
    /// rule, just looped over every bucket instead of one.
    static func ownedStatsAllBuckets(
        app: Application, activeNodes: [ClusterNodeInfo], selfNodeId: UUID
    ) async throws -> (sizeBytes: Int64, objectCount: Int) {
        let key = StorageStatsService.Key(
            scope: .ownedShareAllBuckets, bucketName: "", prefix: "", activeNodes: activeNodes)
        return try await StorageStatsService.shared.stats(for: key) {
            let buckets = await Bucket.all(app: app)
            var sizeBytes: Int64 = 0
            var objectCount = 0
            for bucket in buckets {
                let stats = try await ownedStats(
                    app: app, bucketName: bucket.name, prefix: "", activeNodes: activeNodes,
                    selfNodeId: selfNodeId)
                sizeBytes += stats.sizeBytes
                objectCount += stats.objectCount
            }
            return StorageStatsService.Computation(sizeBytes: sizeBytes, objectCount: objectCount)
        }.summary
    }

    /// `ownedStats` as every caller should run it: cached per (bucket, prefix) and walk-limited.
    /// The raw walker below stays available for the cache to call.
    static func ownedStats(
        app: Application, bucketName: String, prefix: String, activeNodes: [ClusterNodeInfo],
        selfNodeId: UUID
    ) async throws -> (sizeBytes: Int64, objectCount: Int) {
        let key = StorageStatsService.Key(
            scope: .ownedShare, bucketName: bucketName, prefix: prefix, activeNodes: activeNodes)
        return try await StorageStatsService.shared.stats(for: key) {
            try await StorageStatsService.shared.localWalk(app: app) {
                let (sizeBytes, objectCount) = try ownedStats(
                    bucketName: bucketName, prefix: prefix, activeNodes: activeNodes,
                    selfNodeId: selfNodeId)
                return StorageStatsService.Computation(sizeBytes: sizeBytes, objectCount: objectCount)
            }
        }.summary
    }

    /// Each historical version and delete marker is counted toward exactly one node's local
    /// sum: the top-HRW-ranked ("primary") node among currently active nodes for that key. Since
    /// placement is per-key, this avoids the replication-factor triple-count without needing a
    /// full listing merge. Shared by `calculateStats` and by a peer computing its own share via
    /// `InternalClusterListingController.handleOwnedStats`.
    static func ownedStats(
        bucketName: String, prefix: String, activeNodes: [ClusterNodeInfo], selfNodeId: UUID
    ) throws -> (sizeBytes: Int64, objectCount: Int) {
        var sizeBytes: Int64 = 0
        var objectCount = 0
        var keyMarker: String?
        var versionIdMarker: String?
        repeat {
            let (versions, deleteMarkers, _, isTruncated, nextKeyMarker, nextVersionIdMarker) =
                try ObjectFileHandler.listAllVersions(
                    bucketName: bucketName, prefix: prefix, delimiter: nil, keyMarker: keyMarker,
                    versionIdMarker: versionIdMarker, maxKeys: 1000)

            for entry in versions + deleteMarkers {
                let responsible = PlacementService.responsibleNodes(
                    bucketName: bucketName, key: entry.key, activeNodes: activeNodes)
                guard responsible.first?.id == selfNodeId else { continue }
                sizeBytes += Int64(entry.size)
                objectCount += 1
            }

            keyMarker = isTruncated ? nextKeyMarker : nil
            versionIdMarker = isTruncated ? nextVersionIdMarker : nil
        } while keyMarker != nil

        // listAllVersions above already folds in this node's local EC shard-0 entries, and the
        // top-3-is-a-prefix-of-top-(k+m) invariant means `responsible.first?.id == selfNodeId`
        // correctly identifies the EC rank-0 owner too - no separate EC pass needed here.

        return (sizeBytes, objectCount)
    }

    // MARK: - Shared helpers

    private static func activePeers(config: ClusterConfiguration) async -> [ClusterNodeInfo] {
        await ClusterNodeCache.shared.activeNodes().filter { $0.id != config.nodeId }
    }

    // MARK: - Pure merge functions (no DB/HTTP - directly unit-testable)

    struct ObjectsPage {
        let objects: [ObjectMeta]
        let commonPrefixes: [String]
        let isTruncated: Bool
    }

    struct VersionsPage {
        let versions: [ObjectMeta]
        let deleteMarkers: [ObjectMeta]
        let commonPrefixes: [String]
        let isTruncated: Bool
    }

    struct UploadsPage {
        let uploads: [MultipartUploadMeta]
        let isTruncated: Bool
    }

    /// Merges per-node `ListObjects` pages into one globally-correct page. `pages` must have the
    /// coordinator's own local page first - that's what makes "first-seen wins" on a duplicate
    /// key (present on multiple replicas) mean "prefer the local copy." See the type-level doc
    /// comment on `ClusterListingService` for the pagination correctness argument this relies on.
    static func mergeObjectsPages(
        _ pages: [ObjectsPage], maxKeys: Int
    ) -> (objects: [ObjectMeta], commonPrefixes: [String], isTruncated: Bool, nextMarker: String?) {
        enum Entry {
            case object(ObjectMeta)
            case prefix(String)
            var sortKey: String {
                switch self {
                case .object(let m): m.key
                case .prefix(let p): p
                }
            }
        }

        var seenKeys = Set<String>()
        var seenPrefixes = Set<String>()
        var entries: [Entry] = []
        var anyTruncated = false

        for page in pages {
            anyTruncated = anyTruncated || page.isTruncated
            for object in page.objects where seenKeys.insert(object.key).inserted {
                entries.append(.object(object))
            }
            for prefix in page.commonPrefixes where seenPrefixes.insert(prefix).inserted {
                entries.append(.prefix(prefix))
            }
        }
        entries.sort { $0.sortKey < $1.sortKey }

        let dedupedCount = entries.count
        let isTruncated = dedupedCount > maxKeys || anyTruncated
        let limited = Array(entries.prefix(maxKeys))

        var objects: [ObjectMeta] = []
        var commonPrefixes: [String] = []
        for entry in limited {
            switch entry {
            case .object(let m): objects.append(m)
            case .prefix(let p): commonPrefixes.append(p)
            }
        }

        return (
            objects, commonPrefixes, isTruncated,
            isTruncated ? limited.last?.sortKey : nil
        )
    }

    /// Same shape as `mergeObjectsPages`; dedup key is `(key, versionId ?? "null")` since
    /// versions and delete markers share one id-space per key (a given version id is either a
    /// real version or a delete marker, never both). commonPrefixes are unioned/sorted but
    /// deliberately never counted toward `maxKeys`/`isTruncated` - preserving the existing local
    /// `ObjectFileHandler.listAllVersions` quirk rather than "fixing" it as a side effect of
    /// this change.
    static func mergeVersionsPages(
        _ pages: [VersionsPage], maxKeys: Int
    ) -> (
        versions: [ObjectMeta], deleteMarkers: [ObjectMeta], commonPrefixes: [String],
        isTruncated: Bool, nextKeyMarker: String?, nextVersionIdMarker: String?
    ) {
        var seen = Set<String>()
        var seenPrefixes = Set<String>()
        var combined: [(meta: ObjectMeta, isDeleteMarker: Bool)] = []
        var commonPrefixes: [String] = []
        var anyTruncated = false

        for page in pages {
            anyTruncated = anyTruncated || page.isTruncated
            for meta in page.versions
            where seen.insert("\(meta.key)\u{0}\(meta.versionId ?? "null")").inserted {
                combined.append((meta, false))
            }
            for meta in page.deleteMarkers
            where seen.insert("\(meta.key)\u{0}\(meta.versionId ?? "null")").inserted {
                combined.append((meta, true))
            }
            for prefix in page.commonPrefixes where seenPrefixes.insert(prefix).inserted {
                commonPrefixes.append(prefix)
            }
        }
        commonPrefixes.sort()

        combined.sort {
            if $0.meta.key != $1.meta.key { return $0.meta.key < $1.meta.key }
            return $0.meta.updatedAt > $1.meta.updatedAt
        }

        let dedupedCount = combined.count
        let isTruncated = dedupedCount > maxKeys || anyTruncated
        let limited = Array(combined.prefix(maxKeys))

        let versions = limited.filter { !$0.isDeleteMarker }.map(\.meta)
        let deleteMarkers = limited.filter(\.isDeleteMarker).map(\.meta)

        var nextKeyMarker: String?
        var nextVersionIdMarker: String?
        if isTruncated, let last = limited.last {
            nextKeyMarker = last.meta.key
            nextVersionIdMarker = last.meta.versionId
        }

        return (versions, deleteMarkers, commonPrefixes, isTruncated, nextKeyMarker, nextVersionIdMarker)
    }

    /// Same shape again; dedup key is `(key, uploadId)`. In steady state a given in-progress
    /// upload lives on exactly one node (uploads aren't actively replicated mid-flight, only the
    /// final completed object is), so cross-node duplicates should be rare - the dedup here is
    /// cheap defensive coding, not load-bearing for correctness.
    static func mergeUploadsPages(
        _ pages: [UploadsPage], maxUploads: Int
    ) -> (
        uploads: [MultipartUploadMeta], isTruncated: Bool, nextKeyMarker: String?,
        nextUploadIdMarker: String?
    ) {
        var seen = Set<String>()
        var uploads: [MultipartUploadMeta] = []
        var anyTruncated = false

        for page in pages {
            anyTruncated = anyTruncated || page.isTruncated
            for upload in page.uploads
            where seen.insert("\(upload.key)\u{0}\(upload.uploadId)").inserted {
                uploads.append(upload)
            }
        }

        uploads.sort {
            if $0.key != $1.key { return $0.key < $1.key }
            return $0.uploadId < $1.uploadId
        }

        let dedupedCount = uploads.count
        let isTruncated = dedupedCount > maxUploads || anyTruncated
        let limited = Array(uploads.prefix(maxUploads))

        var nextKeyMarker: String?
        var nextUploadIdMarker: String?
        if isTruncated, let last = limited.last {
            nextKeyMarker = last.key
            nextUploadIdMarker = last.uploadId
        }

        return (limited, isTruncated, nextKeyMarker, nextUploadIdMarker)
    }
}
