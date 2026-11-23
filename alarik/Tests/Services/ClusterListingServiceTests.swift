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
import Testing

@testable import Alarik

/// The merge/dedup/truncation algorithm behind cluster-wide listing is a pure function of
/// per-node pages - no DB, no HTTP, no cluster required - so this suite runs everywhere `swift
/// test` runs, same philosophy as `PlacementServiceTests`. The actual multi-node fan-out and a
/// node going unreachable mid-listing are covered by `cluster_tests.sh`'s real 4-node suite.
@Suite("ClusterListingService merge tests")
struct ClusterListingServiceTests {
    private func object(_ key: String, versionId: String? = nil, updatedAt: Date = Date())
        -> ObjectMeta
    {
        ObjectMeta(
            bucketName: "b", key: key, size: 1, contentType: "text/plain", etag: "\"e\"",
            updatedAt: updatedAt, versionId: versionId)
    }

    private func upload(_ key: String, _ uploadId: String) -> MultipartUploadMeta {
        MultipartUploadMeta(
            uploadId: uploadId, bucketName: "b", key: key, contentType: "text/plain",
            metadata: [:], initiated: Date())
    }

    // MARK: - mergeObjectsPages

    @Test("non-overlapping pages merge and sort, objects and commonPrefixes interleaved")
    func objectsNonOverlappingMerge() {
        let local = ClusterListingService.ObjectsPage(
            objects: [object("b-key"), object("d-key")], commonPrefixes: ["c-prefix/"],
            isTruncated: false)
        let peer = ClusterListingService.ObjectsPage(
            objects: [object("a-key")], commonPrefixes: ["e-prefix/"], isTruncated: false)

        let result = ClusterListingService.mergeObjectsPages([local, peer], maxKeys: 1000)

        let allKeys = (result.objects.map(\.key) + result.commonPrefixes)
        // Merge doesn't itself re-sort objects/commonPrefixes into a single returned array (it
        // returns them as separate lists), but the truncation window they were drawn from must
        // still be lexicographically ordered - reconstruct that combined order by re-sorting the
        // union and comparing membership/order isn't directly observable, so assert count/
        // membership instead, and separately spot-check ordering-sensitive fields.
        #expect(Set(allKeys) == ["a-key", "b-key", "c-prefix/", "d-key", "e-prefix/"])
        #expect(result.objects.map(\.key) == ["a-key", "b-key", "d-key"])
        #expect(result.commonPrefixes == ["c-prefix/", "e-prefix/"])
        #expect(result.isTruncated == false)
        #expect(result.nextMarker == nil)
    }

    @Test("duplicate key across pages dedupes to the first (local) page's copy")
    func objectsDedupePrefersLocal() {
        let local = ClusterListingService.ObjectsPage(
            objects: [object("shared", versionId: "local-version")], commonPrefixes: [],
            isTruncated: false)
        let peer = ClusterListingService.ObjectsPage(
            objects: [object("shared", versionId: "peer-version")], commonPrefixes: [],
            isTruncated: false)

        let result = ClusterListingService.mergeObjectsPages([local, peer], maxKeys: 1000)

        #expect(result.objects.count == 1)
        #expect(result.objects.first?.versionId == "local-version")
    }

    @Test("deduped count exceeding maxKeys truncates and derives nextMarker from the last kept key")
    func objectsTruncation() {
        let local = ClusterListingService.ObjectsPage(
            objects: [object("a"), object("b"), object("c")], commonPrefixes: [],
            isTruncated: false)

        let result = ClusterListingService.mergeObjectsPages([local], maxKeys: 2)

        #expect(result.objects.map(\.key) == ["a", "b"])
        #expect(result.isTruncated == true)
        #expect(result.nextMarker == "b")
    }

    @Test("a peer page reporting isTruncated forces the merged result truncated even with a small union")
    func objectsPeerTruncationPropagates() {
        let local = ClusterListingService.ObjectsPage(
            objects: [object("a")], commonPrefixes: [], isTruncated: false)
        let peer = ClusterListingService.ObjectsPage(
            objects: [object("b")], commonPrefixes: [], isTruncated: true)

        let result = ClusterListingService.mergeObjectsPages([local, peer], maxKeys: 1000)

        #expect(result.objects.count == 2)
        #expect(result.isTruncated == true)
        #expect(result.nextMarker == "b")
    }

    @Test("no pages, or only empty pages, yields an empty untruncated result")
    func objectsEmptyPages() {
        let empty = ClusterListingService.mergeObjectsPages([], maxKeys: 1000)
        #expect(empty.objects.isEmpty)
        #expect(empty.commonPrefixes.isEmpty)
        #expect(empty.isTruncated == false)
        #expect(empty.nextMarker == nil)

        let onlyEmptyPages = ClusterListingService.mergeObjectsPages(
            [
                ClusterListingService.ObjectsPage(
                    objects: [], commonPrefixes: [], isTruncated: false)
            ], maxKeys: 1000)
        #expect(onlyEmptyPages.objects.isEmpty)
        #expect(onlyEmptyPages.isTruncated == false)
    }

    // MARK: - mergeVersionsPages

    @Test("versions dedupe by (key, versionId), sorted by key asc then updatedAt desc")
    func versionsDedupeAndSort() {
        let older = Date(timeIntervalSince1970: 1000)
        let newer = Date(timeIntervalSince1970: 2000)

        let local = ClusterListingService.VersionsPage(
            versions: [object("k", versionId: "v1", updatedAt: newer)], deleteMarkers: [],
            commonPrefixes: [], isTruncated: false)
        let peer = ClusterListingService.VersionsPage(
            // Same (key, versionId) as local - must be deduped away, keeping local's copy.
            versions: [object("k", versionId: "v1", updatedAt: older)],
            deleteMarkers: [],
            commonPrefixes: [], isTruncated: false)
        let peer2 = ClusterListingService.VersionsPage(
            versions: [object("k", versionId: "v2", updatedAt: older)], deleteMarkers: [],
            commonPrefixes: [], isTruncated: false)

        let result = ClusterListingService.mergeVersionsPages([local, peer, peer2], maxKeys: 1000)

        #expect(result.versions.count == 2)
        // Same key -> newer updatedAt (v1) sorts before older (v2).
        #expect(result.versions.map(\.versionId) == ["v1", "v2"])
    }

    @Test("versions and delete markers combine into one truncation window, tie-broken correctly")
    func versionsAndDeleteMarkersCombinedTruncation() {
        let t1 = Date(timeIntervalSince1970: 1000)
        let t2 = Date(timeIntervalSince1970: 2000)
        let t3 = Date(timeIntervalSince1970: 3000)

        var deleteMarker = object("k", versionId: "dm", updatedAt: t3)
        deleteMarker.isDeleteMarker = true

        let local = ClusterListingService.VersionsPage(
            versions: [
                object("k", versionId: "v1", updatedAt: t2),
                object("k", versionId: "v2", updatedAt: t1),
            ],
            deleteMarkers: [deleteMarker],
            commonPrefixes: [], isTruncated: false)

        // 3 total combined entries (dm@t3, v1@t2, v2@t1), maxKeys 2 -> keep dm, v1; next marker
        // should point at v1 (the last kept item after sorting key asc/updatedAt desc).
        let result = ClusterListingService.mergeVersionsPages([local], maxKeys: 2)

        #expect(result.isTruncated == true)
        #expect(result.deleteMarkers.map(\.versionId) == ["dm"])
        #expect(result.versions.map(\.versionId) == ["v1"])
        #expect(result.nextKeyMarker == "k")
        #expect(result.nextVersionIdMarker == "v1")
    }

    @Test("versions: common prefixes merge in key order and count toward the page like keys")
    func versionsCommonPrefixesCountTowardTruncation() {
        let local = ClusterListingService.VersionsPage(
            versions: [object("m", versionId: "v1")], deleteMarkers: [],
            commonPrefixes: ["z-prefix/", "a-prefix/"], isTruncated: false)
        let peer = ClusterListingService.VersionsPage(
            versions: [], deleteMarkers: [],
            // Duplicate of local's folder - merged away.
            commonPrefixes: ["a-prefix/"], isTruncated: false)

        let all = ClusterListingService.mergeVersionsPages([local, peer], maxKeys: 1000)
        #expect(all.commonPrefixes == ["a-prefix/", "z-prefix/"])
        #expect(all.versions.map(\.key) == ["m"])
        #expect(all.isTruncated == false)

        // Three entries in key order (a-prefix/, m, z-prefix/); a page of two ends on the key.
        let two = ClusterListingService.mergeVersionsPages([local, peer], maxKeys: 2)
        #expect(two.commonPrefixes == ["a-prefix/"])
        #expect(two.versions.map(\.key) == ["m"])
        #expect(two.isTruncated == true)
        #expect(two.nextKeyMarker == "m")
        #expect(two.nextVersionIdMarker == "v1")

        // A page ending on a folder resumes after the whole folder: key marker only.
        let one = ClusterListingService.mergeVersionsPages([local, peer], maxKeys: 1)
        #expect(one.commonPrefixes == ["a-prefix/"])
        #expect(one.versions.isEmpty)
        #expect(one.isTruncated == true)
        #expect(one.nextKeyMarker == "a-prefix/")
        #expect(one.nextVersionIdMarker == nil)
    }

    // MARK: - mergeUploadsPages

    @Test("uploads dedupe by (key, uploadId), sorted by key then uploadId")
    func uploadsDedupeAndSort() {
        let local = ClusterListingService.UploadsPage(
            uploads: [upload("k2", "u1"), upload("k1", "u2")], isTruncated: false)
        let peer = ClusterListingService.UploadsPage(
            // Duplicate of local's first upload - must be deduped.
            uploads: [upload("k2", "u1"), upload("k1", "u1")], isTruncated: false)

        let result = ClusterListingService.mergeUploadsPages([local, peer], maxUploads: 1000)

        #expect(result.uploads.count == 3)
        #expect(
            result.uploads.map { "\($0.key):\($0.uploadId)" }
                == ["k1:u1", "k1:u2", "k2:u1"])
        #expect(result.isTruncated == false)
    }

    @Test("uploads truncation derives nextKeyMarker/nextUploadIdMarker from the last kept entry")
    func uploadsTruncation() {
        let local = ClusterListingService.UploadsPage(
            uploads: [upload("k1", "u1"), upload("k1", "u2"), upload("k2", "u1")],
            isTruncated: false)

        let result = ClusterListingService.mergeUploadsPages([local], maxUploads: 2)

        #expect(result.uploads.map(\.uploadId) == ["u1", "u2"])
        #expect(result.isTruncated == true)
        #expect(result.nextKeyMarker == "k1")
        #expect(result.nextUploadIdMarker == "u2")
    }
}

/// Search steps merge differently from listings: each node reports where its scan *stopped*,
/// and the merged position must be the earliest of those so no node's keys are ever skipped.
@Suite("ClusterListingService search merge tests")
struct ClusterListingSearchMergeTests {
    private func meta(_ key: String) -> ObjectMeta {
        ObjectMeta(bucketName: "b", key: key, size: 1, contentType: "x", etag: "e", updatedAt: Date())
    }
    private func page(_ keys: [String], stoppedAt: String?) -> ClusterListingService.SearchPage {
        .init(matches: keys.map(meta), isTruncated: stoppedAt != nil, nextMarker: stoppedAt)
    }

    @Test("the merged scan position is the earliest stop, and matches past it are deferred")
    func earliestStopWins() {
        let merged = ClusterListingService.mergeSearchPages([
            page(["a-log", "m-log"], stoppedAt: "p"),
            page(["c-log", "z-log"], stoppedAt: "k"),
        ])
        #expect(merged.matches.map(\.key) == ["a-log", "c-log"])
        #expect(merged.isTruncated && merged.nextMarker == "k")
    }

    @Test("a node that finished contributes everything; the scan ends only when every node did")
    func exhaustedNodes() {
        let partial = ClusterListingService.mergeSearchPages([
            page(["a-log", "z-log"], stoppedAt: nil),
            page(["b-log"], stoppedAt: "q"),
        ])
        #expect(partial.matches.map(\.key) == ["a-log", "b-log"])
        #expect(partial.isTruncated && partial.nextMarker == "q")

        let done = ClusterListingService.mergeSearchPages([
            page(["a-log"], stoppedAt: nil), page(["a-log", "b-log"], stoppedAt: nil),
        ])
        #expect(done.matches.map(\.key) == ["a-log", "b-log"])
        #expect(!done.isTruncated && done.nextMarker == nil)
    }
}
