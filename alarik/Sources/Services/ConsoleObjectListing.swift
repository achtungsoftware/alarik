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

/// The object browser's listing, as cursor pages over the storage layer's own marker
/// pagination (issue #27). A page costs what it returns: the directory fast path reads one
/// header per listed object after the marker, cluster-wide merging is per page, and nothing is
/// ever counted or sorted beyond `per` entries - so a folder with a million objects pages as
/// fast as one with ten, and there is no ceiling on how far a user can page.
///
/// Items come back in S3 order (keys and folders interleaved lexicographically); grouping
/// folders first within a page is a presentation choice left to the client.
enum ConsoleObjectListing {
    /// Hard ceiling on one page - the directory fast path reads a header per returned object.
    static let maxPageSize = 1000
    static let defaultPageSize = 100

    /// Keys each node examines per search request before handing the scan position back to
    /// the client as `nextMarker`, so a search over a huge bucket streams in steps instead of
    /// holding one request open for the whole walk. Headers are read for matches only.
    static let searchScanBudget = 5_000

    /// The one definition of "key matches search term", shared with peers answering a scan.
    static func keyFilter(matching term: String) -> @Sendable (String) -> Bool {
        { $0.localizedCaseInsensitiveContains(term) }
    }

    static func clampedPageSize(_ requested: Int?) -> Int {
        min(max(requested ?? defaultPageSize, 1), maxPageSize)
    }

    /// One page of `prefix` after `marker`: its direct children with the default `/` delimiter,
    /// or a flat recursive listing when `delimiter` is nil.
    static func page(
        req: Request, bucketName: String, prefix: String, delimiter: String? = "/", per: Int,
        marker: String?
    ) async throws -> CursorPage<ObjectMeta.ResponseDTO> {
        let (objects, commonPrefixes, isTruncated, nextMarker) = try await ClusterListingService.listObjects(
            req: req, bucketName: bucketName, prefix: prefix, delimiter: delimiter, maxKeys: per,
            marker: marker)
        return CursorPage(
            items: Self.interleaved(objects: objects, commonPrefixes: commonPrefixes),
            nextMarker: nextMarker, isTruncated: isTruncated)
    }

    /// One search step: objects anywhere under `prefix` whose key contains `term`, among the
    /// next `scanBudget` keys per node. The page may hold fewer than `per` matches - even none -
    /// while `isTruncated` is true: the scan budget ran out first, and `nextMarker` is where to
    /// resume. Folders are not reported while searching; matches are objects.
    static func searchPage(
        req: Request, bucketName: String, prefix: String, term: String, per: Int, marker: String?,
        scanBudget: Int = searchScanBudget
    ) async throws -> CursorPage<ObjectMeta.ResponseDTO> {
        let step = try await ClusterListingService.searchObjects(
            req: req, bucketName: bucketName, prefix: prefix, term: term, scanLimit: scanBudget,
            marker: marker)
        let items = Array(step.matches.prefix(per))
        if step.matches.count > per {
            // More matches than fit: resume right after the last one returned.
            return CursorPage(
                items: items.map(ObjectMeta.ResponseDTO.init(from:)),
                nextMarker: items.last?.key, isTruncated: true)
        }
        // Otherwise resume where the scan stopped. A truncated step with no position to resume
        // from cannot make progress, so it ends the scan rather than looping the client.
        let scanMarker = step.isTruncated ? step.nextMarker : nil
        return CursorPage(
            items: items.map(ObjectMeta.ResponseDTO.init(from:)),
            nextMarker: scanMarker, isTruncated: scanMarker != nil)
    }

    /// S3 order: objects and common prefixes merged lexicographically, as the storage layer
    /// already sorted them.
    static func interleaved(objects: [ObjectMeta], commonPrefixes: [String]) -> [ObjectMeta.ResponseDTO] {
        var items: [ObjectMeta.ResponseDTO] = []
        items.reserveCapacity(objects.count + commonPrefixes.count)
        var objectIndex = 0
        var prefixIndex = 0
        while objectIndex < objects.count || prefixIndex < commonPrefixes.count {
            let nextObject = objectIndex < objects.count ? objects[objectIndex] : nil
            let nextPrefix = prefixIndex < commonPrefixes.count ? commonPrefixes[prefixIndex] : nil
            if let nextObject, nextPrefix == nil || nextObject.key < nextPrefix! {
                items.append(ObjectMeta.ResponseDTO(from: nextObject))
                objectIndex += 1
            } else if let nextPrefix {
                items.append(ObjectMeta.ResponseDTO(folderKey: nextPrefix))
                prefixIndex += 1
            }
        }
        return items
    }
}
