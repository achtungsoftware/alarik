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

/// Slices an in-memory array the same way FluentKit's `QueryBuilder.paginate(_:)` used to slice a
/// query - see `Page.swift` for the (now hand-rolled, Fluent-free) `Page`/`PageMetadata`/
/// `PageRequest` types. Every `MetadataStore`-backed collection is small enough to list in full
/// (see `MetadataListingService`'s doc comment), so pagination only ever needs to happen here, in
/// memory, never against a live query.
extension Array where Element: Sendable {
    func paginated(for req: Request) throws -> Page<Element> {
        let pageRequest = try req.query.decode(PageRequest.self)
        let page = Swift.max(pageRequest.page, 1)
        let per = Swift.max(pageRequest.per, 1)
        let start = Swift.min((page - 1) * per, count)
        let end = Swift.min(start + per, count)
        return Page(
            items: Array(self[start..<end]),
            metadata: PageMetadata(page: page, per: per, total: count))
    }
}
