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

/// A hand-rolled stand-in for FluentKit's `Page`/`PageMetadata`/`PageRequest` - same field names
/// and JSON shape (`items`/`metadata: {page, per, total}`), so every existing API response and
/// every console call site is unaffected by this migration off Fluent entirely. `InMemoryPaginate
/// .swift` is the only thing that actually slices an array into one of these.
struct PageMetadata: Content {
    /// Current page number. Starts at `1`.
    let page: Int
    /// Max items per page.
    let per: Int
    /// Total number of items available.
    let total: Int

    /// Computed total number of pages with `1` being the minimum.
    var pageCount: Int {
        let count = Int((Double(total) / Double(per)).rounded(.up))
        return count < 1 ? 1 : count
    }

    init(page: Int, per: Int, total: Int) {
        self.page = page
        self.per = per
        self.total = total
    }
}

/// Represents information needed to generate a `Page` from the full result set.
struct PageRequest: Content {
    /// Page number to request. Starts at `1`.
    let page: Int
    /// Max items per page.
    let per: Int

    private enum CodingKeys: String, CodingKey {
        case page
        case per
    }

    init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.page = try container.decodeIfPresent(Int.self, forKey: .page) ?? 1
        self.per = try container.decodeIfPresent(Int.self, forKey: .per) ?? 10
    }

    init(page: Int, per: Int) {
        self.page = page
        self.per = per
    }
}

/// A single section of a larger, traversable result set. Generic over any `Sendable` (not just
/// `Content`) so intermediate pages over a raw model - before `.map { $0.toResponseDTO() }`
/// converts to a wire DTO - still type-check; only the version actually returned from a route
/// handler needs `Content`, granted below.
struct Page<T: Sendable>: Sendable {
    /// The page's items.
    let items: [T]
    /// Metadata containing information about current page, items per page, and total items.
    let metadata: PageMetadata

    init(items: [T], metadata: PageMetadata) {
        self.items = items
        self.metadata = metadata
    }

    /// Maps a page's items to a different type using the supplied closure.
    func map<U: Sendable>(_ transform: (T) throws -> U) rethrows -> Page<U> {
        try .init(items: items.map(transform), metadata: metadata)
    }
}

extension Page: Encodable where T: Encodable {}
extension Page: Decodable where T: Decodable {}
extension Page: Content, ResponseEncodable, RequestDecodable, AsyncResponseEncodable,
    AsyncRequestDecodable
where T: Codable {}
