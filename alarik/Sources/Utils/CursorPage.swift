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

/// A page addressed by an opaque continuation marker rather than an offset - the shape for
/// listings whose total is unknown or too expensive to count (object listings: issue #27).
/// Pass `nextMarker` back as `marker` to fetch the following page; `isTruncated == false`
/// means this was the last one. Wire shape: `{ items, nextMarker, isTruncated }`.
struct CursorPage<T: Sendable>: Sendable {
    let items: [T]
    let nextMarker: String?
    let isTruncated: Bool

    init(items: [T], nextMarker: String?, isTruncated: Bool) {
        self.items = items
        // A marker is only meaningful when there is something after it.
        self.nextMarker = isTruncated ? nextMarker : nil
        self.isTruncated = isTruncated
    }

    func map<U: Sendable>(_ transform: (T) throws -> U) rethrows -> CursorPage<U> {
        try .init(items: items.map(transform), nextMarker: nextMarker, isTruncated: isTruncated)
    }
}

extension CursorPage: Encodable where T: Encodable {}
extension CursorPage: Decodable where T: Decodable {}
extension CursorPage: Content, ResponseEncodable, AsyncResponseEncodable, RequestDecodable,
    AsyncRequestDecodable
where T: Codable {}
