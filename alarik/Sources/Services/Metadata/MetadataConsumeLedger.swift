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

/// What a `consumeIfPresent` handed out, kept briefly under the token of the request that
/// consumed it. A consume is single-use by construction, so a coordinator whose forward timed
/// out cannot tell whether the owner consumed the record; retrying the same token here returns
/// the same bytes instead of "nothing to consume" (an OIDC login state that vanished mid-login).
actor MetadataConsumeLedger {
    static let shared = MetadataConsumeLedger()

    /// Longer than a forward's timeout and its one retry; short enough to hold nothing for long.
    static let defaultTTL: TimeInterval = 60

    private struct Entry {
        let value: Data
        let expiresAt: Date
    }

    private var entries: [String: Entry] = [:]

    private func slot(_ collection: String, _ id: String, _ token: String) -> String {
        "\(collection)\u{0}\(id)\u{0}\(token)"
    }

    func remember(
        collection: String, id: String, token: String, value: Data,
        ttl: TimeInterval = MetadataConsumeLedger.defaultTTL, now: Date = Date()
    ) {
        entries = entries.filter { $0.value.expiresAt > now }
        entries[slot(collection, id, token)] = Entry(value: value, expiresAt: now.addingTimeInterval(ttl))
    }

    func replay(collection: String, id: String, token: String, now: Date = Date()) -> Data? {
        guard let entry = entries[slot(collection, id, token)], entry.expiresAt > now else { return nil }
        return entry.value
    }
}
