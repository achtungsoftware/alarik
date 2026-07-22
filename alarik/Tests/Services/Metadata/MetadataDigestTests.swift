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

import Crypto
import Foundation
import Testing

@testable import Alarik

/// The digest gates whether the periodic full reload runs at all, so the properties that matter
/// are: identical record sets agree, ANY change disagrees, and walk order never matters.
@Suite("Metadata digest tests")
struct MetadataDigestTests {
    /// Mirrors `MetadataDigestService.localDigest`'s hashing, over an explicit entry list - the
    /// service itself needs a booted app and real files on disk, while the algebra being relied on
    /// here is worth testing directly.
    private func digest(of entries: [(id: String, updatedAtMillis: Int64, deleted: Bool)]) -> String {
        var accumulator = [UInt8](repeating: 0, count: 32)
        for entry in entries {
            let material = "\(entry.id)\u{0}\(entry.updatedAtMillis)\u{0}\(entry.deleted)"
            let hash = SHA256.hash(data: Data(material.utf8))
            for (index, byte) in hash.enumerated() { accumulator[index] ^= byte }
        }
        return accumulator.map { String(format: "%02x", $0) }.joined()
    }

    @Test("the digest is independent of the order records are walked in")
    func orderIndependent() {
        let a: [(String, Int64, Bool)] = [("u1", 100, false), ("u2", 200, false), ("u3", 300, true)]
        #expect(digest(of: a.map { (id: $0.0, updatedAtMillis: $0.1, deleted: $0.2) })
            == digest(of: a.reversed().map { (id: $0.0, updatedAtMillis: $0.1, deleted: $0.2) }))
    }

    @Test("a new record changes the digest")
    func additionChangesDigest() {
        let before = digest(of: [(id: "u1", updatedAtMillis: 100, deleted: false)])
        let after = digest(of: [
            (id: "u1", updatedAtMillis: 100, deleted: false),
            (id: "u2", updatedAtMillis: 100, deleted: false),
        ])
        #expect(before != after)
    }

    @Test("an updated record changes the digest even though the id set is unchanged")
    func updateChangesDigest() {
        let before = digest(of: [(id: "u1", updatedAtMillis: 100, deleted: false)])
        let after = digest(of: [(id: "u1", updatedAtMillis: 101, deleted: false)])
        #expect(before != after)
    }

    @Test("a delete changes the digest - a tombstone must not look like the record it replaced")
    func tombstoneChangesDigest() {
        // The failure this guards: if `deleted` weren't hashed, revoking a credential would leave
        // the digest identical and the gate would skip the reload that propagates the revocation.
        let live = digest(of: [(id: "key1", updatedAtMillis: 500, deleted: false)])
        let tombstoned = digest(of: [(id: "key1", updatedAtMillis: 500, deleted: true)])
        #expect(live != tombstoned)
    }

    @Test("an empty collection has the all-zero digest and a zero count")
    func emptyIsZero() {
        #expect(digest(of: []) == MetadataDigest.empty.digest)
        #expect(MetadataDigest.empty.count == 0)
    }

    @Test("count distinguishes sets a bare XOR could not")
    func countGuardsAgainstCancellation() {
        // Two copies of one record XOR back to zero, which is exactly the empty digest. `count` is
        // what keeps that from reading as "this collection is empty".
        let doubled = digest(of: [
            (id: "u1", updatedAtMillis: 100, deleted: false),
            (id: "u1", updatedAtMillis: 100, deleted: false),
        ])
        #expect(doubled == MetadataDigest.empty.digest)
        #expect(MetadataDigest(digest: doubled, count: 2) != MetadataDigest.empty)
    }
}
