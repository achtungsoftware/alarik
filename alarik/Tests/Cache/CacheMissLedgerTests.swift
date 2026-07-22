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

/// The negative-lookup ledger in front of `StoreBackedCache`. It exists so a bogus key can't turn
/// every request into a cluster-wide metadata read - so its own memory must stay bounded under
/// exactly that attack, which is a spray of *distinct* keys rather than a repeat of one.
@Suite("CacheMissLedger tests")
struct CacheMissLedgerTests {
    @Test("a noted key reads back as confirmed missing, until its TTL passes")
    func noteThenExpire() {
        var ledger = CacheMissLedger<String>(ttl: 5, capacity: 8)
        let now = Date()
        ledger.note("gone", now: now)

        #expect(ledger.confirmedMissing("gone", now: now))
        #expect(ledger.confirmedMissing("gone", now: now.addingTimeInterval(4)))
        #expect(ledger.confirmedMissing("gone", now: now.addingTimeInterval(6)) == false)
    }

    @Test("clearing a key drops the negative immediately")
    func clearDropsTheNegative() {
        var ledger = CacheMissLedger<String>(ttl: 5, capacity: 8)
        let now = Date()
        ledger.note("gone", now: now)
        ledger.clear("gone")
        #expect(ledger.confirmedMissing("gone", now: now) == false)
    }

    @Test("an unknown key was never confirmed missing")
    func unknownKeyIsNotConfirmed() {
        let ledger = CacheMissLedger<String>(ttl: 5, capacity: 8)
        #expect(ledger.confirmedMissing("never-seen") == false)
    }

    @Test("distinct keys arriving faster than the TTL cannot grow the ledger without bound")
    func staysBoundedUnderDistinctKeySpray() {
        let capacity = 64
        var ledger = CacheMissLedger<String>(ttl: 60, capacity: capacity)
        // Every entry stays inside the TTL, so the expiry sweep alone frees nothing - this is the
        // case that used to grow forever and pay an O(n) scan on every single insert.
        let now = Date()
        for i in 0..<(capacity * 20) {
            ledger.note("bogus-\(i)", now: now)
        }
        #expect(ledger.count <= capacity, "ledger grew to \(ledger.count), capacity is \(capacity)")
    }

    @Test("eviction discards the oldest entries, keeping the most recent negatives useful")
    func evictionKeepsTheNewest() {
        let capacity = 16
        var ledger = CacheMissLedger<String>(ttl: 60, capacity: capacity)
        let start = Date()
        // Strictly increasing timestamps, so "oldest" is unambiguous.
        for i in 0..<(capacity * 4) {
            ledger.note("bogus-\(i)", now: start.addingTimeInterval(Double(i)))
        }
        let now = start.addingTimeInterval(Double(capacity * 4))
        let newest = "bogus-\(capacity * 4 - 1)"
        #expect(ledger.confirmedMissing(newest, now: now), "the most recent negative was evicted")
        #expect(ledger.confirmedMissing("bogus-0", now: now) == false, "the oldest survived eviction")
    }
}
