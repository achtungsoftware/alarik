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
import Vapor

/// Bounded record of keys an authoritative lookup recently proved absent.
///
/// Without it, read-through turns every request carrying a bogus key into a cluster-wide metadata
/// read - a cheap way for a client to generate expensive work. With it, a key confirmed missing
/// stays cheap to reject for a few seconds.
struct CacheMissLedger<Key: Hashable & Sendable>: Sendable {
    private var misses: [Key: Date] = [:]
    private let ttl: TimeInterval
    private let capacity: Int

    init(ttl: TimeInterval = 5, capacity: Int = 1024) {
        self.ttl = ttl
        self.capacity = capacity
    }

    /// Entries currently held, expired ones included - the bound `note` enforces.
    var count: Int { misses.count }

    func confirmedMissing(_ key: Key, now: Date = Date()) -> Bool {
        guard let at = misses[key] else { return false }
        return now.timeIntervalSince(at) <= ttl
    }

    mutating func note(_ key: Key, now: Date = Date()) {
        if misses.count >= capacity {
            misses = misses.filter { now.timeIntervalSince($0.value) <= ttl }
            // The TTL sweep frees nothing when every entry is still fresh - which is exactly what
            // a spray of DISTINCT bogus keys produces, the case this ledger exists to survive.
            // Without a hard eviction the map grows unbounded and every insert pays an O(n) scan,
            // turning the defence into the amplifier. Halving amortises the sort across inserts.
            if misses.count >= capacity {
                let excess = misses.count - capacity / 2
                for stale in misses.sorted(by: { $0.value < $1.value }).prefix(excess) {
                    misses.removeValue(forKey: stale.key)
                }
            }
        }
        misses[key] = now
    }

    mutating func clear(_ key: Key) {
        misses.removeValue(forKey: key)
    }
}

/// The three genuinely different outcomes of an authoritative lookup.
///
/// Collapsing `absent` and `unavailable` into `nil` is correct for authorization (both must
/// deny), but wrong wherever the answer is reported to a client as a fact about the world:
/// telling an S3 client a bucket does not exist, when the truth is "this node couldn't check",
/// is indistinguishable to that client - or to a replication/sync tool - from the bucket having
/// been deleted.
enum StoreBackedResolution<Value: Sendable>: Sendable {
    case found(Value)
    /// Authoritatively determined not to exist.
    case absent
    /// Could not be determined right now (the store was unreadable).
    case unavailable
}

/// A cache that is a *projection* of `MetadataStore`, never a source of truth.
///
/// Conforming makes the correct behaviour the default: consult the cache, and only when it has no
/// answer, ask the store. Callers get `resolve(app:key:)` and should prefer it over the raw cache
/// accessors wherever a miss would otherwise be reported to a client as a definitive answer.
///
/// Fails closed and never caches uncertainty: if the store itself cannot be read, that is
/// reported as "no answer" but deliberately *not* recorded as a miss, so "I couldn't check" can
/// never harden into "it doesn't exist".
protocol StoreBackedCache: Actor {
    associatedtype Key: Hashable & Sendable
    associatedtype Value: Sendable

    /// This node's cached answer, or `nil` if it has none.
    func cachedValue(for key: Key) -> Value?

    /// Records an authoritative answer so subsequent lookups hit cache.
    func absorb(_ value: Value, for key: Key)

    /// Reads the authoritative answer from `MetadataStore`. Throwing means "could not determine",
    /// which is treated differently from returning `nil` ("determined it does not exist").
    func loadFromStore(app: Application, key: Key) async throws -> Value?

    /// Per-cache negative-lookup bookkeeping. Conformers just declare storage for it.
    var missLedger: CacheMissLedger<Key> { get set }
}

extension StoreBackedCache {
    /// Cache first, then the authoritative store. The only lookup that may be used to tell a
    /// client something does not exist.
    ///
    /// Answers `nil` for both "does not exist" and "could not check" - the safe collapse for
    /// authorization decisions, which must deny either way. Callers that report the outcome to a
    /// client as a fact (existence checks especially) want `resolveDistinguishing` instead.
    func resolve(app: Application, key: Key) async -> Value? {
        switch await resolveDistinguishing(app: app, key: key) {
        case .found(let value): return value
        case .absent, .unavailable: return nil
        }
    }

    /// `resolve`, keeping "authoritatively absent" and "couldn't determine" apart.
    func resolveDistinguishing(app: Application, key: Key) async -> StoreBackedResolution<Value> {
        if let cached = cachedValue(for: key) { return .found(cached) }
        if missLedger.confirmedMissing(key) { return .absent }

        let loaded: Value?
        do {
            loaded = try await loadFromStore(app: app, key: key)
        } catch {
            app.logger.warning(
                "\(Self.self) could not consult the metadata store; answering 'unknown' rather than 'absent': \(error)"
            )
            return .unavailable
        }

        guard let loaded else {
            missLedger.note(key)
            return .absent
        }
        absorb(loaded, for: key)
        missLedger.clear(key)
        return .found(loaded)
    }
}
