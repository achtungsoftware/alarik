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

/// Per-`(collection, key)` async mutual exclusion for `MetadataStore.putIfAbsent`/
/// `consumeIfPresent`. Rank-0 pinning (`ObjectRoutingService.erasureCodedRoutingDecision`)
/// already guarantees only one node ever coordinates a write for a given metadata key - but a
/// single Vapor process can still serve two concurrent HTTP requests that both resolve to this
/// node as rank-0 for the same key. This actor closes that intra-process race window, so at most
/// one write for a given key is ever in flight cluster-wide at any instant.
actor MetadataKeyLock {
    static let shared = MetadataKeyLock()

    /// One waiter queue per in-flight key - `Task`s parked on the same key wait for the prior
    /// holder to finish before running, in arrival order. Removed once nobody holds or is
    /// waiting on it, so this never grows unbounded with the total number of keys ever seen.
    private var waiters: [String: [CheckedContinuation<Void, Never>]] = [:]
    private var locked: Set<String> = []

    private func lockKeyName(collection: String, key: String) -> String {
        "\(collection)\u{0}\(key)"
    }

    /// Runs `body` with exclusive access to `(collection, key)`, releasing the lock (and waking
    /// the next waiter, if any) once `body` returns or throws.
    func withLock<T: Sendable>(
        collection: String, key: String, _ body: @Sendable () async throws -> T
    ) async rethrows -> T {
        let lockKey = lockKeyName(collection: collection, key: key)
        await acquire(lockKey)
        defer { release(lockKey) }
        return try await body()
    }

    private func acquire(_ lockKey: String) async {
        if !locked.contains(lockKey) {
            locked.insert(lockKey)
            return
        }
        await withCheckedContinuation { continuation in
            waiters[lockKey, default: []].append(continuation)
        }
    }

    private func release(_ lockKey: String) {
        guard var queue = waiters[lockKey], !queue.isEmpty else {
            locked.remove(lockKey)
            waiters.removeValue(forKey: lockKey)
            return
        }
        // Hand the lock directly to the next waiter (still `locked`) rather than clearing and
        // letting it re-acquire - avoids a window where a third `acquire` could jump the queue.
        let next = queue.removeFirst()
        waiters[lockKey] = queue.isEmpty ? nil : queue
        next.resume()
    }
}
