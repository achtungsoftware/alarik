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

/// Whether this node is ready to be sent *client* traffic - the signal behind `GET /readyz`.
///
/// "Up" and "ready" are genuinely different states here, and conflating them is what makes a
/// freshly-started node answer `InvalidAccessKeyId` or `NoSuchBucket` to perfectly valid requests.
/// A node accepts connections the moment its HTTP server binds, but until it has learned who its
/// peers are and loaded its caches it cannot answer authoritatively about credentials or buckets -
/// its own view is simply empty. A load balancer that routes to it during that window gets
/// confident, wrong answers rather than errors.
///
/// Two independent gates, both required:
///
/// - **Membership bootstrap** - the node has finished seeding its peer list (or has established
///   there are no peers). Without it, cluster-wide reads query nobody.
/// - **Initial cache load** - the first `LoadCacheLifecycle.reloadAll` has completed, so
///   credentials and bucket metadata are present locally.
///
/// In single-node mode the membership gate is satisfied immediately: there is no cluster to
/// bootstrap, so readiness reduces to "the boot cache load finished".
///
/// Deliberately just two booleans. `/readyz` is polled every few seconds per pod, so it must be a
/// cheap local read - never a fan-out, never a disk walk.
actor NodeReadiness {
    static let shared = NodeReadiness()

    private var membershipReady = false
    private var cacheLoaded = false

    /// Set once membership bootstrap completes, or immediately when cluster mode is off.
    func markMembershipReady() {
        membershipReady = true
    }

    /// Set once the boot-time cache load completes.
    func markCacheLoaded() {
        cacheLoaded = true
    }

    var isReady: Bool {
        membershipReady && cacheLoaded
    }

    /// Which gates are still outstanding - surfaced in the `/readyz` body so an operator can tell
    /// "still seeding peers" from "still loading caches" without reading logs.
    var pendingGates: [String] {
        var pending: [String] = []
        if !membershipReady { pending.append("membership") }
        if !cacheLoaded { pending.append("cache") }
        return pending
    }

    /// Test hook: a process-lifetime singleton outlives any one `Application`, so a test booting
    /// its own app would otherwise inherit readiness from a previous one.
    func reset() {
        membershipReady = false
        cacheLoaded = false
    }
}
