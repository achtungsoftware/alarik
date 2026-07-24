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

import Testing
import Vapor
import VaporTesting

@testable import Alarik

/// The probe endpoints an orchestrator polls. The properties that matter are that they need no
/// credentials (an authenticated probe is an unhealthy pod, forever) and that readiness actually
/// gates on convergence rather than merely on the process being up.
// `.serialized`: `NodeReadiness` is a process-lifetime singleton these tests deliberately drive,
// so two of them running concurrently would fight over it.
@Suite("Health probe tests", .serialized)
struct HealthProbeControllerTests {
    /// Boots a real configured app (routes included) rather than VaporTesting's bare `withApp`,
    /// which builds an `Application` with no routes registered at all. Lifecycle handlers are
    /// invoked explicitly, matching how the other controller suites do it.
    private func withApp(
        markReady: Bool = true, _ test: (Application) async throws -> Void
    ) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            await NodeReadiness.shared.reset()
            if markReady {
                try await LoadCacheLifecycle().didBootAsync(app)
                try await ClusterMembershipLifecycle.shared.didBootAsync(app)
            }
            try await test(app)
        } catch {
            try? StorageHelper.cleanStorage()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    @Test("livez answers 200 without any credentials")
    func livezIsUnauthenticated() async throws {
        try await withApp { app in
            // Deliberately no auth headers: a 401 here is the exact failure mode that makes a
            // kubelet restart a perfectly healthy container in a loop.
            try await app.test(
                .GET, "/livez",
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "ok")
                })
        }
    }

    @Test("livez does not depend on readiness - a converging node is still alive")
    func livezIsIndependentOfReadiness() async throws {
        try await withApp(markReady: false) { app in
            // Restarting a container because the cluster is unreachable would turn a partition
            // into a cluster-wide restart loop, so liveness must ignore convergence entirely.
            try await app.test(
                .GET, "/livez", afterResponse: { res in #expect(res.status == .ok) })
        }
    }

    @Test("readyz answers 200 without credentials once the node has converged")
    func readyzIsUnauthenticatedWhenReady() async throws {
        try await withApp { app in
            try await app.test(
                .GET, "/readyz",
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "ready")
                })
        }
    }

    @Test("readyz reports 503 until BOTH gates are satisfied, naming what is outstanding")
    func readyzGatesOnBothSignals() async throws {
        try await withApp(markReady: false) { app in
            try await app.test(
                .GET, "/readyz",
                afterResponse: { res in
                    #expect(res.status == .serviceUnavailable)
                    #expect(res.body.string.contains("membership"))
                    #expect(res.body.string.contains("cache"))
                })

            // Membership alone is not enough - caches still empty, so answers would be wrong.
            await NodeReadiness.shared.markMembershipReady()
            try await app.test(
                .GET, "/readyz",
                afterResponse: { res in
                    #expect(res.status == .serviceUnavailable)
                    #expect(res.body.string.contains("cache"))
                    #expect(res.body.string.contains("membership") == false)
                })

            await NodeReadiness.shared.markCacheLoaded()
            try await app.test(
                .GET, "/readyz", afterResponse: { res in #expect(res.status == .ok) })
        }
    }

    @Test("booting the app satisfies both readiness gates")
    func bootMarksNodeReady() async throws {
        try await withApp { app in
            // `withApp` runs the real lifecycle handlers, so this covers the actual wiring rather
            // than the flags being set by hand.
            #expect(await NodeReadiness.shared.isReady)
            try await app.test(
                .GET, "/readyz", afterResponse: { res in #expect(res.status == .ok) })
        }
    }

    @Test("a bucket cannot be named after a probe path")
    func probePathsAreReservedBucketNames() {
        // Both are valid S3 bucket names, and Vapor resolves a constant segment ahead of
        // `:bucketName` - so without this, such a bucket could be created and then never read
        // back over path-style addressing.
        #expect(MetadataNamespace.isReserved("livez"))
        #expect(MetadataNamespace.isReserved("readyz"))
        #expect(MetadataNamespace.isReserved(MetadataNamespace.bucketName))
        #expect(MetadataNamespace.isReserved("my-normal-bucket") == false)
    }
}
