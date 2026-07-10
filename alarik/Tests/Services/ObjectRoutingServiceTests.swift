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
import Vapor

@testable import Alarik

/// No cluster vars set, no Postgres - exercises the "cluster mode is off" fast path that every
/// non-clustered node (the overwhelming majority of deployments) must take on every single
/// request. This must stay byte-for-byte inert, the same guarantee the SQLite-only control
/// plane holds when Postgres isn't configured.
@Suite("ObjectRoutingService tests (non-clustered)", .serialized)
struct ObjectRoutingServiceTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()
            try await test(app)
            try await app.autoRevert()
        } catch {
            try? StorageHelper.cleanStorage()
            try? await app.autoRevert()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    @Test("routingDecision is always .local with no peers when cluster mode is off")
    func nonClusteredAlwaysLocal() async throws {
        try await withApp { app in
            // configure(app) never stashes ClusterConfigurationKey unless CLUSTER_NODE_ADDRESS
            // was set in the process environment - not the case for the test process.
            #expect(app.storage[ClusterConfigurationKey.self] == nil)

            let req = Request(
                application: app, method: .GET, url: URI(string: "/my-bucket/my-key"),
                on: app.eventLoopGroup.next())

            let decision = await ObjectRoutingService.routingDecision(
                req: req, bucketName: "my-bucket", key: "my-key")

            guard case .local(let peers) = decision else {
                Issue.record("Expected .local, got \(decision)")
                return
            }
            #expect(peers.isEmpty)
        }
    }

    @Test("isTrustedForward is false without a valid cluster secret header")
    func isTrustedForwardFalseWithoutHeader() async throws {
        try await withApp { app in
            let req = Request(
                application: app, method: .PUT, url: URI(string: "/my-bucket/my-key"),
                on: app.eventLoopGroup.next())
            #expect(ClusterForwardAuthenticator.isTrustedForward(req) == false)

            req.headers.replaceOrAdd(name: "X-Alarik-Cluster-Forwarded", value: "true")
            // Still false: no ClusterConfigurationKey stashed (not clustered) and no secret header.
            #expect(ClusterForwardAuthenticator.isTrustedForward(req) == false)
        }
    }

    @Test("coordinationTarget is always local with no peers when cluster mode is off")
    func coordinationTargetNonClusteredAlwaysLocal() async throws {
        try await withApp { app in
            let req = Request(
                application: app, method: .POST, url: URI(string: "/my-bucket"),
                on: app.eventLoopGroup.next())

            let (isLocal, peers, responsible) = await ObjectRoutingService.coordinationTarget(
                req: req, bucketName: "my-bucket", key: "my-key")

            #expect(isLocal)
            #expect(peers.isEmpty)
            #expect(responsible.isEmpty)
        }
    }
}
