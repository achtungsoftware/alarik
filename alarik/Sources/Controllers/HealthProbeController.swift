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

/// Kubernetes-style probe endpoints, deliberately **unauthenticated**.
///
/// The pre-existing `GET /api/v1/health` can't serve this purpose: it sits behind
/// `InternalAuthenticator`, so an unauthenticated `httpGet` probe receives `401` and the kubelet
/// concludes the container is unhealthy - restarting a perfectly good pod forever. These carry no
/// information beyond "up" and "ready", which is why exposing them without auth is safe.
///
/// Both must stay cheap and side-effect-free. Every pod is polled every few seconds, so a probe
/// that fanned out to peers would turn liveness checking into cluster-wide load - and worse, would
/// make one slow peer able to fail everyone else's health check.
struct HealthProbeController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        routes.get("livez", use: livez)
        routes.get("readyz", use: readyz)
        // HEAD costs a probe nothing and some tooling defaults to it.
        routes.on(.HEAD, "livez", use: livez)
        routes.on(.HEAD, "readyz", use: readyz)
    }

    /// The process is up and serving. Always `200` once the server is accepting connections -
    /// reaching this handler at all is the proof.
    ///
    /// Deliberately NOT tied to cluster health. Liveness failure means "restart this container",
    /// and restarting a node because its peers are unreachable is precisely wrong: it would turn a
    /// network partition into a cluster-wide restart loop, destroying the one thing still working.
    @Sendable
    func livez(req: Request) async throws -> Response {
        Response(status: .ok, body: .init(string: "ok"))
    }

    /// Whether this node should receive client traffic: membership bootstrapped AND caches loaded.
    /// `503` until both hold, so the Service keeps traffic away from a node that would otherwise
    /// answer authoritatively from an empty cache.
    @Sendable
    func readyz(req: Request) async throws -> Response {
        let pending = await NodeReadiness.shared.pendingGates
        guard pending.isEmpty else {
            return Response(
                status: .serviceUnavailable,
                body: .init(string: "not ready: \(pending.joined(separator: ","))"))
        }
        return Response(status: .ok, body: .init(string: "ready"))
    }
}
