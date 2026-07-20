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

/// Inter-node auth for cluster traffic - deliberately separate from `InternalAuthenticator`
/// (client-facing SigV4/JWT), since a forwarded request can't be re-signed with a secret key the
/// entry node never had. A single shared bearer secret, checked in constant time, is deliberately
/// simple - cluster traffic is assumed to run on a private network.
enum ClusterForwardAuthenticator {
    static let forwardedHeaderName = "X-Alarik-Cluster-Forwarded"
    static let secretHeaderName = "X-Alarik-Cluster-Secret"

    /// True when `req` carries a valid cluster-secret-signed forward marker from a trusted peer.
    /// `ObjectRoutingService` treats this as "already authenticated and routed by a peer" and
    /// serves locally unconditionally rather than recursing into its own forward decision - a
    /// forwarded request must never itself be forwarded again.
    static func isTrustedForward(_ req: Request) -> Bool {
        guard req.headers.first(name: forwardedHeaderName) == "true" else { return false }
        guard let config = req.application.storage[ClusterConfigurationKey.self] else { return false }
        guard let provided = req.headers.first(name: secretHeaderName) else { return false }
        return provided.constantTimeCompare(to: config.secret)
    }
}

/// Guards the internal-only cluster object push/delete routes (`/internal/cluster/objects/*`) -
/// unlike `ClusterForwardAuthenticator.isTrustedForward`, which is checked inline for the
/// S3-API routes that are also reachable directly by clients, every request to this route group
/// is inter-node-only, so a blanket middleware is the right shape here.
struct ClusterSecretMiddleware: AsyncMiddleware {
    func respond(to request: Request, chainingTo next: any AsyncResponder) async throws -> Response {
        guard let config = request.application.storage[ClusterConfigurationKey.self] else {
            throw Abort(.serviceUnavailable, reason: "This node is not part of a cluster.")
        }
        guard
            let provided = request.headers.first(
                name: ClusterForwardAuthenticator.secretHeaderName),
            provided.constantTimeCompare(to: config.secret)
        else {
            throw Abort(.unauthorized, reason: "Invalid cluster secret.")
        }
        return try await next.respond(to: request)
    }
}
