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

import AsyncHTTPClient
import NIOCore
import Vapor

/// A dedicated `HTTPClient` for small, latency-sensitive cluster control-plane traffic (cache
/// invalidation, membership queries, metadata forwarding/listing, outbox RPCs), kept separate
/// from `app.http.client.shared`'s genuinely bulk object-data traffic so a tiny control message
/// never queues behind a large shard transfer on the same per-host pool. Reuses NIO's shared
/// event loop group rather than spinning up dedicated threads. Short timeouts: a healthy peer on
/// localhost/LAN answers in milliseconds, so a slow response means "unreachable right now," not
/// "keep waiting."
enum LightweightClusterControlClient {
    static let shared: HTTPClient = {
        var configuration = HTTPClient.Configuration()
        configuration.timeout = .init(connect: .milliseconds(500), read: .seconds(2))
        configuration.connectionPool = .init(
            idleTimeout: .seconds(30), concurrentHTTP1ConnectionsPerHostSoftLimit: 64)
        return HTTPClient(
            eventLoopGroupProvider: .shared(MultiThreadedEventLoopGroup.singleton),
            configuration: configuration)
    }()

    /// Registered once, at boot, via `configure.swift`. Idle from Vapor's own shutdown
    /// sequence's perspective (this client was never registered with Vapor to begin with), so it
    /// needs its own explicit teardown to avoid leaking connections/the background thread pool on
    /// process exit.
    struct ShutdownHandler: LifecycleHandler {
        func shutdownAsync(_ app: Application) async {
            try? await LightweightClusterControlClient.shared.shutdown()
        }
    }
}
