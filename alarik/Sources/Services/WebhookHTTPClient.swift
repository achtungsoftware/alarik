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

/// The client webhooks are delivered with. Separate from `app.http.client` for one reason: it
/// never follows redirects. A webhook URL passes the SSRF check as written, so letting the
/// receiver answer `302 Location: http://169.254.169.254/` would hand it exactly the request
/// the check refused. Bounded timeouts keep one stalled receiver from holding a delivery slot.
///
/// One instance per `Application`, owned by `app.storage` and closed by `ShutdownHandler` - a
/// process-wide instance would be shut down by the first application to stop and leave every
/// later one (the test suite boots many) delivering into a closed client.
enum WebhookHTTPClient {
    static let timeout: TimeAmount = .seconds(30)

    struct Key: StorageKey {
        typealias Value = HTTPClient
    }

    static func make() -> HTTPClient {
        var configuration = HTTPClient.Configuration()
        configuration.redirectConfiguration = .disallow
        configuration.timeout = .init(connect: .seconds(10), read: timeout)
        configuration.connectionPool = .init(
            idleTimeout: .seconds(30), concurrentHTTP1ConnectionsPerHostSoftLimit: 16)
        return HTTPClient(
            eventLoopGroupProvider: .shared(MultiThreadedEventLoopGroup.singleton),
            configuration: configuration)
    }

    /// The application's client, created on first use when `configure` has not stored one.
    static func client(for app: Application) -> HTTPClient {
        if let client = app.storage[Key.self] { return client }
        let client = make()
        app.storage[Key.self] = client
        return client
    }

    struct ShutdownHandler: LifecycleHandler {
        func shutdownAsync(_ app: Application) async {
            guard let client = app.storage[Key.self] else { return }
            app.storage[Key.self] = nil
            try? await client.shutdown()
        }
    }
}
