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

/// Feeds every request through `MetricsCollector`: bytes in (from Content-Length - the body may
/// not be collected yet when the middleware runs, and streamed requests without a length are
/// simply counted as 0), bytes out (from the response body's buffered size), and whether the
/// final status was an error. Registered outermost so it also sees error responses after
/// `S3ErrorMiddleware` has converted thrown errors.
struct MetricsMiddleware: AsyncMiddleware {
    func respond(to request: Request, chainingTo next: any AsyncResponder) async throws
        -> Response
    {
        let bytesIn = request.headers.first(name: .contentLength).flatMap(Int.init) ?? 0
        do {
            let response = try await next.respond(to: request)
            let bytesOut = max(0, response.body.count)
            await MetricsCollector.shared.record(
                bytesIn: bytesIn, bytesOut: bytesOut, isError: response.status.code >= 400)
            return response
        } catch {
            await MetricsCollector.shared.record(bytesIn: bytesIn, bytesOut: 0, isError: true)
            throw error
        }
    }
}
