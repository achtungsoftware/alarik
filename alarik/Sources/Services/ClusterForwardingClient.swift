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
import Foundation
import NIOCore
import NIOHTTP1
import Vapor

/// Client-request forwarding for the "any node can front any request" story:
/// `forward(req:candidates:)` re-issues an already-authenticated client request to a peer's
/// *same* S3 route. Distinct from `ClusterReplicationClient`, which speaks the separate
/// internal-only push/fetch/delete protocol used to actually copy object bytes between nodes -
/// this type only ever replays a client's own request verbatim to wherever it's responsible.
///
/// Uses raw `AsyncHTTPClient` (`app.http.client.shared`) rather than Vapor's `Client` wrapper -
/// Vapor's `ClientRequest`/`ClientResponse` only expose a fully-buffered `ByteBuffer?` body
/// (confirmed against the vendored Vapor source), which would defeat the bounded-memory
/// streaming every other object-IO path in this codebase deliberately maintains for
/// multi-gigabyte objects.
enum ClusterForwardingClient {
    /// Whole-request deadline for a single forwarded client request. Generous - large object
    /// transfers must have room to complete, not just connect.
    static let requestTimeout: TimeAmount = .minutes(10)

    /// Forwards `req` to `candidates` in preference order, returning the first success.
    /// Requests that carry a body (`PUT`/`POST` - object PUT, UploadPart, multipart
    /// create/complete) are only ever attempted once, against `candidates.first`: `req.body` is
    /// a single-consume stream, so a second attempt after a partial failure could not safely
    /// replay it. Bodiless requests (`GET`/`HEAD`/`DELETE`) fall back across every candidate in
    /// order - this is the entire read-side failure-tolerance story, no separate mechanism.
    static func forward(req: Request, candidates: [ClusterNodeInfo]) async throws -> Response {
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            throw S3Error(
                status: .internalServerError, code: "InternalError",
                message: "This node is not part of a cluster.", requestId: req.id)
        }
        guard !candidates.isEmpty else {
            throw S3Error(
                status: .serviceUnavailable, code: "ServiceUnavailable",
                message: "No cluster peer is currently available for this object.",
                requestId: req.id)
        }

        let hasBody = req.method == .PUT || req.method == .POST
        let attemptCandidates = hasBody ? [candidates[0]] : candidates

        var lastError: any Error = S3Error(
            status: .serviceUnavailable, code: "ServiceUnavailable",
            message: "No cluster peer could serve this request.", requestId: req.id)
        for node in attemptCandidates {
            do {
                return try await forwardOnce(req: req, to: node, secret: config.secret, streamBody: hasBody)
            } catch {
                lastError = error
                continue
            }
        }
        throw lastError
    }

    private static func forwardOnce(
        req: Request, to node: ClusterNodeInfo, secret: String, streamBody: Bool
    ) async throws -> Response {
        var outbound = HTTPClientRequest(url: node.address + req.url.string)
        outbound.method = HTTPMethod(rawValue: req.method.rawValue)
        outbound.headers = req.headers
        // Deliberately keep the original `Host` header as-is (not the peer's) - the client's
        // SigV4 signature was computed over the canonical request it actually sent, `Host`
        // included, and re-authenticating on the receiving node (see `handleObjectPut` etc.,
        // which call `S3Service.authenticate...` before ever consulting `ObjectRoutingService`)
        // would fail signature verification if this changed to the peer's own host:port -
        // forwarding must replay the exact bytes the client signed, just to a different socket.
        outbound.headers.remove(name: .contentLength)
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.secretHeaderName, value: secret)
        outbound.headers.replaceOrAdd(
            name: ClusterForwardAuthenticator.forwardedHeaderName, value: "true")

        if streamBody {
            outbound.body = .stream(req.body, length: .unknown)
        }

        let client = req.application.http.client.shared
        let response: HTTPClientResponse
        do {
            response = try await client.execute(
                outbound, timeout: requestTimeout, logger: req.logger)
        } catch {
            throw S3Error(
                status: .badGateway, code: "InternalError",
                message: "Failed to reach cluster peer \(node.address): \(error)",
                requestId: req.id)
        }

        // `Response.Body(managedAsyncStream:)` without an explicit `count:` defaults to -1
        // (chunked/unknown length) - Vapor's response encoder then drops any `Content-Length`
        // header in favor of chunked transfer-encoding, even for a HEAD response with zero
        // actual body bytes. That silently breaks clients (botocore raises `KeyError:
        // 'ContentLength'` parsing a chunked HEAD response) - passing the peer's own declared
        // Content-Length through as `count` keeps the forwarded response byte-for-byte
        // equivalent to what the peer would have sent directly.
        let count = response.headers.first(name: .contentLength).flatMap(Int.init) ?? -1

        let vaporResponse = Response(
            status: HTTPResponseStatus(statusCode: Int(response.status.code)),
            headers: response.headers)
        vaporResponse.body = Response.Body(
            managedAsyncStream: { writer in
                for try await chunk in response.body {
                    try await writer.writeBuffer(chunk)
                }
            }, count: count)
        return vaporResponse
    }
}
