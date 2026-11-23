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
import XMLCoder

struct S3ErrorMiddleware: Middleware {
    func respond(to request: Request, chainingTo next: any Responder) -> EventLoopFuture<Response> {
        next.respond(to: request).map { response in
            // S3 stamps every response (success or error) with request IDs - clients
            // and support tooling rely on them for correlation
            Self.addRequestIdHeaders(to: response, requestId: request.id)
            return response
        }.flatMapError { error in
            if let s3Error = error as? S3Error {
                // A 304 must not carry a body (RFC 9110)
                if s3Error.status == .notModified {
                    let response = Response(status: .notModified)
                    Self.addRequestIdHeaders(to: response, requestId: s3Error.requestId)
                    return request.eventLoop.makeSucceededFuture(response)
                }
                return request.eventLoop.makeSucceededFuture(
                    Self.xmlResponse(
                        status: s3Error.status, code: s3Error.code, message: s3Error.message,
                        resource: s3Error.resource, requestId: s3Error.requestId))
            }

            // Anything else on the S3 surface still has to come back as S3 XML: a route miss,
            // an oversized body, a storage-layer error. S3 clients cannot parse Vapor's JSON
            // `{"error":true,"reason":...}` and would surface it as an unknown failure. The
            // console and cluster-internal routes are JSON clients and keep the default.
            guard Self.isS3Route(request) else {
                return self.defaultErrorHandling(request: request, error: error)
            }
            let (status, code, message) = Self.s3Classification(of: error)
            if status.code >= 500 {
                request.logger.report(error: error)
            }
            return request.eventLoop.makeSucceededFuture(
                Self.xmlResponse(
                    status: status, code: code, message: message, resource: request.url.path,
                    requestId: request.id))
        }
    }

    private static func isS3Route(_ request: Request) -> Bool {
        let path = request.url.path
        return !(path.hasPrefix("/api/") || path.hasPrefix("/internal/") || path == "/livez"
            || path == "/readyz")
    }

    /// Maps a non-`S3Error` to the closest S3 error code. Client faults (`Abort` 4xx) keep their
    /// reason; server faults never leak internals into the body.
    private static func s3Classification(of error: any Error) -> (HTTPStatus, String, String) {
        let abort = error as? any AbortError
        let status = abort?.status ?? .internalServerError
        let reason = abort?.reason ?? ""
        switch status {
        case .badRequest: return (status, "InvalidRequest", reason)
        case .unauthorized, .forbidden: return (status, "AccessDenied", "Access Denied")
        case .notFound: return (status, "NotFound", reason.isEmpty ? "Not Found" : reason)
        case .methodNotAllowed:
            return (status, "MethodNotAllowed", "The specified method is not allowed against this resource.")
        case .lengthRequired:
            return (status, "MissingContentLength", "You must provide the Content-Length HTTP header.")
        case .payloadTooLarge:
            return (status, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed size")
        case .rangeNotSatisfiable:
            return (status, "InvalidRange", "The requested range is not satisfiable")
        case .notImplemented:
            return (status, "NotImplemented", "A header you provided implies functionality that is not implemented")
        case .serviceUnavailable:
            return (status, "ServiceUnavailable", "Please reduce your request rate.")
        default:
            return (
                .internalServerError, "InternalError",
                "We encountered an internal error. Please try again."
            )
        }
    }

    private static func xmlResponse(
        status: HTTPStatus, code: String, message: String, resource: String?, requestId: String
    ) -> Response {
        let payload = S3ErrorResponse(
            Code: code, Message: message, Resource: resource, RequestId: requestId)
        let encoder = XMLEncoder()
        encoder.outputFormatting = [.prettyPrinted]
        var headers = HTTPHeaders()
        headers.contentType = .xml
        // Encoding a four-string struct cannot realistically fail; a hand-built document is the
        // fallback so an error response is never itself an error.
        let body =
            (try? encoder.encode(payload, withRootKey: "Error"))
            ?? Data(
                "<Error><Code>\(code.xmlEscaped)</Code><Message>\(message.xmlEscaped)</Message><RequestId>\(requestId.xmlEscaped)</RequestId></Error>"
                    .utf8)
        let response = Response(status: status, headers: headers, body: .init(data: body))
        addRequestIdHeaders(to: response, requestId: requestId)
        return response
    }

    private static func addRequestIdHeaders(to response: Response, requestId: String) {
        if response.headers.first(name: "x-amz-request-id") == nil {
            response.headers.add(name: "x-amz-request-id", value: requestId)
        }
        if response.headers.first(name: "x-amz-id-2") == nil {
            response.headers.add(
                name: "x-amz-id-2",
                value: requestId + "-" + String(Int.random(in: 1000...9999)))
        }
    }

    private func defaultErrorHandling(request: Request, error: any Error) -> EventLoopFuture<
        Response
    > {
        let defaultMiddleware = ErrorMiddleware.default(
            environment: request.application.environment)

        struct ErrorThrowingResponder: Responder {
            let error: any Error

            func respond(to request: Request) -> EventLoopFuture<Response> {
                request.eventLoop.makeFailedFuture(error)
            }
        }

        let errorResponder = ErrorThrowingResponder(error: error)

        return defaultMiddleware.respond(to: request, chainingTo: errorResponder)
    }
}
