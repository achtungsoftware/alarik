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
            // Real S3 stamps every response (success or error) with request IDs - clients
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

                // Build XML response for S3Error
                let payload = S3ErrorResponse(
                    Code: s3Error.code,
                    Message: s3Error.message,
                    Resource: s3Error.resource,
                    RequestId: s3Error.requestId
                )

                let encoder = XMLEncoder()
                encoder.outputFormatting = [.prettyPrinted]

                do {
                    let xmlData = try encoder.encode(payload, withRootKey: "Error")

                    var headers = HTTPHeaders()
                    headers.contentType = .xml

                    let response = Response(
                        status: s3Error.status, headers: headers, body: .init(data: xmlData))
                    Self.addRequestIdHeaders(to: response, requestId: s3Error.requestId)
                    return request.eventLoop.makeSucceededFuture(response)
                } catch {
                    // If encoding fails, fall through to default
                    return self.defaultErrorHandling(request: request, error: error)
                }
            } else {
                // Not S3Error, fall through to default
                return self.defaultErrorHandling(request: request, error: error)
            }
        }
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
