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
        next.respond(to: request).flatMapError { error in
            if let s3Error = error as? S3Error {
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
                    headers.replaceOrAdd(name: "x-amz-request-id", value: s3Error.requestId)
                    headers.replaceOrAdd(
                        name: "x-amz-id-2",
                        value: s3Error.requestId + "-" + String(Int.random(in: 1000...9999)))

                    let response = Response(
                        status: s3Error.status, headers: headers, body: .init(data: xmlData))
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
