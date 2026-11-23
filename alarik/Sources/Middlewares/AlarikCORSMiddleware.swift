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

/// One CORS layer for both surfaces. Console and probe routes keep the static policy the
/// `CORSMiddleware.Configuration` describes (the console's own origin), exactly as Vapor's
/// middleware applies it. S3 routes answer from the bucket's own CORS rules instead
/// (`BucketCORSCache`), which Vapor's single global allow-list could not express: a preflight
/// is answered here without reaching the router (no S3 route serves OPTIONS), and an admitted
/// actual request gets the rule's headers stamped on whatever the handler returns.
struct AlarikCORSMiddleware: AsyncMiddleware {
    let console: CORSMiddleware.Configuration

    func respond(to request: Request, chainingTo next: any AsyncResponder) async throws -> Response {
        guard let origin = request.headers.first(name: .origin) else {
            return try await next.respond(to: request)
        }
        if S3ErrorMiddleware.isS3Route(request) {
            return try await respondS3(to: request, origin: origin, chainingTo: next)
        }
        return try await respondConsole(to: request, chainingTo: next)
    }

    // MARK: - Console

    private func respondConsole(to request: Request, chainingTo next: any AsyncResponder)
        async throws -> Response
    {
        let response = Self.isPreflight(request) ? Response() : try await next.respond(to: request)
        let allowOrigin = console.allowedOrigin.header(forRequest: request)
        if !allowOrigin.isEmpty {
            response.headers.replaceOrAdd(name: .accessControlAllowOrigin, value: allowOrigin)
        }
        response.headers.replaceOrAdd(name: .accessControlAllowHeaders, value: console.allowedHeaders)
        response.headers.replaceOrAdd(name: .accessControlAllowMethods, value: console.allowedMethods)
        if let exposed = console.exposedHeaders {
            response.headers.replaceOrAdd(name: .accessControlExpose, value: exposed)
        }
        if let maxAge = console.cacheExpiration {
            response.headers.replaceOrAdd(name: .accessControlMaxAge, value: String(maxAge))
        }
        if console.allowCredentials {
            response.headers.replaceOrAdd(name: .accessControlAllowCredentials, value: "true")
        }
        if case .originBased = console.allowedOrigin, !allowOrigin.isEmpty {
            response.headers.add(name: .vary, value: "origin")
        }
        return response
    }

    // MARK: - S3

    private func respondS3(
        to request: Request, origin: String, chainingTo next: any AsyncResponder
    ) async throws -> Response {
        let bucketName = Self.bucketName(of: request)
        let config = await BucketCORSCache.shared.resolvedConfig(
            app: request.application, bucket: bucketName)

        if Self.isPreflight(request) {
            // Verified against S3's CORS troubleshooting guide: a preflight no rule admits is a
            // 403 AccessForbidden, with a distinct message when the bucket has no rules at all.
            let method = request.headers.first(name: .accessControlRequestMethod) ?? ""
            let requestHeaders = Self.requestedHeaders(request)
            guard let config else {
                return Self.forbidden(request, "CORSResponse: CORS is not enabled for this bucket.")
            }
            guard let rule = config.rule(admitting: origin, method: method, requestHeaders: requestHeaders)
            else {
                return Self.forbidden(
                    request,
                    "CORSResponse: This CORS request is not allowed. Check the request's Origin, Access-Control-Request-Method, and Access-Control-Request-Headers."
                )
            }
            let response = Response(status: .ok)
            Self.apply(rule, origin: origin, to: response)
            response.headers.replaceOrAdd(
                name: .accessControlAllowMethods, value: rule.allowedMethods.joined(separator: ", "))
            if !requestHeaders.isEmpty {
                response.headers.replaceOrAdd(
                    name: .accessControlAllowHeaders, value: requestHeaders.joined(separator: ", "))
            }
            if let maxAge = rule.maxAgeSeconds {
                response.headers.replaceOrAdd(name: .accessControlMaxAge, value: String(maxAge))
            }
            return response
        }

        // An actual request a rule admits gets the rule's headers; one no rule admits is
        // served as if it had no Origin at all - the browser, not the server, enforces CORS.
        let response = try await next.respond(to: request)
        if let rule = config?.rule(admitting: origin, method: request.method.rawValue) {
            Self.apply(rule, origin: origin, to: response)
        }
        return response
    }

    private static func apply(_ rule: CORSRule, origin: String, to response: Response) {
        let anyOrigin = rule.allowedOrigins.contains("*")
        response.headers.replaceOrAdd(name: .accessControlAllowOrigin, value: anyOrigin ? "*" : origin)
        if !anyOrigin {
            response.headers.replaceOrAdd(name: .accessControlAllowCredentials, value: "true")
        }
        if !rule.exposeHeaders.isEmpty {
            response.headers.replaceOrAdd(
                name: .accessControlExpose, value: rule.exposeHeaders.joined(separator: ", "))
        }
        response.headers.replaceOrAdd(
            name: .vary, value: "Origin, Access-Control-Request-Headers, Access-Control-Request-Method")
    }

    private static func forbidden(_ request: Request, _ message: String) -> Response {
        S3ErrorMiddleware.xmlResponse(
            status: .forbidden, code: "AccessForbidden", message: message,
            resource: request.url.path, requestId: request.id)
    }

    private static func isPreflight(_ request: Request) -> Bool {
        request.method == .OPTIONS && request.headers.first(name: .accessControlRequestMethod) != nil
    }

    /// Path-style addressing: the first path component is the bucket.
    static func bucketName(of request: Request) -> String {
        let path = request.url.path.drop(while: { $0 == "/" })
        let component = path.prefix(while: { $0 != "/" })
        return String(component).removingPercentEncoding ?? String(component)
    }

    static func requestedHeaders(_ request: Request) -> [String] {
        request.headers[.accessControlRequestHeaders]
            .flatMap { $0.split(separator: ",") }
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
    }
}
