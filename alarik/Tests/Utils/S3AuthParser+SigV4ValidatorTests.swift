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

import Foundation
import NIO
import SotoCore
import SotoS3
import SotoSignerV4
import Testing
import Vapor

@testable import Alarik

@Suite("Additional S3AuthParser+SigV4Validator Tests", .serialized)
struct AdditionalS3AuthTests {

    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()
            try await test(app)
            try await app.autoRevert()
        } catch {
            try? StorageHelper.cleanStorage()
            try? await app.autoRevert()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    @Test("Parse from header with security token")
    func testParseFromHeaderWithToken() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let fullDate = "20251129T120000Z"
            let date = "20251129"
            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let region = "us-east-1"
            let service = "s3"
            let token = "AQoDYXdzEJr1K...truncated"
            let signedHeadersStr = "host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
            let signature = "abc123def456"
            let credential = "\(accessKey)/\(date)/\(region)/\(service)/aws4_request"
            let authHeader =
                "AWS4-HMAC-SHA256 Credential=\(credential), SignedHeaders=\(signedHeadersStr), Signature=\(signature)"

            let url = "https://examplebucket.s3.amazonaws.com/"
            let req = Request(application: app, method: .GET, url: URI(string: url), on: eventLoop)
            req.headers.add(name: "authorization", value: authHeader)
            req.headers.add(name: "x-amz-date", value: fullDate)
            req.headers.add(name: "x-amz-security-token", value: token)
            req.headers.add(
                name: "x-amz-content-sha256",
                value: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

            let authInfo = try S3AuthParser.parse(request: req)

            #expect(authInfo.token == token)
            #expect(authInfo.signedHeaders.contains("x-amz-security-token"))
        }
    }

    @Test("Parse from header missing credential")
    func testParseFromHeaderMissingCredential() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let authHeader = "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc123"

            let req = Request(application: app, method: .GET, url: URI(string: "/"), on: eventLoop)
            req.headers.add(name: "authorization", value: authHeader)

            #expect(throws: S3Error.self) {
                try S3AuthParser.parse(request: req)
            }
        }
    }

    @Test("Parse from header invalid credential format")
    func testParseFromHeaderInvalidCredentialFormat() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let authHeader =
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20251129/us-east-1, SignedHeaders=host, Signature=abc123"

            let req = Request(application: app, method: .GET, url: URI(string: "/"), on: eventLoop)
            req.headers.add(name: "authorization", value: authHeader)

            #expect(throws: S3Error.self) {
                try S3AuthParser.parse(request: req)
            }
        }
    }

    @Test("Parse from header empty authorization header")
    func testParseFromHeaderEmpty() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let req = Request(application: app, method: .GET, url: URI(string: "/"), on: eventLoop)
            req.headers.add(name: "authorization", value: "")

            #expect(throws: S3Error.self) {
                try S3AuthParser.parse(request: req)
            }
        }
    }

    @Test("Parse from query with security token")
    func testParseFromQueryWithToken() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let fullDate = "20251129T120000Z"
            let date = "20251129"
            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let region = "us-east-1"
            let service = "s3"
            let token = "AQoDYXdzEJr1K...truncated"
            let credential = "\(accessKey)/\(date)/\(region)/\(service)/aws4_request"
            let encodedCredential =
                credential.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed)
                ?? credential
            let encodedToken =
                token.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? token

            let url =
                "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=\(encodedCredential)&X-Amz-Date=\(fullDate)&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123&X-Amz-Security-Token=\(encodedToken)"

            let req = Request(application: app, method: .GET, url: URI(string: url), on: eventLoop)

            let authInfo = try S3AuthParser.parse(request: req)

            #expect(authInfo.token == token)
            #expect(authInfo.expires == 3600)
        }
    }

    @Test("Parse from query invalid expires too large")
    func testParseFromQueryInvalidExpiresTooLarge() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let fullDate = "20251129T120000Z"
            let date = "20251129"
            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let region = "us-east-1"
            let service = "s3"
            let credential = "\(accessKey)/\(date)/\(region)/\(service)/aws4_request"
            let encodedCredential =
                credential.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed)
                ?? credential

            let url =
                "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=\(encodedCredential)&X-Amz-Date=\(fullDate)&X-Amz-Expires=604801&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123"

            let req = Request(application: app, method: .GET, url: URI(string: url), on: eventLoop)

            #expect(throws: S3Error.self) {
                try S3AuthParser.parse(request: req)
            }
        }
    }

    @Test("Parse from query missing algorithm")
    func testParseFromQueryMissingAlgorithm() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let url =
                "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Credential=test&X-Amz-Date=20251129T120000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123"

            let req = Request(application: app, method: .GET, url: URI(string: url), on: eventLoop)

            #expect(throws: S3Error.self) {
                try S3AuthParser.parse(request: req)
            }
        }
    }

    @Test("Parse from query missing signature")
    func testParseFromQueryMissingSignature() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let fullDate = "20251129T120000Z"
            let date = "20251129"
            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let region = "us-east-1"
            let service = "s3"
            let credential = "\(accessKey)/\(date)/\(region)/\(service)/aws4_request"
            let encodedCredential =
                credential.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed)
                ?? credential

            let url =
                "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=\(encodedCredential)&X-Amz-Date=\(fullDate)&X-Amz-Expires=3600&X-Amz-SignedHeaders=host"

            let req = Request(application: app, method: .GET, url: URI(string: url), on: eventLoop)

            #expect(throws: S3Error.self) {
                try S3AuthParser.parse(request: req)
            }
        }
    }

    @Test("Parse no authentication provided")
    func testParseNoAuthentication() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let req = Request(
                application: app, method: .GET, url: URI(string: "https://example.com/"),
                on: eventLoop)

            #expect(throws: S3Error.self) {
                try S3AuthParser.parse(request: req)
            }
        }
    }

    @Test("Validate missing host header")
    func testValidateMissingHost() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let fullDate = "20251129T222858Z"
            let date = "20251129"
            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            let region = "us-east-1"
            let service = "s3"
            let signedHeadersStr = "x-amz-content-sha256;x-amz-date"
            let signature = "abc123"
            let credential = "\(accessKey)/\(date)/\(region)/\(service)/aws4_request"
            let authHeader =
                "AWS4-HMAC-SHA256 Credential=\(credential), SignedHeaders=\(signedHeadersStr), Signature=\(signature)"

            let url = "https://examplebucket.s3.amazonaws.com/"
            let req = Request(application: app, method: .GET, url: URI(string: url), on: eventLoop)
            req.headers.add(name: "authorization", value: authHeader)
            req.headers.add(name: "x-amz-date", value: fullDate)
            req.headers.add(
                name: "x-amz-content-sha256",
                value: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

            let authInfo = try S3AuthParser.parse(request: req)
            let validator = SigV4Validator(secretKey: secretKey)

            #expect(throws: S3Error.self) {
                try validator.validate(request: req, authInfo: authInfo)
            }
        }
    }

    @Test("Validate missing x-amz-date header")
    func testValidateMissingAmzDate() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let fullDate = "20251129T222858Z"
            let date = "20251129"
            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            let region = "us-east-1"
            let service = "s3"
            let signedHeadersStr = "host;x-amz-content-sha256"
            let signature = "abc123"
            let credential = "\(accessKey)/\(date)/\(region)/\(service)/aws4_request"
            let authHeader =
                "AWS4-HMAC-SHA256 Credential=\(credential), SignedHeaders=\(signedHeadersStr), Signature=\(signature)"

            let url = "https://examplebucket.s3.amazonaws.com/"
            let req = Request(application: app, method: .GET, url: URI(string: url), on: eventLoop)
            req.headers.add(name: "host", value: "examplebucket.s3.amazonaws.com")
            req.headers.add(name: "authorization", value: authHeader)
            req.headers.add(name: "x-amz-date", value: fullDate)
            req.headers.add(
                name: "x-amz-content-sha256",
                value: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

            let authInfo = try S3AuthParser.parse(request: req)
            let validator = SigV4Validator(secretKey: secretKey)

            #expect(throws: S3Error.self) {
                try validator.validate(request: req, authInfo: authInfo)
            }
        }
    }

    @Test("Validate token present but not signed")
    func testValidateTokenNotSigned() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let fullDate = "20251129T222858Z"
            let date = "20251129"
            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            let region = "us-east-1"
            let service = "s3"
            let token = "AQoDYXdzEJr1K...truncated"
            let signedHeadersStr = "host;x-amz-content-sha256;x-amz-date"
            let signature = "abc123"
            let credential = "\(accessKey)/\(date)/\(region)/\(service)/aws4_request"
            let authHeader =
                "AWS4-HMAC-SHA256 Credential=\(credential), SignedHeaders=\(signedHeadersStr), Signature=\(signature)"

            let url = "https://examplebucket.s3.amazonaws.com/"
            let req = Request(application: app, method: .GET, url: URI(string: url), on: eventLoop)
            req.headers.add(name: "host", value: "examplebucket.s3.amazonaws.com")
            req.headers.add(name: "authorization", value: authHeader)
            req.headers.add(name: "x-amz-date", value: fullDate)
            req.headers.add(name: "x-amz-security-token", value: token)
            req.headers.add(
                name: "x-amz-content-sha256",
                value: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

            let authInfo = try S3AuthParser.parse(request: req)
            let validator = SigV4Validator(secretKey: secretKey)

            #expect(throws: S3Error.self) {
                try validator.validate(request: req, authInfo: authInfo)
            }
        }
    }

    @Test("Validate invalid date format")
    func testValidateInvalidDateFormat() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let fullDate = "20251129T22285"  // Missing digit
            let date = "20251129"
            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            let region = "us-east-1"
            let service = "s3"
            let signedHeadersStr = "host;x-amz-content-sha256;x-amz-date"
            let signature = "abc123"
            let credential = "\(accessKey)/\(date)/\(region)/\(service)/aws4_request"
            let authHeader =
                "AWS4-HMAC-SHA256 Credential=\(credential), SignedHeaders=\(signedHeadersStr), Signature=\(signature)"

            let url = "https://examplebucket.s3.amazonaws.com/"
            let req = Request(application: app, method: .GET, url: URI(string: url), on: eventLoop)
            req.headers.add(name: "host", value: "examplebucket.s3.amazonaws.com")
            req.headers.add(name: "authorization", value: authHeader)
            req.headers.add(name: "x-amz-date", value: fullDate)
            req.headers.add(
                name: "x-amz-content-sha256",
                value: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

            let authInfo = try S3AuthParser.parse(request: req)
            let validator = SigV4Validator(secretKey: secretKey)

            #expect(throws: S3Error.self) {
                try validator.validate(request: req, authInfo: authInfo)
            }
        }
    }

    @Test("Validate payload hash mismatch")
    func testValidatePayloadHashMismatch() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let fullDate = "20251129T222858Z"
            let date = "20251129"
            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            let region = "us-east-1"
            let service = "s3"
            let bodyContent = "test content"
            let bodyData = Data(bodyContent.utf8)
            let wrongHash = "0000000000000000000000000000000000000000000000000000000000000000"
            let signedHeadersStr = "host;x-amz-content-sha256;x-amz-date"
            let signature = "abc123"
            let credential = "\(accessKey)/\(date)/\(region)/\(service)/aws4_request"
            let authHeader =
                "AWS4-HMAC-SHA256 Credential=\(credential), SignedHeaders=\(signedHeadersStr), Signature=\(signature)"

            let url = "https://examplebucket.s3.amazonaws.com/testfile.txt"
            var buffer = ByteBufferAllocator().buffer(capacity: bodyData.count)
            buffer.writeBytes(bodyData)

            let req = Request(
                application: app,
                method: .PUT,
                url: URI(string: url),
                collectedBody: buffer,
                on: eventLoop
            )
            req.headers.add(name: "host", value: "examplebucket.s3.amazonaws.com")
            req.headers.add(name: "authorization", value: authHeader)
            req.headers.add(name: "x-amz-date", value: fullDate)
            req.headers.add(name: "x-amz-content-sha256", value: wrongHash)

            let authInfo = try S3AuthParser.parse(request: req)
            let validator = SigV4Validator(secretKey: secretKey)

            #expect(throws: S3Error.self) {
                try validator.validate(request: req, authInfo: authInfo)
            }
        }
    }

    @Test("Validate PUT request with body using Soto")
    func testValidatePutWithBodyUsingSoto() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            let region = "us-east-1"
            let bodyContent = "test content"
            let bodyData = Data(bodyContent.utf8)



            // We'll manually sign a request using Soto's internal signer
            // Create the request
            let url = URL(string: "https://examplebucket.s3.us-east-1.amazonaws.com/testfile.txt")!
            var urlRequest = URLRequest(url: url)
            urlRequest.httpMethod = "PUT"
            urlRequest.httpBody = bodyData

            // Use Soto's AWSSigner to sign the request
            let signer = AWSSigner(
                credentials: StaticCredential(accessKeyId: accessKey, secretAccessKey: secretKey),
                name: "s3",
                region: region
            )

            let signedHeaders = signer.signHeaders(
                url: url,
                method: .PUT,
                headers: ["host": "examplebucket.s3.us-east-1.amazonaws.com"],
                body: .data(bodyData)
            )

            // Create Vapor request with signed headers
            let vaporUrl = "https://examplebucket.s3.us-east-1.amazonaws.com/testfile.txt"
            var buffer = ByteBufferAllocator().buffer(capacity: bodyData.count)
            buffer.writeBytes(bodyData)

            let req = Request(
                application: app,
                method: .PUT,
                url: URI(string: vaporUrl),
                collectedBody: buffer,
                on: eventLoop
            )

            for (key, value) in signedHeaders {
                req.headers.add(name: key, value: value)
            }

            let authInfo = try S3AuthParser.parse(request: req)
            let validator = SigV4Validator(secretKey: secretKey)
            let isValid = try validator.validate(request: req, authInfo: authInfo)

            #expect(isValid == true)
        }
    }

    @Test("Validate GET request with query parameters using Soto")
    func testValidateGetWithQueryParamsUsingSoto() async throws {
        try await withApp { app in
            let eventLoop = app.eventLoopGroup.next()

            let accessKey = "AKIAIOSFODNN7EXAMPLE"
            let secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            let region = "us-east-1"


            // Use Soto's AWSSigner to sign the request
            let signer = AWSSigner(
                credentials: StaticCredential(accessKeyId: accessKey, secretAccessKey: secretKey),
                name: "s3",
                region: region
            )

            let url = URL(
                string:
                    "https://examplebucket.s3.us-east-1.amazonaws.com/?delimiter=/&max-keys=100&prefix=photos/"
            )!

            let signedHeaders = signer.signHeaders(
                url: url,
                method: .GET,
                headers: ["host": "examplebucket.s3.us-east-1.amazonaws.com"],
            )

            let req = Request(
                application: app, method: .GET, url: URI(string: url.absoluteString), on: eventLoop)

            for (key, value) in signedHeaders {
                req.headers.add(name: key, value: value)
            }

            let authInfo = try S3AuthParser.parse(request: req)
            let validator = SigV4Validator(secretKey: secretKey)
            let isValid = try validator.validate(request: req, authInfo: authInfo)

            #expect(isValid == true)
        }
    }
}
