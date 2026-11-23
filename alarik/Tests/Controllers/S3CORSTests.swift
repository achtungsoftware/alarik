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
import Crypto
import Foundation
import NIOCore
import NIOHTTP1
import SotoCore
import SotoS3
import SotoSignerV4
import Testing
import Vapor
import VaporTesting

@testable import Alarik

@Suite("S3 CORS tests", .serialized)
struct S3CORSTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            let loadCacheLifecycle = LoadCacheLifecycle()
            try await loadCacheLifecycle.didBootAsync(app)
            try await test(app)
        } catch {
            try? StorageHelper.cleanStorage()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    private func signedHeaders(
        for method: HTTPMethod,
        path: String,
        query: String? = nil,
        body: Data? = nil,
        additionalHeaders: [String: String] = [:]
    ) -> HTTPHeaders {
        var fullPath = path
        if let query = query, !query.isEmpty {
            fullPath += "?\(query)"
        }

        let urlString = "http://\(host)\(fullPath)"
        guard let url = URL(string: urlString) else {
            Issue.record("Invalid URL: \(urlString)")
            return HTTPHeaders()
        }

        let signer = AWSSigner(
            credentials: StaticCredential(accessKeyId: accessKey, secretAccessKey: secretKey),
            name: "s3",
            region: region
        )

        var headers: [(String, String)] = [("host", host)]
        for (key, value) in additionalHeaders {
            headers.append((key, value))
        }

        let signed = signer.signHeaders(
            url: url,
            method: method,
            headers: HTTPHeaders(headers),
            body: body != nil ? .data(body!) : .none
        )

        return signed
    }

    private func createBucket(_ app: Application, bucketName: String) async throws {
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)")
        try await app.test(
            .PUT, "/\(bucketName)",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func putObject(_ app: Application, bucketName: String, key: String, body: String) async throws {
        let data = Data(body.utf8)
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)/\(key)", body: data)
        try await app.test(
            .PUT, "/\(bucketName)/\(key)",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: data)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func corsXML(origin: String = "https://app.example.com") -> String {
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <CORSRule>
            <ID>web</ID>
            <AllowedOrigin>\(origin)</AllowedOrigin>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedMethod>PUT</AllowedMethod>
            <AllowedHeader>x-amz-*</AllowedHeader>
            <AllowedHeader>Content-Type</AllowedHeader>
            <ExposeHeader>ETag</ExposeHeader>
            <MaxAgeSeconds>600</MaxAgeSeconds>
          </CORSRule>
        </CORSConfiguration>
        """
    }

    private func putCORS(_ app: Application, bucketName: String, xml: String) async throws
        -> TestingHTTPResponse
    {
        let bodyData = Data(xml.utf8)
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)", query: "cors", body: bodyData)
        var captured: TestingHTTPResponse!
        try await app.test(
            .PUT, "/\(bucketName)?cors",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: bodyData)
            },
            afterResponse: { res in
                captured = res
            })
        return captured
    }

    private func getCORS(_ app: Application, bucketName: String, _ check: (TestingHTTPResponse) throws -> Void)
        async throws
    {
        let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "cors")
        try await app.test(
            .GET, "/\(bucketName)?cors",
            beforeRequest: { req in req.headers.add(contentsOf: signed) },
            afterResponse: { res in try check(res) })
    }

    @Test("PUT/GET/DELETE ?cors round-trip, 404 NoSuchCORSConfiguration when unset")
    func testRoundTrip() async throws {
        let bucketName = "test-cors-roundtrip"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            try await getCORS(app, bucketName: bucketName) { res in
                #expect(res.status == .notFound)
                #expect(res.body.string.contains("<Code>NoSuchCORSConfiguration</Code>"))
            }

            let put = try await putCORS(app, bucketName: bucketName, xml: corsXML())
            #expect(put.status == .ok)

            try await getCORS(app, bucketName: bucketName) { res in
                #expect(res.status == .ok)
                #expect(res.body.string.contains("<AllowedOrigin>https://app.example.com</AllowedOrigin>"))
                #expect(res.body.string.contains("<MaxAgeSeconds>600</MaxAgeSeconds>"))
            }

            let deleteSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)", query: "cors")
            try await app.test(
                .DELETE, "/\(bucketName)?cors",
                beforeRequest: { req in req.headers.add(contentsOf: deleteSigned) },
                afterResponse: { res in #expect(res.status == .noContent) })

            try await getCORS(app, bucketName: bucketName) { res in
                #expect(res.status == .notFound)
            }
        }
    }

    @Test("PUT ?cors with an unsupported method is MalformedXML")
    func testPutRejectsBadMethod() async throws {
        let bucketName = "test-cors-bad"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let xml = """
                <CORSConfiguration><CORSRule>
                  <AllowedOrigin>*</AllowedOrigin><AllowedMethod>PATCH</AllowedMethod>
                </CORSRule></CORSConfiguration>
                """
            let put = try await putCORS(app, bucketName: bucketName, xml: xml)
            #expect(put.status == .badRequest)
            #expect(put.body.string.contains("<Code>MalformedXML</Code>"))
        }
    }

    @Test("Preflight is answered from the bucket's rules without touching the router")
    func testPreflight() async throws {
        let bucketName = "test-cors-preflight"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // No rules yet: S3 says CORS is not enabled.
            try await app.test(
                .OPTIONS, "/\(bucketName)/some/key.txt",
                beforeRequest: { req in
                    req.headers.add(name: .origin, value: "https://app.example.com")
                    req.headers.add(name: .accessControlRequestMethod, value: "PUT")
                },
                afterResponse: { res in
                    #expect(res.status == .forbidden)
                    #expect(res.body.string.contains("CORS is not enabled"))
                })

            _ = try await putCORS(app, bucketName: bucketName, xml: corsXML())

            try await app.test(
                .OPTIONS, "/\(bucketName)/some/key.txt",
                beforeRequest: { req in
                    req.headers.add(name: .origin, value: "https://app.example.com")
                    req.headers.add(name: .accessControlRequestMethod, value: "PUT")
                    req.headers.add(name: .accessControlRequestHeaders, value: "content-type, x-amz-date")
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: .accessControlAllowOrigin) == "https://app.example.com")
                    #expect(res.headers.first(name: .accessControlAllowMethods) == "GET, PUT")
                    #expect(res.headers.first(name: .accessControlAllowHeaders) == "content-type, x-amz-date")
                    #expect(res.headers.first(name: .accessControlExpose) == "ETag")
                    #expect(res.headers.first(name: .accessControlMaxAge) == "600")
                    #expect(res.headers.first(name: .accessControlAllowCredentials) == "true")
                    #expect(res.headers.first(name: .vary)?.contains("Origin") == true)
                })

            // Wrong origin, disallowed method, header no rule covers: all 403.
            for (origin, method, header) in [
                ("https://evil.example.com", "GET", nil),
                ("https://app.example.com", "DELETE", nil),
                ("https://app.example.com", "PUT", "authorization"),
            ] as [(String, String, String?)] {
                try await app.test(
                    .OPTIONS, "/\(bucketName)/some/key.txt",
                    beforeRequest: { req in
                        req.headers.add(name: .origin, value: origin)
                        req.headers.add(name: .accessControlRequestMethod, value: method)
                        if let header { req.headers.add(name: .accessControlRequestHeaders, value: header) }
                    },
                    afterResponse: { res in
                        #expect(res.status == .forbidden, "\(origin) \(method) \(header ?? "")")
                        #expect(res.body.string.contains("<Code>AccessForbidden</Code>"))
                        #expect(res.headers.first(name: .accessControlAllowOrigin) == nil)
                    })
            }
        }
    }

    @Test("An admitted actual request carries the rule's headers; others carry none")
    func testActualRequest() async throws {
        let bucketName = "test-cors-actual"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "hello.txt", body: "hi")
            _ = try await putCORS(app, bucketName: bucketName, xml: corsXML())

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)/hello.txt")
            try await app.test(
                .GET, "/\(bucketName)/hello.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.headers.add(name: .origin, value: "https://app.example.com")
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "hi")
                    #expect(res.headers.first(name: .accessControlAllowOrigin) == "https://app.example.com")
                    #expect(res.headers.first(name: .accessControlExpose) == "ETag")
                    // Preflight-only headers stay off actual responses.
                    #expect(res.headers.first(name: .accessControlAllowMethods) == nil)
                })

            try await app.test(
                .GET, "/\(bucketName)/hello.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.headers.add(name: .origin, value: "https://evil.example.com")
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: .accessControlAllowOrigin) == nil)
                })

            // A `*` origin answers with a literal `*` and no credentials flag.
            _ = try await putCORS(app, bucketName: bucketName, xml: corsXML(origin: "*"))
            try await app.test(
                .GET, "/\(bucketName)/hello.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.headers.add(name: .origin, value: "https://anyone.example.com")
                },
                afterResponse: { res in
                    #expect(res.headers.first(name: .accessControlAllowOrigin) == "*")
                    #expect(res.headers.first(name: .accessControlAllowCredentials) == nil)
                })
        }
    }

    @Test("Console routes keep the console origin policy")
    func testConsoleRoutesUnchanged() async throws {
        try await withApp { app in
            let consoleOrigin = ConsoleBaseURL.resolve()
            try await app.test(
                .OPTIONS, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.add(name: .origin, value: consoleOrigin)
                    req.headers.add(name: .accessControlRequestMethod, value: "GET")
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: .accessControlAllowOrigin) == consoleOrigin)
                    #expect(res.headers.first(name: .accessControlAllowMethods)?.contains("GET") == true)
                })
            try await app.test(
                .OPTIONS, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.add(name: .origin, value: "https://evil.example.com")
                    req.headers.add(name: .accessControlRequestMethod, value: "GET")
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: .accessControlAllowOrigin) == nil)
                })
        }
    }
}
