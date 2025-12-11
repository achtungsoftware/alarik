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

let host = "127.0.0.1"
let accessKey = "AKIAIOSFODNN7EXAMPLE"
let secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
let region = "us-east-1"

@Suite("S3Controller tests", .serialized)
struct S3ControllerTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()
            let loadCacheLifecycle = LoadCacheLifecycle()
            try await loadCacheLifecycle.didBootAsync(app)
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

        // Use the full URL with scheme for proper signing
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
            headers: HTTPHeaders(headers),  // Pass as HTTPHeaders directly
            body: body != nil ? .data(body!) : .none
        )

        return signed
    }

    @Test("Create Bucket (PUT /:bucketName)")
    func testCreateBucket() async throws {
        let bucketName = "my-test-bucket"
        try await withApp { app in
            let signed = signedHeaders(for: .PUT, path: "/\(bucketName)")

            try await app.test(
                .PUT, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "Location") == "/\(bucketName)")
                })

            // Verify creation using HEAD
            let headSigned = signedHeaders(for: .HEAD, path: "/\(bucketName)")
            try await app.test(
                .HEAD, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: headSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("Head Bucket (HEAD /:bucketName)")
    func testHeadBucket() async throws {
        let bucketName = "my-test-bucket"
        try await withApp { app in
            // Create bucket via endpoint
            let createSigned = signedHeaders(for: .PUT, path: "/\(bucketName)")
            try await app.test(
                .PUT, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            let signed = signedHeaders(for: .HEAD, path: "/\(bucketName)")

            try await app.test(
                .HEAD, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.readableBytes == 0)
                })
        }
    }

    @Test("Get Bucket Location (GET /:bucketName?location)")
    func testGetBucketLocation() async throws {
        let bucketName = "my-test-bucket"
        try await withApp { app in
            // Create bucket via endpoint
            let createSigned = signedHeaders(for: .PUT, path: "/\(bucketName)")
            try await app.test(
                .PUT, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "location")

            try await app.test(
                .GET, "/\(bucketName)?location",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .xml)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("us-east-1"))
                })
        }
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

    private func putObject(_ app: Application, bucketName: String, key: String, content: String)
        async throws
    {
        let data = content.data(using: .utf8)!
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

    @Test("List Objects V1 - Empty bucket")
    func testListObjectsV1Empty() async throws {
        let bucketName = "test-list-empty"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)")

            try await app.test(
                .GET, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .xml)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Name>\(bucketName)</Name>"))
                    #expect(bodyString.contains("<IsTruncated>false</IsTruncated>"))
                    #expect(bodyString.contains("<MaxKeys>1000</MaxKeys>"))
                })
        }
    }

    @Test("List Objects V1 - With objects")
    func testListObjectsV1WithObjects() async throws {
        let bucketName = "test-list-objects"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Create test objects
            try await putObject(app, bucketName: bucketName, key: "file1.txt", content: "content1")
            try await putObject(app, bucketName: bucketName, key: "file2.txt", content: "content2")
            try await putObject(app, bucketName: bucketName, key: "file3.txt", content: "content3")

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)")

            try await app.test(
                .GET, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Key>file1.txt</Key>"))
                    #expect(bodyString.contains("<Key>file2.txt</Key>"))
                    #expect(bodyString.contains("<Key>file3.txt</Key>"))
                    #expect(bodyString.contains("<IsTruncated>false</IsTruncated>"))
                })
        }
    }

    @Test("List Objects V1 - With prefix")
    func testListObjectsV1WithPrefix() async throws {
        let bucketName = "test-list-prefix"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            try await putObject(
                app, bucketName: bucketName, key: "docs/file1.txt", content: "content1")
            try await putObject(
                app, bucketName: bucketName, key: "docs/file2.txt", content: "content2")
            try await putObject(
                app, bucketName: bucketName, key: "images/photo.jpg", content: "photo")

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "prefix=docs/")

            try await app.test(
                .GET, "/\(bucketName)?prefix=docs/",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Prefix>docs/</Prefix>"))
                    #expect(bodyString.contains("<Key>docs/file1.txt</Key>"))
                    #expect(bodyString.contains("<Key>docs/file2.txt</Key>"))
                    #expect(!bodyString.contains("<Key>images/photo.jpg</Key>"))
                })
        }
    }

    @Test("List Objects V1 - With delimiter")
    func testListObjectsV1WithDelimiter() async throws {
        let bucketName = "test-list-delimiter"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            try await putObject(app, bucketName: bucketName, key: "root.txt", content: "root")
            try await putObject(
                app, bucketName: bucketName, key: "docs/file1.txt", content: "content1")
            try await putObject(
                app, bucketName: bucketName, key: "docs/file2.txt", content: "content2")
            try await putObject(
                app, bucketName: bucketName, key: "images/photo.jpg", content: "photo")

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "delimiter=/")

            try await app.test(
                .GET, "/\(bucketName)?delimiter=/",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Key>root.txt</Key>"))
                    #expect(bodyString.contains("<Prefix>docs/</Prefix>"))
                    #expect(bodyString.contains("<Prefix>images/</Prefix>"))
                    // Should not contain the nested files directly
                    #expect(!bodyString.contains("<Key>docs/file1.txt</Key>"))
                })
        }
    }

    @Test("List Objects V1 - With maxKeys")
    func testListObjectsV1WithMaxKeys() async throws {
        let bucketName = "test-list-maxkeys"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            for i in 1...5 {
                try await putObject(
                    app, bucketName: bucketName, key: "file\(i).txt", content: "content\(i)")
            }

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "max-keys=2")

            try await app.test(
                .GET, "/\(bucketName)?max-keys=2",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<MaxKeys>2</MaxKeys>"))
                    #expect(bodyString.contains("<IsTruncated>true</IsTruncated>"))
                    #expect(bodyString.contains("<NextMarker>"))
                })
        }
    }

    @Test("List Objects V1 - With marker pagination")
    func testListObjectsV1WithMarker() async throws {
        let bucketName = "test-list-marker"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            try await putObject(app, bucketName: bucketName, key: "file1.txt", content: "content1")
            try await putObject(app, bucketName: bucketName, key: "file2.txt", content: "content2")
            try await putObject(app, bucketName: bucketName, key: "file3.txt", content: "content3")

            // First page
            let signed1 = signedHeaders(for: .GET, path: "/\(bucketName)", query: "max-keys=2")
            var nextMarker: String?

            try await app.test(
                .GET, "/\(bucketName)?max-keys=2",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed1)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<IsTruncated>true</IsTruncated>"))

                    // Extract next marker (simplified parsing)
                    if let range = bodyString.range(of: "<NextMarker>") {
                        let start = range.upperBound
                        if let endRange = bodyString[start...].range(of: "</NextMarker>") {
                            nextMarker = String(bodyString[start..<endRange.lowerBound])
                        }
                    }
                })

            // Second page with marker
            if let marker = nextMarker {
                let signed2 = signedHeaders(
                    for: .GET, path: "/\(bucketName)", query: "marker=\(marker)")

                try await app.test(
                    .GET, "/\(bucketName)?marker=\(marker)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed2)
                    },
                    afterResponse: { res in
                        #expect(res.status == .ok)

                        let bodyString = res.body.string
                        #expect(bodyString.contains("<Marker>\(marker)</Marker>"))
                    })
            }
        }
    }

    @Test("List Objects V2 - Empty bucket")
    func testListObjectsV2Empty() async throws {
        let bucketName = "test-list-v2-empty"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "list-type=2")

            try await app.test(
                .GET, "/\(bucketName)?list-type=2",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .xml)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Name>\(bucketName)</Name>"))
                    #expect(bodyString.contains("<KeyCount>0</KeyCount>"))
                    #expect(bodyString.contains("<IsTruncated>false</IsTruncated>"))
                })
        }
    }

    @Test("List Objects V2 - With objects")
    func testListObjectsV2WithObjects() async throws {
        let bucketName = "test-list-v2-objects"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            try await putObject(app, bucketName: bucketName, key: "alpha.txt", content: "content1")
            try await putObject(app, bucketName: bucketName, key: "beta.txt", content: "content2")
            try await putObject(app, bucketName: bucketName, key: "gamma.txt", content: "content3")

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "list-type=2")

            try await app.test(
                .GET, "/\(bucketName)?list-type=2",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<KeyCount>3</KeyCount>"))
                    #expect(bodyString.contains("<Key>alpha.txt</Key>"))
                    #expect(bodyString.contains("<Key>beta.txt</Key>"))
                    #expect(bodyString.contains("<Key>gamma.txt</Key>"))
                })
        }
    }

    @Test("List Objects V2 - With prefix and delimiter")
    func testListObjectsV2WithPrefixAndDelimiter() async throws {
        let bucketName = "test-list-v2-prefix-delim"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            try await putObject(
                app, bucketName: bucketName, key: "docs/api/file1.txt", content: "content1")
            try await putObject(
                app, bucketName: bucketName, key: "docs/api/file2.txt", content: "content2")
            try await putObject(
                app, bucketName: bucketName, key: "docs/guides/guide1.txt", content: "guide1")
            try await putObject(
                app, bucketName: bucketName, key: "docs/readme.txt", content: "readme")

            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "list-type=2&prefix=docs/&delimiter=/")

            try await app.test(
                .GET, "/\(bucketName)?list-type=2&prefix=docs/&delimiter=/",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Prefix>docs/</Prefix>"))
                    #expect(bodyString.contains("<Key>docs/readme.txt</Key>"))
                    #expect(bodyString.contains("<Prefix>docs/api/</Prefix>"))
                    #expect(bodyString.contains("<Prefix>docs/guides/</Prefix>"))
                    #expect(!bodyString.contains("<Key>docs/api/file1.txt</Key>"))
                })
        }
    }

    @Test("List Objects V2 - With continuation token")
    func testListObjectsV2WithContinuationToken() async throws {
        let bucketName = "test-list-v2-token"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            for i in 1...5 {
                try await putObject(
                    app, bucketName: bucketName, key: "file\(i).txt", content: "content\(i)")
            }

            // First page
            let signed1 = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "list-type=2&max-keys=2")
            var nextToken: String?

            try await app.test(
                .GET, "/\(bucketName)?list-type=2&max-keys=2",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed1)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<KeyCount>2</KeyCount>"))
                    #expect(bodyString.contains("<IsTruncated>true</IsTruncated>"))

                    // Extract continuation token
                    if let range = bodyString.range(of: "<NextContinuationToken>") {
                        let start = range.upperBound
                        if let endRange = bodyString[start...].range(of: "</NextContinuationToken>")
                        {
                            nextToken = String(bodyString[start..<endRange.lowerBound])
                        }
                    }
                })

            // Second page
            if let token = nextToken {
                let signed2 = signedHeaders(
                    for: .GET, path: "/\(bucketName)",
                    query: "list-type=2&continuation-token=\(token)")

                try await app.test(
                    .GET, "/\(bucketName)?list-type=2&continuation-token=\(token)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed2)
                    },
                    afterResponse: { res in
                        #expect(res.status == .ok)

                        let bodyString = res.body.string
                        #expect(
                            bodyString.contains("<ContinuationToken>\(token)</ContinuationToken>"))
                    })
            }
        }
    }

    @Test("List Objects V2 - With start-after")
    func testListObjectsV2WithStartAfter() async throws {
        let bucketName = "test-list-v2-start-after"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            try await putObject(app, bucketName: bucketName, key: "file1.txt", content: "content1")
            try await putObject(app, bucketName: bucketName, key: "file2.txt", content: "content2")
            try await putObject(app, bucketName: bucketName, key: "file3.txt", content: "content3")

            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "list-type=2&start-after=file1.txt")

            try await app.test(
                .GET, "/\(bucketName)?list-type=2&start-after=file1.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<StartAfter>file1.txt</StartAfter>"))
                    #expect(!bodyString.contains("<Key>file1.txt</Key>"))
                    #expect(bodyString.contains("<Key>file2.txt</Key>"))
                    #expect(bodyString.contains("<Key>file3.txt</Key>"))
                })
        }
    }

    @Test("List Objects - Non-existent bucket returns 404")
    func testListObjectsNonExistentBucket() async throws {
        let bucketName = "non-existent-bucket"
        try await withApp { app in
            let signed = signedHeaders(for: .GET, path: "/\(bucketName)")

            try await app.test(
                .GET, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Code>NoSuchBucket</Code>"))
                })
        }
    }

    @Test("List Objects - Unauthorized access returns 403")
    func testListObjectsUnauthorized() async throws {
        let bucketName = "test-list-unauth"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Try to access without proper signature
            try await app.test(
                .GET, "/\(bucketName)",
                afterResponse: { res in
                    // Should fail due to missing/invalid auth
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })
        }
    }

    @Test("Delete Empty Bucket - Should succeed")
    func testDeleteEmptyBucket() async throws {
        let bucketName = "test-delete-empty"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let signed = signedHeaders(for: .DELETE, path: "/\(bucketName)")
            try await app.test(
                .DELETE, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })
        }
    }

    @Test("Delete Non-Empty Bucket - Should fail with BucketNotEmpty")
    func testDeleteNonEmptyBucket() async throws {
        let bucketName = "test-delete-non-empty"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "test.txt", content: "content")

            let signed = signedHeaders(for: .DELETE, path: "/\(bucketName)")
            try await app.test(
                .DELETE, "/\(bucketName)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .conflict)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Code>BucketNotEmpty</Code>"))
                })
        }
    }

    @Test("GET Object with Range - Complete range (bytes=0-9)")
    func testGetObjectWithCompleteRange() async throws {
        let bucketName = "test-range-complete"
        let content = "0123456789ABCDEF"  // 16 bytes
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "test.txt", content: content)

            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt",
                additionalHeaders: ["range": "bytes=0-9"])

            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .partialContent)
                    #expect(res.headers.first(name: "Content-Range") == "bytes 0-9/16")
                    #expect(res.headers.first(name: "Content-Length") == "10")
                    #expect(res.headers.first(name: "Accept-Ranges") == "bytes")
                    let bodyString = res.body.string
                    #expect(bodyString == "0123456789")
                })
        }
    }

    @Test("GET Object with Range - Suffix range (bytes=-5)")
    func testGetObjectWithSuffixRange() async throws {
        let bucketName = "test-range-suffix"
        let content = "0123456789ABCDEF"  // 16 bytes
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "test.txt", content: content)

            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt",
                additionalHeaders: ["range": "bytes=-5"])

            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .partialContent)
                    #expect(res.headers.first(name: "Content-Range") == "bytes 11-15/16")
                    #expect(res.headers.first(name: "Content-Length") == "5")
                    let bodyString = res.body.string
                    #expect(bodyString == "BCDEF")
                })
        }
    }

    @Test("GET Object with Range - Open-ended range (bytes=10-)")
    func testGetObjectWithOpenEndedRange() async throws {
        let bucketName = "test-range-open"
        let content = "0123456789ABCDEF"  // 16 bytes
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "test.txt", content: content)

            let signed = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt",
                additionalHeaders: ["range": "bytes=10-"])

            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .partialContent)
                    #expect(res.headers.first(name: "Content-Range") == "bytes 10-15/16")
                    #expect(res.headers.first(name: "Content-Length") == "6")
                    let bodyString = res.body.string
                    #expect(bodyString == "ABCDEF")
                })
        }
    }

    @Test("GET Object without Range - Should return full content with Accept-Ranges")
    func testGetObjectWithoutRange() async throws {
        let bucketName = "test-no-range"
        let content = "Hello World"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "test.txt", content: content)

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)/test.txt")

            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "Accept-Ranges") == "bytes")
                    #expect(res.headers.first(name: "Content-Range") == nil)
                    let bodyString = res.body.string
                    #expect(bodyString == content)
                })
        }
    }

    @Test("HEAD Object - Should include Accept-Ranges header")
    func testHeadObjectIncludesAcceptRanges() async throws {
        let bucketName = "test-head-ranges"
        let content = "Test content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "test.txt", content: content)

            let signed = signedHeaders(for: .HEAD, path: "/\(bucketName)/test.txt")

            try await app.test(
                .HEAD, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "Accept-Ranges") == "bytes")
                    #expect(res.headers.first(name: "Content-Length") == "12")
                    #expect(res.body.readableBytes == 0)
                })
        }
    }

    @Test("GET Object with If-Match - Matching ETag should succeed")
    func testGetObjectWithIfMatchSuccess() async throws {
        let bucketName = "test-if-match"
        let content = "Test content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // First upload to get the ETag
            let putSigned = signedHeaders(for: .PUT, path: "/\(bucketName)/test.txt", body: Data(content.utf8))
            var etag: String = ""
            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(string: content)
                },
                afterResponse: { res in
                    etag = res.headers.first(name: "ETag") ?? ""
                })

            // Now GET with If-Match using the correct ETag
            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt",
                additionalHeaders: ["if-match": etag])

            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == content)
                })
        }
    }

    @Test("GET Object with If-Match - Non-matching ETag should fail with 412")
    func testGetObjectWithIfMatchFailure() async throws {
        let bucketName = "test-if-match-fail"
        let content = "Test content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "test.txt", content: content)

            // GET with If-Match using incorrect ETag
            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt",
                additionalHeaders: ["if-match": "\"wrongetag\""])

            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .preconditionFailed)
                })
        }
    }

    @Test("GET Object with If-None-Match - Non-matching ETag should succeed")
    func testGetObjectWithIfNoneMatchSuccess() async throws {
        let bucketName = "test-if-none-match"
        let content = "Test content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "test.txt", content: content)

            // GET with If-None-Match using incorrect ETag
            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt",
                additionalHeaders: ["if-none-match": "\"wrongetag\""])

            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == content)
                })
        }
    }

    @Test("GET Object with If-None-Match - Matching ETag should return 304 Not Modified")
    func testGetObjectWithIfNoneMatchNotModified() async throws {
        let bucketName = "test-if-none-match-304"
        let content = "Test content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // First upload to get the ETag
            let putSigned = signedHeaders(for: .PUT, path: "/\(bucketName)/test.txt", body: Data(content.utf8))
            var etag: String = ""
            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(string: content)
                },
                afterResponse: { res in
                    etag = res.headers.first(name: "ETag") ?? ""
                })

            // GET with If-None-Match using the same ETag
            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/test.txt",
                additionalHeaders: ["if-none-match": etag])

            try await app.test(
                .GET, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notModified)
                })
        }
    }

    @Test("HEAD Object with If-Match - Should validate ETag")
    func testHeadObjectWithIfMatch() async throws {
        let bucketName = "test-head-if-match"
        let content = "Test content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // First upload to get the ETag
            let putSigned = signedHeaders(for: .PUT, path: "/\(bucketName)/test.txt", body: Data(content.utf8))
            var etag: String = ""
            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(string: content)
                },
                afterResponse: { res in
                    etag = res.headers.first(name: "ETag") ?? ""
                })

            // HEAD with correct If-Match
            let headSigned = signedHeaders(
                for: .HEAD, path: "/\(bucketName)/test.txt",
                additionalHeaders: ["if-match": etag])

            try await app.test(
                .HEAD, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: headSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("PUT Object with correct Content-MD5 should succeed")
    func testPutObjectWithCorrectContentMD5() async throws {
        let bucketName = "test-md5-correct"
        let content = "Test content for MD5"
        let data = Data(content.utf8)
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Calculate MD5 and encode as base64
            let md5Hash = Insecure.MD5.hash(data: data)
            let md5Base64 = Data(md5Hash).base64EncodedString()

            let signed = signedHeaders(
                for: .PUT, path: "/\(bucketName)/test.txt",
                body: data,
                additionalHeaders: ["content-md5": md5Base64])

            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: data)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "ETag") != nil)
                })
        }
    }

    @Test("PUT Object with incorrect Content-MD5 should fail with BadDigest")
    func testPutObjectWithIncorrectContentMD5() async throws {
        let bucketName = "test-md5-incorrect"
        let content = "Test content for MD5"
        let data = Data(content.utf8)
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Use an incorrect MD5
            let wrongMD5 = "wrongmd5base64encoded=="

            let signed = signedHeaders(
                for: .PUT, path: "/\(bucketName)/test.txt",
                body: data,
                additionalHeaders: ["content-md5": wrongMD5])

            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: data)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Code>BadDigest</Code>"))
                })
        }
    }

    @Test("PUT Object without Content-MD5 should succeed (optional header)")
    func testPutObjectWithoutContentMD5() async throws {
        let bucketName = "test-md5-optional"
        let content = "Test content"
        let data = Data(content.utf8)
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let signed = signedHeaders(for: .PUT, path: "/\(bucketName)/test.txt", body: data)

            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: data)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("Copy Object - Basic copy within same bucket")
    func testCopyObjectWithinBucket() async throws {
        let bucketName = "test-copy-within"
        let sourceKey = "source.txt"
        let destKey = "destination.txt"
        let content = "Content to copy"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: sourceKey, content: content)

            // Copy the object
            let signed = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/\(destKey)",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/\(sourceKey)"]
            )

            try await app.test(
                .PUT, "/\(bucketName)/\(destKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<CopyObjectResult>"))
                    #expect(bodyString.contains("<ETag>"))
                    #expect(bodyString.contains("<LastModified>"))
                })

            // Verify destination exists and has same content
            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/\(destKey)")
            try await app.test(
                .GET, "/\(bucketName)/\(destKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == content)
                })
        }
    }

    @Test("Copy Object - Copy to different bucket")
    func testCopyObjectBetweenBuckets() async throws {
        let sourceBucket = "test-copy-source"
        let destBucket = "test-copy-dest"
        let key = "test.txt"
        let content = "Cross-bucket copy"
        try await withApp { app in
            try await createBucket(app, bucketName: sourceBucket)
            try await createBucket(app, bucketName: destBucket)
            try await putObject(app, bucketName: sourceBucket, key: key, content: content)

            // Copy between buckets
            let signed = signedHeaders(
                for: .PUT,
                path: "/\(destBucket)/\(key)",
                additionalHeaders: ["x-amz-copy-source": "/\(sourceBucket)/\(key)"]
            )

            try await app.test(
                .PUT, "/\(destBucket)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            // Verify destination
            let getSigned = signedHeaders(for: .GET, path: "/\(destBucket)/\(key)")
            try await app.test(
                .GET, "/\(destBucket)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == content)
                })
        }
    }

    @Test("Copy Object - Copy non-existent object should fail")
    func testCopyObjectNonExistent() async throws {
        let bucketName = "test-copy-nonexist"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let signed = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/destination.txt",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/nonexistent.txt"]
            )

            try await app.test(
                .PUT, "/\(bucketName)/destination.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Copy Object - With metadata directive REPLACE")
    func testCopyObjectReplaceMetadata() async throws {
        let bucketName = "test-copy-replace-meta"
        let sourceKey = "source.txt"
        let destKey = "dest.txt"
        let content = "Test content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Upload with text/plain
            let putData = Data(content.utf8)
            let putSigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/\(sourceKey)",
                body: putData,
                additionalHeaders: ["content-type": "text/plain"]
            )
            try await app.test(
                .PUT, "/\(bucketName)/\(sourceKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: putData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            // Copy with REPLACE directive and new content-type
            let copySigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/\(destKey)",
                additionalHeaders: [
                    "x-amz-copy-source": "/\(bucketName)/\(sourceKey)",
                    "x-amz-metadata-directive": "REPLACE",
                    "content-type": "application/json"
                ]
            )

            try await app.test(
                .PUT, "/\(bucketName)/\(destKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            // Verify new content-type
            let headSigned = signedHeaders(for: .HEAD, path: "/\(bucketName)/\(destKey)")
            try await app.test(
                .HEAD, "/\(bucketName)/\(destKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: headSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "Content-Type") == "application/json")
                })
        }
    }

    @Test("Copy Object - With If-Match condition (matching)")
    func testCopyObjectIfMatchSuccess() async throws {
        let bucketName = "test-copy-if-match"
        let content = "Test content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Upload source
            let putSigned = signedHeaders(for: .PUT, path: "/\(bucketName)/source.txt", body: Data(content.utf8))
            var etag: String = ""
            try await app.test(
                .PUT, "/\(bucketName)/source.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(string: content)
                },
                afterResponse: { res in
                    etag = res.headers.first(name: "ETag") ?? ""
                })

            // Copy with matching If-Match
            let copySigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/dest.txt",
                additionalHeaders: [
                    "x-amz-copy-source": "/\(bucketName)/source.txt",
                    "x-amz-copy-source-if-match": etag
                ]
            )

            try await app.test(
                .PUT, "/\(bucketName)/dest.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("Copy Object - With If-Match condition (non-matching)")
    func testCopyObjectIfMatchFailure() async throws {
        let bucketName = "test-copy-if-match-fail"
        let content = "Test content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "source.txt", content: content)

            // Copy with non-matching If-Match
            let copySigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/dest.txt",
                additionalHeaders: [
                    "x-amz-copy-source": "/\(bucketName)/source.txt",
                    "x-amz-copy-source-if-match": "\"wrongetag\""
                ]
            )

            try await app.test(
                .PUT, "/\(bucketName)/dest.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .preconditionFailed)
                })
        }
    }

    @Test("Copy Object - Security: Source bucket authentication required")
    func testCopyObjectSourceBucketAuth() async throws {
        let sourceBucket = "test-copy-source-auth"
        let destBucket = "test-copy-dest-auth"
        try await withApp { app in
            // Create both buckets (they belong to the test user)
            try await createBucket(app, bucketName: sourceBucket)
            try await createBucket(app, bucketName: destBucket)
            try await putObject(app, bucketName: sourceBucket, key: "file.txt", content: "data")

            // Verify copy succeeds when user has access to both buckets
            let signed = signedHeaders(
                for: .PUT,
                path: "/\(destBucket)/copy.txt",
                additionalHeaders: ["x-amz-copy-source": "/\(sourceBucket)/file.txt"]
            )

            try await app.test(
                .PUT, "/\(destBucket)/copy.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    // Should succeed - user owns both buckets
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("Copy Object - Rename pattern (copy + delete)")
    func testCopyObjectRenamePattern() async throws {
        let bucketName = "test-copy-rename"
        let oldKey = "old-name.txt"
        let newKey = "new-name.txt"
        let content = "Content to rename"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: oldKey, content: content)

            // Copy to new name
            let copySigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/\(newKey)",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/\(oldKey)"]
            )
            try await app.test(
                .PUT, "/\(bucketName)/\(newKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            // Delete old object
            let deleteSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)/\(oldKey)")
            try await app.test(
                .DELETE, "/\(bucketName)/\(oldKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            // Verify new exists and old doesn't
            let getNewSigned = signedHeaders(for: .GET, path: "/\(bucketName)/\(newKey)")
            try await app.test(
                .GET, "/\(bucketName)/\(newKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getNewSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == content)
                })

            let getOldSigned = signedHeaders(for: .GET, path: "/\(bucketName)/\(oldKey)")
            try await app.test(
                .GET, "/\(bucketName)/\(oldKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getOldSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }
}
