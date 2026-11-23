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
                    // S3's us-east-1 quirk: LocationConstraint is empty for that region,
                    // not the literal string "us-east-1" (verified against the
                    // GetBucketLocation API reference).
                    #expect(bodyString.contains("<LocationConstraint"))
                    #expect(bodyString.contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></LocationConstraint>"))
                })
        }
    }

    /// Overrides the process-wide `ALARIK_REGION` for the duration of `body`, restoring
    /// whatever was there before. Safe under `swift test --no-parallel` (this suite is already
    /// `.serialized`), same reasoning as `AlarikRegionTests`.
    private func withRegion(_ region: String, _ body: () async throws -> Void) async throws {
        let original = ProcessInfo.processInfo.environment["ALARIK_REGION"]
        setenv("ALARIK_REGION", region, 1)
        defer {
            if let original {
                setenv("ALARIK_REGION", original, 1)
            } else {
                unsetenv("ALARIK_REGION")
            }
        }
        try await body()
    }

    @Test("HeadBucket reports the configured region via x-amz-bucket-region")
    func testHeadBucketReportsRegion() async throws {
        let bucketName = "region-head-bucket"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let signed = signedHeaders(for: .HEAD, path: "/\(bucketName)")
            try await app.test(
                .HEAD, "/\(bucketName)",
                beforeRequest: { req in req.headers.add(contentsOf: signed) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "x-amz-bucket-region") == "us-east-1")
                })
        }
    }

    @Test("GetBucketLocation reports a non-default configured region as element text")
    func testGetBucketLocationNonDefaultRegion() async throws {
        let bucketName = "region-location-bucket"
        try await withRegion("eu-central-1") {
            try await withApp { app in
                let signer = AWSSigner(
                    credentials: StaticCredential(accessKeyId: accessKey, secretAccessKey: secretKey),
                    name: "s3", region: "eu-central-1")
                let createURL = URL(string: "http://\(host)/\(bucketName)")!
                let createSigned = signer.signHeaders(
                    url: createURL, method: .PUT, headers: HTTPHeaders([("host", host)]))
                try await app.test(
                    .PUT, "/\(bucketName)",
                    beforeRequest: { req in req.headers.add(contentsOf: createSigned) },
                    afterResponse: { res in #expect(res.status == .ok) })

                let locationURL = URL(string: "http://\(host)/\(bucketName)?location")!
                let locationSigned = signer.signHeaders(
                    url: locationURL, method: .GET, headers: HTTPHeaders([("host", host)]))
                try await app.test(
                    .GET, "/\(bucketName)?location",
                    beforeRequest: { req in req.headers.add(contentsOf: locationSigned) },
                    afterResponse: { res in
                        #expect(res.status == .ok)
                        let bodyString = res.body.string
                        #expect(bodyString.contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">eu-central-1</LocationConstraint>"))
                    })
            }
        }
    }

    @Test("re-creating your own bucket is idempotent (200) only in us-east-1; other regions return 409 BucketAlreadyOwnedByYou")
    func testCreateBucketIdempotencyIsRegionSpecific() async throws {
        let bucketName = "region-recreate-bucket"
        try await withRegion("eu-central-1") {
            try await withApp { app in
                let signer = AWSSigner(
                    credentials: StaticCredential(accessKeyId: accessKey, secretAccessKey: secretKey),
                    name: "s3", region: "eu-central-1")
                let url = URL(string: "http://\(host)/\(bucketName)")!

                let firstSigned = signer.signHeaders(
                    url: url, method: .PUT, headers: HTTPHeaders([("host", host)]))
                try await app.test(
                    .PUT, "/\(bucketName)",
                    beforeRequest: { req in req.headers.add(contentsOf: firstSigned) },
                    afterResponse: { res in #expect(res.status == .ok) })

                let secondSigned = signer.signHeaders(
                    url: url, method: .PUT, headers: HTTPHeaders([("host", host)]))
                try await app.test(
                    .PUT, "/\(bucketName)",
                    beforeRequest: { req in req.headers.add(contentsOf: secondSigned) },
                    afterResponse: { res async throws in
                        #expect(res.status == .conflict)
                        let body = res.body.string
                        #expect(body.contains("BucketAlreadyOwnedByYou"))
                    })
            }
        }
    }

    @Test("a request signed for the wrong region is rejected end-to-end")
    func testRequestWrongRegionRejectedEndToEnd() async throws {
        let bucketName = "region-mismatch-bucket"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Server is configured for the default region (us-east-1, ALARIK_REGION unset) -
            // sign this request for a different region entirely.
            let signer = AWSSigner(
                credentials: StaticCredential(accessKeyId: accessKey, secretAccessKey: secretKey),
                name: "s3", region: "ap-southeast-2")
            let url = URL(string: "http://\(host)/\(bucketName)")!
            let signed = signer.signHeaders(
                url: url, method: .HEAD, headers: HTTPHeaders([("host", host)]))

            try await app.test(
                .HEAD, "/\(bucketName)",
                beforeRequest: { req in req.headers.add(contentsOf: signed) },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
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

    private func enableVersioning(_ app: Application, bucketName: String) async throws {
        let body = """
            <?xml version="1.0" encoding="UTF-8"?>
            <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Enabled</Status>
            </VersioningConfiguration>
            """
        let bodyData = Data(body.utf8)
        let signed = signedHeaders(
            for: .PUT, path: "/\(bucketName)", query: "versioning", body: bodyData)

        try await app.test(
            .PUT, "/\(bucketName)?versioning",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: bodyData)
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

    @Test(
        "List Objects V1 - A prefix VALUE containing 'uploads' or 'versions' must not be misrouted to ListMultipartUploads/ListVersions"
    )
    func testListObjectsV1PrefixValueNotMisroutedAsSubresource() async throws {
        let bucketName = "test-list-prefix-not-subresource"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(
                app, bucketName: bucketName, key: "uploads/2024/photo.jpg", content: "photo")
            try await putObject(
                app, bucketName: bucketName, key: "schema-versions/v1.json", content: "{}")

            // The old substring-based check (`query.contains("uploads")`) matched this prefix
            // VALUE and misrouted the request to ListMultipartUploads, returning an empty
            // ListMultipartUploadsResult instead of the actual object listing.
            let uploadsQuery = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "prefix=uploads/2024/photo.jpg")
            try await app.test(
                .GET, "/\(bucketName)?prefix=uploads/2024/photo.jpg",
                beforeRequest: { req in req.headers.add(contentsOf: uploadsQuery) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let body = res.body.string
                    #expect(body.contains("<ListBucketResult"))
                    #expect(!body.contains("ListMultipartUploadsResult"))
                    #expect(body.contains("<Key>uploads/2024/photo.jpg</Key>"))
                })

            let versionsQuery = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "prefix=schema-versions/v1.json")
            try await app.test(
                .GET, "/\(bucketName)?prefix=schema-versions/v1.json",
                beforeRequest: { req in req.headers.add(contentsOf: versionsQuery) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let body = res.body.string
                    #expect(body.contains("<ListBucketResult"))
                    #expect(!body.contains("ListVersionsResult"))
                    #expect(body.contains("<Key>schema-versions/v1.json</Key>"))
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
            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/test.txt", body: Data(content.utf8))
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
            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/test.txt", body: Data(content.utf8))
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

    @Test("PUT Object with If-None-Match: * - Succeeds when the key doesn't exist yet")
    func testPutObjectIfNoneMatchStarSucceedsWhenAbsent() async throws {
        let bucketName = "test-put-if-none-match-absent"
        let content = "new content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/new.txt", body: Data(content.utf8),
                additionalHeaders: ["if-none-match": "*"])

            try await app.test(
                .PUT, "/\(bucketName)/new.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(string: content)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("PUT Object with If-None-Match: * - Blocks overwriting an existing key")
    func testPutObjectIfNoneMatchStarBlocksOverwrite() async throws {
        let bucketName = "test-put-if-none-match-exists"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "existing.txt", content: "v1")

            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/existing.txt", body: Data("v2".utf8),
                additionalHeaders: ["if-none-match": "*"])

            try await app.test(
                .PUT, "/\(bucketName)/existing.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(string: "v2")
                },
                afterResponse: { res in
                    #expect(res.status == .preconditionFailed)
                })
        }
    }

    @Test("PUT Object with If-None-Match: * - Blocks overwriting in a versioned bucket too")
    func testPutObjectIfNoneMatchStarBlocksOverwriteVersionedBucket() async throws {
        let bucketName = "test-put-if-none-match-versioned"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "existing.txt", content: "v1")

            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/existing.txt", body: Data("v2".utf8),
                additionalHeaders: ["if-none-match": "*"])

            try await app.test(
                .PUT, "/\(bucketName)/existing.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(string: "v2")
                },
                afterResponse: { res in
                    #expect(res.status == .preconditionFailed)
                })
        }
    }

    @Test("PUT Object with If-Match - Matching ETag allows the overwrite")
    func testPutObjectIfMatchSuccess() async throws {
        let bucketName = "test-put-if-match-success"
        let content = "v1"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let firstPut = signedHeaders(
                for: .PUT, path: "/\(bucketName)/test.txt", body: Data(content.utf8))
            var etag: String = ""
            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: firstPut)
                    req.body = ByteBuffer(string: content)
                },
                afterResponse: { res in
                    etag = res.headers.first(name: "ETag") ?? ""
                })

            let secondPut = signedHeaders(
                for: .PUT, path: "/\(bucketName)/test.txt", body: Data("v2".utf8),
                additionalHeaders: ["if-match": etag])

            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: secondPut)
                    req.body = ByteBuffer(string: "v2")
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("PUT Object with If-Match - Non-matching ETag fails with 412")
    func testPutObjectIfMatchFailure() async throws {
        let bucketName = "test-put-if-match-fail"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "test.txt", content: "v1")

            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/test.txt", body: Data("v2".utf8),
                additionalHeaders: ["if-match": "\"wrongetag\""])

            try await app.test(
                .PUT, "/\(bucketName)/test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(string: "v2")
                },
                afterResponse: { res in
                    #expect(res.status == .preconditionFailed)
                })
        }
    }

    @Test("PUT Object with If-Match - No existing object fails with 412")
    func testPutObjectIfMatchFailureNoExistingObject() async throws {
        let bucketName = "test-put-if-match-no-object"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/missing.txt", body: Data("v1".utf8),
                additionalHeaders: ["if-match": "*"])

            try await app.test(
                .PUT, "/\(bucketName)/missing.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(string: "v1")
                },
                afterResponse: { res in
                    #expect(res.status == .preconditionFailed)
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
            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/test.txt", body: Data(content.utf8))
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
                    "content-type": "application/json",
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

    @Test("Copy Object - Default directive carries user metadata and tags to the destination")
    func testCopyObjectDefaultDirectiveCarriesMetadataAndTags() async throws {
        let bucketName = "test-copy-carry-meta"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Upload with user metadata and inline tags
            let putData = Data("carry me".utf8)
            let putSigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/source.txt",
                body: putData,
                additionalHeaders: [
                    "content-type": "text/plain",
                    "x-amz-meta-origin": "the-source",
                    "x-amz-tagging": "team=storage",
                ]
            )
            try await app.test(
                .PUT, "/\(bucketName)/source.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: putData)
                },
                afterResponse: { res in #expect(res.status == .ok) })

            // Plain copy - no directive headers - must carry metadata + tags over (S3's
            // default COPY behavior, verified against the CopyObject API reference)
            let copySigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/copied.txt",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/source.txt"]
            )
            try await app.test(
                .PUT, "/\(bucketName)/copied.txt",
                beforeRequest: { req in req.headers.add(contentsOf: copySigned) },
                afterResponse: { res in #expect(res.status == .ok) })

            let headSigned = signedHeaders(for: .HEAD, path: "/\(bucketName)/copied.txt")
            try await app.test(
                .HEAD, "/\(bucketName)/copied.txt",
                beforeRequest: { req in req.headers.add(contentsOf: headSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "x-amz-meta-origin") == "the-source")
                    #expect(res.headers.first(name: "Content-Type") == "text/plain")
                })

            let tagSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/copied.txt", query: "tagging")
            try await app.test(
                .GET, "/\(bucketName)/copied.txt?tagging",
                beforeRequest: { req in req.headers.add(contentsOf: tagSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<Key>team</Key>"))
                    #expect(res.body.string.contains("<Value>storage</Value>"))
                })
        }
    }

    @Test("Copy Object - REPLACE directive takes metadata from the request, dropping the source's")
    func testCopyObjectReplaceDirectiveTakesRequestMetadata() async throws {
        let bucketName = "test-copy-replace-usermeta"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let putData = Data("replace me".utf8)
            let putSigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/source.txt",
                body: putData,
                additionalHeaders: ["x-amz-meta-origin": "the-source"]
            )
            try await app.test(
                .PUT, "/\(bucketName)/source.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: putData)
                },
                afterResponse: { res in #expect(res.status == .ok) })

            let copySigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/replaced.txt",
                additionalHeaders: [
                    "x-amz-copy-source": "/\(bucketName)/source.txt",
                    "x-amz-metadata-directive": "REPLACE",
                    "x-amz-meta-fresh": "brand-new",
                ]
            )
            try await app.test(
                .PUT, "/\(bucketName)/replaced.txt",
                beforeRequest: { req in req.headers.add(contentsOf: copySigned) },
                afterResponse: { res in #expect(res.status == .ok) })

            let headSigned = signedHeaders(for: .HEAD, path: "/\(bucketName)/replaced.txt")
            try await app.test(
                .HEAD, "/\(bucketName)/replaced.txt",
                beforeRequest: { req in req.headers.add(contentsOf: headSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "x-amz-meta-fresh") == "brand-new")
                    // Metadata is all-or-nothing per directive, never merged
                    #expect(res.headers.first(name: "x-amz-meta-origin") == nil)
                })
        }
    }

    @Test("Put Object - a 'folder/' marker key keeps its trailing slash and lists as a folder")
    func testFolderMarkerKeyKeepsTrailingSlash() async throws {
        let bucketName = "test-folder-marker-key"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            // What `aws s3api put-object --key docs/` (and the AWS console) send for a folder.
            try await putObject(app, bucketName: bucketName, key: "docs/", content: "")

            let headSigned = signedHeaders(for: .HEAD, path: "/\(bucketName)/docs/")
            try await app.test(
                .HEAD, "/\(bucketName)/docs/",
                beforeRequest: { req in req.headers.add(contentsOf: headSigned) },
                afterResponse: { res in #expect(res.status == .ok) })

            let listSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "list-type=2&delimiter=/")
            try await app.test(
                .GET, "/\(bucketName)?list-type=2&delimiter=/",
                beforeRequest: { req in req.headers.add(contentsOf: listSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<Prefix>docs/</Prefix>"))
                    #expect(!res.body.string.contains("<Key>docs</Key>"))
                })

            // Deleting the marker removes the folder entirely.
            let deleteSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)/docs/")
            try await app.test(
                .DELETE, "/\(bucketName)/docs/",
                beforeRequest: { req in req.headers.add(contentsOf: deleteSigned) },
                afterResponse: { res in #expect(res.status == .noContent) })
            try await app.test(
                .GET, "/\(bucketName)?list-type=2&delimiter=/",
                beforeRequest: { req in req.headers.add(contentsOf: listSigned) },
                afterResponse: { res in
                    #expect(!res.body.string.contains("docs"))
                })
        }
    }

    @Test("Delete Object - DELETE of 'folder/' removes a phantom empty folder from the listing")
    func testDeleteFolderKeyRemovesPhantomDirectory() async throws {
        let bucketName = "test-delete-phantom-folder"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "ghost/file.txt", content: "boo")
            // Leave the bare directory behind, as an erasure-coded delete used to (issue #25).
            try FileManager.default.removeItem(
                atPath: ObjectFileHandler.storagePath(for: bucketName, key: "ghost/file.txt"))

            func listing() async throws -> String {
                let signed = signedHeaders(
                    for: .GET, path: "/\(bucketName)", query: "list-type=2&delimiter=/")
                var body = ""
                try await app.test(
                    .GET, "/\(bucketName)?list-type=2&delimiter=/",
                    beforeRequest: { req in req.headers.add(contentsOf: signed) },
                    afterResponse: { res in
                        #expect(res.status == .ok)
                        body = res.body.string
                    })
                return body
            }
            #expect(try await listing().contains("<Prefix>ghost/</Prefix>"))

            // No `ghost/` marker object exists - S3 still answers 204, and the folder must go.
            let signed = signedHeaders(for: .DELETE, path: "/\(bucketName)/ghost/")
            try await app.test(
                .DELETE, "/\(bucketName)/ghost/",
                beforeRequest: { req in req.headers.add(contentsOf: signed) },
                afterResponse: { res in #expect(res.status == .noContent) })

            #expect(!(try await listing().contains("<Prefix>ghost/</Prefix>")))
        }
    }

    @Test("Delete Object - versionId 'null' removes a never-versioned key's plain object")
    func testDeleteNullVersionRemovesPlainObject() async throws {
        let bucketName = "test-delete-null-version"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "plain.txt", content: "bytes")

            // Versions listings report never-versioned objects as VersionId "null" (matching
            // S3), and clients like mc echo that id back in versioned deletes - which
            // must remove the plain object, not silently no-op.
            let deleteSigned = signedHeaders(
                for: .DELETE, path: "/\(bucketName)/plain.txt", query: "versionId=null")
            try await app.test(
                .DELETE, "/\(bucketName)/plain.txt?versionId=null",
                beforeRequest: { req in req.headers.add(contentsOf: deleteSigned) },
                afterResponse: { res in #expect(res.status == .noContent) })

            let headSigned = signedHeaders(for: .HEAD, path: "/\(bucketName)/plain.txt")
            try await app.test(
                .HEAD, "/\(bucketName)/plain.txt",
                beforeRequest: { req in req.headers.add(contentsOf: headSigned) },
                afterResponse: { res in #expect(res.status == .notFound) })
        }
    }

    @Test("Copy Object - With If-Match condition (matching)")
    func testCopyObjectIfMatchSuccess() async throws {
        let bucketName = "test-copy-if-match"
        let content = "Test content"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Upload source
            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/source.txt", body: Data(content.utf8))
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
                    "x-amz-copy-source-if-match": etag,
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
                    "x-amz-copy-source-if-match": "\"wrongetag\"",
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

    @Test("CreateMultipartUpload - POST /:bucket/:key?uploads")
    func testCreateMultipartUpload() async throws {
        let bucketName = "test-multipart-create"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let signed = signedHeaders(
                for: .POST,
                path: "/\(bucketName)/test-file.txt",
                query: "uploads"
            )

            try await app.test(
                .POST, "/\(bucketName)/test-file.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .xml)

                    let bodyString = res.body.string
                    #expect(bodyString.contains("<InitiateMultipartUploadResult"))
                    #expect(bodyString.contains("<Bucket>\(bucketName)</Bucket>"))
                    #expect(bodyString.contains("<Key>test-file.txt</Key>"))
                    #expect(bodyString.contains("<UploadId>"))
                })
        }
    }

    @Test("UploadPart - PUT /:bucket/:key?partNumber=X&uploadId=Y")
    func testUploadPart() async throws {
        let bucketName = "test-multipart-upload-part"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // First create the multipart upload
            let createSigned = signedHeaders(
                for: .POST,
                path: "/\(bucketName)/test-file.txt",
                query: "uploads"
            )

            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/test-file.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    // Extract uploadId from XML response
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })

            #expect(!uploadId.isEmpty)

            // Upload part 1
            let partData = Data("Hello, World!".utf8)
            let partSigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/test-file.txt",
                query: "partNumber=1&uploadId=\(uploadId)",
                body: partData
            )

            try await app.test(
                .PUT, "/\(bucketName)/test-file.txt?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: partSigned)
                    req.body = ByteBuffer(data: partData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "ETag") != nil)
                })
        }
    }

    @Test(
        "UploadPart - A non-numeric partNumber returns InvalidArgument, not a silent whole-object overwrite"
    )
    func testUploadPartRejectsNonNumericPartNumber() async throws {
        let bucketName = "test-multipart-bad-part-number"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // A real prior object at this key - the bug this guards against would silently
            // overwrite it with the "part" body below instead of rejecting the request, since
            // the routing decision (partNumber present at all) and the dispatch check (partNumber
            // must parse as Int) used to disagree.
            try await putObject(
                app, bucketName: bucketName, key: "test-file.txt", content: "original content")

            let createSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/test-file.txt", query: "uploads")
            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/test-file.txt?uploads",
                beforeRequest: { req in req.headers.add(contentsOf: createSigned) },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })
            #expect(!uploadId.isEmpty)

            let partData = Data("malicious overwrite attempt".utf8)
            let partSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/test-file.txt",
                query: "partNumber=abc&uploadId=\(uploadId)", body: partData)

            try await app.test(
                .PUT, "/\(bucketName)/test-file.txt?partNumber=abc&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: partSigned)
                    req.body = ByteBuffer(data: partData)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    #expect(res.body.string.contains("InvalidArgument"))
                })

            // The original object must be completely untouched.
            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/test-file.txt")
            try await app.test(
                .GET, "/\(bucketName)/test-file.txt",
                beforeRequest: { req in req.headers.add(contentsOf: getSigned) },
                afterResponse: { res in
                    #expect(res.body.string == "original content")
                })
        }
    }

    @Test("CompleteMultipartUpload - POST /:bucket/:key?uploadId=Y")
    func testCompleteMultipartUpload() async throws {
        let bucketName = "test-multipart-complete"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Create multipart upload
            let createSigned = signedHeaders(
                for: .POST,
                path: "/\(bucketName)/complete-test.txt",
                query: "uploads"
            )

            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/complete-test.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })

            // Upload two parts - every part but the last must meet the 5 MiB minimum
            let part1Data =
                Data(repeating: 0x61, count: MultipartFileHandler.minimumPartSize) + Data("Hello, ".utf8)
            let part2Data = Data("World!".utf8)

            var etag1: String = ""
            var etag2: String = ""

            let part1Signed = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/complete-test.txt",
                query: "partNumber=1&uploadId=\(uploadId)",
                body: part1Data
            )
            try await app.test(
                .PUT, "/\(bucketName)/complete-test.txt?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: part1Signed)
                    req.body = ByteBuffer(data: part1Data)
                },
                afterResponse: { res in
                    etag1 = res.headers.first(name: "ETag") ?? ""
                })

            let part2Signed = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/complete-test.txt",
                query: "partNumber=2&uploadId=\(uploadId)",
                body: part2Data
            )
            try await app.test(
                .PUT, "/\(bucketName)/complete-test.txt?partNumber=2&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: part2Signed)
                    req.body = ByteBuffer(data: part2Data)
                },
                afterResponse: { res in
                    etag2 = res.headers.first(name: "ETag") ?? ""
                })

            // Complete the upload
            let completeBody = """
                <CompleteMultipartUpload>
                    <Part><PartNumber>1</PartNumber><ETag>\(etag1)</ETag></Part>
                    <Part><PartNumber>2</PartNumber><ETag>\(etag2)</ETag></Part>
                </CompleteMultipartUpload>
                """
            let completeData = Data(completeBody.utf8)

            let completeSigned = signedHeaders(
                for: .POST,
                path: "/\(bucketName)/complete-test.txt",
                query: "uploadId=\(uploadId)",
                body: completeData
            )

            try await app.test(
                .POST, "/\(bucketName)/complete-test.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<CompleteMultipartUploadResult"))
                    #expect(bodyString.contains("<Bucket>\(bucketName)</Bucket>"))
                    #expect(bodyString.contains("<Key>complete-test.txt</Key>"))
                    #expect(bodyString.contains("<ETag>"))
                })

            // Verify the object exists and has correct content
            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/complete-test.txt")
            try await app.test(
                .GET, "/\(bucketName)/complete-test.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.readableBytes == part1Data.count + part2Data.count)
                    #expect(res.body.string.hasSuffix("Hello, World!"))
                })
        }
    }

    @Test("GET/HEAD ?partNumber serves one part of a multipart object with x-amz-mp-parts-count")
    func testGetObjectByPartNumber() async throws {
        let bucketName = "test-multipart-partnumber"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let key = "parts.bin"
            var uploadId = ""
            let createSigned = signedHeaders(for: .POST, path: "/\(bucketName)/\(key)", query: "uploads")
            try await app.test(
                .POST, "/\(bucketName)/\(key)?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let body = res.body.string
                    if let r = body.range(of: "<UploadId>"),
                        let e = body[r.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(body[r.upperBound..<e.lowerBound])
                    }
                })

            let part1 = Data(repeating: 0x41, count: MultipartFileHandler.minimumPartSize)
            let part2 = Data("second-part".utf8)
            var etags: [String] = []
            for (number, data) in [(1, part1), (2, part2)] {
                let signed = signedHeaders(
                    for: .PUT, path: "/\(bucketName)/\(key)",
                    query: "partNumber=\(number)&uploadId=\(uploadId)", body: data)
                try await app.test(
                    .PUT, "/\(bucketName)/\(key)?partNumber=\(number)&uploadId=\(uploadId)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed)
                        req.body = ByteBuffer(data: data)
                    },
                    afterResponse: { res in
                        etags.append(res.headers.first(name: "ETag") ?? "")
                    })
            }
            let completeBody = Data(
                """
                <CompleteMultipartUpload>
                    <Part><PartNumber>1</PartNumber><ETag>\(etags[0])</ETag></Part>
                    <Part><PartNumber>2</PartNumber><ETag>\(etags[1])</ETag></Part>
                </CompleteMultipartUpload>
                """.utf8)
            let completeSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/\(key)", query: "uploadId=\(uploadId)",
                body: completeBody)
            try await app.test(
                .POST, "/\(bucketName)/\(key)?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeBody)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            // Part 2 alone, as a 206 with the part's range and the total part count.
            let getPart2 = signedHeaders(for: .GET, path: "/\(bucketName)/\(key)", query: "partNumber=2")
            try await app.test(
                .GET, "/\(bucketName)/\(key)?partNumber=2",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getPart2)
                },
                afterResponse: { res in
                    #expect(res.status == .partialContent)
                    #expect(res.body.string == "second-part")
                    #expect(res.headers.first(name: "x-amz-mp-parts-count") == "2")
                    #expect(
                        res.headers.first(name: "Content-Range")
                            == "bytes \(part1.count)-\(part1.count + part2.count - 1)/\(part1.count + part2.count)")
                })

            // HEAD reports the part's length and the count without a body.
            let headPart1 = signedHeaders(for: .HEAD, path: "/\(bucketName)/\(key)", query: "partNumber=1")
            try await app.test(
                .HEAD, "/\(bucketName)/\(key)?partNumber=1",
                beforeRequest: { req in
                    req.headers.add(contentsOf: headPart1)
                },
                afterResponse: { res in
                    #expect(res.status == .partialContent)
                    #expect(res.headers.first(name: .contentLength) == String(part1.count))
                    #expect(res.headers.first(name: "x-amz-mp-parts-count") == "2")
                })

            // Beyond the last part is 416 InvalidPartNumber; combined with Range is 400.
            let getPart3 = signedHeaders(for: .GET, path: "/\(bucketName)/\(key)", query: "partNumber=3")
            try await app.test(
                .GET, "/\(bucketName)/\(key)?partNumber=3",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getPart3)
                },
                afterResponse: { res in
                    #expect(res.status == .rangeNotSatisfiable)
                    #expect(res.body.string.contains("<Code>InvalidPartNumber</Code>"))
                })
            let withRange = signedHeaders(
                for: .GET, path: "/\(bucketName)/\(key)", query: "partNumber=1",
                additionalHeaders: ["Range": "bytes=0-1"])
            try await app.test(
                .GET, "/\(bucketName)/\(key)?partNumber=1",
                beforeRequest: { req in
                    req.headers.add(contentsOf: withRange)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                })

            // A single-part object is part 1 of 1.
            try await putObject(app, bucketName: bucketName, key: "single.txt", content: "solo")
            let single = signedHeaders(for: .GET, path: "/\(bucketName)/single.txt", query: "partNumber=1")
            try await app.test(
                .GET, "/\(bucketName)/single.txt?partNumber=1",
                beforeRequest: { req in
                    req.headers.add(contentsOf: single)
                },
                afterResponse: { res in
                    #expect(res.status == .partialContent)
                    #expect(res.body.string == "solo")
                    #expect(res.headers.first(name: "x-amz-mp-parts-count") == "1")
                })
        }
    }

    @Test("AbortMultipartUpload - DELETE /:bucket/:key?uploadId=Y")
    func testAbortMultipartUpload() async throws {
        let bucketName = "test-multipart-abort"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Create multipart upload
            let createSigned = signedHeaders(
                for: .POST,
                path: "/\(bucketName)/abort-test.txt",
                query: "uploads"
            )

            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/abort-test.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })

            // Upload a part
            let partData = Data("test data".utf8)
            let partSigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/abort-test.txt",
                query: "partNumber=1&uploadId=\(uploadId)",
                body: partData
            )
            try await app.test(
                .PUT, "/\(bucketName)/abort-test.txt?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: partSigned)
                    req.body = ByteBuffer(data: partData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            // Abort the upload
            let abortSigned = signedHeaders(
                for: .DELETE,
                path: "/\(bucketName)/abort-test.txt",
                query: "uploadId=\(uploadId)"
            )

            try await app.test(
                .DELETE, "/\(bucketName)/abort-test.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: abortSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            // Trying to abort again should fail
            try await app.test(
                .DELETE, "/\(bucketName)/abort-test.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: abortSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("ListParts - GET /:bucket/:key?uploadId=Y")
    func testListParts() async throws {
        let bucketName = "test-multipart-list-parts"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Create multipart upload
            let createSigned = signedHeaders(
                for: .POST,
                path: "/\(bucketName)/list-parts-test.txt",
                query: "uploads"
            )

            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/list-parts-test.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })

            // Upload 3 parts
            for i in 1...3 {
                let partData = Data("part \(i) data".utf8)
                let partSigned = signedHeaders(
                    for: .PUT,
                    path: "/\(bucketName)/list-parts-test.txt",
                    query: "partNumber=\(i)&uploadId=\(uploadId)",
                    body: partData
                )
                try await app.test(
                    .PUT, "/\(bucketName)/list-parts-test.txt?partNumber=\(i)&uploadId=\(uploadId)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: partSigned)
                        req.body = ByteBuffer(data: partData)
                    },
                    afterResponse: { res in
                        #expect(res.status == .ok)
                    })
            }

            // List parts
            let listSigned = signedHeaders(
                for: .GET,
                path: "/\(bucketName)/list-parts-test.txt",
                query: "uploadId=\(uploadId)"
            )

            try await app.test(
                .GET, "/\(bucketName)/list-parts-test.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: listSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<ListPartsResult"))
                    #expect(bodyString.contains("<Bucket>\(bucketName)</Bucket>"))
                    #expect(bodyString.contains("<UploadId>\(uploadId)</UploadId>"))
                    #expect(bodyString.contains("<PartNumber>1</PartNumber>"))
                    #expect(bodyString.contains("<PartNumber>2</PartNumber>"))
                    #expect(bodyString.contains("<PartNumber>3</PartNumber>"))
                })
        }
    }

    @Test("ListMultipartUploads - GET /:bucket?uploads")
    func testListMultipartUploads() async throws {
        let bucketName = "test-multipart-list-uploads"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Create multiple multipart uploads
            for i in 1...3 {
                let createSigned = signedHeaders(
                    for: .POST,
                    path: "/\(bucketName)/file\(i).txt",
                    query: "uploads"
                )
                try await app.test(
                    .POST, "/\(bucketName)/file\(i).txt?uploads",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: createSigned)
                    },
                    afterResponse: { res in
                        #expect(res.status == .ok)
                    })
            }

            // List multipart uploads
            let listSigned = signedHeaders(
                for: .GET,
                path: "/\(bucketName)",
                query: "uploads"
            )

            try await app.test(
                .GET, "/\(bucketName)?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: listSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<ListMultipartUploadsResult"))
                    #expect(bodyString.contains("<Bucket>\(bucketName)</Bucket>"))
                    #expect(bodyString.contains("<Key>file1.txt</Key>"))
                    #expect(bodyString.contains("<Key>file2.txt</Key>"))
                    #expect(bodyString.contains("<Key>file3.txt</Key>"))
                    #expect(bodyString.contains("<UploadId>"))
                })
        }
    }

    @Test(
        "ListMultipartUploads - A client-supplied prefix containing XML special characters is escaped, never left to break the response"
    )
    func testListMultipartUploadsEscapesPrefix() async throws {
        let bucketName = "test-multipart-xml-escape"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let maliciousPrefix = "<x>&\"'"
            // `.urlQueryAllowed` leaves `&` (and other query-delimiter characters) unescaped,
            // which would prematurely split this manually-built query string - percent-encode
            // everything except alphanumerics to keep the whole value intact as one parameter.
            let encodedPrefix =
                maliciousPrefix.addingPercentEncoding(withAllowedCharacters: .alphanumerics)
                ?? maliciousPrefix
            let query = "uploads&prefix=\(encodedPrefix)"

            let listSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: query)

            try await app.test(
                .GET, "/\(bucketName)?\(query)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: listSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    // The raw value must never appear literally in the XML body - it would
                    // otherwise break out of the <Prefix> element entirely (or, depending on
                    // the client's XML parser, be interpreted as markup rather than data).
                    #expect(!bodyString.contains(maliciousPrefix))
                    #expect(
                        bodyString.contains(
                            "<Prefix>&lt;x&gt;&amp;&quot;&apos;</Prefix>"))
                })
        }
    }

    @Test("UploadPart fails for non-existent upload")
    func testUploadPartNonExistentUpload() async throws {
        let bucketName = "test-multipart-nonexistent"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let partData = Data("test".utf8)
            let partSigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/test.txt",
                query: "partNumber=1&uploadId=nonexistent",
                body: partData
            )

            try await app.test(
                .PUT, "/\(bucketName)/test.txt?partNumber=1&uploadId=nonexistent",
                beforeRequest: { req in
                    req.headers.add(contentsOf: partSigned)
                    req.body = ByteBuffer(data: partData)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Code>NoSuchUpload</Code>"))
                })
        }
    }

    @Test("CompleteMultipartUpload rejects a non-final part under 5 MiB with EntityTooSmall, not InvalidPart")
    func testCompleteRejectsSmallNonFinalPart() async throws {
        let bucketName = "test-multipart-too-small"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            var uploadId = ""
            let createSigned = signedHeaders(for: .POST, path: "/\(bucketName)/small.bin", query: "uploads")
            try await app.test(
                .POST, "/\(bucketName)/small.bin?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let body = res.body.string
                    if let r = body.range(of: "<UploadId>"),
                        let e = body[r.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(body[r.upperBound..<e.lowerBound])
                    }
                })
            var etags: [String] = []
            for (number, text) in [(1, "tiny"), (2, "")] {
                let data = Data(text.utf8)
                let signed = signedHeaders(
                    for: .PUT, path: "/\(bucketName)/small.bin",
                    query: "partNumber=\(number)&uploadId=\(uploadId)", body: data)
                try await app.test(
                    .PUT, "/\(bucketName)/small.bin?partNumber=\(number)&uploadId=\(uploadId)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed)
                        req.body = ByteBuffer(data: data)
                    },
                    afterResponse: { res in
                        // A zero-byte part is accepted at upload time, like S3.
                        #expect(res.status == .ok)
                        etags.append(res.headers.first(name: "ETag") ?? "")
                    })
            }
            let completeBody = Data(
                """
                <CompleteMultipartUpload>
                    <Part><PartNumber>1</PartNumber><ETag>\(etags[0])</ETag></Part>
                    <Part><PartNumber>2</PartNumber><ETag>\(etags[1])</ETag></Part>
                </CompleteMultipartUpload>
                """.utf8)
            let completeSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/small.bin", query: "uploadId=\(uploadId)",
                body: completeBody)
            try await app.test(
                .POST, "/\(bucketName)/small.bin?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeBody)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    #expect(res.body.string.contains("<Code>EntityTooSmall</Code>"))
                })
        }
    }

    @Test("UploadPartCopy - Copies a whole source object into a part")
    func testUploadPartCopyBasic() async throws {
        let bucketName = "test-upload-part-copy-basic"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(
                app, bucketName: bucketName, key: "source.txt",
                content: String(repeating: "a", count: MultipartFileHandler.minimumPartSize) + "Hello, ")

            let createSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/dest.txt", query: "uploads")
            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/dest.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })
            #expect(!uploadId.isEmpty)

            // Part 1: copied from source.txt
            let copySigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/dest.txt",
                query: "partNumber=1&uploadId=\(uploadId)",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/source.txt"]
            )
            try await app.test(
                .PUT, "/\(bucketName)/dest.txt?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<CopyPartResult"))
                    #expect(bodyString.contains("<ETag>"))
                    #expect(bodyString.contains("<LastModified>"))
                })

            // Part 2: regular uploaded data
            let part2Data = Data("World!".utf8)
            let part2Signed = signedHeaders(
                for: .PUT, path: "/\(bucketName)/dest.txt",
                query: "partNumber=2&uploadId=\(uploadId)", body: part2Data)
            var etag1: String = ""
            var etag2: String = ""
            try await app.test(
                .PUT, "/\(bucketName)/dest.txt?partNumber=2&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: part2Signed)
                    req.body = ByteBuffer(data: part2Data)
                },
                afterResponse: { res in
                    etag2 = res.headers.first(name: "ETag") ?? ""
                })

            // Need part 1's ETag for completion - fetch via ListParts
            let listSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/dest.txt", query: "uploadId=\(uploadId)")
            try await app.test(
                .GET, "/\(bucketName)/dest.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: listSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<ETag>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</ETag>")
                    {
                        etag1 = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })
            #expect(!etag1.isEmpty)

            let completeBody = """
                <CompleteMultipartUpload>
                    <Part><PartNumber>1</PartNumber><ETag>\(etag1)</ETag></Part>
                    <Part><PartNumber>2</PartNumber><ETag>\(etag2)</ETag></Part>
                </CompleteMultipartUpload>
                """
            let completeData = Data(completeBody.utf8)
            let completeSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/dest.txt", query: "uploadId=\(uploadId)",
                body: completeData)
            try await app.test(
                .POST, "/\(bucketName)/dest.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/dest.txt")
            try await app.test(
                .GET, "/\(bucketName)/dest.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.hasSuffix("Hello, World!"))
                    #expect(res.body.readableBytes == MultipartFileHandler.minimumPartSize + 13)
                })
        }
    }

    @Test("UploadPartCopy - Copies a byte range via x-amz-copy-source-range")
    func testUploadPartCopyWithRange() async throws {
        let bucketName = "test-upload-part-copy-range"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(
                app, bucketName: bucketName, key: "source.txt", content: "0123456789ABCDEF")

            let createSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/dest.txt", query: "uploads")
            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/dest.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })

            // Copy only bytes 0-9 ("0123456789") as part 1
            let copySigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/dest.txt",
                query: "partNumber=1&uploadId=\(uploadId)",
                additionalHeaders: [
                    "x-amz-copy-source": "/\(bucketName)/source.txt",
                    "x-amz-copy-source-range": "bytes=0-9",
                ]
            )
            try await app.test(
                .PUT, "/\(bucketName)/dest.txt?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            var etag1: String = ""
            let listSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/dest.txt", query: "uploadId=\(uploadId)")
            try await app.test(
                .GET, "/\(bucketName)/dest.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: listSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<ETag>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</ETag>")
                    {
                        etag1 = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                    // The copied part should only be 10 bytes
                    #expect(bodyString.contains("<Size>10</Size>"))
                })

            let completeBody = """
                <CompleteMultipartUpload>
                    <Part><PartNumber>1</PartNumber><ETag>\(etag1)</ETag></Part>
                </CompleteMultipartUpload>
                """
            let completeData = Data(completeBody.utf8)
            let completeSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/dest.txt", query: "uploadId=\(uploadId)",
                body: completeData)
            try await app.test(
                .POST, "/\(bucketName)/dest.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/dest.txt")
            try await app.test(
                .GET, "/\(bucketName)/dest.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "0123456789")
                })
        }
    }

    @Test("UploadPartCopy - Non-existent source key fails with NoSuchKey")
    func testUploadPartCopyNonExistentSource() async throws {
        let bucketName = "test-upload-part-copy-no-source"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let createSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/dest.txt", query: "uploads")
            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/dest.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })

            let copySigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/dest.txt",
                query: "partNumber=1&uploadId=\(uploadId)",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/does-not-exist.txt"]
            )
            try await app.test(
                .PUT, "/\(bucketName)/dest.txt?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    #expect(res.body.string.contains("<Code>NoSuchKey</Code>"))
                })
        }
    }

    @Test("UploadPartCopy - Non-existent upload fails with NoSuchUpload")
    func testUploadPartCopyNonExistentUpload() async throws {
        let bucketName = "test-upload-part-copy-no-upload"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "source.txt", content: "data")

            let copySigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/dest.txt",
                query: "partNumber=1&uploadId=nonexistent",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/source.txt"]
            )
            try await app.test(
                .PUT, "/\(bucketName)/dest.txt?partNumber=1&uploadId=nonexistent",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    #expect(res.body.string.contains("<Code>NoSuchUpload</Code>"))
                })
        }
    }

    @Test("UploadPartCopy - Failing If-Match condition rejects the copy")
    func testUploadPartCopyIfMatchFailure() async throws {
        let bucketName = "test-upload-part-copy-if-match"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "source.txt", content: "data")

            let createSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/dest.txt", query: "uploads")
            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/dest.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })

            let copySigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/dest.txt",
                query: "partNumber=1&uploadId=\(uploadId)",
                additionalHeaders: [
                    "x-amz-copy-source": "/\(bucketName)/source.txt",
                    "x-amz-copy-source-if-match": "\"wrongetag\"",
                ]
            )
            try await app.test(
                .PUT, "/\(bucketName)/dest.txt?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .preconditionFailed)
                })

            // ListParts should show no parts since the copy never wrote one
            let listSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/dest.txt", query: "uploadId=\(uploadId)")
            try await app.test(
                .GET, "/\(bucketName)/dest.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: listSigned)
                },
                afterResponse: { res in
                    #expect(res.body.string.contains("<IsTruncated>false</IsTruncated>"))
                    #expect(!res.body.string.contains("<Part>"))
                })
        }
    }

    @Test("UploadPartCopy - Copies a part from a different bucket")
    func testUploadPartCopyAcrossBuckets() async throws {
        let sourceBucket = "test-upload-part-copy-cross-source"
        let destBucket = "test-upload-part-copy-cross-dest"
        try await withApp { app in
            try await createBucket(app, bucketName: sourceBucket)
            try await createBucket(app, bucketName: destBucket)
            try await putObject(
                app, bucketName: sourceBucket, key: "source.txt", content: "cross-bucket data")

            let createSigned = signedHeaders(
                for: .POST, path: "/\(destBucket)/dest.txt", query: "uploads")
            var uploadId: String = ""
            try await app.test(
                .POST, "/\(destBucket)/dest.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })

            var etag1: String = ""
            let copySigned = signedHeaders(
                for: .PUT, path: "/\(destBucket)/dest.txt",
                query: "partNumber=1&uploadId=\(uploadId)",
                additionalHeaders: ["x-amz-copy-source": "/\(sourceBucket)/source.txt"]
            )
            try await app.test(
                .PUT, "/\(destBucket)/dest.txt?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    if let range = res.body.string.range(of: "<ETag>"),
                        let endRange = res.body.string[range.upperBound...].range(
                            of: "</ETag>")
                    {
                        etag1 = String(res.body.string[range.upperBound..<endRange.lowerBound])
                    }
                })

            let completeBody = """
                <CompleteMultipartUpload>
                    <Part><PartNumber>1</PartNumber><ETag>\(etag1)</ETag></Part>
                </CompleteMultipartUpload>
                """
            let completeData = Data(completeBody.utf8)
            let completeSigned = signedHeaders(
                for: .POST, path: "/\(destBucket)/dest.txt", query: "uploadId=\(uploadId)",
                body: completeData)
            try await app.test(
                .POST, "/\(destBucket)/dest.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(destBucket)/dest.txt")
            try await app.test(
                .GET, "/\(destBucket)/dest.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "cross-bucket data")
                })
        }
    }

    @Test("CompleteMultipartUpload fails with duplicate part numbers")
    func testCompleteMultipartUploadDuplicateParts() async throws {
        let bucketName = "test-multipart-duplicate-parts"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Create multipart upload
            let createSigned = signedHeaders(
                for: .POST,
                path: "/\(bucketName)/duplicate-parts.txt",
                query: "uploads"
            )

            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/duplicate-parts.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })

            // Upload part 1
            let partData = Data("part 1".utf8)
            var etag1: String = ""

            let partSigned = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/duplicate-parts.txt",
                query: "partNumber=1&uploadId=\(uploadId)",
                body: partData
            )
            try await app.test(
                .PUT, "/\(bucketName)/duplicate-parts.txt?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: partSigned)
                    req.body = ByteBuffer(data: partData)
                },
                afterResponse: { res in
                    etag1 = res.headers.first(name: "ETag") ?? ""
                })

            // Try to complete with duplicate part 1 (should fail)
            let completeBody = """
                <CompleteMultipartUpload>
                    <Part><PartNumber>1</PartNumber><ETag>\(etag1)</ETag></Part>
                    <Part><PartNumber>1</PartNumber><ETag>\(etag1)</ETag></Part>
                </CompleteMultipartUpload>
                """
            let completeData = Data(completeBody.utf8)

            let completeSigned = signedHeaders(
                for: .POST,
                path: "/\(bucketName)/duplicate-parts.txt",
                query: "uploadId=\(uploadId)",
                body: completeData
            )

            try await app.test(
                .POST, "/\(bucketName)/duplicate-parts.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeData)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("CompleteMultipartUpload succeeds with non-sequential parts")
    func testCompleteMultipartUploadNonSequential() async throws {
        let bucketName = "test-multipart-nonseq"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Create multipart upload
            let createSigned = signedHeaders(
                for: .POST,
                path: "/\(bucketName)/nonseq.txt",
                query: "uploads"
            )

            var uploadId: String = ""
            try await app.test(
                .POST, "/\(bucketName)/nonseq.txt?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let bodyString = res.body.string
                    if let range = bodyString.range(of: "<UploadId>"),
                        let endRange = bodyString[range.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(bodyString[range.upperBound..<endRange.lowerBound])
                    }
                })

            // Upload parts 1 and 3 (skipping 2) - S3 allows this
            let part1Data = Data(repeating: 0x31, count: MultipartFileHandler.minimumPartSize)
            let part3Data = Data("part 3".utf8)
            var etag1: String = ""
            var etag3: String = ""

            let part1Signed = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/nonseq.txt",
                query: "partNumber=1&uploadId=\(uploadId)",
                body: part1Data
            )
            try await app.test(
                .PUT, "/\(bucketName)/nonseq.txt?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: part1Signed)
                    req.body = ByteBuffer(data: part1Data)
                },
                afterResponse: { res in
                    etag1 = res.headers.first(name: "ETag") ?? ""
                })

            let part3Signed = signedHeaders(
                for: .PUT,
                path: "/\(bucketName)/nonseq.txt",
                query: "partNumber=3&uploadId=\(uploadId)",
                body: part3Data
            )
            try await app.test(
                .PUT, "/\(bucketName)/nonseq.txt?partNumber=3&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: part3Signed)
                    req.body = ByteBuffer(data: part3Data)
                },
                afterResponse: { res in
                    etag3 = res.headers.first(name: "ETag") ?? ""
                })

            // Complete with parts 1 and 3 - should succeed
            let completeBody = """
                <CompleteMultipartUpload>
                    <Part><PartNumber>1</PartNumber><ETag>\(etag1)</ETag></Part>
                    <Part><PartNumber>3</PartNumber><ETag>\(etag3)</ETag></Part>
                </CompleteMultipartUpload>
                """
            let completeData = Data(completeBody.utf8)

            let completeSigned = signedHeaders(
                for: .POST,
                path: "/\(bucketName)/nonseq.txt",
                query: "uploadId=\(uploadId)",
                body: completeData
            )

            try await app.test(
                .POST, "/\(bucketName)/nonseq.txt?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<ETag>"))
                    // ETag should end with -2 (2 parts)
                    #expect(bodyString.contains("-2"))
                })
        }
    }

    @Test("DeleteObjects - Results stay in request order with parallel deletes")
    func testDeleteObjectsOrderedWithManyKeys() async throws {
        let bucketName = "test-delete-objects-ordered"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            // More keys than the delete concurrency, so completion order differs from request order.
            let keys = (0..<25).map { "obj-\(String(format: "%02d", $0)).txt" }
            for key in keys {
                try await putObject(app, bucketName: bucketName, key: key, content: key)
            }
            // Request them in reverse, with an invalid empty key in the middle.
            var requested = Array(keys.reversed())
            requested.insert("", at: 10)
            let body = Data(
                ("<Delete>" + requested.map { "<Object><Key>\($0)</Key></Object>" }.joined() + "</Delete>").utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: body,
                additionalHeaders: ["Content-MD5": Data(Insecure.MD5.hash(data: body)).base64EncodedString()])
            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: body)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    // Pretty-printed XML: match structurally, not on exact whitespace.
                    let xml = res.body.string.replacingOccurrences(
                        of: ">\\s+<", with: "><", options: .regularExpression)
                    let deletedKeys = xml.components(separatedBy: "<Deleted><Key>").dropFirst()
                        .compactMap { $0.components(separatedBy: "</Key>").first }
                    #expect(deletedKeys == Array(keys.reversed()))
                    #expect(xml.contains("<Code>InvalidArgument</Code>"))
                })
            for key in keys {
                let head = signedHeaders(for: .HEAD, path: "/\(bucketName)/\(key)")
                try await app.test(
                    .HEAD, "/\(bucketName)/\(key)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: head)
                    },
                    afterResponse: { res in
                        #expect(res.status == .notFound)
                    })
            }
        }
    }

    @Test("DeleteObjects - Multi-object delete removes all listed keys")
    func testDeleteObjectsBasic() async throws {
        let bucketName = "test-delete-objects-basic"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "a.txt", content: "A")
            try await putObject(app, bucketName: bucketName, key: "b.txt", content: "B")

            let deleteBody = """
                <Delete>
                    <Object><Key>a.txt</Key></Object>
                    <Object><Key>b.txt</Key></Object>
                </Delete>
                """
            let bodyData = Data(deleteBody.utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: bodyData)

            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .xml)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<DeleteResult"))
                    #expect(bodyString.contains("<Key>a.txt</Key>"))
                    #expect(bodyString.contains("<Key>b.txt</Key>"))
                    #expect(!bodyString.contains("<Error>"))
                })

            for key in ["a.txt", "b.txt"] {
                let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/\(key)")
                try await app.test(
                    .GET, "/\(bucketName)/\(key)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: getSigned)
                    },
                    afterResponse: { res in
                        #expect(res.status == .notFound)
                    })
            }
        }
    }

    @Test("DeleteObjects - Quiet mode omits Deleted entries")
    func testDeleteObjectsQuietMode() async throws {
        let bucketName = "test-delete-objects-quiet"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "a.txt", content: "A")

            let deleteBody = """
                <Delete>
                    <Quiet>true</Quiet>
                    <Object><Key>a.txt</Key></Object>
                </Delete>
                """
            let bodyData = Data(deleteBody.utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: bodyData)

            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(!bodyString.contains("<Deleted>"))
                    #expect(!bodyString.contains("a.txt"))
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/a.txt")
            try await app.test(
                .GET, "/\(bucketName)/a.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("DeleteObjects - Non-existent keys are treated as successfully deleted")
    func testDeleteObjectsNonExistentKeysSucceed() async throws {
        let bucketName = "test-delete-objects-nonexistent"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let deleteBody = """
                <Delete>
                    <Object><Key>does-not-exist.txt</Key></Object>
                </Delete>
                """
            let bodyData = Data(deleteBody.utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: bodyData)

            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Key>does-not-exist.txt</Key>"))
                    #expect(!bodyString.contains("<Error>"))
                })
        }
    }

    @Test("DeleteObjects - Creates delete markers when versioning enabled")
    func testDeleteObjectsCreatesDeleteMarkers() async throws {
        let bucketName = "test-delete-objects-versioning"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            try await putObject(app, bucketName: bucketName, key: "a.txt", content: "A")

            let deleteBody = """
                <Delete>
                    <Object><Key>a.txt</Key></Object>
                </Delete>
                """
            let bodyData = Data(deleteBody.utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: bodyData)

            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Key>a.txt</Key>"))
                    #expect(bodyString.contains("<DeleteMarker>true</DeleteMarker>"))
                    #expect(bodyString.contains("<DeleteMarkerVersionId>"))
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/a.txt")
            try await app.test(
                .GET, "/\(bucketName)/a.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("DeleteObjects - Permanent delete of a specific version via VersionId")
    func testDeleteObjectsPermanentVersionDelete() async throws {
        let bucketName = "test-delete-objects-version-id"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            var versionId1: String = ""
            let put1Data = Data("Version 1".utf8)
            let put1Signed = signedHeaders(for: .PUT, path: "/\(bucketName)/a.txt", body: put1Data)
            try await app.test(
                .PUT, "/\(bucketName)/a.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: put1Signed)
                    req.body = ByteBuffer(data: put1Data)
                },
                afterResponse: { res in
                    versionId1 = res.headers.first(name: "x-amz-version-id") ?? ""
                })
            #expect(!versionId1.isEmpty)

            let put2Data = Data("Version 2".utf8)
            let put2Signed = signedHeaders(for: .PUT, path: "/\(bucketName)/a.txt", body: put2Data)
            try await app.test(
                .PUT, "/\(bucketName)/a.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: put2Signed)
                    req.body = ByteBuffer(data: put2Data)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            let deleteBody = """
                <Delete>
                    <Object><Key>a.txt</Key><VersionId>\(versionId1)</VersionId></Object>
                </Delete>
                """
            let bodyData = Data(deleteBody.utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: bodyData)

            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<VersionId>\(versionId1)</VersionId>"))
                    #expect(!bodyString.contains("<DeleteMarker>true</DeleteMarker>"))
                })

            let getV1Signed = signedHeaders(
                for: .GET, path: "/\(bucketName)/a.txt", query: "versionId=\(versionId1)")
            try await app.test(
                .GET, "/\(bucketName)/a.txt?versionId=\(versionId1)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getV1Signed)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/a.txt")
            try await app.test(
                .GET, "/\(bucketName)/a.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "Version 2")
                })
        }
    }

    @Test("DeleteObjects - Malformed body without Object entries fails")
    func testDeleteObjectsMalformedBody() async throws {
        let bucketName = "test-delete-objects-malformed"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let deleteBody = "<Delete></Delete>"
            let bodyData = Data(deleteBody.utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: bodyData)

            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    #expect(res.body.string.contains("<Code>MalformedXML</Code>"))
                })
        }
    }

    @Test("DeleteObjects - Empty Key element produces an Error entry, not a request failure")
    func testDeleteObjectsEmptyKeyProducesError() async throws {
        let bucketName = "test-delete-objects-empty-key"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let deleteBody = """
                <Delete>
                    <Object><Key></Key></Object>
                </Delete>
                """
            let bodyData = Data(deleteBody.utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: bodyData)

            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let bodyString = res.body.string
                    #expect(bodyString.contains("<Error>"))
                    #expect(bodyString.contains("<Code>InvalidArgument</Code>"))
                })
        }
    }

    @Test("DeleteObjects - More than 1000 keys is rejected")
    func testDeleteObjectsExceedsMaxKeys() async throws {
        let bucketName = "test-delete-objects-too-many"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            var deleteBody = "<Delete>"
            for i in 0..<1001 {
                deleteBody += "<Object><Key>key-\(i).txt</Key></Object>"
            }
            deleteBody += "</Delete>"
            let bodyData = Data(deleteBody.utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: bodyData)

            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    #expect(res.body.string.contains("<Code>MalformedXML</Code>"))
                })
        }
    }

    @Test("DeleteObjects - Quiet mode still reports Error entries")
    func testDeleteObjectsQuietModeStillReportsErrors() async throws {
        let bucketName = "test-delete-objects-quiet-errors"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let deleteBody = """
                <Delete>
                    <Quiet>true</Quiet>
                    <Object><Key></Key></Object>
                </Delete>
                """
            let bodyData = Data(deleteBody.utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: bodyData)

            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<Error>"))
                })
        }
    }

    @Test("DeleteObjects - Non-existent bucket fails")
    func testDeleteObjectsNonExistentBucket() async throws {
        let bucketName = "test-delete-objects-no-bucket"
        try await withApp { app in
            let deleteBody = """
                <Delete>
                    <Object><Key>a.txt</Key></Object>
                </Delete>
                """
            let bodyData = Data(deleteBody.utf8)
            let signed = signedHeaders(
                for: .POST, path: "/\(bucketName)", query: "delete", body: bodyData)

            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("DeleteObjects - Unauthorized request without a signature is rejected")
    func testDeleteObjectsUnauthorized() async throws {
        let bucketName = "test-delete-objects-unauth"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "a.txt", content: "A")

            let deleteBody = """
                <Delete>
                    <Object><Key>a.txt</Key></Object>
                </Delete>
                """
            let bodyData = Data(deleteBody.utf8)

            // No Authorization header/signature attached
            try await app.test(
                .POST, "/\(bucketName)?delete",
                beforeRequest: { req in
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .forbidden || res.status == .badRequest)
                })

            // Object must still exist - the delete must not have gone through
            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/a.txt")
            try await app.test(
                .GET, "/\(bucketName)/a.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "A")
                })
        }
    }

    @Test("List Objects - negative max-keys is InvalidArgument and oversized max-keys is capped, never a trap")
    func testListLimitsValidated() async throws {
        let bucketName = "list-limit-clamp-bucket"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "a.txt", content: "a")

            for query in ["max-keys=-1", "list-type=2&max-keys=-5", "versions&max-keys=-1"] {
                let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: query)
                try await app.test(
                    .GET, "/\(bucketName)?\(query)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed)
                    },
                    afterResponse: { res in
                        #expect(res.status == .badRequest, "\(query)")
                        #expect(res.body.string.contains("<Code>InvalidArgument</Code>"), "\(query)")
                    })
            }

            let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: "max-keys=99999")
            try await app.test(
                .GET, "/\(bucketName)?max-keys=99999",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<MaxKeys>1000</MaxKeys>"))
                })
        }
    }

    @Test("Object key with a literal percent sequence is stored verbatim, not decoded twice")
    func testPercentEncodedKeyNotDoubleDecoded() async throws {
        let bucketName = "percent-key-bucket"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            // What a client sends for the key `report%20final.pdf`: the `%` itself is encoded.
            try await putObject(
                app, bucketName: bucketName, key: "report%2520final.pdf", content: "x")

            #expect(
                FileManager.default.fileExists(
                    atPath: ObjectFileHandler.storagePath(
                        for: bucketName, key: "report%20final.pdf")))
            #expect(
                !FileManager.default.fileExists(
                    atPath: ObjectFileHandler.storagePath(for: bucketName, key: "report final.pdf")))

            let signed = signedHeaders(for: .HEAD, path: "/\(bucketName)/report%2520final.pdf")
            try await app.test(
                .HEAD, "/\(bucketName)/report%2520final.pdf",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("ListObjectsV2 - continuation-token takes precedence over a resent start-after")
    func testContinuationTokenBeatsStartAfter() async throws {
        let bucketName = "list-v2-start-after-bucket"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            for key in ["a.txt", "b.txt", "c.txt", "d.txt"] {
                try await putObject(app, bucketName: bucketName, key: key, content: key)
            }

            func page(_ query: String) async throws -> String {
                let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: query)
                var body = ""
                try await app.test(
                    .GET, "/\(bucketName)?\(query)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed)
                    },
                    afterResponse: { res in
                        #expect(res.status == .ok)
                        body = res.body.string
                    })
                return body
            }

            let first = try await page("list-type=2&max-keys=1&start-after=a.txt")
            #expect(first.contains("<Key>b.txt</Key>"))
            let token = try #require(
                first.range(of: "<NextContinuationToken>").map {
                    String(first[$0.upperBound...].prefix { $0 != "<" })
                })

            // A paginator resends start-after with the token; the token must win.
            let second = try await page(
                "list-type=2&max-keys=1&start-after=a.txt&continuation-token=\(token)")
            #expect(second.contains("<Key>c.txt</Key>"), "\(second)")
            #expect(!second.contains("<Key>b.txt</Key>"))
        }
    }

    @Test("Conditional GET - dates compare at second precision and If-Match overrides If-Unmodified-Since")
    func testConditionalReadHeaders() async throws {
        let bucketName = "conditional-read-bucket"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "c.txt", content: "payload")

            var lastModified = ""
            var etag = ""
            let head = signedHeaders(for: .HEAD, path: "/\(bucketName)/c.txt")
            try await app.test(
                .HEAD, "/\(bucketName)/c.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: head)
                },
                afterResponse: { res in
                    lastModified = res.headers.first(name: "Last-Modified") ?? ""
                    etag = res.headers.first(name: "ETag") ?? ""
                })
            #expect(!lastModified.isEmpty && !etag.isEmpty)

            func get(_ conditions: [String: String]) async throws -> HTTPStatus {
                let signed = signedHeaders(
                    for: .GET, path: "/\(bucketName)/c.txt", additionalHeaders: conditions)
                var status = HTTPStatus.internalServerError
                try await app.test(
                    .GET, "/\(bucketName)/c.txt",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed)
                    },
                    afterResponse: { res in
                        status = res.status
                    })
                return status
            }

            let longAgo = "Mon, 01 Jan 2001 00:00:00 GMT"
            // Echoing Last-Modified back is the common cache-validation case: 304, not a miss
            // on the sub-second part the header could not carry.
            #expect(try await get(["If-Modified-Since": lastModified]) == .notModified)
            #expect(try await get(["If-Unmodified-Since": lastModified]) == .ok)
            #expect(try await get(["If-Modified-Since": longAgo]) == .ok)
            #expect(try await get(["If-Unmodified-Since": longAgo]) == .preconditionFailed)
            // If-Match true wins over a failing If-Unmodified-Since.
            #expect(try await get(["If-Match": etag, "If-Unmodified-Since": longAgo]) == .ok)
            // If-None-Match matching wins over a passing If-Modified-Since.
            #expect(try await get(["If-None-Match": etag, "If-Modified-Since": longAgo]) == .notModified)
            #expect(try await get(["If-None-Match": "\"other\""]) == .ok)
        }
    }

    @Test("Non-S3 errors on S3 routes come back as S3 XML, while console routes keep JSON")
    func testNonS3ErrorsAreXMLOnS3Routes() async throws {
        try await withApp { app in
            // A method the S3 surface does not route at all.
            try await app.test(
                .PATCH, "/some-bucket/some-key",
                afterResponse: { res in
                    #expect(res.status.code >= 400)
                    #expect(res.headers.contentType == .xml)
                    #expect(res.body.string.contains("<Error>"))
                    #expect(res.body.string.contains("<Code>"))
                    #expect(res.headers.first(name: "x-amz-request-id") != nil)
                })
            try await app.test(
                .GET, "/api/v1/definitely-not-a-route",
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    #expect(res.body.string.contains("\"error\":true"))
                })
        }
    }

    @Test("PUT and DELETE on a missing bucket answer NoSuchBucket, not AccessDenied")
    func testMissingBucketIsNoSuchBucket() async throws {
        try await withApp { app in
            let data = Data("x".utf8)
            let put = signedHeaders(for: .PUT, path: "/no-such-bucket-here/key.txt", body: data)
            try await app.test(
                .PUT, "/no-such-bucket-here/key.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: put)
                    req.body = ByteBuffer(data: data)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    #expect(res.body.string.contains("<Code>NoSuchBucket</Code>"))
                })
            let delete = signedHeaders(for: .DELETE, path: "/no-such-bucket-here/key.txt")
            try await app.test(
                .DELETE, "/no-such-bucket-here/key.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: delete)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    #expect(res.body.string.contains("<Code>NoSuchBucket</Code>"))
                })
        }
    }

    @Test("Standard object headers are stored on PUT, replayed on GET/HEAD, copied by directive, and overridable per request")
    func testStoredStandardHeaders() async throws {
        let bucketName = "test-stored-headers"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let data = Data("compressed-bytes".utf8)
            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/archive.gz", body: data,
                additionalHeaders: [
                    "Content-Encoding": "gzip", "Cache-Control": "max-age=60",
                    "Content-Disposition": "attachment; filename=\"archive.gz\"",
                    "Content-Language": "en", "Expires": "Thu, 01 Dec 2030 16:00:00 GMT",
                ])
            try await app.test(
                .PUT, "/\(bucketName)/archive.gz",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: data)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            for method in [HTTPMethod.GET, .HEAD] {
                let signed = signedHeaders(for: method, path: "/\(bucketName)/archive.gz")
                try await app.test(
                    method, "/\(bucketName)/archive.gz",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed)
                    },
                    afterResponse: { res in
                        #expect(res.status == .ok)
                        #expect(res.headers.first(name: "Content-Encoding") == "gzip")
                        #expect(res.headers.first(name: "Cache-Control") == "max-age=60")
                        #expect(res.headers.first(name: "Content-Disposition") == "attachment; filename=\"archive.gz\"")
                        #expect(res.headers.first(name: "Content-Language") == "en")
                        #expect(res.headers.first(name: "Expires") == "Thu, 01 Dec 2030 16:00:00 GMT")
                    })
            }

            // Signed `response-*` overrides win for that response only.
            let overridden = signedHeaders(
                for: .GET, path: "/\(bucketName)/archive.gz",
                query: "response-content-disposition=inline&response-content-type=text/plain")
            try await app.test(
                .GET, "/\(bucketName)/archive.gz?response-content-disposition=inline&response-content-type=text/plain",
                beforeRequest: { req in
                    req.headers.add(contentsOf: overridden)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "Content-Disposition") == "inline")
                    #expect(res.headers.first(name: "Content-Type") == "text/plain")
                    #expect(res.headers.first(name: "Content-Encoding") == "gzip")
                })

            // COPY (default directive) carries them over; REPLACE takes them from the request.
            let copySigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/copied.gz",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/archive.gz"])
            try await app.test(
                .PUT, "/\(bucketName)/copied.gz",
                beforeRequest: { req in
                    req.headers.add(contentsOf: copySigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
            let replaceSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/replaced.gz",
                additionalHeaders: [
                    "x-amz-copy-source": "/\(bucketName)/archive.gz",
                    "x-amz-metadata-directive": "REPLACE", "Cache-Control": "no-store",
                ])
            try await app.test(
                .PUT, "/\(bucketName)/replaced.gz",
                beforeRequest: { req in
                    req.headers.add(contentsOf: replaceSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
            for (key, encoding, cache) in [("copied.gz", "gzip", "max-age=60"), ("replaced.gz", nil, "no-store")] {
                let head = signedHeaders(for: .HEAD, path: "/\(bucketName)/\(key)")
                try await app.test(
                    .HEAD, "/\(bucketName)/\(key)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: head)
                    },
                    afterResponse: { res in
                        #expect(res.headers.first(name: "Content-Encoding") == encoding, "\(key)")
                        #expect(res.headers.first(name: "Cache-Control") == cache, "\(key)")
                    })
            }

            // CreateMultipartUpload headers end up on the completed object.
            var uploadId = ""
            let createSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/mp.bin", query: "uploads",
                additionalHeaders: ["Cache-Control": "public"])
            try await app.test(
                .POST, "/\(bucketName)/mp.bin?uploads",
                beforeRequest: { req in
                    req.headers.add(contentsOf: createSigned)
                },
                afterResponse: { res in
                    let body = res.body.string
                    if let r = body.range(of: "<UploadId>"),
                        let e = body[r.upperBound...].range(of: "</UploadId>")
                    {
                        uploadId = String(body[r.upperBound..<e.lowerBound])
                    }
                })
            let part = Data("only-part".utf8)
            var etag = ""
            let partSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/mp.bin", query: "partNumber=1&uploadId=\(uploadId)",
                body: part)
            try await app.test(
                .PUT, "/\(bucketName)/mp.bin?partNumber=1&uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: partSigned)
                    req.body = ByteBuffer(data: part)
                },
                afterResponse: { res in
                    etag = res.headers.first(name: "ETag") ?? ""
                })
            let completeBody = Data(
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\(etag)</ETag></Part></CompleteMultipartUpload>".utf8)
            let completeSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/mp.bin", query: "uploadId=\(uploadId)",
                body: completeBody)
            try await app.test(
                .POST, "/\(bucketName)/mp.bin?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeBody)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })
            let headMp = signedHeaders(for: .HEAD, path: "/\(bucketName)/mp.bin")
            try await app.test(
                .HEAD, "/\(bucketName)/mp.bin",
                beforeRequest: { req in
                    req.headers.add(contentsOf: headMp)
                },
                afterResponse: { res in
                    #expect(res.headers.first(name: "Cache-Control") == "public")
                })
        }
    }

    @Test("Listings carry Delimiter, Owner (V1 always, V2 on fetch-owner) and encoding-type=url")
    func testListingXmlFields() async throws {
        let bucketName = "test-listing-fields"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "docs/a b.txt", content: "a")
            try await putObject(app, bucketName: bucketName, key: "top.txt", content: "t")

            func list(_ query: String) async throws -> String {
                let signed = signedHeaders(for: .GET, path: "/\(bucketName)", query: query)
                var body = ""
                try await app.test(
                    .GET, "/\(bucketName)?\(query)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed)
                    },
                    afterResponse: { res in
                        #expect(res.status == .ok, "\(query)")
                        body = res.body.string
                    })
                return body
            }

            let v1 = try await list("delimiter=/")
            #expect(v1.contains("<Delimiter>/</Delimiter>"))
            #expect(v1.contains("<Owner>") && v1.contains("<ID>") && v1.contains("<DisplayName>alarik</DisplayName>"))
            #expect(v1.contains("<Prefix>docs/</Prefix>"))

            let v2 = try await list("list-type=2")
            #expect(!v2.contains("<Owner>"))
            let v2Owner = try await list("list-type=2&fetch-owner=true")
            #expect(v2Owner.contains("<Owner>"))

            let encoded = try await list("list-type=2&encoding-type=url")
            #expect(encoded.contains("<EncodingType>url</EncodingType>"))
            #expect(encoded.contains("<Key>docs/a%20b.txt</Key>"))
            #expect(!encoded.contains("<Key>docs/a b.txt</Key>"))

            let bad = signedHeaders(for: .GET, path: "/\(bucketName)", query: "encoding-type=base64")
            try await app.test(
                .GET, "/\(bucketName)?encoding-type=base64",
                beforeRequest: { req in
                    req.headers.add(contentsOf: bad)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    #expect(res.body.string.contains("<Code>InvalidArgument</Code>"))
                })
        }
    }

    @Test("ListMultipartUploads honours delimiter and both multipart listings report Initiator and Owner")
    func testMultipartListingsFields() async throws {
        let bucketName = "test-multipart-listing-fields"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            var uploadIds: [String: String] = [:]
            for key in ["folder/inner.bin", "root.bin"] {
                let signed = signedHeaders(for: .POST, path: "/\(bucketName)/\(key)", query: "uploads")
                try await app.test(
                    .POST, "/\(bucketName)/\(key)?uploads",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed)
                    },
                    afterResponse: { res in
                        let body = res.body.string
                        if let r = body.range(of: "<UploadId>"),
                            let e = body[r.upperBound...].range(of: "</UploadId>")
                        {
                            uploadIds[key] = String(body[r.upperBound..<e.lowerBound])
                        }
                    })
            }

            let listSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "uploads&delimiter=/")
            try await app.test(
                .GET, "/\(bucketName)?uploads&delimiter=/",
                beforeRequest: { req in
                    req.headers.add(contentsOf: listSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let body = res.body.string
                    #expect(body.contains("<Delimiter>/</Delimiter>"))
                    #expect(body.contains("<CommonPrefixes>") && body.contains("<Prefix>folder/</Prefix>"))
                    #expect(body.contains("<Key>root.bin</Key>"))
                    #expect(!body.contains("<Key>folder/inner.bin</Key>"))
                    #expect(body.contains("<Initiator>") && body.contains("<DisplayName>alarik</DisplayName>"))
                    #expect(body.contains("<StorageClass>STANDARD</StorageClass>"))
                })

            let uploadId = try #require(uploadIds["root.bin"])
            let partsSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/root.bin", query: "uploadId=\(uploadId)")
            try await app.test(
                .GET, "/\(bucketName)/root.bin?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: partsSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<Initiator>"))
                    #expect(res.body.string.contains("<Owner>"))
                    #expect(res.body.string.contains("<StorageClass>STANDARD</StorageClass>"))
                })
        }
    }
}
