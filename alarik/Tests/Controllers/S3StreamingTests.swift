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
import SotoSignerV4
import Testing
import Vapor
import VaporTesting

@testable import Alarik

/// End-to-end coverage for the bounded-memory object paths: spooled PUT, streamed GET
/// (payloads above `Constants.streamingThreshold` serve via file streaming), streamed
/// multipart assembly, and file-to-file copies. Payloads here are deliberately larger than
/// the 4 MiB streaming threshold so the streaming branches are the ones under test.
@Suite("S3 streaming object path tests", .serialized)
struct S3StreamingTests {
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
        return signer.signHeaders(
            url: url,
            method: method,
            headers: HTTPHeaders(headers),
            body: body != nil ? .data(body!) : .none
        )
    }

    private func createBucket(_ app: Application, bucketName: String) async throws {
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)")
        try await app.test(
            .PUT, "/\(bucketName)",
            beforeRequest: { req in req.headers.add(contentsOf: signed) },
            afterResponse: { res in #expect(res.status == .ok) })
    }

    private func enableVersioning(_ app: Application, bucketName: String) async throws {
        let body = Data(
            """
            <?xml version="1.0" encoding="UTF-8"?>
            <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Enabled</Status>
            </VersioningConfiguration>
            """.utf8)
        let signed = signedHeaders(
            for: .PUT, path: "/\(bucketName)", query: "versioning", body: body)
        try await app.test(
            .PUT, "/\(bucketName)?versioning",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: body)
            },
            afterResponse: { res in #expect(res.status == .ok) })
    }

    /// Deterministic pseudo-random payload - incompressible-ish and unique per seed
    private func makePayload(size: Int, seed: UInt8 = 5) -> Data {
        var data = Data(capacity: size)
        var state: UInt8 = seed
        for i in 0..<size {
            state = state &* 31 &+ UInt8(truncatingIfNeeded: i) &+ 11
            data.append(state)
        }
        return data
    }

    // 6 MiB - comfortably above the 4 MiB streaming threshold
    private let largeSize = 6 * 1024 * 1024

    @Test("large object PUT/GET round-trips byte-identically through the streaming paths")
    func largeObjectRoundTrip() async throws {
        let bucketName = "streaming-roundtrip-bucket"
        let key = "big/object.bin"
        let payload = makePayload(size: largeSize)
        let expectedETag = Insecure.MD5.hash(data: payload).hex

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let putSigned = signedHeaders(for: .PUT, path: "/\(bucketName)/\(key)", body: payload)
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: payload)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "ETag") == "\"\(expectedETag)\"")
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/\(key)")
            try await app.test(
                .GET, "/\(bucketName)/\(key)",
                beforeRequest: { req in req.headers.add(contentsOf: getSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "Content-Length") == String(payload.count))
                    #expect(Data(res.body.readableBytesView) == payload)
                })

            // The spool directory must not accumulate files after the request completed
            let spoolLeftovers =
                (try? FileManager.default.contentsOfDirectory(
                    atPath: Constants.spoolDirectory)) ?? []
            #expect(spoolLeftovers.isEmpty)
        }
    }

    @Test("range GET on a large object streams exactly the requested window")
    func largeObjectRangeGet() async throws {
        let bucketName = "streaming-range-bucket"
        let key = "ranged.bin"
        let payload = makePayload(size: largeSize, seed: 9)

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let putSigned = signedHeaders(for: .PUT, path: "/\(bucketName)/\(key)", body: payload)
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: payload)
                },
                afterResponse: { res in #expect(res.status == .ok) })

            // A >4 MiB window (streamed) and a small window (buffered) must both be exact
            let ranges: [(start: Int, end: Int)] = [
                (1_000_000, 6_000_000),  // 5 MB and change - streaming branch
                (12345, 23456),  // small - buffered branch
            ]
            for range in ranges {
                let signed = signedHeaders(
                    for: .GET, path: "/\(bucketName)/\(key)",
                    additionalHeaders: ["range": "bytes=\(range.start)-\(range.end)"])
                try await app.test(
                    .GET, "/\(bucketName)/\(key)",
                    beforeRequest: { req in req.headers.add(contentsOf: signed) },
                    afterResponse: { res in
                        #expect(res.status == .partialContent)
                        let expected = payload.subdata(in: range.start..<(range.end + 1))
                        #expect(
                            res.headers.first(name: "Content-Range")
                                == "bytes \(range.start)-\(range.end)/\(payload.count)")
                        #expect(Data(res.body.readableBytesView) == expected)
                    })
            }
        }
    }

    @Test("large PUT on a versioned bucket round-trips and reports a version id")
    func largeVersionedPut() async throws {
        let bucketName = "streaming-versioned-bucket"
        let key = "versioned-big.bin"
        let payload = makePayload(size: largeSize, seed: 13)

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            var versionId: String?
            let putSigned = signedHeaders(for: .PUT, path: "/\(bucketName)/\(key)", body: payload)
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: payload)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    versionId = res.headers.first(name: "x-amz-version-id")
                    #expect(versionId != nil)
                })

            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/\(key)", query: "versionId=\(versionId ?? "")")
            try await app.test(
                .GET, "/\(bucketName)/\(key)?versionId=\(versionId ?? "")",
                beforeRequest: { req in req.headers.add(contentsOf: getSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(Data(res.body.readableBytesView) == payload)
                })
        }
    }

    @Test("PUT with a wrong Content-MD5 is rejected with BadDigest and stores nothing")
    func wrongContentMD5Rejected() async throws {
        let bucketName = "streaming-badmd5-bucket"
        let key = "bad-md5.bin"
        let payload = makePayload(size: 100_000, seed: 21)
        let wrongMD5 = Data(Insecure.MD5.hash(data: Data("other".utf8))).base64EncodedString()

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let signed = signedHeaders(
                for: .PUT, path: "/\(bucketName)/\(key)", body: payload,
                additionalHeaders: ["content-md5": wrongMD5])
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: payload)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    #expect(res.body.string.contains("BadDigest"))
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/\(key)")
            try await app.test(
                .GET, "/\(bucketName)/\(key)",
                beforeRequest: { req in req.headers.add(contentsOf: getSigned) },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("PUT whose body doesn't match the signed x-amz-content-sha256 is rejected")
    func wrongPayloadHashRejected() async throws {
        let bucketName = "streaming-badsha-bucket"
        let key = "bad-sha.bin"
        let signedPayload = makePayload(size: 50_000, seed: 30)
        let actualPayload = makePayload(size: 50_000, seed: 31)

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Sign the request for one body, send different bytes: the declared (and signed)
            // payload hash no longer matches what arrives
            let signed = signedHeaders(
                for: .PUT, path: "/\(bucketName)/\(key)", body: signedPayload)
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: actualPayload)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    #expect(res.body.string.contains("InvalidDigest"))
                })
        }
    }

    @Test("multipart upload assembles parts into a byte-identical large object")
    func multipartStreamedAssembly() async throws {
        let bucketName = "streaming-multipart-bucket"
        let key = "assembled/big.bin"
        // Two 5 MiB parts + a smaller final part, all unique bytes
        let part1 = makePayload(size: 5 * 1024 * 1024, seed: 41)
        let part2 = makePayload(size: 5 * 1024 * 1024, seed: 42)
        let part3 = makePayload(size: 1 * 1024 * 1024 + 333, seed: 43)
        let fullPayload = part1 + part2 + part3

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Initiate
            var uploadId = ""
            let initSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/\(key)", query: "uploads")
            try await app.test(
                .POST, "/\(bucketName)/\(key)?uploads",
                beforeRequest: { req in req.headers.add(contentsOf: initSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let body = res.body.string
                    if let start = body.range(of: "<UploadId>"),
                        let end = body.range(of: "</UploadId>")
                    {
                        uploadId = String(body[start.upperBound..<end.lowerBound])
                    }
                    #expect(!uploadId.isEmpty)
                })

            // Upload parts
            var partETags: [String] = []
            for (number, part) in [part1, part2, part3].enumerated() {
                let partNumber = number + 1
                let query = "partNumber=\(partNumber)&uploadId=\(uploadId)"
                let signed = signedHeaders(
                    for: .PUT, path: "/\(bucketName)/\(key)", query: query, body: part)
                try await app.test(
                    .PUT, "/\(bucketName)/\(key)?\(query)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed)
                        req.body = ByteBuffer(data: part)
                    },
                    afterResponse: { res in
                        #expect(res.status == .ok)
                        let etag = res.headers.first(name: "ETag") ?? ""
                        #expect(etag == "\"\(Insecure.MD5.hash(data: part).hex)\"")
                        partETags.append(etag)
                    })
            }

            // Complete
            var completeXML = "<CompleteMultipartUpload>"
            for (index, etag) in partETags.enumerated() {
                completeXML +=
                    "<Part><PartNumber>\(index + 1)</PartNumber><ETag>\(etag)</ETag></Part>"
            }
            completeXML += "</CompleteMultipartUpload>"
            let completeBody = Data(completeXML.utf8)
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
                    // Multipart ETag: MD5 of concatenated binary part MD5s + "-3"
                    #expect(res.body.string.contains("-3"))
                })

            // Full round-trip
            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/\(key)")
            try await app.test(
                .GET, "/\(bucketName)/\(key)",
                beforeRequest: { req in req.headers.add(contentsOf: getSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "Content-Length") == String(fullPayload.count))
                    #expect(Data(res.body.readableBytesView) == fullPayload)
                })
        }
    }

    @Test("CopyObject streams a large object file-to-file with the correct ETag")
    func copyObjectStreamed() async throws {
        let bucketName = "streaming-copy-bucket"
        let sourceKey = "source-big.bin"
        let destKey = "dest-big.bin"
        let payload = makePayload(size: largeSize, seed: 55)
        let expectedETag = Insecure.MD5.hash(data: payload).hex

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/\(sourceKey)", body: payload)
            try await app.test(
                .PUT, "/\(bucketName)/\(sourceKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: payload)
                },
                afterResponse: { res in #expect(res.status == .ok) })

            let copySigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/\(destKey)",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/\(sourceKey)"])
            try await app.test(
                .PUT, "/\(bucketName)/\(destKey)",
                beforeRequest: { req in req.headers.add(contentsOf: copySigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains(expectedETag))
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/\(destKey)")
            try await app.test(
                .GET, "/\(bucketName)/\(destKey)",
                beforeRequest: { req in req.headers.add(contentsOf: getSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(Data(res.body.readableBytesView) == payload)
                })
        }
    }

    @Test("UploadPartCopy copies a byte range of a large object into a part")
    func uploadPartCopyStreamed() async throws {
        let bucketName = "streaming-partcopy-bucket"
        let sourceKey = "partcopy-source.bin"
        let destKey = "partcopy-dest.bin"
        let payload = makePayload(size: largeSize, seed: 66)
        // A 5 MiB slice from the middle + a directly-uploaded final part
        let sliceStart = 500_000
        let sliceEnd = sliceStart + 5 * 1024 * 1024 - 1
        let slice = payload.subdata(in: sliceStart..<(sliceEnd + 1))
        let tailPart = makePayload(size: 100_000, seed: 67)

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/\(sourceKey)", body: payload)
            try await app.test(
                .PUT, "/\(bucketName)/\(sourceKey)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: payload)
                },
                afterResponse: { res in #expect(res.status == .ok) })

            var uploadId = ""
            let initSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/\(destKey)", query: "uploads")
            try await app.test(
                .POST, "/\(bucketName)/\(destKey)?uploads",
                beforeRequest: { req in req.headers.add(contentsOf: initSigned) },
                afterResponse: { res in
                    let body = res.body.string
                    if let start = body.range(of: "<UploadId>"),
                        let end = body.range(of: "</UploadId>")
                    {
                        uploadId = String(body[start.upperBound..<end.lowerBound])
                    }
                    #expect(!uploadId.isEmpty)
                })

            // Part 1 via UploadPartCopy with a range
            var partETags: [String] = []
            let copyQuery = "partNumber=1&uploadId=\(uploadId)"
            let copySigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/\(destKey)", query: copyQuery,
                additionalHeaders: [
                    "x-amz-copy-source": "/\(bucketName)/\(sourceKey)",
                    "x-amz-copy-source-range": "bytes=\(sliceStart)-\(sliceEnd)",
                ])
            try await app.test(
                .PUT, "/\(bucketName)/\(destKey)?\(copyQuery)",
                beforeRequest: { req in req.headers.add(contentsOf: copySigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let expectedPartETag = Insecure.MD5.hash(data: slice).hex
                    #expect(res.body.string.contains(expectedPartETag))
                    partETags.append("\"\(expectedPartETag)\"")
                })

            // Part 2 uploaded directly
            let part2Query = "partNumber=2&uploadId=\(uploadId)"
            let part2Signed = signedHeaders(
                for: .PUT, path: "/\(bucketName)/\(destKey)", query: part2Query, body: tailPart)
            try await app.test(
                .PUT, "/\(bucketName)/\(destKey)?\(part2Query)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: part2Signed)
                    req.body = ByteBuffer(data: tailPart)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    partETags.append(res.headers.first(name: "ETag") ?? "")
                })

            var completeXML = "<CompleteMultipartUpload>"
            for (index, etag) in partETags.enumerated() {
                completeXML +=
                    "<Part><PartNumber>\(index + 1)</PartNumber><ETag>\(etag)</ETag></Part>"
            }
            completeXML += "</CompleteMultipartUpload>"
            let completeBody = Data(completeXML.utf8)
            let completeSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/\(destKey)", query: "uploadId=\(uploadId)",
                body: completeBody)
            try await app.test(
                .POST, "/\(bucketName)/\(destKey)?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeBody)
                },
                afterResponse: { res in #expect(res.status == .ok) })

            let expected = slice + tailPart
            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/\(destKey)")
            try await app.test(
                .GET, "/\(bucketName)/\(destKey)",
                beforeRequest: { req in req.headers.add(contentsOf: getSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(Data(res.body.readableBytesView) == expected)
                })
        }
    }

    @Test("copy of a multipart object recomputes a plain MD5 ETag for the destination")
    func copyOfMultipartObjectETag() async throws {
        let bucketName = "streaming-mpcopy-bucket"
        let sourceKey = "mp-source.bin"
        let destKey = "mp-dest.bin"
        let part1 = makePayload(size: 5 * 1024 * 1024, seed: 71)
        let part2 = makePayload(size: 200_000, seed: 72)
        let fullPayload = part1 + part2
        let expectedCopyETag = Insecure.MD5.hash(data: fullPayload).hex

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Build the source via multipart so its ETag has the "-2" suffix
            var uploadId = ""
            let initSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/\(sourceKey)", query: "uploads")
            try await app.test(
                .POST, "/\(bucketName)/\(sourceKey)?uploads",
                beforeRequest: { req in req.headers.add(contentsOf: initSigned) },
                afterResponse: { res in
                    let body = res.body.string
                    if let start = body.range(of: "<UploadId>"),
                        let end = body.range(of: "</UploadId>")
                    {
                        uploadId = String(body[start.upperBound..<end.lowerBound])
                    }
                })

            var partETags: [String] = []
            for (number, part) in [part1, part2].enumerated() {
                let query = "partNumber=\(number + 1)&uploadId=\(uploadId)"
                let signed = signedHeaders(
                    for: .PUT, path: "/\(bucketName)/\(sourceKey)", query: query, body: part)
                try await app.test(
                    .PUT, "/\(bucketName)/\(sourceKey)?\(query)",
                    beforeRequest: { req in
                        req.headers.add(contentsOf: signed)
                        req.body = ByteBuffer(data: part)
                    },
                    afterResponse: { res in
                        partETags.append(res.headers.first(name: "ETag") ?? "")
                    })
            }

            var completeXML = "<CompleteMultipartUpload>"
            for (index, etag) in partETags.enumerated() {
                completeXML +=
                    "<Part><PartNumber>\(index + 1)</PartNumber><ETag>\(etag)</ETag></Part>"
            }
            completeXML += "</CompleteMultipartUpload>"
            let completeBody = Data(completeXML.utf8)
            let completeSigned = signedHeaders(
                for: .POST, path: "/\(bucketName)/\(sourceKey)", query: "uploadId=\(uploadId)",
                body: completeBody)
            try await app.test(
                .POST, "/\(bucketName)/\(sourceKey)?uploadId=\(uploadId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: completeSigned)
                    req.body = ByteBuffer(data: completeBody)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("-2"))
                })

            // Copy it - the destination must get a plain (recomputed) MD5, not the "-2" ETag
            let copySigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/\(destKey)",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/\(sourceKey)"])
            try await app.test(
                .PUT, "/\(bucketName)/\(destKey)",
                beforeRequest: { req in req.headers.add(contentsOf: copySigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains(expectedCopyETag))
                    #expect(!res.body.string.contains("-2</ETag>"))
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)/\(destKey)")
            try await app.test(
                .GET, "/\(bucketName)/\(destKey)",
                beforeRequest: { req in req.headers.add(contentsOf: getSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "ETag") == "\"\(expectedCopyETag)\"")
                    #expect(Data(res.body.readableBytesView) == fullPayload)
                })
        }
    }

    /// Object writes (spool -> hash -> fsync -> rename) are offloaded to Vapor's blocking-IO
    /// thread pool instead of running on Swift's shared concurrent executor - a CPU profile of
    /// a PUT-heavy benchmark found the opposite (blocking disk syscalls tying up that shared
    /// executor) starving throughput under concurrency. This doesn't assert on timing (that
    /// would be flaky), but it does prove many concurrent PUTs against the refactored async
    /// spool/write pipeline all complete correctly and with distinct, byte-correct payloads -
    /// exactly the scenario a data race or an incorrect Sendable capture in that refactor
    /// would corrupt.
    @Test("many concurrent PUTs against distinct keys all complete correctly")
    func concurrentPutsCompleteCorrectly() async throws {
        let bucketName = "streaming-concurrent-bucket"
        let concurrency = 24

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<concurrency {
                    group.addTask {
                        let key = "concurrent-\(i).bin"
                        let payload = self.makePayload(size: 200_000 + i * 37, seed: UInt8(i))
                        let expectedETag = Insecure.MD5.hash(data: payload).hex

                        let putSigned = self.signedHeaders(
                            for: .PUT, path: "/\(bucketName)/\(key)", body: payload)
                        try await app.test(
                            .PUT, "/\(bucketName)/\(key)",
                            beforeRequest: { req in
                                req.headers.add(contentsOf: putSigned)
                                req.body = ByteBuffer(data: payload)
                            },
                            afterResponse: { res in
                                #expect(res.status == .ok)
                                #expect(res.headers.first(name: "ETag") == "\"\(expectedETag)\"")
                            })

                        let getSigned = self.signedHeaders(
                            for: .GET, path: "/\(bucketName)/\(key)")
                        try await app.test(
                            .GET, "/\(bucketName)/\(key)",
                            beforeRequest: { req in req.headers.add(contentsOf: getSigned) },
                            afterResponse: { res in
                                #expect(res.status == .ok)
                                #expect(Data(res.body.readableBytesView) == payload)
                            })
                    }
                }
                try await group.waitForAll()
            }

            // No leftover spool files after every request has completed
            let spoolLeftovers =
                (try? FileManager.default.contentsOfDirectory(atPath: Constants.spoolDirectory))
                ?? []
            #expect(spoolLeftovers.isEmpty)
        }
    }
}
