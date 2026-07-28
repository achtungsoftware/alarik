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

/// End-to-end coverage for aws-chunked uploads that carry a trailing flexible checksum - the
/// `STREAMING-UNSIGNED-PAYLOAD-TRAILER` form modern AWS SDKs (e.g. AWS SDK for .NET v4) send by
/// default now that request checksums are on. Soto's signer can't produce this form, so the
/// requests here are SigV4-signed by hand, exactly as a real client would.
@Suite("S3 trailer-checksum upload tests", .serialized)
struct S3TrailerChecksumTests {
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

    // MARK: - Manual SigV4 signing for the unsigned-trailer streaming form

    private func hmac(_ key: Data, _ data: String) -> Data {
        Data(HMAC<SHA256>.authenticationCode(for: Data(data.utf8), using: SymmetricKey(data: key)))
    }

    private func amzTimestamps() -> (amzDate: String, dateStamp: String) {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "UTC")
        formatter.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
        let amzDate = formatter.string(from: Date())
        return (amzDate, String(amzDate.prefix(8)))
    }

    /// Signs a `STREAMING-UNSIGNED-PAYLOAD-TRAILER` PUT the way a real SDK does: the sentinel is
    /// the canonical payload hash, and `content-encoding`, `x-amz-decoded-content-length` and
    /// `x-amz-trailer` are all signed headers.
    private func signUnsignedTrailer(
        path: String, decodedLength: Int, trailerHeader: String
    ) -> HTTPHeaders {
        let payloadHash = S3PayloadHash.streamingUnsignedTrailer
        let (amzDate, dateStamp) = amzTimestamps()

        // Sorted by lowercased name, as SigV4 requires.
        let signed: [(String, String)] = [
            ("content-encoding", "aws-chunked"),
            ("host", host),
            ("x-amz-content-sha256", payloadHash),
            ("x-amz-date", amzDate),
            ("x-amz-decoded-content-length", String(decodedLength)),
            ("x-amz-trailer", trailerHeader),
        ]
        let signedHeaderNames = signed.map { $0.0 }.joined(separator: ";")
        let canonicalHeaders = signed.map { "\($0.0):\($0.1)\n" }.joined()
        let canonicalRequest = [
            "PUT", path, "", canonicalHeaders, signedHeaderNames, payloadHash,
        ].joined(separator: "\n")

        let scope = "\(dateStamp)/\(region)/s3/aws4_request"
        let stringToSign = [
            "AWS4-HMAC-SHA256", amzDate, scope,
            Crypto.SHA256.hash(data: Data(canonicalRequest.utf8)).hexString(),
        ].joined(separator: "\n")

        let kDate = hmac(Data("AWS4\(secretKey)".utf8), dateStamp)
        let kRegion = hmac(kDate, region)
        let kService = hmac(kRegion, "s3")
        let kSigning = hmac(kService, "aws4_request")
        let signature = hmac(kSigning, stringToSign).hexString()

        var headers = HTTPHeaders()
        for (name, value) in signed where name != "host" {
            headers.add(name: name, value: value)
        }
        headers.add(name: "host", value: host)
        headers.add(
            name: "authorization",
            value:
                "AWS4-HMAC-SHA256 Credential=\(accessKey)/\(scope), "
                + "SignedHeaders=\(signedHeaderNames), Signature=\(signature)")
        return headers
    }

    /// Frames `payload` as an unsigned aws-chunked body with a trailing `x-amz-checksum-crc32`.
    /// Pass `overrideChecksum` to forge a wrong trailer value.
    private func frameUnsignedTrailer(
        _ payload: Data, chunkSize: Int = 64 * 1024, overrideChecksum: String? = nil
    ) -> Data {
        var wire = Data()
        var offset = 0
        while offset < payload.count {
            let end = Swift.min(offset + chunkSize, payload.count)
            let chunk = payload.subdata(in: offset..<end)
            wire.append(Data("\(String(chunk.count, radix: 16))\r\n".utf8))
            wire.append(chunk)
            wire.append(Data("\r\n".utf8))
            offset = end
        }
        wire.append(Data("0\r\n".utf8))
        var crc = TrailerChecksum(trailerHeaderName: "x-amz-checksum-crc32")!
        crc.update(ByteBuffer(data: payload).readableBytesView)
        let value = overrideChecksum ?? crc.finalizeBase64()
        wire.append(Data("x-amz-checksum-crc32:\(value)\r\n\r\n".utf8))
        return wire
    }

    /// Soto-signed headers for GET/PUT requests that carry a query string and/or extra headers
    /// (CopyObject, GetObjectAttributes) - simpler than hand-signing a canonical query string.
    private func sotoSigned(
        method: HTTPMethod, path: String, query: String? = nil,
        additionalHeaders: [String: String] = [:]
    ) -> HTTPHeaders {
        var fullPath = path
        if let query, !query.isEmpty { fullPath += "?\(query)" }
        let url = URL(string: "http://\(host)\(fullPath)")!
        let signer = AWSSigner(
            credentials: StaticCredential(accessKeyId: accessKey, secretAccessKey: secretKey),
            name: "s3", region: region)
        var headers: [(String, String)] = [("host", host)]
        for (key, value) in additionalHeaders { headers.append((key, value)) }
        return signer.signHeaders(
            url: url, method: method, headers: HTTPHeaders(headers), body: .none)
    }

    private func createBucket(_ app: Application, bucketName: String) async throws {
        let headers = signEmptyBody(method: "PUT", path: "/\(bucketName)")
        try await app.test(
            .PUT, "/\(bucketName)",
            beforeRequest: { req in req.headers.add(contentsOf: headers) },
            afterResponse: { res in #expect(res.status == .ok) })
    }

    private func makePayload(size: Int, seed: UInt8 = 5) -> Data {
        var data = Data(capacity: size)
        var state: UInt8 = seed
        for i in 0..<size {
            state = state &* 31 &+ UInt8(truncatingIfNeeded: i) &+ 11
            data.append(state)
        }
        return data
    }

    @Test("PUT with STREAMING-UNSIGNED-PAYLOAD-TRAILER round-trips and validates the CRC32")
    func unsignedTrailerRoundTrip() async throws {
        let bucketName = "trailer-roundtrip-bucket"
        let key = "docs/report.pdf"
        let payload = makePayload(size: 200_000, seed: 7)
        let expectedETag = Insecure.MD5.hash(data: payload).hex

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let wire = frameUnsignedTrailer(payload)
            let signed = signUnsignedTrailer(
                path: "/\(bucketName)/\(key)", decodedLength: payload.count,
                trailerHeader: "x-amz-checksum-crc32")
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: wire)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "ETag") == "\"\(expectedETag)\"")
                })

            // GET must return the decoded payload byte-for-byte (framing fully stripped).
            let get = signEmptyBody(method: "GET", path: "/\(bucketName)/\(key)")
            try await app.test(
                .GET, "/\(bucketName)/\(key)",
                beforeRequest: { req in req.headers.add(contentsOf: get) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "Content-Length") == String(payload.count))
                    #expect(Data(res.body.readableBytesView) == payload)
                })
        }
    }

    @Test("PutObject stores the checksum; GET/HEAD return it only with checksum-mode ENABLED")
    func checksumStoredAndReturnedOnDemand() async throws {
        let bucketName = "trailer-store-bucket"
        let key = "stored.bin"
        let payload = makePayload(size: 100_000, seed: 44)

        // The exact value the client's trailer declares - the server must store and echo it.
        var crc = TrailerChecksum(trailerHeaderName: "x-amz-checksum-crc32")!
        crc.update(ByteBuffer(data: payload).readableBytesView)
        let expectedChecksum = crc.finalizeBase64()

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let wire = frameUnsignedTrailer(payload)
            let signed = signUnsignedTrailer(
                path: "/\(bucketName)/\(key)", decodedLength: payload.count,
                trailerHeader: "x-amz-checksum-crc32")
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: wire)
                },
                afterResponse: { res in
                    // PutObject always echoes the checksum it stored.
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "x-amz-checksum-crc32") == expectedChecksum)
                    #expect(res.headers.first(name: "x-amz-checksum-type") == "FULL_OBJECT")
                })

            // Plain GET: no checksum headers unless asked.
            let get = signEmptyBody(method: "GET", path: "/\(bucketName)/\(key)")
            try await app.test(
                .GET, "/\(bucketName)/\(key)",
                beforeRequest: { req in req.headers.add(contentsOf: get) },
                afterResponse: { res in
                    #expect(res.headers.first(name: "x-amz-checksum-crc32") == nil)
                })

            // HEAD with x-amz-checksum-mode: ENABLED returns the stored checksum.
            let head = signEmptyBody(method: "HEAD", path: "/\(bucketName)/\(key)")
            try await app.test(
                .HEAD, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: head)
                    req.headers.add(name: "x-amz-checksum-mode", value: "ENABLED")
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "x-amz-checksum-crc32") == expectedChecksum)
                    #expect(res.headers.first(name: "x-amz-checksum-type") == "FULL_OBJECT")
                })
        }
    }

    @Test("a ranged GET must not return the whole-object checksum")
    func rangedGetOmitsChecksum() async throws {
        // Regression: aws s3 cp downloads large objects with parallel ranged GETs and validates the
        // returned checksum against each range - so a partial response must never carry the
        // whole-object checksum, or the download fails with a spurious mismatch.
        let bucketName = "ranged-checksum-bucket"
        let key = "ranged.bin"
        let payload = makePayload(size: 100_000, seed: 91)
        var crc = TrailerChecksum(trailerHeaderName: "x-amz-checksum-crc32")!
        crc.update(ByteBuffer(data: payload).readableBytesView)
        let expectedChecksum = crc.finalizeBase64()

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            let wire = frameUnsignedTrailer(payload)
            let signed = signUnsignedTrailer(
                path: "/\(bucketName)/\(key)", decodedLength: payload.count,
                trailerHeader: "x-amz-checksum-crc32")
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: wire)
                },
                afterResponse: { res in #expect(res.status == .ok) })

            // Ranged GET with checksum-mode ENABLED: partial content, NO checksum header.
            let rangedGet = signEmptyBody(method: "GET", path: "/\(bucketName)/\(key)")
            try await app.test(
                .GET, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: rangedGet)
                    req.headers.add(name: "x-amz-checksum-mode", value: "ENABLED")
                    req.headers.add(name: "range", value: "bytes=0-1023")
                },
                afterResponse: { res in
                    #expect(res.status == .partialContent)
                    #expect(res.headers.first(name: "x-amz-checksum-crc32") == nil)
                })

            // Whole-object GET with checksum-mode ENABLED: full content, checksum present.
            let wholeGet = signEmptyBody(method: "GET", path: "/\(bucketName)/\(key)")
            try await app.test(
                .GET, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: wholeGet)
                    req.headers.add(name: "x-amz-checksum-mode", value: "ENABLED")
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "x-amz-checksum-crc32") == expectedChecksum)
                })
        }
    }

    @Test("a precalculated x-amz-checksum-* header is validated and stored")
    func headerFormChecksum() async throws {
        let bucketName = "header-checksum-bucket"
        let key = "header.bin"
        let payload = makePayload(size: 40_000, seed: 51)
        var crc = TrailerChecksum(trailerHeaderName: "x-amz-checksum-crc32")!
        crc.update(ByteBuffer(data: payload).readableBytesView)
        let expectedChecksum = crc.finalizeBase64()

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Sign a plain (non-chunked) PUT that carries the checksum as a header, not a trailer.
            let signed = signPutWithChecksumHeader(
                path: "/\(bucketName)/\(key)", payload: payload, checksum: expectedChecksum)
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: payload)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "x-amz-checksum-crc32") == expectedChecksum)
                })

            // A wrong header value must be rejected.
            let badSigned = signPutWithChecksumHeader(
                path: "/\(bucketName)/bad.bin", payload: payload, checksum: "AAAAAA==")
            try await app.test(
                .PUT, "/\(bucketName)/bad.bin",
                beforeRequest: { req in
                    req.headers.add(contentsOf: badSigned)
                    req.body = ByteBuffer(data: payload)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    #expect(res.body.string.contains("BadDigest"))
                })
        }
    }

    @Test("large disk-spilling trailer upload round-trips and stores the right checksum")
    func largeUnsignedTrailerRoundTrip() async throws {
        let bucketName = "trailer-large-bucket"
        let key = "large.bin"
        // 6 MiB - above the 4 MiB streaming threshold, so the spool spills to disk.
        let payload = makePayload(size: 6 * 1024 * 1024, seed: 88)
        let expectedETag = Insecure.MD5.hash(data: payload).hex
        var crc = TrailerChecksum(trailerHeaderName: "x-amz-checksum-crc32")!
        crc.update(ByteBuffer(data: payload).readableBytesView)
        let expectedChecksum = crc.finalizeBase64()

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let wire = frameUnsignedTrailer(payload)
            let signed = signUnsignedTrailer(
                path: "/\(bucketName)/\(key)", decodedLength: payload.count,
                trailerHeader: "x-amz-checksum-crc32")
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: wire)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "ETag") == "\"\(expectedETag)\"")
                    #expect(res.headers.first(name: "x-amz-checksum-crc32") == expectedChecksum)
                })

            let get = signEmptyBody(method: "GET", path: "/\(bucketName)/\(key)")
            try await app.test(
                .GET, "/\(bucketName)/\(key)",
                beforeRequest: { req in req.headers.add(contentsOf: get) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(Data(res.body.readableBytesView) == payload)
                })
        }
    }

    @Test("PUT whose trailer CRC32 doesn't match is rejected and stores nothing")
    func unsignedTrailerWrongChecksum() async throws {
        let bucketName = "trailer-badcrc-bucket"
        let key = "bad.bin"
        let payload = makePayload(size: 120_000, seed: 19)

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let wire = frameUnsignedTrailer(payload, overrideChecksum: "AAAAAA==")
            let signed = signUnsignedTrailer(
                path: "/\(bucketName)/\(key)", decodedLength: payload.count,
                trailerHeader: "x-amz-checksum-crc32")
            try await app.test(
                .PUT, "/\(bucketName)/\(key)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: wire)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    #expect(res.body.string.contains("BadDigest"))
                })

            let get = signEmptyBody(method: "GET", path: "/\(bucketName)/\(key)")
            try await app.test(
                .GET, "/\(bucketName)/\(key)",
                beforeRequest: { req in req.headers.add(contentsOf: get) },
                afterResponse: { res in #expect(res.status == .notFound) })
        }
    }

    @Test("CopyObject recomputes a FULL_OBJECT checksum for the destination")
    func copyObjectChecksum() async throws {
        let bucketName = "copy-checksum-bucket"
        let payload = makePayload(size: 60_000, seed: 61)
        var crc = TrailerChecksum(trailerHeaderName: "x-amz-checksum-crc32")!
        crc.update(ByteBuffer(data: payload).readableBytesView)
        let expectedChecksum = crc.finalizeBase64()

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            // Source PUT carrying a CRC32 checksum header.
            let putSigned = signPutWithChecksumHeader(
                path: "/\(bucketName)/source.bin", payload: payload, checksum: expectedChecksum)
            try await app.test(
                .PUT, "/\(bucketName)/source.bin",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: payload)
                },
                afterResponse: { res in #expect(res.status == .ok) })

            // Copy it - the destination gets a freshly computed FULL_OBJECT checksum.
            let copySigned = sotoSigned(
                method: .PUT, path: "/\(bucketName)/dest.bin",
                additionalHeaders: ["x-amz-copy-source": "/\(bucketName)/source.bin"])
            try await app.test(
                .PUT, "/\(bucketName)/dest.bin",
                beforeRequest: { req in req.headers.add(contentsOf: copySigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.headers.first(name: "x-amz-checksum-crc32") == expectedChecksum)
                })

            // And it's retrievable on the destination with checksum-mode ENABLED.
            let headSigned = sotoSigned(
                method: .HEAD, path: "/\(bucketName)/dest.bin",
                additionalHeaders: ["x-amz-checksum-mode": "ENABLED"])
            try await app.test(
                .HEAD, "/\(bucketName)/dest.bin",
                beforeRequest: { req in req.headers.add(contentsOf: headSigned) },
                afterResponse: { res in
                    #expect(res.headers.first(name: "x-amz-checksum-crc32") == expectedChecksum)
                })
        }
    }

    @Test("GetObjectAttributes returns the requested attributes including the checksum")
    func getObjectAttributes() async throws {
        let bucketName = "attributes-bucket"
        let payload = makePayload(size: 30_000, seed: 71)
        var crc = TrailerChecksum(trailerHeaderName: "x-amz-checksum-crc32")!
        crc.update(ByteBuffer(data: payload).readableBytesView)
        let expectedChecksum = crc.finalizeBase64()

        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let putSigned = signPutWithChecksumHeader(
                path: "/\(bucketName)/obj.bin", payload: payload, checksum: expectedChecksum)
            try await app.test(
                .PUT, "/\(bucketName)/obj.bin",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: payload)
                },
                afterResponse: { res in #expect(res.status == .ok) })

            let attrSigned = sotoSigned(
                method: .GET, path: "/\(bucketName)/obj.bin", query: "attributes",
                additionalHeaders: ["x-amz-object-attributes": "ETag,Checksum,ObjectSize"])
            try await app.test(
                .GET, "/\(bucketName)/obj.bin?attributes",
                beforeRequest: { req in req.headers.add(contentsOf: attrSigned) },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    let body = res.body.string
                    #expect(body.contains("<ChecksumCRC32>\(expectedChecksum)</ChecksumCRC32>"))
                    #expect(body.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"))
                    #expect(body.contains("<ObjectSize>\(payload.count)</ObjectSize>"))
                })
        }
    }

    /// Signs a plain (non-chunked) PUT that carries a precalculated checksum as a signed
    /// `x-amz-checksum-crc32` header, with the real payload hash as `x-amz-content-sha256`.
    private func signPutWithChecksumHeader(path: String, payload: Data, checksum: String)
        -> HTTPHeaders
    {
        let (amzDate, dateStamp) = amzTimestamps()
        let payloadHash = Crypto.SHA256.hash(data: payload).hexString()
        let signed: [(String, String)] = [
            ("host", host),
            ("x-amz-checksum-crc32", checksum),
            ("x-amz-content-sha256", payloadHash),
            ("x-amz-date", amzDate),
        ]
        let names = signed.map { $0.0 }.joined(separator: ";")
        let canonicalHeaders = signed.map { "\($0.0):\($0.1)\n" }.joined()
        let canonicalRequest = ["PUT", path, "", canonicalHeaders, names, payloadHash]
            .joined(separator: "\n")
        let scope = "\(dateStamp)/\(region)/s3/aws4_request"
        let stringToSign = [
            "AWS4-HMAC-SHA256", amzDate, scope,
            Crypto.SHA256.hash(data: Data(canonicalRequest.utf8)).hexString(),
        ].joined(separator: "\n")
        let kDate = hmac(Data("AWS4\(secretKey)".utf8), dateStamp)
        let kSigning = hmac(hmac(hmac(kDate, region), "s3"), "aws4_request")
        let signature = hmac(kSigning, stringToSign).hexString()
        var headers = HTTPHeaders()
        for (name, value) in signed where name != "host" { headers.add(name: name, value: value) }
        headers.add(name: "host", value: host)
        headers.add(
            name: "authorization",
            value:
                "AWS4-HMAC-SHA256 Credential=\(accessKey)/\(scope), SignedHeaders=\(names), "
                + "Signature=\(signature)")
        return headers
    }

    /// A normal SigV4 header set for a request with an empty body (used for GET verification).
    private func signEmptyBody(method: String, path: String) -> HTTPHeaders {
        let (amzDate, dateStamp) = amzTimestamps()
        let emptyHash = Crypto.SHA256.hash(data: Data()).hexString()
        let signed: [(String, String)] = [
            ("host", host),
            ("x-amz-content-sha256", emptyHash),
            ("x-amz-date", amzDate),
        ]
        let names = signed.map { $0.0 }.joined(separator: ";")
        let canonicalHeaders = signed.map { "\($0.0):\($0.1)\n" }.joined()
        let canonicalRequest = [method, path, "", canonicalHeaders, names, emptyHash]
            .joined(separator: "\n")
        let scope = "\(dateStamp)/\(region)/s3/aws4_request"
        let stringToSign = [
            "AWS4-HMAC-SHA256", amzDate, scope,
            Crypto.SHA256.hash(data: Data(canonicalRequest.utf8)).hexString(),
        ].joined(separator: "\n")
        let kDate = hmac(Data("AWS4\(secretKey)".utf8), dateStamp)
        let kSigning = hmac(hmac(hmac(kDate, region), "s3"), "aws4_request")
        let signature = hmac(kSigning, stringToSign).hexString()
        var headers = HTTPHeaders()
        for (name, value) in signed where name != "host" { headers.add(name: name, value: value) }
        headers.add(name: "host", value: host)
        headers.add(
            name: "authorization",
            value:
                "AWS4-HMAC-SHA256 Credential=\(accessKey)/\(scope), SignedHeaders=\(names), "
                + "Signature=\(signature)")
        return headers
    }
}
