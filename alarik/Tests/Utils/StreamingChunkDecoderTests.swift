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
import Testing

@testable import Alarik

@Suite("StreamingChunkDecoder Tests")
struct StreamingChunkDecoderTests {

    private static let emptyHash =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    private let signingKey = Data("test-signing-key-material-000000".utf8)
    private let fullDate = "20260707T120000Z"
    private let scope = "20260707/us-east-1/s3/aws4_request"
    private let seedSignature = "seedseedseedseedseedseedseedseedseedseedseedseedseedseedseedseed"

    /// Computes the chunk signature exactly like a SigV4 client would - the decoder must
    /// reproduce this chain.
    private func chunkSignature(previous: String, payload: Data) -> String {
        let payloadHash =
            payload.isEmpty
            ? Self.emptyHash : Crypto.SHA256.hash(data: payload).hexString()
        let stringToSign = """
            AWS4-HMAC-SHA256-PAYLOAD
            \(fullDate)
            \(scope)
            \(previous)
            \(Self.emptyHash)
            \(payloadHash)
            """
        let mac = HMAC<SHA256>.authenticationCode(
            for: Data(stringToSign.utf8), using: SymmetricKey(data: signingKey))
        return Data(mac).hexString()
    }

    /// Builds a complete aws-chunked wire body (including the terminating zero chunk) with a
    /// valid signature chain, returning the wire bytes and the expected decoded payload.
    private func makeWireBody(chunks: [Data]) -> (wire: Data, payload: Data) {
        var wire = Data()
        var payload = Data()
        var previous = seedSignature
        for chunk in chunks {
            let signature = chunkSignature(previous: previous, payload: chunk)
            wire.append(Data("\(String(chunk.count, radix: 16));chunk-signature=\(signature)\r\n".utf8))
            wire.append(chunk)
            wire.append(Data("\r\n".utf8))
            payload.append(chunk)
            previous = signature
        }
        let finalSignature = chunkSignature(previous: previous, payload: Data())
        wire.append(Data("0;chunk-signature=\(finalSignature)\r\n\r\n".utf8))
        return (wire, payload)
    }

    private func makeDecoder() -> StreamingChunkDecoder {
        StreamingChunkDecoder(
            signatureValidator: ChunkSignatureValidator(
                signingKey: signingKey, fullDate: fullDate, credentialScope: scope,
                seedSignature: seedSignature))
    }

    /// Feeds `wire` to a fresh decoder in `pieceSize`-byte buffers, returning the decoded
    /// payload.
    private func decode(_ wire: Data, pieceSize: Int) async throws -> (
        data: Data, decoder: StreamingChunkDecoder
    ) {
        let decoder = makeDecoder()
        var decoded = Data()
        var offset = 0
        while offset < wire.count {
            let end = Swift.min(offset + pieceSize, wire.count)
            let buffer = ByteBuffer(data: wire.subdata(in: offset..<end))
            try await decoder.feed(buffer) { decoded.append(contentsOf: $0) }
            offset = end
        }
        return (decoded, decoder)
    }

    @Test("valid multi-chunk stream decodes and verifies at any buffer split size")
    func validStreamAllSplitSizes() async throws {
        let chunkA = Data((0..<70000).map { UInt8($0 % 251) })
        let chunkB = Data((0..<1234).map { UInt8(($0 * 3) % 256) })
        let (wire, payload) = makeWireBody(chunks: [chunkA, chunkB])

        // Whole-buffer, mid-size, and pathological 1-byte deliveries must all agree -
        // the state machine can be split at literally any byte boundary
        for pieceSize in [wire.count, 8192, 3, 1] {
            let (decoded, decoder) = try await decode(wire, pieceSize: pieceSize)
            #expect(decoded == payload)
            #expect(decoder.decodedLength == payload.count)
            try decoder.verifyComplete(declaredDecodedLength: payload.count)
        }
    }

    @Test("tampered chunk payload breaks the signature chain")
    func tamperedPayloadRejected() async throws {
        let chunk = Data(repeating: 0xAB, count: 5000)
        var (wire, _) = makeWireBody(chunks: [chunk])
        // Flip one payload byte (past the size line, before the trailing CRLF)
        let sizeLineEnd = wire.range(of: Data("\r\n".utf8))!.upperBound
        wire[sizeLineEnd + 100] ^= 0xFF

        await #expect(throws: (any Error).self) {
            _ = try await decode(wire, pieceSize: 4096)
        }
    }

    @Test("tampered chunk signature is rejected")
    func tamperedSignatureRejected() async throws {
        let chunk = Data(repeating: 0x11, count: 300)
        let (wire, _) = makeWireBody(chunks: [chunk])
        var wireString = String(data: wire, encoding: .isoLatin1)!
        // Corrupt one hex digit of the first chunk signature
        wireString = wireString.replacingOccurrences(
            of: "chunk-signature=\(chunkSignature(previous: seedSignature, payload: chunk))",
            with: "chunk-signature=\(String(chunkSignature(previous: seedSignature, payload: chunk).dropLast()))0")
        let tampered = wireString.data(using: .isoLatin1)!

        await #expect(throws: (any Error).self) {
            _ = try await decode(tampered, pieceSize: 4096)
        }
    }

    @Test("missing terminating zero chunk fails verifyComplete")
    func missingZeroChunkRejected() async throws {
        let chunk = Data(repeating: 0x22, count: 100)
        var (wire, _) = makeWireBody(chunks: [chunk])
        // Drop the zero-chunk terminator entirely
        let zeroStart = wire.range(of: Data("0;chunk-signature=".utf8))!.lowerBound
        wire = wire.prefix(upTo: zeroStart)

        let (_, decoder) = try await decode(wire, pieceSize: 4096)
        #expect(throws: (any Error).self) {
            try decoder.verifyComplete(declaredDecodedLength: 100)
        }
    }

    @Test("declared decoded length mismatch is rejected")
    func decodedLengthMismatchRejected() async throws {
        let chunk = Data(repeating: 0x33, count: 100)
        let (wire, _) = makeWireBody(chunks: [chunk])
        let (_, decoder) = try await decode(wire, pieceSize: 4096)
        #expect(throws: (any Error).self) {
            try decoder.verifyComplete(declaredDecodedLength: 99)
        }
        await #expect(throws: (any Error).self) {
            let (_, decoder2) = try await decode(wire, pieceSize: 4096)
            try decoder2.verifyComplete(declaredDecodedLength: nil)
        }
    }

    @Test("trailer bytes after the zero chunk are ignored")
    func trailerBytesIgnored() async throws {
        let chunk = Data(repeating: 0x44, count: 64)
        var (wire, payload) = makeWireBody(chunks: [chunk])
        wire.append(Data("x-amz-checksum-crc32:AAAAAA==\r\n\r\n".utf8))

        let (decoded, decoder) = try await decode(wire, pieceSize: 7)
        #expect(decoded == payload)
        try decoder.verifyComplete(declaredDecodedLength: payload.count)
    }

    @Test("missing chunk-signature in size line is rejected")
    func missingSignatureRejected() async throws {
        let wire = Data("40\r\n".utf8) + Data(repeating: 0x55, count: 64) + Data("\r\n0\r\n\r\n".utf8)
        await #expect(throws: (any Error).self) {
            _ = try await decode(wire, pieceSize: 4096)
        }
    }
}
