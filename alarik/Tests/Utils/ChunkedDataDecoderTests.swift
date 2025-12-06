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
import Testing

@testable import Alarik

@Suite("ChunkedDataDecoder Tests", .serialized)
struct ChunkedDataDecoderTests {

    @Test("Decode simple single chunk without signature")
    func testSimpleSingleChunk() throws {
        var buffer = ByteBuffer()
        buffer.writeString("5\r\nHello\r\n0\r\n\r\n")

        let result = try ChunkedDataDecoder.decode(buffer: &buffer)
        let expected = Data("Hello".utf8)

        #expect(result == expected)
    }

    @Test("Decode single chunk with signature")
    func testSingleChunkWithSignature() throws {
        var buffer = ByteBuffer()
        buffer.writeString("5;chunk-signature=abc123\r\nWorld\r\n0\r\n\r\n")

        let result = try ChunkedDataDecoder.decode(buffer: &buffer)
        let expected = Data("World".utf8)

        #expect(result == expected)
    }

    @Test("Decode multiple chunks")
    func testMultipleChunks() throws {
        var buffer = ByteBuffer()
        buffer.writeString("3\r\nHel\r\n2\r\nlo\r\n1\r\n \r\n6\r\nWorld!\r\n0\r\n\r\n")

        let result = try ChunkedDataDecoder.decode(buffer: &buffer)
        let expected = Data("Hello World!".utf8)

        #expect(result == expected)
    }

    @Test("Decode multiple chunks with signatures")
    func testMultipleChunksWithSignatures() throws {
        var buffer = ByteBuffer()
        buffer.writeString(
            "3;chunk-signature=sig1\r\nHel\r\n2;chunk-signature=sig2\r\nlo\r\n6;chunk-signature=sig3\r\nWorld!\r\n0;chunk-signature=finalsig\r\n\r\n"
        )

        let result = try ChunkedDataDecoder.decode(buffer: &buffer)
        let expected = Data("HelloWorld!".utf8)

        #expect(result == expected)
    }

    @Test("Decode with trailers")
    func testWithTrailers() throws {
        var buffer = ByteBuffer()
        buffer.writeString(
            "5\r\nHello\r\n0\r\nx-amz-trailer: value\r\nAnother-Trailer: test\r\n\r\n")

        let result = try ChunkedDataDecoder.decode(buffer: &buffer)
        let expected = Data("Hello".utf8)

        #expect(result == expected)
    }

    @Test("Decode empty body (just zero chunk)")
    func testEmptyBody() throws {
        var buffer = ByteBuffer()
        buffer.writeString("0\r\n\r\n")

        let result = try ChunkedDataDecoder.decode(buffer: &buffer)
        let expected = Data()

        #expect(result == expected)
    }

    @Test("Error on incomplete data (missing chunk data)")
    func testIncompleteChunkData() throws {
        var buffer = ByteBuffer()
        buffer.writeString("5\r\nHell")  // Missing 'o' and \r\n

        #expect(throws: ChunkedDataDecoder.Error.incompleteData) {
            try ChunkedDataDecoder.decode(buffer: &buffer)
        }
    }

    @Test("Error on invalid chunk size (not hex)")
    func testInvalidChunkSize() throws {
        var buffer = ByteBuffer()
        buffer.writeString("XYZ\r\nHello\r\n0\r\n\r\n")

        #expect(throws: ChunkedDataDecoder.Error.invalidChunkSize) {
            try ChunkedDataDecoder.decode(buffer: &buffer)
        }
    }

    @Test("Error on missing CRLF after chunk")
    func testMissingCRLFAfterChunk() throws {
        var buffer = ByteBuffer()
        buffer.writeString("5\r\nHello0\r\n\r\n")  // No \r\n after "Hello"

        #expect(throws: ChunkedDataDecoder.Error.decodingFailed) {
            try ChunkedDataDecoder.decode(buffer: &buffer)
        }
    }

    @Test("Error on incomplete chunk size line")
    func testIncompleteChunkSizeLine() throws {
        var buffer = ByteBuffer()
        buffer.writeString("5")  // No \r\n

        #expect(throws: ChunkedDataDecoder.Error.incompleteData) {
            try ChunkedDataDecoder.decode(buffer: &buffer)
        }
    }

    @Test("Handle non-standard LF only (without CR)")
    func testLFOnly() throws {
        var buffer = ByteBuffer()
        buffer.writeString("5\nHello\n0\n\n")  // Using \n instead of \r\n

        let result = try ChunkedDataDecoder.decode(buffer: &buffer)
        let expected = Data("Hello".utf8)

        #expect(result == expected)
    }

    @Test("Decode large chunk size")
    func testLargeChunkSize() throws {
        let largeData = Data(repeating: 0x41, count: 0x10000)  // 65536 bytes of 'A'
        var buffer = ByteBuffer()
        buffer.writeString("10000\r\n")
        buffer.writeData(largeData)
        buffer.writeString("\r\n0\r\n\r\n")

        let result = try ChunkedDataDecoder.decode(buffer: &buffer)

        #expect(result == largeData)
    }
}
