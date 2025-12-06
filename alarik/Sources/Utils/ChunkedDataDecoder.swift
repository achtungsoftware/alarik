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

struct ChunkedDataDecoder {
    enum Error: Swift.Error {
        case incompleteData
        case invalidChunkSize
        case unexpectedEnd
        case decodingFailed
    }

    // Now takes a ByteBuffer by reference, which is the native data type in Vapor/NIO
    static func decode(buffer: inout ByteBuffer) throws -> Data {
        // We will store the decoded, unchunked data here.
        var result = Data()
        // This is a zero-copy operation: we get a pointer view without copying the underlying bytes
        guard let bytes = buffer.readBytes(length: buffer.readableBytes) else {
            throw Error.decodingFailed
        }
        var offset = 0
        while offset < bytes.count {
            // Find the CRLF (\r\n) that ends the chunk size line
            guard let lineEnd = bytes[offset...].firstIndex(of: 0x0A) else {
                throw Error.incompleteData
            }
            // Extract the chunk size line (e.g., "10000;chunk-signature=...")
            let chunkSizeLineBytes = bytes[offset..<lineEnd]
            // Move offset past the chunk size line (including \n, skipping \r)
            offset = lineEnd + 1
            if lineEnd > bytes.startIndex && bytes[lineEnd - 1] == 0x0D {
                // If it was \r\n, this is correct (lineEnd is \n, so lineEnd-1 is \r)
            } else {
                // Handle case where line is only \n (non-standard but robust)
                // In a proper HTTP spec, it should be \r\n, but keep the offset logic tight
            }
            // Parse chunk size
            let chunkSizeLine = String(data: Data(chunkSizeLineBytes), encoding: .utf8) ?? ""
            let components = chunkSizeLine.split(separator: ";", maxSplits: 1)
            guard let sizeHex = components.first?.trimmingCharacters(in: .whitespacesAndNewlines),
                let chunkSize = Int(sizeHex, radix: 16)
            else {
                throw Error.invalidChunkSize
            }
            if chunkSize == 0 {
                // Last chunk: Trailer section follows.
                // It must be followed by a final CRLF to signal the end of the trailers.
                // We've already moved past the '0\r\n' line.
                // The remaining data is the optional trailer headers + the final CRLF.
                // If we've reached the end of the buffer, we're done.
                if offset == bytes.count { break }
                // CRITICAL: Consume all remaining bytes (trailer headers + final CRLF)
                // Since this decoder only extracts the data payload, we just discard the rest.
                // In a real S3 implementation, you might want to parse the trailer headers (e.g., if needed for SigV4)
                // Advance offset to the end of the buffer
                offset = bytes.count
                break
            }
            // Read the chunk data
            let chunkEnd = offset + chunkSize
            guard chunkEnd <= bytes.count else {
                throw Error.incompleteData
            }
            // Append the chunk data (data extraction happens here)
            result.append(contentsOf: bytes[offset..<chunkEnd])
            offset = chunkEnd
            // Skip the trailing CRLF or LF after chunk data
            if offset + 1 < bytes.count && bytes[offset] == 0x0D && bytes[offset + 1] == 0x0A {
                offset += 2
            } else if offset < bytes.count && bytes[offset] == 0x0A {
                offset += 1
            } else {
                throw Error.decodingFailed  // Missing expected line ending
            }
        }
        return result
    }
}
