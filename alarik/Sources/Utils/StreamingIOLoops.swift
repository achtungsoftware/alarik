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

import NIOCore
import Vapor

/// Shared write/read loop primitives for moving object bytes between memory and disk in
/// bounded-memory chunks - spooling an incoming request/cluster-push body to a file, and
/// windowed reads back out for GET/HEAD/cluster-fetch/cluster-push responses. Both throw a
/// generic `IOLoopError` on a failed syscall - callers translate that into whatever domain
/// error (`S3Error`, `Abort`, `ClusterProxyError`) fits their context, since the same "the
/// underlying file operation failed" condition is reported differently across the client-facing
/// S3 API, the internal cluster routes, and the inter-node HTTP client.
enum StreamingIOLoops {
    /// Writes every byte in `raw` to `fd`, looping over partial writes (`write(2)` doesn't
    /// guarantee writing the whole buffer in one call).
    static func writeFully(fd: Int32, _ raw: UnsafeRawBufferPointer) throws {
        guard let base = raw.baseAddress, raw.count > 0 else { return }
        var offset = 0
        while offset < raw.count {
            let written = POSIXFile.write(fd, base + offset, raw.count - offset)
            guard written > 0 else { throw IOLoopError.writeFailed }
            offset += written
        }
    }

    /// Windowed `pread` loop: reads `length` bytes starting at `offset` from `fd` in chunks of
    /// at most `chunkSize`, hopping onto `threadPool` for every read syscall and invoking
    /// `onChunk` with each `ByteBuffer` as it arrives, in order.
    static func readWindowed(
        threadPool: NIOThreadPool, fd: Int32, offset: Int, length: Int, chunkSize: Int,
        onChunk: (ByteBuffer) async throws -> Void
    ) async throws {
        let allocator = ByteBufferAllocator()
        var position = offset
        var remaining = length
        while remaining > 0 {
            let toRead = Swift.min(chunkSize, remaining)
            let readPosition = position
            let chunk = try await threadPool.runIfActive { () -> ByteBuffer in
                var buffer = allocator.buffer(capacity: toRead)
                _ = try buffer.writeWithUnsafeMutableBytes(
                    minimumWritableBytes: toRead
                ) { raw in
                    let bytesRead = POSIXFile.pread(
                        fd, raw.baseAddress!, toRead, off_t(readPosition))
                    guard bytesRead > 0 else { throw IOLoopError.readFailed }
                    return bytesRead
                }
                return buffer
            }
            position += chunk.readableBytes
            remaining -= chunk.readableBytes
            try await onChunk(chunk)
        }
    }
}

enum IOLoopError: Error {
    case writeFailed
    case readFailed
}
