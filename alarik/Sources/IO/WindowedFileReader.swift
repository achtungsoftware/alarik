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

/// Streams a file region through one reusable window of `Constants.fileCopyWindowSize` bytes,
/// so a copy, hash or verify touches one window of memory however large the payload. Reuse a
/// reader across many regions (a multipart assembly) to allocate the window once.
struct WindowedFileReader {
    private var window = [UInt8](repeating: 0, count: Constants.fileCopyWindowSize)

    init() {}

    /// `size` bytes from `offset` of `fd`, window by window. `path` names the file in errors.
    mutating func read(
        fd: Int32, offset: Int, size: Int, path: String,
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) throws {
        _ = POSIXFile.lseek(fd, off_t(offset), SEEK_SET)
        var remaining = size
        while remaining > 0 {
            let toRead = Swift.min(window.count, remaining)
            let bytesRead = POSIXFile.read(fd, &window, toRead)
            // A region that ends early is damage, never a short object.
            guard bytesRead > 0 else { throw StorageError.truncatedFile(path: path) }
            try window.withUnsafeBytes { try body(UnsafeRawBufferPointer(rebasing: $0.prefix(bytesRead))) }
            remaining -= bytesRead
        }
    }

    /// A region of the file at `path`, opened for the call.
    mutating func read(
        path: String, offset: Int, size: Int, _ body: (UnsafeRawBufferPointer) throws -> Void
    ) throws {
        let fd = try Self.open(path)
        defer { _ = POSIXFile.close(fd) }
        try read(fd: fd, offset: offset, size: size, path: path, body)
    }

    /// The whole file at `path`.
    mutating func readFile(at path: String, _ body: (UnsafeRawBufferPointer) throws -> Void) throws {
        let fd = try Self.open(path)
        defer { _ = POSIXFile.close(fd) }
        var statInfo = stat()
        guard POSIXFile.fstat(fd, &statInfo) == 0 else {
            throw StorageError.statFailed(path: path, errno: errno)
        }
        try read(fd: fd, offset: 0, size: Int(statInfo.st_size), path: path, body)
    }

    private static func open(_ path: String) throws -> Int32 {
        let fd = POSIXFile.open(path, O_RDONLY)
        guard fd >= 0 else { throw StorageError.openFailed(path: path, errno: errno) }
        return fd
    }
}
