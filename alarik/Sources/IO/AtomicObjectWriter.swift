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
import NIOCore
import Vapor

/// Whether writes are flushed to stable storage before being acknowledged, via `ALARIK_FSYNC`.
/// On by default - an acknowledged PUT must survive a power failure.
/// `ALARIK_FSYNC=false` opts out for deployments that prefer throughput
/// over crash-durability - benchmarks, scratch data, or CI. The write remains atomic via
/// temp-file + rename either way; only the flush-to-medium is skipped.
///
/// Deliberately does **not** additionally fsync the parent directory after the rename
enum Durability {
    static var fsyncEnabled: Bool {
        Environment.sanitizedGet("ALARIK_FSYNC")?.lowercased() != "false"
    }

    /// Flushes `fd` to stable storage if fsync is enabled. Returns 0 on success (and always
    /// when disabled).
    static func flush(_ fd: Int32) -> Int32 {
        guard fsyncEnabled else { return 0 }
        return POSIXFile.fsyncData(fd)
    }
}

/// Writes a file atomically and durably: bytes go to a temp file in the destination's
/// directory, are fsynced (F_FULLFSYNC on Darwin), then renamed into place. Readers never
/// observe a partial file, and once `finish()` returns the payload is on stable storage
/// (unless `ALARIK_FSYNC=false`) - see `Durability` for why the rename's directory entry
/// itself isn't separately fsynced.
///
/// This replaces Foundation's `.atomic` write option, which gives the same atomicity but
/// no durability - the OS could still be holding everything in the page cache when the
/// write call returns.
struct AtomicObjectWriter {
    enum WriteError: Error, CustomStringConvertible {
        case openFailed(path: String, errno: Int32)
        case writeFailed(errno: Int32)
        case fsyncFailed(errno: Int32)
        case renameFailed(errno: Int32)
        case alreadyFinished

        var description: String {
            switch self {
            case .openFailed(let path, let code): "Could not open '\(path)' for writing (errno \(code))"
            case .writeFailed(let code): "Write failed (errno \(code))"
            case .fsyncFailed(let code): "Flush to stable storage failed (errno \(code))"
            case .renameFailed(let code): "Rename failed (errno \(code))"
            case .alreadyFinished: "Writer already finished or aborted"
            }
        }
    }

    private let fd: Int32
    private let tempPath: String
    private let finalPath: String
    private var finished = false

    /// Opens a temp file next to `finalPath` (same directory, so the rename is atomic on the
    /// same filesystem), creating parent directories as needed. The temp name never ends in
    /// `.obj`, so listings and bucket-emptiness checks can never observe an in-flight write.
    init(finalPath: String) throws {
        let fileURL = URL(fileURLWithPath: finalPath)
        let folderURL = fileURL.deletingLastPathComponent()

        self.finalPath = finalPath
        self.tempPath = folderURL.path + "/.tmp-" + UUID().uuidString

        // Optimistically open first instead of always stat-ing the parent directory before
        // every write - the directory already exists for the overwhelming majority of writes
        // (another object going into an already-established bucket/prefix), so this turns
        // the common case from two syscalls into one. Only the rare first-write-into-a-new-
        // prefix case pays for the extra round trip (open fails ENOENT, create, reopen).
        var fd = POSIXFile.openWrite(tempPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
        if fd < 0 && errno == ENOENT {
            try FileManager.default.createDirectory(
                at: folderURL, withIntermediateDirectories: true)
            fd = POSIXFile.openWrite(tempPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
        }
        guard fd >= 0 else {
            throw WriteError.openFailed(path: tempPath, errno: errno)
        }
        self.fd = fd
    }

    func write(_ data: Data) throws {
        try data.withUnsafeBytes { raw in
            try writeRaw(raw)
        }
    }

    func write(_ buffer: ByteBuffer) throws {
        try buffer.withUnsafeReadableBytes { raw in
            try writeRaw(raw)
        }
    }

    func writeRaw(_ raw: UnsafeRawBufferPointer) throws {
        guard let base = raw.baseAddress, raw.count > 0 else { return }
        var offset = 0
        while offset < raw.count {
            let written = POSIXFile.write(fd, base + offset, raw.count - offset)
            guard written > 0 else {
                throw WriteError.writeFailed(errno: errno)
            }
            offset += written
        }
    }

    /// Flushes (when enabled) and renames into place. After this returns, the object is
    /// visible to readers and - with fsync on - its payload is on stable storage.
    ///
    /// With fsync on, a *failed* flush fails the whole write: acknowledging a PUT whose bytes
    /// may still be sitting in a volatile cache would be exactly the false durability promise
    /// this writer exists to prevent.
    mutating func finish() throws {
        guard !finished else { throw WriteError.alreadyFinished }
        finished = true

        guard Durability.flush(fd) == 0 else {
            let code = errno
            _ = POSIXFile.close(fd)
            _ = POSIXFile.unlink(tempPath)
            throw WriteError.fsyncFailed(errno: code)
        }
        _ = POSIXFile.close(fd)

        guard POSIXFile.rename(tempPath, finalPath) == 0 else {
            _ = POSIXFile.unlink(tempPath)
            throw WriteError.renameFailed(errno: errno)
        }
    }

    /// Discards everything written so far. Safe to call after a failed `finish()`.
    mutating func abort() {
        guard !finished else { return }
        finished = true
        _ = POSIXFile.close(fd)
        _ = POSIXFile.unlink(tempPath)
    }
}
