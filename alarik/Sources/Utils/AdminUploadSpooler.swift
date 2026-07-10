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
import Vapor

/// The console's single-file upload endpoint (`InternalBucketController.uploadObject`) used to
/// decode its whole `multipart/form-data` body via `Content.decode`, which buffers the entire
/// file in memory (Vapor's own multipart decode, plus a second `Data(buffer:)` copy on top) -
/// unbounded, unlike every object-payload path in the S3 API, which streams to disk once a body
/// exceeds `Constants.streamingThreshold` (see `StreamingBodySpooler`). This does the same for
/// the one file part this endpoint's form ever sends: `MultipartParser` (from Vapor's
/// re-exported `MultipartKit`) is fed the request body chunk by chunk as it arrives (the route
/// must be registered `body: .stream` for that), and each decoded body slice is written straight
/// through to memory-or-spool-file rather than accumulated into one big buffer first.
enum AdminUploadSpooler {
    struct SpooledFile {
        let filename: String
        let contentType: String?
        let storage: SpooledBody.Storage
        let size: Int
        let md5Hex: String

        /// Removes the spool file, if any. Call from every exit path once the payload has been
        /// written to its final destination (or the request failed).
        func cleanup() {
            if case .file(let path) = storage {
                _ = POSIXFile.unlink(path)
            }
        }
    }

    /// Streams and spools the request's one multipart file part with bounded memory. Throws a
    /// plain `Abort` (not `S3Error`) on every failure path - this endpoint lives under the
    /// admin/console JSON API, not the S3 XML API, and isn't wrapped by `S3ErrorMiddleware`.
    static func spool(req: Request) async throws -> SpooledFile {
        guard let boundary = req.headers.contentType?.parameters["boundary"] else {
            throw Abort(.badRequest, reason: "Missing multipart boundary")
        }

        let parser = MultipartParser(boundary: boundary)

        var currentHeaders = HTTPHeaders()
        var filename: String?
        var contentType: String?
        var pendingChunks: [ByteBuffer] = []

        parser.onHeader = { name, value in
            currentHeaders.add(name: name, value: value)
        }
        parser.onBody = { buffer in
            // Headers always fully precede a part's body in the multipart wire format, so by
            // the first `onBody` call for this (the only) part, `currentHeaders` already holds
            // everything needed to identify it.
            if filename == nil {
                filename = currentHeaders.first(name: "Content-Disposition")
                    .flatMap { Self.parameter("filename", from: $0) }
                contentType = currentHeaders.first(name: "Content-Type")
            }
            pendingChunks.append(buffer)
        }

        var memory = Data()
        var fd: Int32 = -1
        var filePath: String?
        var size = 0
        var md5 = Insecure.MD5()
        var succeeded = false
        defer {
            // Mirrors `SpoolSink.discardOnError()` in `StreamingBodySpooler` - a close/unlink on
            // the error path is cheap enough that even that hot-path-conscious sink does it
            // synchronously rather than hopping to the thread pool.
            if !succeeded, fd >= 0 {
                _ = POSIXFile.close(fd)
            }
            if !succeeded, let filePath {
                _ = POSIXFile.unlink(filePath)
            }
        }

        let threadPool = req.application.threadPool
        let maxBodySize = req.application.routes.defaultMaxBodySize.value

        func spillToDisk() async throws {
            let path = Constants.spoolDirectory + ".admin-upload-" + UUID().uuidString
            let memorySnapshot = memory
            let newFd = try await threadPool.runIfActive { () -> Int32 in
                var newFd = POSIXFile.openWrite(path, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
                if newFd < 0 && errno == ENOENT {
                    try FileManager.default.createDirectory(
                        atPath: Constants.spoolDirectory, withIntermediateDirectories: true)
                    newFd = POSIXFile.openWrite(path, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
                }
                guard newFd >= 0 else {
                    throw Abort(.internalServerError, reason: "Failed to open upload spool file")
                }
                try memorySnapshot.withUnsafeBytes { raw in
                    try StreamingIOLoops.writeFully(fd: newFd, raw)
                }
                return newFd
            }
            fd = newFd
            filePath = path
            memory.removeAll(keepingCapacity: false)
        }

        func write(_ buffer: ByteBuffer) async throws {
            let view = buffer.readableBytesView
            guard !view.isEmpty else { return }
            md5.update(data: view)
            size += view.count
            guard size <= maxBodySize else {
                throw Abort(.payloadTooLarge, reason: "Upload exceeds the maximum allowed size")
            }
            if fd < 0 {
                if memory.count + view.count <= Constants.streamingThreshold {
                    memory.append(contentsOf: view)
                    return
                }
                try await spillToDisk()
            }
            let writeFd = fd
            try await threadPool.runIfActive {
                try view.withUnsafeBytes { raw in
                    try StreamingIOLoops.writeFully(fd: writeFd, raw)
                }
            }
        }

        for try await chunk in req.body {
            try parser.execute(chunk)
            for pending in pendingChunks {
                try await write(pending)
            }
            pendingChunks.removeAll(keepingCapacity: true)
        }

        guard let filename, !filename.isEmpty else {
            throw Abort(.badRequest, reason: "File must have a filename")
        }

        let storage: SpooledBody.Storage
        if fd >= 0, let filePath {
            let closingFd = fd
            _ = try? await threadPool.runIfActive { POSIXFile.close(closingFd) }
            storage = .file(path: filePath)
        } else {
            storage = .memory(memory)
        }
        succeeded = true

        return SpooledFile(
            filename: filename, contentType: contentType, storage: storage, size: size,
            md5Hex: md5.finalize().hexString())
    }

    /// Extracts `key="value"` (or unquoted `key=value`) from a `;`-separated header value like
    /// `form-data; name="data"; filename="photo.png"`. `MultipartKit` has its own version of
    /// this, but it's `internal` to that module, not visible here.
    private static func parameter(_ key: String, from headerValue: String) -> String? {
        for part in headerValue.split(separator: ";") {
            let trimmed = part.trimmingCharacters(in: .whitespaces)
            guard trimmed.hasPrefix("\(key)=") else { continue }
            let value = trimmed.dropFirst(key.count + 1)
            return value.trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
        }
        return nil
    }
}
