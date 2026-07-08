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
import Vapor

/// A request body consumed with bounded memory. Small bodies (up to
/// `Constants.streamingThreshold`) stay in memory - the hot path for object-count-heavy
/// workloads pays no extra file IO. Larger bodies spill to a spool file on disk. Either way
/// the bytes are the *decoded* payload (aws-chunked framing already stripped) and the digests
/// were computed incrementally while the body arrived.
struct SpooledBody {
    enum Storage {
        case memory(Data)
        case file(path: String)
    }

    let storage: Storage
    let size: Int
    let md5Hex: String
    let md5Base64: String
    let sha256Hex: String

    /// Removes the spool file, if any. Call from every exit path once the payload has been
    /// consumed (or the request failed) - the spool is a transient, never the stored object
    /// itself. Safe to call after the file was renamed away (unlink of a missing path is a
    /// no-op here).
    func cleanup() {
        if case .file(let path) = storage {
            _ = POSIXFile.unlink(path)
        }
    }
}

/// Streams a request body off the wire with bounded memory, replacing
/// `S3Service.collectBodyData` for object-payload routes registered with `body: .stream`.
///
/// Everything the buffered path validated still gets validated here, just incrementally:
/// - aws-chunked framing is decoded on the fly, with every chunk's SigV4 signature verified
///   against the chain seeded by the (already header-verified) request signature
/// - the declared `x-amz-content-sha256` is checked against the actual payload
/// - `x-amz-decoded-content-length` must match the decoded byte count
/// - the configured max body size is enforced as bytes arrive, not after
enum StreamingBodySpooler {

    private static let streamingPayloadHash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    private static let unsignedPayloadHash = "UNSIGNED-PAYLOAD"

    /// Whether the whole-body SHA256 needs to be computed at all - it exists purely to be
    /// checked against `declaredSha`, so this must be false in every case where that check
    /// would be skipped anyway: aws-chunked bodies (verified per-chunk instead, by
    /// `StreamingChunkDecoder`), query-auth requests (presigned URLs don't sign the payload),
    /// and requests that didn't declare a real hash to check. Getting this wrong in either
    /// direction is a real bug, not just style: too eager skips a security check, too
    /// conservative burns CPU hashing bytes nobody will ever compare (this function exists
    /// because a production CPU profile caught exactly that mistake - the chunked path was
    /// unconditionally hashing the whole body a second time, on top of the per-chunk hashing
    /// already required for signature verification).
    static func needsWholeBodyHashVerification(
        isChunked: Bool, isQueryAuth: Bool, declaredSha: String?
    ) -> Bool {
        !isChunked && !isQueryAuth && declaredSha != nil && declaredSha != unsignedPayloadHash
    }

    /// Deletes leftover spool files from a previous unclean shutdown. Safe to call at boot:
    /// nothing references a spool file across process lifetimes.
    static func cleanupOrphans() {
        guard
            let entries = try? FileManager.default.contentsOfDirectory(
                atPath: Constants.spoolDirectory)
        else { return }
        for entry in entries {
            try? FileManager.default.removeItem(atPath: Constants.spoolDirectory + entry)
        }
    }

    /// Consumes the request body. `authInfo` must be the result of the already-completed
    /// request authentication - it seeds chunk-signature verification for aws-chunked bodies.
    /// Pass nil only for anonymous/presigned requests, which never use signed chunked
    /// encoding.
    static func spool(req: Request, authInfo: S3AuthInfo?) async throws -> SpooledBody {
        let declaredSha = req.headers.first(name: "x-amz-content-sha256")
        let isChunked = declaredSha == streamingPayloadHash

        var decoder: StreamingChunkDecoder? = nil
        if isChunked {
            guard let authInfo else {
                throw S3Error(
                    status: .forbidden, code: "AccessDenied", message: "Access Denied",
                    requestId: req.id)
            }
            decoder = StreamingChunkDecoder(
                signatureValidator: try await SigV4Validator.chunkSignatureValidator(
                    for: authInfo))
        }

        var sink = SpoolSink(requestId: req.id, threadPool: req.application.threadPool)
        defer { sink.discardOnError() }

        // The whole-body SHA256 is only meaningful when something will actually check it
        // against the client's declared hash. When the body is aws-chunked, that check is
        // moot - StreamingChunkDecoder already verifies every chunk's own SHA256 against its
        // SigV4 signature, so a second whole-body pass over the same bytes would just be
        // hashing them twice for no reason (this is precisely what a CPU time profile of a
        // PUT-heavy benchmark flagged: SHA256/MD5 compute dominated CPU time, and the
        // whole-body SHA256 in the chunked path was pure waste - it's discarded a few lines
        // below without ever being compared to anything).
        let isQueryAuth =
            req.headers.first(name: "authorization") == nil
            && req.query[String.self, at: "X-Amz-Algorithm"] != nil
        let needsWholeBodySha256 = needsWholeBodyHashVerification(
            isChunked: isChunked, isQueryAuth: isQueryAuth, declaredSha: declaredSha)

        var md5 = Insecure.MD5()
        var sha256 = needsWholeBodySha256 ? Crypto.SHA256() : nil
        var size = 0
        let maxBodySize = req.application.routes.defaultMaxBodySize.value

        func consume(_ payload: ByteBufferView) async throws {
            md5.update(data: payload)
            sha256?.update(data: payload)
            size += payload.count
            guard size <= maxBodySize else {
                throw S3Error(
                    status: .payloadTooLarge, code: "EntityTooLarge",
                    message: "Your proposed upload exceeds the maximum allowed size",
                    requestId: req.id)
            }
            try await sink.write(payload)
        }

        for try await buffer in req.body {
            if let decoder {
                try await decoder.feed(buffer, emit: consume)
            } else {
                try await consume(buffer.readableBytesView)
            }
        }

        if let decoder {
            let declaredLength = req.headers
                .first(name: "x-amz-decoded-content-length")
                .flatMap(Int.init)
            try decoder.verifyComplete(declaredDecodedLength: declaredLength)
        }

        // The deferred counterpart of the payload-hash check the SigV4 validator does for
        // buffered bodies: the signature proved the client *declared* this hash; now the bytes
        // have actually arrived, they must match it.
        var sha256Hex = ""
        if let sha256 {
            sha256Hex = sha256.finalize().hexString()
            guard let declaredSha, sha256Hex == declaredSha.lowercased() else {
                throw S3Error(
                    status: .badRequest, code: "InvalidDigest",
                    message: "Payload hash mismatch", requestId: req.id)
            }
        }

        let md5Digest = md5.finalize()
        let storage = await sink.finish()
        return SpooledBody(
            storage: storage,
            size: size,
            md5Hex: md5Digest.hexString(),
            md5Base64: Data(md5Digest).base64EncodedString(),
            sha256Hex: sha256Hex
        )
    }
}

/// Where the spooled bytes actually go: memory until `Constants.streamingThreshold`, then a
/// spool file (the buffered prefix is flushed to it on spill). Owns the file descriptor; one
/// of `finish()` / `discardOnError()` must run - the spooler pairs a `defer`red discard with
/// an explicit finish, so error paths never leak an fd or a file.
///
/// The memory path does a plain in-process append - no syscall, so no reason to leave the
/// calling executor. The disk path is real blocking IO (open/write, and directory creation
/// the first time), so every disk-touching operation here hops onto `threadPool` instead of
/// running on whatever executor is driving the request's body stream (Swift's shared
/// concurrent executor, which every other async task in the process also depends on).
private struct SpoolSink {
    private let requestId: String
    private let threadPool: NIOThreadPool
    private var memory = Data()
    private var fd: Int32 = -1
    private var filePath: String? = nil
    private var finished = false

    init(requestId: String, threadPool: NIOThreadPool) {
        self.requestId = requestId
        self.threadPool = threadPool
    }

    mutating func write(_ payload: ByteBufferView) async throws {
        if fd < 0 {
            if memory.count + payload.count <= Constants.streamingThreshold {
                memory.append(contentsOf: payload)
                return
            }
            try await spillToDisk()
        }
        try await writeToFile(payload)
    }

    /// Seals the sink and returns where the payload ended up.
    mutating func finish() async -> SpooledBody.Storage {
        finished = true
        guard fd >= 0, let filePath else {
            return .memory(memory)
        }
        let closingFd = fd
        fd = -1
        _ = try? await threadPool.runIfActive { POSIXFile.close(closingFd) }
        return .file(path: filePath)
    }

    /// No-op after a successful `finish()`; otherwise closes and deletes the spool file.
    mutating func discardOnError() {
        guard !finished else { return }
        if fd >= 0 {
            _ = POSIXFile.close(fd)
            fd = -1
        }
        if let filePath {
            _ = POSIXFile.unlink(filePath)
        }
    }

    private mutating func spillToDisk() async throws {
        let path = Constants.spoolDirectory + ".spool-" + UUID().uuidString
        let requestId = requestId
        let memorySnapshot = memory
        let newFd = try await threadPool.runIfActive { () -> Int32 in
            // Optimistic open first (see AtomicObjectWriter.init for the same pattern) - the
            // spool directory exists after the first large upload of the process's lifetime,
            // so this only pays for a stat+mkdir on the rare cold-start case.
            var newFd = POSIXFile.openWrite(path, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            if newFd < 0 && errno == ENOENT {
                try FileManager.default.createDirectory(
                    atPath: Constants.spoolDirectory, withIntermediateDirectories: true)
                newFd = POSIXFile.openWrite(path, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            }
            guard newFd >= 0 else {
                throw S3Error(
                    status: .internalServerError, code: "InternalError",
                    message: "We encountered an internal error. Please try again.",
                    requestId: requestId)
            }
            // Flush the in-memory prefix so the file holds the payload from byte 0
            try memorySnapshot.withUnsafeBytes { raw in
                try Self.writeFully(fd: newFd, raw, requestId: requestId)
            }
            return newFd
        }
        fd = newFd
        filePath = path
        memory.removeAll(keepingCapacity: false)
    }

    private func writeToFile(_ payload: ByteBufferView) async throws {
        guard !payload.isEmpty else { return }
        let fd = fd
        let requestId = requestId
        try await threadPool.runIfActive {
            try payload.withUnsafeBytes { raw in
                try Self.writeFully(fd: fd, raw, requestId: requestId)
            }
        }
    }

    private static func writeFully(
        fd: Int32, _ raw: UnsafeRawBufferPointer, requestId: String
    ) throws {
        guard let base = raw.baseAddress, raw.count > 0 else { return }
        var offset = 0
        while offset < raw.count {
            let written = POSIXFile.write(fd, base + offset, raw.count - offset)
            guard written > 0 else {
                throw S3Error(
                    status: .internalServerError, code: "InternalError",
                    message: "We encountered an internal error. Please try again.",
                    requestId: requestId)
            }
            offset += written
        }
    }
}

/// Incremental aws-chunked decoder: feeds arriving `ByteBuffer`s through a small state
/// machine, emitting only decoded payload bytes and verifying each chunk's SigV4 signature
/// as soon as that chunk's payload has fully passed through. The buffered equivalent lives
/// in `SigV4Validator.validateChunked` + `ChunkedDataDecoder`; this replaces both for
/// streaming routes.
///
/// Wire format per chunk: `<hex-size>;chunk-signature=<sig>\r\n<payload>\r\n`, terminated by
/// a zero-size chunk (whose signature is also verified). Bytes after the final chunk
/// (trailers) are ignored, matching the buffered decoder.
final class StreamingChunkDecoder {
    private enum State {
        case sizeLine
        case payload(remaining: Int)
        case trailingCRLF(remaining: Int)
        case done
    }

    private var state: State = .sizeLine
    private var lineBuffer: [UInt8] = []
    private var signatureValidator: ChunkSignatureValidator
    private var chunkHasher = Crypto.SHA256()
    private var pendingSignature = ""
    private(set) var decodedLength = 0

    private static let maxSizeLineLength = 4096
    private static let emptyPayloadHash =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    init(signatureValidator: ChunkSignatureValidator) {
        self.signatureValidator = signatureValidator
    }

    /// Feeds one arriving buffer through the state machine. `emit` receives each decoded
    /// payload slice exactly once, in order.
    func feed(_ buffer: ByteBuffer, emit: (ByteBufferView) async throws -> Void) async throws {
        let view = buffer.readableBytesView
        var pos = view.startIndex

        while pos < view.endIndex {
            switch state {
            case .done:
                // Trailer bytes after the zero chunk - ignored (parity with the buffered
                // decoder, which stops at the final chunk)
                return

            case .sizeLine:
                var completedLine = false
                while pos < view.endIndex {
                    let byte = view[pos]
                    pos = view.index(after: pos)
                    if byte == 10 {  // LF terminates the size line (CR stripped below)
                        completedLine = true
                        break
                    }
                    lineBuffer.append(byte)
                    guard lineBuffer.count <= Self.maxSizeLineLength else {
                        throw S3Error(
                            status: .badRequest, code: "InvalidArgument",
                            message: "Invalid chunk format")
                    }
                }
                if completedLine {
                    try parseSizeLine()
                }

            case .payload(let remaining):
                let available = view.distance(from: pos, to: view.endIndex)
                let take = Swift.min(remaining, available)
                let end = view.index(pos, offsetBy: take)
                let slice = view[pos..<end]
                chunkHasher.update(data: slice)
                try await emit(slice)
                pos = end
                if take == remaining {
                    try finishChunk()
                } else {
                    state = .payload(remaining: remaining - take)
                }

            case .trailingCRLF(var remaining):
                while remaining > 0 && pos < view.endIndex {
                    let byte = view[pos]
                    pos = view.index(after: pos)
                    let expected: UInt8 = remaining == 2 ? 13 : 10
                    guard byte == expected else {
                        throw S3Error(
                            status: .badRequest, code: "InvalidArgument",
                            message: "Missing trailing CRLF")
                    }
                    remaining -= 1
                }
                state = remaining == 0 ? .sizeLine : .trailingCRLF(remaining: remaining)
            }
        }
    }

    /// Must be called after the last buffer: verifies the terminating zero chunk arrived and
    /// the decoded byte count matches the signed x-amz-decoded-content-length.
    func verifyComplete(declaredDecodedLength: Int?) throws {
        guard case .done = state else {
            throw S3Error(
                status: .badRequest, code: "IncompleteBody",
                message: "The request body terminated unexpectedly")
        }
        guard let declaredDecodedLength else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Missing or invalid x-amz-decoded-content-length")
        }
        guard decodedLength == declaredDecodedLength else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Decoded content length mismatch")
        }
    }

    private func parseSizeLine() throws {
        if lineBuffer.last == 13 {
            lineBuffer.removeLast()
        }
        // Chunks after the first are preceded by a CRLF handled in .trailingCRLF, so the line
        // here is always exactly `<hex>;chunk-signature=<sig>`
        guard let line = String(bytes: lineBuffer, encoding: .utf8) else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid chunk size line")
        }
        lineBuffer.removeAll(keepingCapacity: true)

        let parts = line.components(separatedBy: ";")
        guard parts.count == 2, parts[1].hasPrefix("chunk-signature=") else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Missing chunk-signature")
        }
        guard let chunkSize = Int(parts[0], radix: 16), chunkSize >= 0 else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid chunk size hex")
        }
        pendingSignature = String(parts[1].dropFirst("chunk-signature=".count))

        if chunkSize == 0 {
            // Final chunk: empty payload, signature still part of the chain
            try signatureValidator.verify(
                chunkPayloadHashHex: Self.emptyPayloadHash,
                declaredSignature: pendingSignature)
            state = .done
        } else {
            decodedLength += chunkSize
            chunkHasher = Crypto.SHA256()
            state = .payload(remaining: chunkSize)
        }
    }

    private func finishChunk() throws {
        let chunkHash = chunkHasher.finalize().hexString()
        try signatureValidator.verify(
            chunkPayloadHashHex: chunkHash, declaredSignature: pendingSignature)
        state = .trailingCRLF(remaining: 2)
    }
}
