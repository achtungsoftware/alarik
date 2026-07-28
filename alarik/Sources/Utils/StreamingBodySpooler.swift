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
    /// The flexible checksum the client supplied and this spool validated (via the aws-chunked
    /// trailer or an `x-amz-checksum-*` header), for storing with the object. nil when none given.
    let checksum: ObjectChecksum?

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

/// Streams a request body off the wire with bounded memory, for routes registered with
/// `body: .stream`. Validates incrementally instead of after full buffering:
/// - aws-chunked framing is decoded on the fly, each chunk's SigV4 signature verified
/// - declared `x-amz-content-sha256` / `x-amz-decoded-content-length` checked against actuals
/// - the configured max body size is enforced as bytes arrive, not after
enum StreamingBodySpooler {

    /// Builds the checksum computer for whatever algorithm the request's `x-amz-trailer` header
    /// declares, or nil when none is declared or it names an algorithm we don't implement (those
    /// bodies are still decoded and stored - just not checksum-verified, see `TrailerChecksum`).
    private static func trailerChecksum(from req: Request) -> TrailerChecksum? {
        guard let trailer = req.headers.first(name: "x-amz-trailer") else { return nil }
        for name in trailer.split(separator: ",") {
            if let checksum = TrailerChecksum(
                trailerHeaderName: name.trimmingCharacters(in: .whitespaces))
            {
                return checksum
            }
        }
        return nil
    }

    /// Wraps a streaming-body route handler so that if `operation` throws before ever touching
    /// `req.body` (an early auth/admission rejection), the body still gets drained instead of
    /// abandoned. Vapor's `Request.BodyStream` asserts on deinit if it never receives a terminal
    /// `.end`/`.error` write - which never lands if nothing is left reading `req.body` after an
    /// early error response, crashing the process. Bounded by a 30s timeout in case `operation`
    /// already fully drained the body itself, so this second attempt would otherwise hang forever.
    static func withGuaranteedBodyDrain<T: Sendable>(
        req: Request, _ operation: () async throws -> T
    ) async throws -> T {
        do {
            return try await operation()
        } catch {
            let requestBody = req.body
            Task {
                await withTaskGroup(of: Void.self) { group in
                    group.addTask {
                        do {
                            for try await _ in requestBody {}
                        } catch {
                            // Already erroring/closing - nothing more to drain.
                        }
                    }
                    group.addTask {
                        try? await Task.sleep(for: .seconds(30))
                    }
                    await group.next()
                    group.cancelAll()
                }
            }
            throw error
        }
    }

    /// Whether the whole-body SHA256 needs to be computed at all - it exists purely to be
    /// checked against `declaredSha`, so this must be false wherever that check is skipped
    /// anyway: aws-chunked bodies (verified per-chunk by `StreamingChunkDecoder`), query-auth
    /// requests (presigned URLs don't sign the payload), and requests with no real hash to check.
    /// Getting this wrong either skips a security check or wastes CPU hashing bytes never compared.
    static func needsWholeBodyHashVerification(
        isChunked: Bool, isQueryAuth: Bool, declaredSha: String?
    ) -> Bool {
        !isChunked && !isQueryAuth && declaredSha != nil && declaredSha != S3PayloadHash.unsigned
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
        let isChunked = S3PayloadHash.isStreaming(declaredSha)

        var decoder: StreamingChunkDecoder? = nil
        if let declaredSha, isChunked {
            var signatureValidator: ChunkSignatureValidator? = nil
            if S3PayloadHash.hasChunkSignatures(declaredSha) {
                guard let authInfo else {
                    throw S3Error(
                        status: .forbidden, code: "AccessDenied", message: "Access Denied",
                        requestId: req.id)
                }
                signatureValidator = try await SigV4Validator.chunkSignatureValidator(
                    for: authInfo)
            }
            let expectsTrailer = S3PayloadHash.hasTrailer(declaredSha)
            decoder = StreamingChunkDecoder(
                signatureValidator: signatureValidator,
                expectsTrailer: expectsTrailer,
                trailerChecksum: expectsTrailer ? Self.trailerChecksum(from: req) : nil)
        }

        var sink = SpoolSink(requestId: req.id, threadPool: req.application.threadPool)
        defer { sink.discardOnError() }

        // The whole-body SHA256 is only meaningful when something will check it against the
        // client's declared hash. For aws-chunked bodies that check is moot - StreamingChunkDecoder
        // already verifies each chunk's SHA256 against its SigV4 signature, so hashing the whole
        // body again would be pure wasted CPU for a value nobody compares.
        let isQueryAuth =
            req.headers.first(name: "authorization") == nil
            && req.query[String.self, at: "X-Amz-Algorithm"] != nil
        let needsWholeBodySha256 = needsWholeBodyHashVerification(
            isChunked: isChunked, isQueryAuth: isQueryAuth, declaredSha: declaredSha)

        // Precalculated-checksum form: instead of a trailer, a client can send the value directly
        // as an `x-amz-checksum-<algo>` header (typical for non-chunked PUTs). Compute it over the
        // payload and validate below, exactly like the trailer path. A pure trailer request has no
        // such value header (only `x-amz-trailer`), so this never double-counts.
        var headerChecksum: TrailerChecksum? = nil
        var headerChecksumExpected: String? = nil
        for (name, value) in req.headers {
            if let computer = TrailerChecksum(trailerHeaderName: name) {
                headerChecksum = computer
                headerChecksumExpected = value.trimmingCharacters(in: .whitespaces)
                break
            }
        }

        var md5 = Insecure.MD5()
        var sha256 = needsWholeBodySha256 ? Crypto.SHA256() : nil
        var size = 0
        let maxBodySize = req.application.routes.defaultMaxBodySize.value

        func consume(_ payload: ByteBufferView) async throws {
            md5.update(data: payload)
            sha256?.update(data: payload)
            headerChecksum?.update(payload)
            size += payload.count
            guard size <= maxBodySize else {
                throw S3Error(
                    status: .payloadTooLarge, code: "EntityTooLarge",
                    message: "Your proposed upload exceeds the maximum allowed size",
                    requestId: req.id)
            }
            try await sink.write(payload)
        }

        // A thrown error here (oversized body, bad chunk signature, spool I/O failure) must not
        // abandon `req.body` mid-stream: the caller responds with an error immediately after, and
        // an abandoned stream never receives the terminal `.end`/`.error` write its `deinit`
        // requires, crashing the process. Drain (discard) the rest in a best-effort task instead,
        // then rethrow immediately so the client still gets a fast error response.
        do {
            for try await buffer in req.body {
                if let decoder {
                    try await decoder.feed(buffer, emit: consume)
                } else {
                    try await consume(buffer.readableBytesView)
                }
            }
        } catch {
            let requestBody = req.body
            Task {
                do {
                    for try await _ in requestBody {}
                } catch {
                    // Already erroring/closing - nothing more to drain.
                }
            }
            throw error
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

        // The stored object checksum: the trailer (validated in the decoder) takes precedence;
        // otherwise the precalculated header form is validated here now the bytes have arrived.
        var checksum = decoder?.validatedChecksum
        if checksum == nil, let headerChecksum, let expected = headerChecksumExpected {
            let computed = headerChecksum.finalizeBase64()
            guard computed == expected else {
                throw S3Error(
                    status: .badRequest, code: "BadDigest",
                    message:
                        "The \(headerChecksum.headerName) you specified did not match the "
                        + "calculated checksum.", requestId: req.id)
            }
            let algorithm = String(headerChecksum.headerName.dropFirst("x-amz-checksum-".count))
            checksum = ObjectChecksum(algorithm: algorithm, value: expected, type: .fullObject)
        }

        let md5Digest = md5.finalize()
        let storage = await sink.finish()
        return SpooledBody(
            storage: storage,
            size: size,
            md5Hex: md5Digest.hexString(),
            md5Base64: Data(md5Digest).base64EncodedString(),
            sha256Hex: sha256Hex,
            checksum: checksum
        )
    }
}

/// Where the spooled bytes actually go: memory until `Constants.streamingThreshold`, then a
/// spool file. Owns the file descriptor; one of `finish()` / `discardOnError()` must run so
/// error paths never leak an fd or a file. The memory path is a plain in-process append; the
/// disk path is real blocking IO, so it hops onto `threadPool` instead of running on the shared
/// concurrent executor every other async task in the process depends on.
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
        do {
            try StreamingIOLoops.writeFully(fd: fd, raw)
        } catch {
            throw S3Error(
                status: .internalServerError, code: "InternalError",
                message: "We encountered an internal error. Please try again.",
                requestId: requestId)
        }
    }
}

/// Incremental aws-chunked decoder: feeds arriving `ByteBuffer`s through a small state
/// machine, emitting only decoded payload bytes.
///
/// Handles the three SigV4 streaming forms (see `S3PayloadHash`):
/// - signed chunks (`<hex>;chunk-signature=<sig>\r\n<payload>\r\n`), each verified against the
///   SigV4 chunk-signature chain as its payload passes through;
/// - unsigned chunks (`<hex>\r\n<payload>\r\n`), which carry no signature - integrity rests on
///   TLS plus the optional checksum trailer;
/// - a trailer after the terminating zero chunk (`x-amz-checksum-<algo>:<base64>\r\n\r\n`),
///   whose checksum is recomputed over the decoded payload and compared, for algorithms we
///   implement (`TrailerChecksum`).
///
/// The terminating zero chunk's own (empty-payload) signature is verified in the signed forms.
/// The trailer's own `x-amz-trailer-signature` is not verified: in the signed forms every payload
/// byte is already authenticated by the chunk-signature chain, so the trailer signature adds no
/// payload-integrity guarantee; in the unsigned form there is no chain to anchor it to.
final class StreamingChunkDecoder {
    private enum State {
        case sizeLine
        case payload(remaining: Int)
        case trailingCRLF(remaining: Int)
        case trailer
        case done
    }

    private var state: State = .sizeLine
    private var lineBuffer: [UInt8] = []
    /// nil for the unsigned streaming form, where chunks carry no `chunk-signature`.
    private var signatureValidator: ChunkSignatureValidator?
    private let expectsTrailer: Bool
    /// Accumulates over the whole decoded payload (spans all chunks), to check the checksum
    /// trailer. nil when the request declared no trailer checksum we can compute.
    private var trailerChecksum: TrailerChecksum?
    private var trailerChecksumVerified = false
    private var trailerBytesConsumed = 0
    /// The checksum the trailer declared and this decoder validated, for storing with the object.
    private(set) var validatedChecksum: ObjectChecksum?
    private var chunkHasher = Crypto.SHA256()
    private var pendingSignature = ""
    private(set) var decodedLength = 0

    private static let maxSizeLineLength = 4096
    private static let maxTrailerBytes = 8192
    private static let emptyPayloadHash =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    /// - Parameters:
    ///   - signatureValidator: verifies each chunk signature; nil for the unsigned streaming form.
    ///   - expectsTrailer: whether trailer headers follow the terminating zero chunk.
    ///   - trailerChecksum: recomputes the declared trailer checksum over the payload; nil when
    ///     none was declared or its algorithm is one we don't implement (then accepted unverified).
    init(
        signatureValidator: ChunkSignatureValidator?,
        expectsTrailer: Bool = false,
        trailerChecksum: TrailerChecksum? = nil
    ) {
        self.signatureValidator = signatureValidator
        self.expectsTrailer = expectsTrailer
        self.trailerChecksum = trailerChecksum
    }

    /// Feeds one arriving buffer through the state machine. `emit` receives each decoded
    /// payload slice exactly once, in order.
    func feed(_ buffer: ByteBuffer, emit: (ByteBufferView) async throws -> Void) async throws {
        let view = buffer.readableBytesView
        var pos = view.startIndex

        while pos < view.endIndex {
            switch state {
            case .done:
                // Bytes after the trailer's terminating blank line - nothing more to consume.
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
                if signatureValidator != nil {
                    chunkHasher.update(data: slice)
                }
                trailerChecksum?.update(slice)
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

            case .trailer:
                var completedLine = false
                while pos < view.endIndex {
                    let byte = view[pos]
                    pos = view.index(after: pos)
                    // Bound the whole trailer section, not just one line: trailer bytes aren't
                    // emitted as payload, so they bypass the body-size limit in the spool loop.
                    trailerBytesConsumed += 1
                    guard trailerBytesConsumed <= Self.maxTrailerBytes else {
                        throw S3Error(
                            status: .badRequest, code: "InvalidArgument",
                            message: "Trailer too large")
                    }
                    if byte == 10 {
                        completedLine = true
                        break
                    }
                    lineBuffer.append(byte)
                }
                if completedLine {
                    let done = try consumeTrailerLine()
                    if done {
                        try finalizeTrailer()
                        state = .done
                    }
                }
            }
        }
    }

    /// Must be called after the last buffer: verifies the terminating zero chunk arrived and
    /// the decoded byte count matches the signed x-amz-decoded-content-length. Tolerates a
    /// trailer whose closing blank line never arrived, as long as the declared checksum did.
    func verifyComplete(declaredDecodedLength: Int?) throws {
        if case .trailer = state {
            // A last trailer line with no closing blank line: parse the residue, then finalize.
            if !lineBuffer.isEmpty {
                _ = try consumeTrailerLine()
            }
            try finalizeTrailer()
            state = .done
        }
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
        // here is exactly `<hex>` (unsigned) or `<hex>;chunk-signature=<sig>` (signed).
        guard let line = String(bytes: lineBuffer, encoding: .utf8) else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid chunk size line")
        }
        lineBuffer.removeAll(keepingCapacity: true)

        let parts = line.components(separatedBy: ";")
        guard let chunkSize = Int(parts[0], radix: 16), chunkSize >= 0 else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid chunk size hex")
        }
        if signatureValidator != nil {
            guard parts.count == 2, parts[1].hasPrefix("chunk-signature=") else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument",
                    message: "Missing chunk-signature")
            }
            pendingSignature = String(parts[1].dropFirst("chunk-signature=".count))
        }

        if chunkSize == 0 {
            // Final chunk: empty payload, signature still part of the chain in the signed forms.
            try signatureValidator?.verify(
                chunkPayloadHashHex: Self.emptyPayloadHash, declaredSignature: pendingSignature)
            state = expectsTrailer ? .trailer : .done
        } else {
            decodedLength += chunkSize
            if signatureValidator != nil {
                chunkHasher = Crypto.SHA256()
            }
            state = .payload(remaining: chunkSize)
        }
    }

    private func finishChunk() throws {
        if signatureValidator != nil {
            let chunkHash = chunkHasher.finalize().hexString()
            try signatureValidator?.verify(
                chunkPayloadHashHex: chunkHash, declaredSignature: pendingSignature)
        }
        state = .trailingCRLF(remaining: 2)
    }

    /// Parses one accumulated trailer line. Returns true when it was the terminating blank line.
    /// Validates the declared checksum trailer against the payload as soon as it arrives.
    private func consumeTrailerLine() throws -> Bool {
        if lineBuffer.last == 13 {
            lineBuffer.removeLast()
        }
        guard let line = String(bytes: lineBuffer, encoding: .utf8) else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid trailer")
        }
        lineBuffer.removeAll(keepingCapacity: true)

        if line.isEmpty { return true }

        guard let colon = line.firstIndex(of: ":") else {
            // A trailer line without a colon (e.g. stray whitespace) - ignore it.
            return false
        }
        let name = line[..<colon].lowercased()
        let value = line[line.index(after: colon)...].trimmingCharacters(in: .whitespaces)

        if let trailerChecksum, name == trailerChecksum.headerName {
            let computed = trailerChecksum.finalizeBase64()
            guard computed == value else {
                throw S3Error(
                    status: .badRequest, code: "BadDigest",
                    message: "The \(name) you specified did not match the calculated checksum.")
            }
            trailerChecksumVerified = true
            let algorithm = String(name.dropFirst("x-amz-checksum-".count))
            validatedChecksum = ObjectChecksum(
                algorithm: algorithm, value: value, type: .fullObject)
        }
        return false
    }

    /// After the trailer: a declared, computable checksum must actually have been checked.
    private func finalizeTrailer() throws {
        if trailerChecksum != nil && !trailerChecksumVerified {
            throw S3Error(
                status: .badRequest, code: "InvalidRequest",
                message: "The declared checksum trailer was not sent with the request.")
        }
    }
}
