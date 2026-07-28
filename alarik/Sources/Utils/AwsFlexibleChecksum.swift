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

/// One reflected, table-driven CRC (init 0xFFFFFFFF, final XOR 0xFFFFFFFF) - the shape S3's
/// flexible-checksum trailers use for both CRC32 (IEEE 802.3) and CRC32C (Castagnoli). The two
/// differ only in the polynomial, so they share this code and just differ by table.
struct ReflectedCRC32 {
    private let table: [UInt32]
    private var crc: UInt32 = 0xFFFF_FFFF

    fileprivate init(table: [UInt32]) {
        self.table = table
    }

    mutating func update(_ bytes: ByteBufferView) {
        var c = crc
        for byte in bytes {
            c = table[Int((c ^ UInt32(byte)) & 0xFF)] ^ (c >> 8)
        }
        crc = c
    }

    mutating func update(_ raw: UnsafeRawBufferPointer) {
        var c = crc
        for byte in raw {
            c = table[Int((c ^ UInt32(byte)) & 0xFF)] ^ (c >> 8)
        }
        crc = c
    }

    /// The 4-byte big-endian checksum, matching how S3 base64-encodes CRC trailer values.
    func finalizeBytes() -> [UInt8] {
        let value = crc ^ 0xFFFF_FFFF
        return [
            UInt8((value >> 24) & 0xFF), UInt8((value >> 16) & 0xFF),
            UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF),
        ]
    }

    private static func makeTable(reversedPolynomial poly: UInt32) -> [UInt32] {
        var table = [UInt32](repeating: 0, count: 256)
        for i in 0..<256 {
            var c = UInt32(i)
            for _ in 0..<8 {
                c = (c & 1) != 0 ? (poly ^ (c >> 1)) : (c >> 1)
            }
            table[i] = c
        }
        return table
    }

    // Tables are ~1KB each and built once; the reversed polynomials are the standard constants
    // for IEEE 802.3 (0xEDB88320) and Castagnoli/CRC32C (0x82F63B78).
    static func crc32() -> ReflectedCRC32 {
        ReflectedCRC32(table: ieeeTable)
    }
    static func crc32c() -> ReflectedCRC32 {
        ReflectedCRC32(table: castagnoliTable)
    }
    private static let ieeeTable = makeTable(reversedPolynomial: 0xEDB8_8320)
    private static let castagnoliTable = makeTable(reversedPolynomial: 0x82F6_3B78)
}

/// The 64-bit reflected CRC S3 uses for `crc64nvme` (init/final XOR all-ones), same table-driven
/// shape as `ReflectedCRC32` at 64 bits. The reversed polynomial `0x9A6C9329AC4BC9B5` is the
/// reflection of the CRC-64/NVME polynomial `0xAD93D23594C93659`.
struct ReflectedCRC64 {
    private let table: [UInt64]
    private var crc: UInt64 = 0xFFFF_FFFF_FFFF_FFFF

    fileprivate init(table: [UInt64]) {
        self.table = table
    }

    mutating func update(_ bytes: ByteBufferView) {
        var c = crc
        for byte in bytes {
            c = table[Int((c ^ UInt64(byte)) & 0xFF)] ^ (c >> 8)
        }
        crc = c
    }

    mutating func update(_ raw: UnsafeRawBufferPointer) {
        var c = crc
        for byte in raw {
            c = table[Int((c ^ UInt64(byte)) & 0xFF)] ^ (c >> 8)
        }
        crc = c
    }

    /// The 8-byte big-endian checksum, matching how S3 base64-encodes CRC64NVME trailer values.
    func finalizeBytes() -> [UInt8] {
        let value = crc ^ 0xFFFF_FFFF_FFFF_FFFF
        return (0..<8).map { UInt8((value >> (56 - $0 * 8)) & 0xFF) }
    }

    private static func makeTable(reversedPolynomial poly: UInt64) -> [UInt64] {
        var table = [UInt64](repeating: 0, count: 256)
        for i in 0..<256 {
            var c = UInt64(i)
            for _ in 0..<8 {
                c = (c & 1) != 0 ? (poly ^ (c >> 1)) : (c >> 1)
            }
            table[i] = c
        }
        return table
    }

    static func crc64nvme() -> ReflectedCRC64 {
        ReflectedCRC64(table: nvmeTable)
    }
    private static let nvmeTable = makeTable(reversedPolynomial: 0x9A6C_9329_AC4B_C9B5)
}

/// Combines two reflected-CRC values into the CRC of their concatenation without re-reading the
/// data - the GF(2) matrix method zlib's `crc32_combine` uses, generalized to any width and
/// reversed polynomial. This is what lets a multipart FULL_OBJECT CRC be derived from the parts'
/// stored CRCs (CRC linearization is exactly why S3 restricts full-object multipart to CRCs).
enum CRCCombine {
    /// `crc(dataA || dataB)` given `crc1 = crc(dataA)`, `crc2 = crc(dataB)`, and `len2 = |dataB|`,
    /// operating on the final (conditioned) CRC values as stored.
    static func combine(
        _ crc1: UInt64, _ crc2: UInt64, len2: Int, width: Int, reversedPolynomial: UInt64
    ) -> UInt64 {
        guard len2 > 0 else { return crc1 }
        var even = [UInt64](repeating: 0, count: width)  // operator for 2^k zero bits
        var odd = [UInt64](repeating: 0, count: width)  // operator for one zero bit, then doubled

        odd[0] = reversedPolynomial
        var row: UInt64 = 1
        for n in 1..<width {
            odd[n] = row
            row <<= 1
        }
        square(&even, odd, width)  // 2 zero bits
        square(&odd, even, width)  // 4 zero bits

        var crc = crc1
        var len = UInt64(len2)
        repeat {
            square(&even, odd, width)
            if len & 1 != 0 { crc = times(even, crc) }
            len >>= 1
            if len == 0 { break }
            square(&odd, even, width)
            if len & 1 != 0 { crc = times(odd, crc) }
            len >>= 1
        } while len != 0

        crc ^= crc2
        return width >= 64 ? crc : (crc & ((UInt64(1) << width) - 1))
    }

    private static func times(_ matrix: [UInt64], _ vector: UInt64) -> UInt64 {
        var sum: UInt64 = 0
        var vec = vector
        var index = 0
        while vec != 0 {
            if vec & 1 != 0 { sum ^= matrix[index] }
            vec >>= 1
            index += 1
        }
        return sum
    }

    private static func square(_ result: inout [UInt64], _ matrix: [UInt64], _ width: Int) {
        for n in 0..<width {
            result[n] = times(matrix, matrix[n])
        }
    }
}

/// Incrementally computes the S3 flexible-checksum trailer value for one algorithm, so it can be
/// checked against the `x-amz-checksum-*` value a client sends after an aws-chunked body. The
/// algorithm is chosen from the trailer header name; algorithms we can't compute (e.g. CRC64NVME)
/// return `nil` from `init`, and the caller accepts those bodies unverified rather than rejecting
/// a valid upload it merely can't check.
struct TrailerChecksum {
    /// The `x-amz-checksum-<name>` trailer header this computer validates.
    let headerName: String

    private enum Backing {
        case crc(ReflectedCRC32)
        case crc64(ReflectedCRC64)
        case sha256(Crypto.SHA256)
        case sha1(Crypto.Insecure.SHA1)
    }
    private var backing: Backing

    /// Builds a computer for a trailer header name like `x-amz-checksum-crc32`. Case-insensitive.
    /// Returns nil for a name that is not an `x-amz-checksum-*` header or names an algorithm we do
    /// not implement.
    init?(trailerHeaderName rawName: String) {
        let name = rawName.lowercased()
        guard name.hasPrefix("x-amz-checksum-") else { return nil }
        let algorithm = String(name.dropFirst("x-amz-checksum-".count))
        switch algorithm {
        case "crc32": backing = .crc(.crc32())
        case "crc32c": backing = .crc(.crc32c())
        case "crc64nvme": backing = .crc64(.crc64nvme())
        case "sha256": backing = .sha256(Crypto.SHA256())
        case "sha1": backing = .sha1(Crypto.Insecure.SHA1())
        default: return nil  // any future algorithm: accepted, not verified
        }
        headerName = name
    }

    mutating func update(_ bytes: ByteBufferView) {
        switch backing {
        case .crc(var crc):
            crc.update(bytes)
            backing = .crc(crc)
        case .crc64(var crc):
            crc.update(bytes)
            backing = .crc64(crc)
        case .sha256(var hasher):
            hasher.update(data: bytes)
            backing = .sha256(hasher)
        case .sha1(var hasher):
            hasher.update(data: bytes)
            backing = .sha1(hasher)
        }
    }

    /// Streaming update from a raw buffer (e.g. a file-copy window), no intermediate allocation.
    mutating func update(rawBufferPointer raw: UnsafeRawBufferPointer) {
        switch backing {
        case .crc(var crc):
            crc.update(raw)
            backing = .crc(crc)
        case .crc64(var crc):
            crc.update(raw)
            backing = .crc64(crc)
        case .sha256(var hasher):
            hasher.update(bufferPointer: raw)
            backing = .sha256(hasher)
        case .sha1(var hasher):
            hasher.update(bufferPointer: raw)
            backing = .sha1(hasher)
        }
    }

    /// The base64 checksum, in the exact form S3 puts in the trailer header value.
    func finalizeBase64() -> String {
        switch backing {
        case .crc(let crc):
            return Data(crc.finalizeBytes()).base64EncodedString()
        case .crc64(let crc):
            return Data(crc.finalizeBytes()).base64EncodedString()
        case .sha256(let hasher):
            return Data(hasher.finalize()).base64EncodedString()
        case .sha1(let hasher):
            return Data(hasher.finalize()).base64EncodedString()
        }
    }
}

/// A checksum stored with an object and echoed back on GET/HEAD (when the request asks for it) and
/// on PutObject/CompleteMultipartUpload. `algorithm` is the lowercase S3 suffix (`crc32`,
/// `crc32c`, `sha1`, `sha256`); `value` is the base64 checksum, carrying a `-N` part-count suffix
/// for a COMPOSITE multipart checksum.
struct ObjectChecksum: Codable, Equatable {
    enum ChecksumType: String, Codable {
        case fullObject = "FULL_OBJECT"
        case composite = "COMPOSITE"
    }

    var algorithm: String
    var value: String
    var type: ChecksumType

    /// The `x-amz-checksum-<algo>` response header carrying this checksum.
    var headerName: String { "x-amz-checksum-\(algorithm)" }

    /// The S3 COMPOSITE object checksum for a multipart upload: the checksum (in the same
    /// algorithm) over the concatenated *raw* digest bytes of each part in order, base64-encoded,
    /// with a `-<partCount>` suffix. `partChecksumsBase64` are the parts' stored per-part
    /// checksums, already in ascending part-number order. Returns nil for an algorithm we don't
    /// implement or a malformed stored part checksum.
    static func composite(algorithm: String, partChecksumsBase64: [String]) -> ObjectChecksum? {
        guard !partChecksumsBase64.isEmpty,
            var accumulator = TrailerChecksum(trailerHeaderName: "x-amz-checksum-\(algorithm)")
        else { return nil }
        var concatenated = Data()
        for base64 in partChecksumsBase64 {
            guard let raw = Data(base64Encoded: base64) else { return nil }
            concatenated.append(raw)
        }
        accumulator.update(ByteBuffer(data: concatenated).readableBytesView)
        return ObjectChecksum(
            algorithm: algorithm,
            value: "\(accumulator.finalizeBase64())-\(partChecksumsBase64.count)",
            type: .composite)
    }

    /// Width and reversed polynomial for a CRC algorithm, or nil for non-CRC (SHA) algorithms -
    /// only CRCs linearize, which is why S3 restricts full-object multipart checksums to them.
    private static func crcParameters(_ algorithm: String) -> (
        width: Int, reversedPolynomial: UInt64, byteCount: Int
    )? {
        switch algorithm {
        case "crc32": return (32, 0xEDB8_8320, 4)
        case "crc32c": return (32, 0x82F6_3B78, 4)
        case "crc64nvme": return (64, 0x9A6C_9329_AC4B_C9B5, 8)
        default: return nil
        }
    }

    /// The S3 FULL_OBJECT multipart checksum: the CRC of the whole object, derived from the parts'
    /// CRCs and lengths via `CRCCombine` (no data re-read), with no `-N` suffix. CRC-only; returns
    /// nil for SHA algorithms or a malformed stored part checksum.
    static func fullObject(
        algorithm: String, parts: [(checksumBase64: String, length: Int)]
    ) -> ObjectChecksum? {
        guard let params = crcParameters(algorithm), !parts.isEmpty else { return nil }
        func toUInt(_ base64: String) -> UInt64? {
            guard let raw = Data(base64Encoded: base64), raw.count == params.byteCount else {
                return nil
            }
            return raw.reduce(UInt64(0)) { ($0 << 8) | UInt64($1) }
        }
        guard var accumulator = toUInt(parts[0].checksumBase64) else { return nil }
        for part in parts.dropFirst() {
            guard let partCRC = toUInt(part.checksumBase64) else { return nil }
            accumulator = CRCCombine.combine(
                accumulator, partCRC, len2: part.length, width: params.width,
                reversedPolynomial: params.reversedPolynomial)
        }
        let bytes = (0..<params.byteCount).map {
            UInt8((accumulator >> ((params.byteCount - 1 - $0) * 8)) & 0xFF)
        }
        return ObjectChecksum(
            algorithm: algorithm, value: Data(bytes).base64EncodedString(), type: .fullObject)
    }

    /// The checksum type S3 uses for a multipart object in `algorithm`, honoring the client's
    /// requested type where the algorithm allows a choice: CRC64NVME is full-object only, SHA is
    /// composite only, and CRC32/CRC32C default to composite unless FULL_OBJECT was requested.
    static func multipartChecksumType(algorithm: String, requested: ChecksumType?) -> ChecksumType {
        switch algorithm {
        case "crc64nvme": return .fullObject
        case "crc32", "crc32c": return requested ?? .composite
        default: return .composite  // sha1 / sha256
        }
    }
}
