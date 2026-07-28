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
import Testing

@testable import Alarik

@Suite("AwsFlexibleChecksum Tests")
struct AwsFlexibleChecksumTests {

    private func base64(_ headerName: String, _ input: Data) -> String? {
        guard var checksum = TrailerChecksum(trailerHeaderName: headerName) else { return nil }
        checksum.update(ByteBuffer(data: input).readableBytesView)
        return checksum.finalizeBase64()
    }

    @Test("CRC32 matches the canonical check value for \"123456789\"")
    func crc32KnownVector() {
        // 0xCBF43926, big-endian, base64-encoded.
        #expect(base64("x-amz-checksum-crc32", Data("123456789".utf8)) == "y/Q5Jg==")
    }

    @Test("CRC32C matches the canonical check value for \"123456789\"")
    func crc32cKnownVector() {
        // 0xE3069283, big-endian, base64-encoded.
        #expect(base64("x-amz-checksum-crc32c", Data("123456789".utf8)) == "4waSgw==")
    }

    @Test("CRC over empty input is zero")
    func crcEmpty() {
        #expect(base64("x-amz-checksum-crc32", Data()) == "AAAAAA==")
        #expect(base64("x-amz-checksum-crc32c", Data()) == "AAAAAA==")
    }

    @Test("SHA256 and SHA1 trailers match the known empty-input digests")
    func shaEmpty() {
        #expect(
            base64("x-amz-checksum-sha256", Data())
                == "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=")
        #expect(base64("x-amz-checksum-sha1", Data()) == "2jmj7l5rSw0yVb/vlWAYkK/YBwk=")
    }

    @Test("checksum is split-invariant - updating in pieces equals updating whole")
    func splitInvariant() {
        let data = Data((0..<10_000).map { UInt8(($0 * 31 + 7) % 256) })
        var whole = TrailerChecksum(trailerHeaderName: "x-amz-checksum-crc32c")!
        whole.update(ByteBuffer(data: data).readableBytesView)

        var pieced = TrailerChecksum(trailerHeaderName: "x-amz-checksum-crc32c")!
        var offset = 0
        for step in [1, 7, 100, 1000, 4096] {
            let end = Swift.min(offset + step, data.count)
            if offset < end {
                pieced.update(ByteBuffer(data: data.subdata(in: offset..<end)).readableBytesView)
            }
            offset = end
        }
        if offset < data.count {
            pieced.update(ByteBuffer(data: data.subdata(in: offset..<data.count)).readableBytesView)
        }
        #expect(whole.finalizeBase64() == pieced.finalizeBase64())
    }

    @Test("unknown and non-checksum header names yield no computer")
    func unsupportedNames() {
        #expect(TrailerChecksum(trailerHeaderName: "x-amz-checksum-md5") == nil)
        #expect(TrailerChecksum(trailerHeaderName: "x-amz-trailer-signature") == nil)
        #expect(TrailerChecksum(trailerHeaderName: "content-length") == nil)
    }

    @Test("header name matching is case-insensitive")
    func caseInsensitive() {
        let checksum = TrailerChecksum(trailerHeaderName: "X-Amz-Checksum-CRC32")
        #expect(checksum?.headerName == "x-amz-checksum-crc32")
    }

    @Test("COMPOSITE checksum is checksum-of-raw-part-digests with a -N suffix")
    func compositeChecksum() {
        // Two parts whose stored per-part checksums are the base64 of these raw digest bytes.
        let part1 = Data([0x01, 0x02, 0x03, 0x04])
        let part2 = Data([0x05, 0x06, 0x07, 0x08])
        let parts = [part1.base64EncodedString(), part2.base64EncodedString()]

        let composite = ObjectChecksum.composite(algorithm: "crc32", partChecksumsBase64: parts)

        // Reference: CRC32 over the concatenated raw digests, base64, then "-<partCount>".
        var reference = ReflectedCRC32.crc32()
        reference.update(ByteBuffer(data: part1 + part2).readableBytesView)
        let expected = Data(reference.finalizeBytes()).base64EncodedString() + "-2"

        #expect(composite?.value == expected)
        #expect(composite?.type == .composite)
        #expect(composite?.algorithm == "crc32")
    }

    @Test("COMPOSITE returns nil for empty parts")
    func compositeGuards() {
        #expect(ObjectChecksum.composite(algorithm: "crc32", partChecksumsBase64: []) == nil)
    }

    @Test("CRC64NVME matches the canonical check value for \"123456789\"")
    func crc64nvmeKnownVector() {
        // CRC-64/NVME("123456789") == 0xAE8B14860A799888, big-endian, base64-encoded.
        var crc = ReflectedCRC64.crc64nvme()
        crc.update(ByteBuffer(data: Data("123456789".utf8)).readableBytesView)
        let expected: [UInt8] = [0xAE, 0x8B, 0x14, 0x86, 0x0A, 0x79, 0x98, 0x88]
        #expect(crc.finalizeBytes() == expected)
        #expect(base64("x-amz-checksum-crc64nvme", Data("123456789".utf8)) == Data(expected).base64EncodedString())
    }

    /// The definitive combine test: for data split at an arbitrary point, combining the two part
    /// CRCs must equal the CRC of the whole - across all three CRC algorithms.
    @Test("CRC-combine equals the CRC of the concatenation")
    func crcCombineMatchesWhole() {
        let whole = Data((0..<9001).map { UInt8(($0 * 37 + 5) % 256) })
        for splitAt in [1, 8, 4096, 9000] {
            let a = whole.prefix(splitAt)
            let b = whole.suffix(from: splitAt)
            for (algorithm, headerAlgo) in [
                ("crc32", "crc32"), ("crc32c", "crc32c"), ("crc64nvme", "crc64nvme"),
            ] {
                let expected = base64("x-amz-checksum-\(headerAlgo)", whole)!
                let full = ObjectChecksum.fullObject(
                    algorithm: algorithm,
                    parts: [
                        (base64("x-amz-checksum-\(headerAlgo)", Data(a))!, a.count),
                        (base64("x-amz-checksum-\(headerAlgo)", Data(b))!, b.count),
                    ])
                #expect(full?.value == expected, "combine mismatch for \(algorithm) at \(splitAt)")
                #expect(full?.type == .fullObject)
            }
        }
    }

    @Test("CRC-combine is correct at real multipart part sizes (8 MiB + 2 MiB)")
    func crcCombineLargeParts() {
        func makeBytes(count: Int, seed: UInt8) -> Data {
            var bytes = [UInt8](repeating: 0, count: count)
            var state: UInt8 = seed
            for i in 0..<count {
                state = state &* 31 &+ UInt8(truncatingIfNeeded: i) &+ 11
                bytes[i] = state
            }
            return Data(bytes)
        }
        let part1 = makeBytes(count: 8 * 1024 * 1024, seed: 3)
        let part2 = makeBytes(count: 2 * 1024 * 1024, seed: 200)
        let whole = part1 + part2
        for algo in ["crc32", "crc32c", "crc64nvme"] {
            let expected = base64("x-amz-checksum-\(algo)", whole)!
            let full = ObjectChecksum.fullObject(
                algorithm: algo,
                parts: [
                    (base64("x-amz-checksum-\(algo)", part1)!, part1.count),
                    (base64("x-amz-checksum-\(algo)", part2)!, part2.count),
                ])
            #expect(full?.value == expected, "combine mismatch for \(algo) at 8MiB+2MiB")
        }
    }

    @Test("FULL_OBJECT is CRC-only and carries no -N suffix")
    func fullObjectGuards() {
        #expect(
            ObjectChecksum.fullObject(algorithm: "sha256", parts: [("AAAA", 1)]) == nil)
        let crc = ObjectChecksum.fullObject(
            algorithm: "crc32", parts: [(base64("x-amz-checksum-crc32", Data("x".utf8))!, 1)])
        #expect(crc?.value.contains("-") == false)
    }

    @Test("multipartChecksumType pins CRC64NVME to full-object and SHA to composite")
    func multipartTypeResolution() {
        #expect(ObjectChecksum.multipartChecksumType(algorithm: "crc64nvme", requested: .composite) == .fullObject)
        #expect(ObjectChecksum.multipartChecksumType(algorithm: "sha256", requested: .fullObject) == .composite)
        #expect(ObjectChecksum.multipartChecksumType(algorithm: "crc32", requested: nil) == .composite)
        #expect(ObjectChecksum.multipartChecksumType(algorithm: "crc32", requested: .fullObject) == .fullObject)
    }
}
