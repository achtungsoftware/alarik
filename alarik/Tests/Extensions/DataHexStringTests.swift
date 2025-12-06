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
import Testing

@testable import Alarik

@Suite("Data and Digest HexString Extension Tests", .serialized)
struct DataHexStringTests {

    // MARK: - Data.hexString() Tests

    @Test("Empty Data returns empty string")
    func emptyData() {
        let data = Data()
        #expect(data.hexString() == "")
    }

    @Test("Single byte converts correctly")
    func singleByte() {
        let data = Data([0x00])
        #expect(data.hexString() == "00")

        let data2 = Data([0xFF])
        #expect(data2.hexString() == "ff")

        let data3 = Data([0x0A])
        #expect(data3.hexString() == "0a")
    }

    @Test("Multiple bytes convert correctly")
    func multipleBytes() {
        let data = Data([0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF])
        #expect(data.hexString() == "0123456789abcdef")
    }

    @Test("All possible byte values (0x00-0xFF)")
    func allByteValues() {
        var bytes = [UInt8]()
        for i in 0...255 {
            bytes.append(UInt8(i))
        }
        let data = Data(bytes)
        let hex = data.hexString()

        // Verify length
        #expect(hex.count == 512)  // 256 bytes * 2 chars each

        // Verify lowercase
        #expect(hex.allSatisfy({ $0.isHexDigit }))
        #expect(hex == hex.lowercased())
    }

    @Test("UTF-8 string to hex")
    func utf8StringToHex() {
        let string = "Hello"
        let data = Data(string.utf8)
        #expect(data.hexString() == "48656c6c6f")
    }

    @Test("Binary data with zeros")
    func binaryWithZeros() {
        let data = Data([0x00, 0x00, 0x00, 0x00])
        #expect(data.hexString() == "00000000")
    }

    @Test("SHA256 hash converts to 64-character hex string")
    func sha256Hash() {
        let data = Data("test".utf8)
        let hash = SHA256.hash(data: data)
        let hexString = Data(hash).hexString()

        #expect(hexString.count == 64)  // SHA256 = 32 bytes = 64 hex chars
        #expect(hexString.allSatisfy({ $0.isHexDigit }))
    }

    // MARK: - Digest.hexString() Tests

    @Test("SHA256 Digest converts correctly")
    func sha256DigestConversion() {
        let data = Data("Hello, World!".utf8)
        let digest = SHA256.hash(data: data)
        let hexString = digest.hexString()

        // SHA256 always produces 32 bytes = 64 hex characters
        #expect(hexString.count == 64)
        #expect(hexString.allSatisfy({ $0.isHexDigit }))
        #expect(hexString == hexString.lowercased())
    }

    @Test("Empty data SHA256 hash")
    func emptyDataDigest() {
        let digest = SHA256.hash(data: Data())
        let hexString = digest.hexString()

        // Known SHA256 hash of empty string
        #expect(hexString == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    }

    @Test("Known SHA256 test vectors")
    func knownTestVectors() {
        // Test vector 1: "abc"
        let data1 = Data("abc".utf8)
        let digest1 = SHA256.hash(data: data1)
        #expect(
            digest1.hexString()
                == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")

        // Test vector 2: empty string
        let data2 = Data()
        let digest2 = SHA256.hash(data: data2)
        #expect(
            digest2.hexString()
                == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    }

    @Test("Digest and Data hexString produce same result")
    func digestAndDataConsistency() {
        let testData = Data("consistency check".utf8)
        let digest = SHA256.hash(data: testData)

        let digestHex = digest.hexString()
        let dataHex = Data(digest).hexString()

        #expect(digestHex == dataHex)
    }

    // MARK: - Data.constantTimeCompare() Tests

    @Test("Identical strings compare as equal")
    func identicalStrings() {
        let data = Data([0x01, 0x23, 0x45, 0x67])
        let hexString = "01234567"

        #expect(data.constantTimeCompare(to: hexString) == true)
    }

    @Test("Different strings compare as not equal")
    func differentStrings() {
        let data = Data([0x01, 0x23, 0x45, 0x67])
        let hexString = "01234568"  // Last digit different

        #expect(data.constantTimeCompare(to: hexString) == false)
    }

    @Test("Different length strings return false")
    func differentLengths() {
        let data = Data([0x01, 0x23, 0x45, 0x67])
        let hexString = "012345"  // Too short

        #expect(data.constantTimeCompare(to: hexString) == false)
    }

    @Test("Empty data and empty string compare as equal")
    func emptyComparison() {
        let data = Data()
        #expect(data.constantTimeCompare(to: "") == true)
    }

    @Test("Case sensitivity in comparison")
    func caseSensitivity() {
        let data = Data([0xAB, 0xCD, 0xEF])

        // Lowercase should match (our hexString produces lowercase)
        #expect(data.constantTimeCompare(to: "abcdef") == true)

        // Uppercase should NOT match (constant time compare is case-sensitive)
        #expect(data.constantTimeCompare(to: "ABCDEF") == false)
    }

    @Test("Single bit difference detected")
    func singleBitDifference() {
        let data = Data([0x00])

        #expect(data.constantTimeCompare(to: "00") == true)
        #expect(data.constantTimeCompare(to: "01") == false)  // Single bit flip
        #expect(data.constantTimeCompare(to: "80") == false)  // Different bit flip
    }

    @Test("SHA256 signature comparison")
    func signatureComparison() {
        let secretKey = Data("my-secret-key".utf8)
        let message = Data("important message".utf8)

        // Create HMAC signature
        let key = SymmetricKey(data: secretKey)
        let signature = HMAC<SHA256>.authenticationCode(for: message, using: key)
        let signatureData = Data(signature)

        // Valid signature should match
        let hexSignature = signatureData.hexString()
        #expect(signatureData.constantTimeCompare(to: hexSignature) == true)

        // Modified signature should not match
        var tamperedHex = hexSignature
        let lastIndex = tamperedHex.index(before: tamperedHex.endIndex)
        tamperedHex.replaceSubrange(lastIndex...lastIndex, with: "0")
        #expect(signatureData.constantTimeCompare(to: tamperedHex) == false)
    }

    @Test("All zeros vs all ones")
    func extremeValues() {
        let zeros = Data([0x00, 0x00, 0x00, 0x00])
        let ones = Data([0xFF, 0xFF, 0xFF, 0xFF])

        #expect(zeros.constantTimeCompare(to: "00000000") == true)
        #expect(zeros.constantTimeCompare(to: "ffffffff") == false)
        #expect(ones.constantTimeCompare(to: "ffffffff") == true)
        #expect(ones.constantTimeCompare(to: "00000000") == false)
    }

    @Test("Long signature comparison (64 bytes)")
    func longSignatureComparison() {
        // Simulate a 64-character (32-byte) signature
        var bytes = [UInt8]()
        for i in 0..<32 {
            bytes.append(UInt8(i))
        }
        let data = Data(bytes)
        let hexString = data.hexString()

        #expect(hexString.count == 64)
        #expect(data.constantTimeCompare(to: hexString) == true)

        // Tamper with middle character
        var tamperedHex = hexString
        let midIndex = hexString.index(hexString.startIndex, offsetBy: 32)
        tamperedHex.replaceSubrange(midIndex...midIndex, with: "x")
        #expect(data.constantTimeCompare(to: tamperedHex) == false)
    }

    // MARK: - Performance Characteristics Tests

    @Test("Constant time comparison always processes full string")
    func constantTimeCharacteristic() {
        let data = Data([0x01, 0x02, 0x03, 0x04])

        // These should all take similar time (constant time property)
        // First character different
        let result1 = data.constantTimeCompare(to: "FF020304")

        // Last character different
        let result2 = data.constantTimeCompare(to: "010203FF")

        // All characters different
        let result3 = data.constantTimeCompare(to: "FFFFFFFF")

        #expect(result1 == false)
        #expect(result2 == false)
        #expect(result3 == false)

        // This test validates behavior but not actual timing
        // In a real security audit, you'd measure timing with high-precision timers
    }

    @Test("Hex string conversion is deterministic")
    func deterministicConversion() {
        let data = Data([0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0])

        // Convert multiple times and ensure consistency
        let hex1 = data.hexString()
        let hex2 = data.hexString()
        let hex3 = data.hexString()

        #expect(hex1 == hex2)
        #expect(hex2 == hex3)
        #expect(hex1 == "123456789abcdef0")
    }
}
