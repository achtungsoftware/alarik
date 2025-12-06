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
import Testing

@testable import Alarik

@Suite("S3KeyGeneratorTests Tests", .serialized)
struct S3KeyGeneratorTests {


    @Test("Access Key ID has correct length")
    func testAccessKeyIdLength() {
        let accessKeyId = S3KeyGenerator.generateAccessKeyId()
        #expect(accessKeyId.count == 20)
    }

    @Test("Access Key ID contains only valid characters")
    func testAccessKeyIdCharacters() {
        let accessKeyId = S3KeyGenerator.generateAccessKeyId()
        let validChars = CharacterSet(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
        let accessKeyChars = CharacterSet(charactersIn: accessKeyId)
        #expect(validChars.isSuperset(of: accessKeyChars))
    }

    @Test("Access Key ID is uppercase only")
    func testAccessKeyIdUppercase() {
        let accessKeyId = S3KeyGenerator.generateAccessKeyId()
        #expect(accessKeyId == accessKeyId.uppercased())
    }

    @Test("Access Key ID generates unique values")
    func testAccessKeyIdUniqueness() {
        let count = 100
        var keys = Set<String>()

        for _ in 0..<count {
            keys.insert(S3KeyGenerator.generateAccessKeyId())
        }

        // With 36^20 possible combinations, collisions should be extremely rare
        #expect(keys.count == count)
    }

    @Test("Access Key ID does not contain lowercase letters")
    func testAccessKeyIdNoLowercase() {
        let accessKeyId = S3KeyGenerator.generateAccessKeyId()
        let lowercaseChars = CharacterSet.lowercaseLetters
        let accessKeyChars = CharacterSet(charactersIn: accessKeyId)
        #expect(lowercaseChars.intersection(accessKeyChars).isEmpty)
    }

    @Test("Secret Access Key has correct length")
    func testSecretAccessKeyLength() {
        let secretKey = S3KeyGenerator.generateSecretAccessKey()
        #expect(secretKey.count == 40)
    }

    @Test("Secret Access Key is valid Base64")
    func testSecretAccessKeyBase64Format() {
        let secretKey = S3KeyGenerator.generateSecretAccessKey()
        let validBase64Chars = CharacterSet(
            charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=")
        let secretKeyChars = CharacterSet(charactersIn: secretKey)
        #expect(validBase64Chars.isSuperset(of: secretKeyChars))
    }

    @Test("Secret Access Key can be Base64 decoded")
    func testSecretAccessKeyDecodable() {
        let secretKey = S3KeyGenerator.generateSecretAccessKey()
        let decoded = Data(base64Encoded: secretKey)
        #expect(decoded != nil)
        #expect(decoded?.count == 30)  // 30 bytes of random data
    }

    @Test("Secret Access Key generates unique values")
    func testSecretAccessKeyUniqueness() {
        let count = 100
        var keys = Set<String>()

        for _ in 0..<count {
            keys.insert(S3KeyGenerator.generateSecretAccessKey())
        }

        #expect(keys.count == count)
    }

    @Test("Secret Access Key has sufficient entropy")
    func testSecretAccessKeyEntropy() {
        let secretKey = S3KeyGenerator.generateSecretAccessKey()
        let uniqueChars = Set(secretKey)

        // Base64 encoded 30 random bytes should have good character distribution
        // Expect at least 15 unique characters out of the 64 possible Base64 chars
        #expect(uniqueChars.count >= 15)
    }

    @Test("Generates valid credentials")
    func testGenerateCredentials() {
        let credentials = S3KeyGenerator.generateCredentials()

        #expect(credentials.accessKeyId.count == 20)
        #expect(credentials.secretAccessKey.count == 40)
        #expect(credentials.createdAt <= Date())
    }

    @Test("Credentials timestamp is accurate")
    func testCredentialsTimestamp() {
        let beforeGeneration = Date()
        let credentials = S3KeyGenerator.generateCredentials()
        let afterGeneration = Date()

        #expect(credentials.createdAt >= beforeGeneration)
        #expect(credentials.createdAt <= afterGeneration)
    }

    @Test("Multiple credentials are unique")
    func testMultipleCredentialsUniqueness() {
        let count = 50
        var accessKeyIds = Set<String>()
        var secretKeys = Set<String>()

        for _ in 0..<count {
            let credentials = S3KeyGenerator.generateCredentials()
            accessKeyIds.insert(credentials.accessKeyId)
            secretKeys.insert(credentials.secretAccessKey)
        }

        #expect(accessKeyIds.count == count)
        #expect(secretKeys.count == count)
    }

    @Test("Credentials have independent randomness")
    func testCredentialsIndependentRandomness() {
        // Verify that access key and secret key don't have obvious correlation
        let credentials1 = S3KeyGenerator.generateCredentials()
        let credentials2 = S3KeyGenerator.generateCredentials()

        // Keys should be different
        #expect(credentials1.accessKeyId != credentials2.accessKeyId)
        #expect(credentials1.secretAccessKey != credentials2.secretAccessKey)

        // Access key shouldn't be derivable from secret key
        #expect(!credentials1.secretAccessKey.contains(credentials1.accessKeyId))
    }

    @Test("Access Key ID matches AWS format")
    func testAccessKeyIdAWSFormat() {
        // AWS access keys typically start with "AKIA" for IAM users
        // but programmatically generated ones just need to be 20 alphanumeric uppercase
        let accessKeyId = S3KeyGenerator.generateAccessKeyId()
        let awsPattern = "^[A-Z0-9]{20}$"
        let regex = try! NSRegularExpression(pattern: awsPattern)
        let range = NSRange(accessKeyId.startIndex..., in: accessKeyId)
        let matches = regex.firstMatch(in: accessKeyId, range: range)

        #expect(matches != nil)
    }

    @Test("Secret Access Key matches AWS format")
    func testSecretAccessKeyAWSFormat() {
        // AWS secret keys are 40 characters of base64
        let secretKey = S3KeyGenerator.generateSecretAccessKey()
        let awsPattern = "^[A-Za-z0-9+/]{40}$"
        let regex = try! NSRegularExpression(pattern: awsPattern)
        let range = NSRange(secretKey.startIndex..., in: secretKey)
        let matches = regex.firstMatch(in: secretKey, range: range)

        #expect(matches != nil)
    }

    @Test("Access Key ID has no obvious patterns")
    func testAccessKeyIdNoPatterns() {
        let keys = (0..<10).map { _ in S3KeyGenerator.generateAccessKeyId() }

        // Check that we don't see repeated sequences
        for key in keys {
            let chars = Array(key)
            var hasLongRepetition = false
            for i in 0..<(chars.count - 3) {
                if chars[i] == chars[i + 1] && chars[i + 1] == chars[i + 2]
                    && chars[i + 2] == chars[i + 3]
                {
                    hasLongRepetition = true
                    break
                }
            }
            #expect(!hasLongRepetition, "Key should not have 4+ repeated characters: \(key)")
        }
    }

    @Test("Secret Key bytes are cryptographically random")
    func testSecretKeyRandomness() {
        // Generate multiple keys and verify statistical randomness
        let keys = (0..<20).map { _ in S3KeyGenerator.generateSecretAccessKey() }
        
        // Decode to bytes
        let bytesArrays = keys.compactMap { Data(base64Encoded: $0)?.map { $0 } }
        #expect(bytesArrays.count == 20)
        
        // Check that byte values are well distributed
        let allBytes = bytesArrays.flatMap { $0 }
        let sum = allBytes.reduce(0) { Int($0) + Int($1) }
        let average = Double(sum) / Double(allBytes.count)
        
        // Average of random bytes should be close to 127.5
        // Allow wider range for smaller sample sizes
        #expect(average > 90 && average < 165, "Average byte value \(average) should be roughly centered around 127.5")
    }
}
