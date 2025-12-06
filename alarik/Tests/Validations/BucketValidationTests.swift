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

import Fluent
import Testing
import Vapor
import VaporTesting

@testable import Alarik

@Suite("Bucket validation tests", .serialized)
struct BucketValidationTests {

    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try await configure(app)
            try await app.autoMigrate()
            try await test(app)
            try await app.autoRevert()
        } catch {
            try? await app.autoRevert()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    @Test("Valid S3 Bucket Names")
    func validBucketNames() {
        let validNames = [
            "mybucket",  // Standard
            "my-bucket",  // Hyphens
            "my.bucket",  // Dots
            "123bucket",  // Starts with number
            "bucket123",  // Ends with number
            "my-bucket-123",  // Mixed
            "a.b.c",  // Multiple dots (non-adjacent)
            "abc",  // Min length (3)
            String(repeating: "a", count: 63),  // Max length (63)
        ]

        for name in validNames {
            let result = Validator.bucketName.validate(name)
            #expect(!result.isFailure, "Expected '\(name)' to be VALID, but it failed.")
        }
    }

    @Test("Invalid S3 Bucket Names - Formatting & Characters")
    func invalidBucketFormats() {
        let invalidNames = [
            "MyBucket",  // Uppercase not allowed
            "my_bucket",  // Underscores not allowed
            "-mybucket",  // Cannot start with hyphen
            "mybucket-",  // Cannot end with hyphen
            ".mybucket",  // Cannot start with dot
            "mybucket.",  // Cannot end with dot
            "my..bucket",  // Cannot have adjacent dots
            "my.-bucket",  // Dashes next to dots are technically usually okay, but strictly adjacent dots are the main failure.
            "my bucket",  // No spaces
            "my@bucket",  // No special chars
            "my-.bucket"
        ]

        for name in invalidNames {
            let result = Validator.bucketName.validate(name)
            #expect(result.isFailure, "Expected '\(name)' to be INVALID, but it succeeded.")
        }
    }

    @Test("Invalid S3 Bucket Names - Length Constraints")
    func invalidBucketLengths() {
        let tooShort = "ab"  // 2 chars
        let tooLong = String(repeating: "a", count: 64)  // 64 chars

        #expect(Validator.bucketName.validate(tooShort).isFailure, "Should fail length < 3")
        #expect(Validator.bucketName.validate(tooLong).isFailure, "Should fail length > 63")
    }

    @Test("Invalid S3 Bucket Names - IP Address Format")
    func invalidBucketIPFormat() {
        // S3 buckets cannot look exactly like an IP address
        let ipAddresses = [
            "192.168.1.1",
            "127.0.0.1",
            "8.8.8.8",
        ]

        for ip in ipAddresses {
            let result = Validator.bucketName.validate(ip)
            #expect(
                result.isFailure,
                "Bucket name '\(ip)' should fail because it looks like an IP address.")
        }
    }
}
