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

@Suite("Content Type validation tests", .serialized)
struct ContentTypeValidationTests {

    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try await configure(app)  // Ensure your configure logic is accessible
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

    @Test("Valid Content Types")
    func validContentTypes() {
        let validTypes = [
            "application/json",  // Standard
            "text/html",  // Standard
            "image/png",  // Standard
            "application/vnd.api+json",  // Special characters in subtype (+.)
            "x-world/x-3dmf",  // X-prefix
            "text/html; charset=utf-8",  // With parameters
            "text/plain;charset=us-ascii",  // No space before param
            "multipart/form-data; boundary=something",  // Complex params
            "application/atom+xml",  // Plus sign
            "application/x-www-form-urlencoded",  // Long standard type
        ]

        for type in validTypes {
            let result = Validator.contentType.validate(type)
            #expect(!result.isFailure, "Expected '\(type)' to be VALID, but it failed.")
        }
    }

    @Test("Invalid Content Types - Formatting")
    func invalidContentTypeFormats() {
        let invalidTypes = [
            "application",  // Missing subtype
            "/json",  // Missing type
            "json",  // No slash
            "application/",  // Empty subtype
            "/",  // Just slash
            "application /json",  // Space in type
            "application/ json",  // Space in subtype (leading)
            "app@lication/json",  // Illegal character (@) in type
            "application/js(on",  // Illegal character (() in subtype
            " application/json",  // Leading space
            "application/json ",  // Trailing space (unless handled by trimming elsewhere, strictly invalid in regex)
        ]

        for type in invalidTypes {
            let result = Validator.contentType.validate(type)
            #expect(result.isFailure, "Expected '\(type)' to be INVALID, but it succeeded.")
        }
    }

    @Test("Invalid Content Types - Security & Control Characters")
    func securityChecks() {
        let dangerousTypes = [
            "application/json\r\n",  // CRLF Injection
            "application/json\n",  // Newline Injection
            "application/json\t",  // Tab character
            "application/json; charset=utf-8\r\nSet-Cookie: evil=true",  // Header Injection attempt
            String(format: "application/json\0", 0x00),  // Null byte
        ]

        for type in dangerousTypes {
            let result = Validator.contentType.validate(type)
            #expect(result.isFailure, "Expected dangerous string '\(type)' to be INVALID.")
        }
    }

    @Test("Invalid Content Types - Length Constraints")
    func invalidLengthChecks() {
        let empty = ""
        // Generate a string longer than 256 characters
        let tooLong = "application/" + String(repeating: "a", count: 250)

        #expect(Validator.contentType.validate(empty).isFailure, "Should fail empty string")
        #expect(Validator.contentType.validate(tooLong).isFailure, "Should fail length > 256")
    }
}
