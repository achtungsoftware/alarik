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
import Testing

@testable import Alarik

/// `resolve()` reads a process-wide environment variable, so that test sets and restores it -
/// `.serialized` mirrors the other env-reading suites.
@Suite("CORSAllowedOrigins tests", .serialized)
struct CORSAllowedOriginsTests {

    @Test("unset or blank allows no extra origins")
    func emptyWhenUnsetOrBlank() throws {
        #expect(try CORSAllowedOrigins.parse(nil) == [])
        #expect(try CORSAllowedOrigins.parse("") == [])
        #expect(try CORSAllowedOrigins.parse(" , ,") == [])
    }

    @Test("a comma-separated list is split and trimmed, in order")
    func splitsAndTrims() throws {
        let origins = try CORSAllowedOrigins.parse(
            " https://app.example.com ,http://100.64.0.1:3500, https://app.example.com:8443, http://app.example.com:443")
        #expect(
            origins == [
                "https://app.example.com", "http://100.64.0.1:3500",
                "https://app.example.com:8443", "http://app.example.com:443",
            ])
    }

    @Test("entries are lowercased, the way browsers send the Origin header")
    func lowercases() throws {
        #expect(try CORSAllowedOrigins.parse("HTTPS://App.Example.com") == ["https://app.example.com"])
    }

    @Test(
        "an entry that can never match an Origin header fails instead of being ignored",
        arguments: [
            "https://app.example.com/",
            "https://app.example.com/upload",
            "app.example.com",
            "ftp://app.example.com",
            "https://app.example.com?x=1",
            "https://user:pass@app.example.com",
            "https://*.example.com",
            "https://app.example.com:443",
            "http://app.example.com:80",
        ])
    func rejectsNonOrigins(entry: String) {
        #expect(throws: CORSAllowedOriginsError.self) {
            try CORSAllowedOrigins.parse("https://ok.example.com,\(entry)")
        }
    }

    @Test("resolve() reads CORS_ALLOWED_ORIGINS")
    func resolveReadsTheEnvironment() throws {
        let original = ProcessInfo.processInfo.environment["CORS_ALLOWED_ORIGINS"]
        defer {
            if let original {
                setenv("CORS_ALLOWED_ORIGINS", original, 1)
            } else {
                unsetenv("CORS_ALLOWED_ORIGINS")
            }
        }

        setenv("CORS_ALLOWED_ORIGINS", "\"https://app.example.com\"", 1)
        #expect(try CORSAllowedOrigins.resolve() == ["https://app.example.com"])

        unsetenv("CORS_ALLOWED_ORIGINS")
        #expect(try CORSAllowedOrigins.resolve() == [])
    }
}
