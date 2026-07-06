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
import Vapor

@testable import Alarik

/// The `setenv`-based tests mutate process-wide environment state - `.serialized` (mirroring
/// `AlarikRegionTests`, which does the same) keeps that safe under `swift test --no-parallel`.
@Suite("Environment.sanitizedGet tests", .serialized)
struct EnvironmentSanitizedTests {

    // MARK: - stripSurroundingQuotes

    @Test("a matching pair of double quotes is stripped")
    func stripsDoubleQuotes() {
        #expect(Environment.stripSurroundingQuotes("\"alarik\"") == "alarik")
    }

    @Test("a matching pair of single quotes is stripped")
    func stripsSingleQuotes() {
        #expect(Environment.stripSurroundingQuotes("'alarik'") == "alarik")
    }

    @Test("only one pair is stripped - nested quotes survive")
    func stripsOnlyOnePair() {
        #expect(Environment.stripSurroundingQuotes("\"\"alarik\"\"") == "\"alarik\"")
    }

    @Test("unquoted values pass through byte-identical")
    func unquotedUntouched() {
        #expect(Environment.stripSurroundingQuotes("alarik") == "alarik")
        #expect(Environment.stripSurroundingQuotes("") == "")
        #expect(Environment.stripSurroundingQuotes(" spaced ") == " spaced ")
    }

    @Test("mismatched or one-sided quotes are left untouched")
    func mismatchedQuotesUntouched() {
        #expect(Environment.stripSurroundingQuotes("\"alarik'") == "\"alarik'")
        #expect(Environment.stripSurroundingQuotes("\"alarik") == "\"alarik")
        #expect(Environment.stripSurroundingQuotes("alarik\"") == "alarik\"")
        // A single quote character is its own first AND last character - it must not be
        // treated as an (empty) quoted pair.
        #expect(Environment.stripSurroundingQuotes("\"") == "\"")
        #expect(Environment.stripSurroundingQuotes("'") == "'")
    }

    @Test("interior quotes are preserved")
    func interiorQuotesPreserved() {
        #expect(Environment.stripSurroundingQuotes("pass\"word") == "pass\"word")
        #expect(Environment.stripSurroundingQuotes("\"pass\"word\"") == "pass\"word")
    }

    @Test("an empty quoted string becomes empty")
    func emptyQuotedString() {
        #expect(Environment.stripSurroundingQuotes("\"\"") == "")
        #expect(Environment.stripSurroundingQuotes("''") == "")
    }

    // MARK: - sanitizedGet end-to-end (the docker-compose scenario from GitHub issue #8)

    @Test("sanitizedGet strips the quotes docker-compose list syntax passes through literally")
    func sanitizedGetStripsQuotes() {
        let key = "ALARIK_TEST_SANITIZED_ENV"
        setenv(key, "\"alarik\"", 1)
        defer { unsetenv(key) }

        #expect(Environment.get(key) == "\"alarik\"")
        #expect(Environment.sanitizedGet(key) == "alarik")
    }

    @Test("sanitizedGet returns nil for an unset variable")
    func sanitizedGetUnset() {
        unsetenv("ALARIK_TEST_SANITIZED_ENV_UNSET")
        #expect(Environment.sanitizedGet("ALARIK_TEST_SANITIZED_ENV_UNSET") == nil)
    }
}
