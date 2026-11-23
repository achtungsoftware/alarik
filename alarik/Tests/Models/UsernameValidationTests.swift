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

import Testing
import Vapor

@testable import Alarik

@Suite("Username validator tests")
struct UsernameValidationTests {
    private func isValid(_ value: String) -> Bool {
        !Validator<String>.username.validate(value).isFailure
    }

    @Test("accepts emails, handles, spaces and non-ASCII letters")
    func accepts() {
        for value in ["alice", "alice@example.com", "first.last+tag@corp.io", "a_b-c", "Jürgen Müller", "0x1"] {
            #expect(isValid(value), "\(value)")
        }
        #expect(isValid(String(repeating: "a", count: 128)))
    }

    @Test("rejects path separators, control characters, dot-prefixed, blank and overlong values")
    func rejects() {
        let bad = ["", "   ", "a/b", "a\\b", "../admin", ".lead", "a\nb", "a\0b", "a\tb",
                   String(repeating: "a", count: 129)]
        for value in bad {
            #expect(!isValid(value), "\(value.debugDescription)")
        }
    }
}
