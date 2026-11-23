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

@testable import Alarik

@Suite("Object key validation")
struct ObjectKeyValidationTests {
    @Test("ordinary keys pass, including the longest allowed key and component")
    func validKeys() {
        let component = String(repeating: "a", count: ObjectKeyValidation.maxComponentBytes)
        // Four full components plus slashes, padded to exactly 1024 bytes.
        var longest = ([component, component, component, component]).joined(separator: "/")
        longest += "/" + String(repeating: "b", count: ObjectKeyValidation.maxKeyBytes - longest.utf8.count - 1)
        #expect(longest.utf8.count == ObjectKeyValidation.maxKeyBytes)

        for key in ["a", "docs/report.pdf", "folder/", "//", ".env", "ünïcödé/ключ.txt", longest] {
            #expect(ObjectKeyValidation.failure(of: key) == nil, "\(key)")
        }
    }

    @Test("a NUL is InvalidArgument; too many bytes overall or between slashes is KeyTooLongError")
    func invalidKeys() {
        #expect(ObjectKeyValidation.failure(of: "a\u{0}b") == .containsNUL)
        #expect(ObjectKeyValidation.failure(of: "a\u{0}b")?.s3Code == "InvalidArgument")

        let tooLong = String(repeating: "a/", count: ObjectKeyValidation.maxKeyBytes / 2) + "x"
        #expect(tooLong.utf8.count == ObjectKeyValidation.maxKeyBytes + 1)
        #expect(ObjectKeyValidation.failure(of: tooLong) == .tooLong)
        #expect(ObjectKeyValidation.failure(of: tooLong)?.s3Code == "KeyTooLongError")

        let component = String(repeating: "a", count: ObjectKeyValidation.maxComponentBytes + 1)
        #expect(ObjectKeyValidation.failure(of: "docs/\(component)") == .componentTooLong)
        #expect(ObjectKeyValidation.failure(of: "docs/\(component)")?.s3Code == "KeyTooLongError")

        // Limits are bytes, not characters: 81 three-byte characters are 243 bytes.
        let multibyte = String(repeating: "€", count: 81)
        #expect(multibyte.count < ObjectKeyValidation.maxComponentBytes)
        #expect(ObjectKeyValidation.failure(of: multibyte) == .componentTooLong)
    }
}
