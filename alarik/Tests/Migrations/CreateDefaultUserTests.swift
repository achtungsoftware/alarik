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

/// `generateRandomPassword` is the fallback used when a release deployment boots without
/// `ADMIN_PASSWORD` set - it must never degrade into a fixed, guessable value.
@Suite("CreateDefaultUser tests")
struct CreateDefaultUserTests {
    @Test("generateRandomPassword returns a 48-character hex string")
    func generatesHexStringOfExpectedLength() {
        let password = CreateDefaultUser.generateRandomPassword()
        #expect(password.count == 48)
        #expect(password.allSatisfy { $0.isHexDigit })
    }

    @Test("generateRandomPassword is different on every call")
    func generatesUniqueValues() {
        let passwords = (0..<20).map { _ in CreateDefaultUser.generateRandomPassword() }
        #expect(Set(passwords).count == passwords.count)
    }
}
