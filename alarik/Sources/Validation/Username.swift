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

import Vapor

extension ValidatorResults {
    public struct Username {
        public let isValidUsername: Bool
    }
}

extension ValidatorResults.Username: ValidatorResult {
    public var isFailure: Bool {
        !self.isValidUsername
    }

    public var successDescription: String? {
        "is a valid username"
    }

    public var failureDescription: String? {
        "must be 1-128 characters with no slashes or control characters, and not start with a dot"
    }
}

extension Validator where T == String {
    /// Usernames double as metadata-store ids and on-disk path segments. This is the minimum
    /// that keeps them safe there, not a style rule: bounded length, no path separators, no
    /// control characters, not dot-prefixed (hidden on disk), not blank.
    public static var username: Validator<T> {
        .init { input in
            let hasForbiddenCharacter = input.contains { character in
                character == "/" || character == "\\"
                    || character.unicodeScalars.contains {
                        $0.properties.generalCategory == .control
                    }
            }
            let isValid =
                !input.isEmpty && input.count <= 128 && !input.hasPrefix(".")
                && !input.allSatisfy(\.isWhitespace) && !hasForbiddenCharacter
            return ValidatorResults.Username(isValidUsername: isValid)
        }
    }
}
