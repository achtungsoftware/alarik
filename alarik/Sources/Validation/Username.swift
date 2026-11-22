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
        "must be 1-128 characters: letters, digits, or @ . _ + - (starting with a letter or digit)"
    }
}

private let usernameRegex: String = "^[A-Za-z0-9][A-Za-z0-9@._+-]{0,127}$"

extension Validator where T == String {
    /// Usernames double as metadata-store ids and on-disk path segments, so the charset is a
    /// whitelist: no `/`, whitespace or control characters, bounded length.
    public static var username: Validator<T> {
        .init { input in
            guard let range = input.range(of: usernameRegex, options: .regularExpression),
                range.lowerBound == input.startIndex,
                range.upperBound == input.endIndex
            else {
                return ValidatorResults.Username(isValidUsername: false)
            }
            return ValidatorResults.Username(isValidUsername: true)
        }
    }
}
