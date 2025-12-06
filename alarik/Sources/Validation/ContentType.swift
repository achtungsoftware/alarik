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
import Vapor

extension ValidatorResults {
    /// Result structure for Content-Type validation
    public struct ContentType {
        public let isValidContentType: Bool
    }
}

extension ValidatorResults.ContentType: ValidatorResult {
    public var isFailure: Bool {
        !self.isValidContentType
    }

    public var successDescription: String? {
        "is a valid content type"
    }

    public var failureDescription: String? {
        "is not a valid content type (expected format: type/subtype, e.g., 'image/png')"
    }
}

// RFC 6838 / RFC 2045 Compliant Regex
// - Matches type/subtype
// - Optional parameters starting with ;
// - Restricts whitespace to space only (no tabs/newlines)
private let contentTypeRegex: String =
    "^[a-zA-Z0-9!#$%&'*+\\-.^_`|~]+/[a-zA-Z0-9!#$%&'*+\\-.^_`|~]+(?: *;.*)?$"

extension Validator where T == String {
    /// Validates whether a `String` is a valid MIME Content-Type.
    public static var contentType: Validator<T> {
        .init { input in
            // 1. Length Check
            guard input.count < 256, !input.isEmpty else {
                return ValidatorResults.ContentType(isValidContentType: false)
            }

            // 2. Security Check: Whitelist Printable ASCII (0x20 - 0x7E)
            // This strictly forbids:
            // - C0 Controls (0x00-0x1F) -> Null, Tab, Newline, etc.
            // - DEL (0x7F)
            // - Extended ASCII / Unicode (0x80+)
            let isClean = input.unicodeScalars.allSatisfy { scalar in
                scalar.value >= 0x20 && scalar.value <= 0x7E
            }

            guard isClean else {
                return ValidatorResults.ContentType(isValidContentType: false)
            }

            // 3. Regex Check
            // Matches strict structure.
            guard let range = input.range(of: contentTypeRegex, options: .regularExpression),
                range.lowerBound == input.startIndex,
                range.upperBound == input.endIndex
            else {
                return ValidatorResults.ContentType(isValidContentType: false)
            }

            return ValidatorResults.ContentType(isValidContentType: true)
        }
    }
}
