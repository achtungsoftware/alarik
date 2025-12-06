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
    public struct BucketName {
        public let isValidBucketName: Bool
    }
}

extension ValidatorResults.BucketName: ValidatorResult {
    public var isFailure: Bool {
        !self.isValidBucketName
    }

    public var successDescription: String? {
        "is a valid bucket name"
    }

    public var failureDescription: String? {
        "is not a valid bucket name"
    }
}

private let bucketNameRegex: String = "^(?![0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$)(?!.*\\.\\.)(?!.*\\.-)(?!.*-\\.)[a-z0-9][a-z0-9.\\-]{1,61}[a-z0-9]$"

extension Validator where T == String {
    /// Validates whether a `String` is a valid s3 bucket name.
    public static var bucketName: Validator<T> {
        .init { input in
            guard let range = input.range(of: bucketNameRegex, options: .regularExpression),
                range.lowerBound == input.startIndex,
                range.upperBound == input.endIndex
            else {
                return ValidatorResults.BucketName(isValidBucketName: false)
            }
            return ValidatorResults.BucketName(isValidBucketName: true)
        }
    }
}
