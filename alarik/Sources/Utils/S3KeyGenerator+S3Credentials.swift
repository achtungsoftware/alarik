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
import Vapor

struct S3Credentials: Content {
    let accessKeyId: String
    let secretAccessKey: String
    let createdAt: Date
}

struct S3KeyGenerator {
    /// Generates an S3-compatible access key ID
    /// Format: 20 alphanumeric characters (uppercase)
    static func generateAccessKeyId() -> String {
        let chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return String((0..<20).map { _ in chars.randomElement()! })
    }

    /// Generates an S3-compatible secret access key
    /// Format: 40 base64 characters
    static func generateSecretAccessKey() -> String {
        Data(SecureRandomBytes.generate(count: 30)).base64EncodedString()
    }

    /// Generates a complete set of S3 credentials
    static func generateCredentials() -> S3Credentials {
        return S3Credentials(
            accessKeyId: generateAccessKeyId(),
            secretAccessKey: generateSecretAccessKey(),
            createdAt: Date()
        )
    }
}
