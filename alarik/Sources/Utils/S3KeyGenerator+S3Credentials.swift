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

#if canImport(Security)
    import Security
#endif

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
        var bytes = [UInt8](repeating: 0, count: 30)

        #if canImport(Security)
            // macOS/iOS path - use Security framework
            let result = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
            guard result == errSecSuccess else {
                fatalError("Failed to generate random bytes")
            }
        #else
            // Linux path - use /dev/urandom
            guard let file = fopen("/dev/urandom", "r") else {
                fatalError("Failed to open /dev/urandom")
            }
            defer { fclose(file) }

            let bytesRead = fread(&bytes, 1, bytes.count, file)
            guard bytesRead == bytes.count else {
                fatalError("Failed to read random bytes from /dev/urandom")
            }
        #endif

        return Data(bytes).base64EncodedString()
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
