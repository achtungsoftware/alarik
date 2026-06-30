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

#if canImport(Security)
    import Security
#endif

/// Cryptographically secure random bytes, cross-platform (Security framework on Apple
/// platforms, `/dev/urandom` on Linux).
enum SecureRandomBytes {
    static func generate(count: Int) -> [UInt8] {
        var bytes = [UInt8](repeating: 0, count: count)

        #if canImport(Security)
            let result = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
            guard result == errSecSuccess else {
                fatalError("Failed to generate random bytes")
            }
        #else
            guard let file = fopen("/dev/urandom", "r") else {
                fatalError("Failed to open /dev/urandom")
            }
            defer { fclose(file) }

            let bytesRead = fread(&bytes, 1, bytes.count, file)
            guard bytesRead == bytes.count else {
                fatalError("Failed to read random bytes from /dev/urandom")
            }
        #endif

        return bytes
    }
}
