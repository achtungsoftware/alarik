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

import Crypto
import Foundation

enum OIDCPKCE {
    /// Cryptographically random, base64url-encoded (no padding) string - suitable for `state`,
    /// `nonce`, and the PKCE `code_verifier` (all of which just need to be unguessable,
    /// URL-safe, and reasonably long).
    static func randomURLSafeString(byteCount: Int = 32) -> String {
        base64URLEncode(Data(SecureRandomBytes.generate(count: byteCount)))
    }

    /// PKCE `code_challenge` for the `S256` method, derived from a `code_verifier`.
    static func codeChallenge(forVerifier verifier: String) -> String {
        let digest = SHA256.hash(data: Data(verifier.utf8))
        return base64URLEncode(Data(digest))
    }

    private static func base64URLEncode(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }
}
