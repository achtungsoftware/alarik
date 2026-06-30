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

import JWTKit

/// The OIDC ID token claims Alarik cares about. `iss`/`aud`/`nonce` are checked explicitly
/// against the expected issuer/client ID/per-attempt nonce after signature verification - those
/// expected values are request-specific (the nonce especially, tied to one login attempt's
/// `OIDCStateCache` entry) and can't be known inside `verify(using:)`, which only ever sees the
/// signing algorithm.
struct OIDCIDToken: JWTPayload {
    let iss: IssuerClaim
    let aud: AudienceClaim
    let exp: ExpirationClaim
    let sub: SubjectClaim
    let email: String?
    /// Must be checked before trusting `email` to link/find a local account - an IdP can issue a
    /// token with an attacker-supplied, unverified `email` claim (e.g. a self-service profile
    /// field), and `username` doubles as the trusted join key in this system, so skipping this
    /// check would let anyone log in as any existing user just by knowing their username.
    let emailVerified: Bool?
    let nonce: String?

    enum CodingKeys: String, CodingKey {
        case iss, aud, exp, sub, email, nonce
        case emailVerified = "email_verified"
    }

    func verify(using algorithm: some JWTAlgorithm) throws {
        try exp.verifyNotExpired()
    }
}
