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

/// OIDC SSO login - sits alongside the existing local username/password login
/// (`InternalUserController.login`), issuing the exact same `SessionToken` JWT on success.
/// Providers are admin-managed (see `InternalAdminOIDCProviderController`), not per-user - a
/// deployment can offer several simultaneously, each shown as its own button on the login page.
///
/// A local `User` must already exist with `username` equal to the value the IdP sends in the ID
/// token's `email` claim. The first successful OIDC login for that email links the account to
/// that specific provider (stores `oidcProviderId` + the IdP's `sub` - subjects are only unique
/// within one provider's namespace, so both are required to identify a link); every login after
/// that is matched directly by the `(providerId, sub)` pair.
///
/// If no local account matches: when `ALLOW_ACCOUNT_CREATION=true` (the same env switch that
/// gates the console's local signup form) a new non-admin account is auto-provisioned with
/// `username` = the verified email and linked immediately; otherwise the login is rejected.
struct InternalAuthOIDCController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("auth", "oidc").get("providers", use: self.providers)
        routes.grouped("auth", "oidc").get("login", ":providerId", use: self.login)
        routes.grouped("auth", "oidc").get("callback", use: self.callback)
    }

    @Sendable
    func providers(req: Request) async throws -> [OIDCProvider.PublicDTO] {
        let providers = try await OIDCProvider.query(on: req.db)
            .filter(\.$enabled == true)
            .sort(\.$createdAt, .ascending)
            .all()

        return try providers.map { OIDCProvider.PublicDTO(id: try $0.requireID(), name: $0.name) }
    }

    @Sendable
    func login(req: Request) async throws -> Response {
        guard let providerId = req.parameters.get("providerId", as: UUID.self) else {
            throw Abort(.notFound)
        }
        guard
            let provider = try await OIDCProvider.query(on: req.db)
                .filter(\.$id == providerId)
                .filter(\.$enabled == true)
                .first()
        else {
            throw Abort(.notFound)
        }

        let (discovery, _) = try await OIDCDiscoveryCache.shared.resolve(
            issuerURL: provider.issuerURL, client: req.client)

        let state = OIDCPKCE.randomURLSafeString()
        let nonce = OIDCPKCE.randomURLSafeString()
        let codeVerifier = OIDCPKCE.randomURLSafeString()
        let codeChallenge = OIDCPKCE.codeChallenge(forVerifier: codeVerifier)

        try await OIDCStateCache.shared.store(
            on: req.db, state: state, providerId: providerId, nonce: nonce,
            codeVerifier: codeVerifier)

        var components = URLComponents(string: discovery.authorizationEndpoint)
        components?.queryItems = [
            URLQueryItem(name: "client_id", value: provider.clientId),
            URLQueryItem(name: "redirect_uri", value: Self.redirectURL),
            URLQueryItem(name: "response_type", value: "code"),
            URLQueryItem(name: "scope", value: "openid email profile"),
            URLQueryItem(name: "state", value: state),
            URLQueryItem(name: "nonce", value: nonce),
            URLQueryItem(name: "code_challenge", value: codeChallenge),
            URLQueryItem(name: "code_challenge_method", value: "S256"),
        ]

        guard let authorizationURL = components?.url?.absoluteString else {
            throw Abort(.internalServerError, reason: "Failed to build OIDC authorization URL.")
        }

        let response = req.redirect(to: authorizationURL)
        // Binds this flow to the browser that actually started it - without this, `state` alone
        // only proves the callback carries a value this server once issued to *someone*, not
        // that it's the same browser. An attacker can start their own login (getting a valid
        // code+state for their own IdP identity) and lure a victim into loading the callback URL
        // with the attacker's values, silently signing the victim into the attacker's account
        // (login CSRF). Requiring the callback to also present this cookie means only the
        // browser that received this exact redirect can complete the flow. HttpOnly (never
        // needed by JS), SameSite=Lax (must survive the top-level redirect back from the IdP,
        // which Strict would block), short-lived (only needs to outlive the IdP round trip).
        response.cookies["oidc_state"] = HTTPCookies.Value(
            string: state, expires: Date().addingTimeInterval(600), maxAge: 600,
            isSecure: !Self.redirectURL.hasPrefix("http://localhost"), isHTTPOnly: true,
            sameSite: .lax)
        return response
    }

    /// Single fixed callback URL shared by every provider - admins register this exact value
    /// with whichever IdP they connect, regardless of how many providers are configured.
    static var redirectURL: String {
        "\(apiBaseURL)/api/v1/auth/oidc/callback"
    }

    private struct TokenExchangeRequest: Content {
        let grant_type: String
        let code: String
        let redirect_uri: String
        let client_id: String
        let client_secret: String
        let code_verifier: String
    }

    private struct TokenExchangeResponse: Content {
        let id_token: String
    }

    @Sendable
    func callback(req: Request) async throws -> Response {
        let completeURL = "\(ConsoleBaseURL.resolve())/auth/oidc/complete"

        func completeRedirect(fragment: String) -> Response {
            var components = URLComponents(string: completeURL)
            components?.fragment = fragment
            let response = req.redirect(to: components?.url?.absoluteString ?? completeURL)
            // One-shot: whether this flow succeeded or failed, the cookie set in login() has
            // done its job and must not be replayable for a second attempt.
            response.cookies["oidc_state"] = HTTPCookies.Value(string: "", expires: .distantPast)
            return response
        }

        func errorRedirect(_ code: String) -> Response {
            completeRedirect(fragment: "error=\(code)")
        }

        guard
            let code = req.query[String.self, at: "code"],
            let state = req.query[String.self, at: "state"]
        else {
            return errorRedirect("missing_code_or_state")
        }

        // The cookie set in login() proves this callback is being completed by the same browser
        // that started the flow - without it, `state` alone only proves the value came from
        // this server at some point, not that it's bound to this browser (see login()'s comment
        // for the login-CSRF scenario this closes). Checked before consuming the state entry so
        // a forged callback can't burn a legitimate, still-pending state.
        guard let stateCookie = req.cookies["oidc_state"]?.string, stateCookie == state else {
            return errorRedirect("missing_or_mismatched_state_cookie")
        }

        guard let stateEntry = try await OIDCStateCache.shared.consume(on: req.db, state: state)
        else {
            return errorRedirect("invalid_or_expired_state")
        }

        guard let provider = try await OIDCProvider.find(stateEntry.providerId, on: req.db) else {
            return errorRedirect("invalid_or_expired_state")
        }

        let (discovery, _) = try await OIDCDiscoveryCache.shared.resolve(
            issuerURL: provider.issuerURL, client: req.client)

        let tokenResponse = try await req.client.post(URI(string: discovery.tokenEndpoint)) {
            clientReq in
            try clientReq.content.encode(
                TokenExchangeRequest(
                    grant_type: "authorization_code",
                    code: code,
                    redirect_uri: Self.redirectURL,
                    client_id: provider.clientId,
                    client_secret: provider.clientSecret,
                    code_verifier: stateEntry.codeVerifier
                ),
                as: .urlEncodedForm
            )
        }.get()

        guard tokenResponse.status == .ok else {
            return errorRedirect("token_exchange_failed")
        }

        guard let tokenBody = try? tokenResponse.content.decode(TokenExchangeResponse.self) else {
            return errorRedirect("token_exchange_failed")
        }

        let idToken: OIDCIDToken
        do {
            idToken = try await OIDCDiscoveryCache.shared.verifyIDToken(
                tokenBody.id_token, issuerURL: provider.issuerURL, client: req.client)
        } catch {
            // Logged (not just a generic redirect) so an IdP outage during the key-rotation
            // retry is distinguishable in logs from an actually forged/malformed token.
            req.logger.error("OIDC ID token verification failed: \(error)")
            return errorRedirect("invalid_id_token")
        }

        guard idToken.iss.value == discovery.issuer else {
            return errorRedirect("issuer_mismatch")
        }

        guard (try? idToken.aud.verifyIntendedAudience(includes: provider.clientId)) != nil else {
            return errorRedirect("audience_mismatch")
        }

        guard let nonce = idToken.nonce, nonce == stateEntry.nonce else {
            return errorRedirect("nonce_mismatch")
        }

        guard let email = idToken.email, !email.isEmpty else {
            return errorRedirect("missing_email_claim")
        }

        let providerId = try provider.requireID()

        let user: User
        if let existingByLink = try await User.query(on: req.db)
            .filter(\.$oidcProviderId == providerId)
            .filter(\.$oidcSubject == idToken.sub.value)
            .first()
        {
            // Already an established link from a prior verified login - re-trusting `email`
            // isn't necessary here, the link itself is the trust anchor.
            user = existingByLink
        } else {
            // First-login linking trusts the `email` claim to find an existing account, so the
            // claim must be verified - an IdP that lets a caller set an arbitrary, unverified
            // `email` would otherwise let anyone log in as any existing user just by knowing
            // their username (which doubles as their email in this system). The same applies to
            // auto-provisioning below: an unverified email must never mint an account either.
            guard idToken.emailVerified == true else {
                return errorRedirect("email_not_verified")
            }
            if let existingByUsername = try await User.query(on: req.db)
                .filter(\.$username == email)
                .first()
            {
                existingByUsername.oidcProviderId = providerId
                existingByUsername.oidcSubject = idToken.sub.value
                try await existingByUsername.save(on: req.db)
                user = existingByUsername
            } else if Environment.sanitizedGet("ALLOW_ACCOUNT_CREATION") == "true" {
                // Auto-provision, gated by the same env switch as the console's local signup
                // form. Deliberately a strict string comparison with no DEBUG bypass - an
                // account springing into existence from an SSO login is exactly the kind of
                // thing that must never happen because of a build-configuration default.
                let newUser = User(
                    name: idToken.name ?? email,
                    username: email,
                    // Random throwaway password: the account starts SSO-only. The user can set
                    // a real password later via account settings if local login is wanted.
                    passwordHash: try Bcrypt.hash(OIDCPKCE.randomURLSafeString()),
                    isAdmin: false
                )
                newUser.oidcProviderId = providerId
                newUser.oidcSubject = idToken.sub.value
                do {
                    try await newUser.save(on: req.db)
                } catch {
                    // Unique-constraint race: another request created this username between our
                    // lookup and save. Surface a retryable error rather than a 500.
                    if let dbError = error as? any DatabaseError, dbError.isConstraintFailure {
                        return errorRedirect("account_creation_failed")
                    }
                    throw error
                }
                user = newUser
            } else {
                return errorRedirect("no_matching_account")
            }
        }

        let payload = try SessionToken(with: user)
        let token = try await req.jwt.sign(payload)

        return completeRedirect(fragment: "token=\(token)")
    }
}
