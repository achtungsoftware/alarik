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
import Foundation
import JWTKit
import Testing
import Vapor
import VaporTesting

@testable import Alarik

@Suite("InternalAuthOIDCController tests", .serialized)
struct InternalAuthOIDCControllerTests {

    // MARK: - Fake IdP

    /// A minimal fake OIDC identity provider: a real, listening HTTP server (not a stand-in
    /// seam in production code) serving a discovery document, a JWKS endpoint, and a token
    /// endpoint, so these tests exercise the actual HTTP + JWT verification code paths in
    /// `InternalAuthOIDCController` rather than something that could pass while the real wiring
    /// is broken.
    final class FakeOIDCProvider: Sendable {
        /// An actor, not a plain class: its properties are written from the test's task and
        /// read from the fake server's route closures, which run on SwiftNIO's own event-loop
        /// threads - a plain `@unchecked Sendable` class here previously caused writes from the
        /// test to not reliably be visible to the route closures (no memory barrier between the
        /// two threads), making every token come back signed with stale claims.
        actor State {
            var issuerURL = ""
            var sub = "fake-sub"
            var email: String?
            // Defaults to verified, matching real IdPs (Google/Okta/etc. always send this once
            // the `email` scope is granted) - tests that specifically need an unverified email
            // override this explicitly, exercising the rejection path on its own.
            var emailVerified: Bool? = true
            var nonce: String?
            var audience = ""
            var expired = false
            var tokenStatus: HTTPStatus = .ok
            /// When set, the discovery document reports THIS as its `issuer` instead of the
            /// real `issuerURL` - used to test that a self-reported issuer mismatch is rejected.
            var discoveryIssuerOverride: String?

            func set(
                issuerURL: String? = nil, sub: String? = nil, email: String?? = nil,
                emailVerified: Bool?? = nil, nonce: String?? = nil, audience: String? = nil,
                expired: Bool? = nil, tokenStatus: HTTPStatus? = nil,
                discoveryIssuerOverride: String?? = nil
            ) {
                if let issuerURL { self.issuerURL = issuerURL }
                if let sub { self.sub = sub }
                if let email { self.email = email }
                if let emailVerified { self.emailVerified = emailVerified }
                if let nonce { self.nonce = nonce }
                if let audience { self.audience = audience }
                if let expired { self.expired = expired }
                if let tokenStatus { self.tokenStatus = tokenStatus }
                if let discoveryIssuerOverride { self.discoveryIssuerOverride = discoveryIssuerOverride }
            }
        }

        let app: Application
        let state: State
        let baseURL: String
        let clientId: String
        let clientSecret = "test-client-secret"

        private init(app: Application, state: State, baseURL: String, clientId: String) {
            self.app = app
            self.state = state
            self.baseURL = baseURL
            self.clientId = clientId
        }

        static func start(clientId: String = "test-client-id") async throws -> FakeOIDCProvider {
            let app = try await Application.make(.testing)
            let state = State()
            await state.set(audience: clientId)

            let privateKey = try EdDSA.PrivateKey(curve: .ed25519)
            let kid = JWKIdentifier(string: "test-kid")
            let signingKeys = await JWTKeyCollection().add(eddsa: privateKey, kid: kid)

            let xBase64URL =
                privateKey.publicKey.rawRepresentation.base64EncodedString()
                .replacingOccurrences(of: "+", with: "-")
                .replacingOccurrences(of: "/", with: "_")
                .replacingOccurrences(of: "=", with: "")

            let jwksJSON =
                """
                {"keys":[{"kty":"OKP","crv":"Ed25519","kid":"test-kid","x":"\(xBase64URL)"}]}
                """

            app.get(".well-known", "openid-configuration") { req -> Response in
                let issuerURL = await state.issuerURL
                let reportedIssuer = await state.discoveryIssuerOverride ?? issuerURL
                let res = Response()
                try res.content.encode(
                    OIDCDiscoveryDocument(
                        issuer: reportedIssuer,
                        authorizationEndpoint: "\(issuerURL)/authorize",
                        tokenEndpoint: "\(issuerURL)/token",
                        jwksURI: "\(issuerURL)/jwks"
                    ))
                return res
            }

            app.get("jwks") { req -> Response in
                Response(
                    status: .ok, headers: ["content-type": "application/json"],
                    body: .init(string: jwksJSON))
            }

            app.post("token") { req -> Response in
                guard await state.tokenStatus == .ok else {
                    return Response(status: await state.tokenStatus)
                }
                let idToken = OIDCIDToken(
                    iss: IssuerClaim(value: await state.issuerURL),
                    aud: AudienceClaim(value: [await state.audience]),
                    exp: ExpirationClaim(
                        value: await state.expired
                            ? Date().addingTimeInterval(-60) : Date().addingTimeInterval(300)),
                    sub: SubjectClaim(value: await state.sub),
                    email: await state.email,
                    emailVerified: await state.emailVerified,
                    nonce: await state.nonce
                )
                let jwt = try await signingKeys.sign(idToken, kid: kid)
                let res = Response()
                try res.content.encode(["id_token": jwt])
                return res
            }

            try await app.server.start(address: .hostname("127.0.0.1", port: 0))
            guard let port = app.http.server.shared.localAddress?.port else {
                throw Abort(.internalServerError, reason: "Fake IdP failed to bind a port.")
            }
            let issuerURL = "http://127.0.0.1:\(port)"
            await state.set(issuerURL: issuerURL)

            return FakeOIDCProvider(app: app, state: state, baseURL: issuerURL, clientId: clientId)
        }

        func shutdown() async throws {
            await app.server.shutdown()
            try await app.asyncShutdown()
        }
    }

    // MARK: - Test app helpers

    /// Creates the main test app, optionally pre-loading a DB `OIDCProvider` row pointing at
    /// `fakeProvider` (enabled by default - pass `enabled: false` to test the disabled path).
    /// Returns the created provider's id to the test closure, or `nil` if none was created.
    private func withApp(
        oidc fakeProvider: FakeOIDCProvider? = nil,
        enabled: Bool = true,
        _ test: (Application, UUID?) async throws -> Void
    ) async throws {
        let app = try await Application.make(.testing)

        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()

            var providerId: UUID?
            if let fakeProvider {
                let dbProvider = OIDCProvider(
                    name: "Test SSO",
                    issuerURL: fakeProvider.baseURL,
                    clientId: fakeProvider.clientId,
                    clientSecret: fakeProvider.clientSecret,
                    enabled: enabled
                )
                try await dbProvider.save(on: app.db)
                providerId = try dbProvider.requireID()
            }

            try await test(app, providerId)
            try await app.autoRevert()
        } catch {
            try? StorageHelper.cleanStorage()
            try? await app.autoRevert()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    private func createUser(_ app: Application, username: String, isAdmin: Bool = false) async throws
        -> User
    {
        let user = User(
            name: "OIDC Test User", username: username,
            passwordHash: try Bcrypt.hash("TestPass123!"), isAdmin: isAdmin)
        try await user.save(on: app.db)
        return user
    }

    /// Hits `/login/:providerId`, captures the `state`/`nonce` the controller generated from the
    /// redirect's query string - needed so the fake provider's next ID token can be issued with
    /// a matching `nonce`, and so `/callback` can be called with a known `state`.
    private func performLoginRedirect(_ app: Application, providerId: UUID) async throws -> (
        state: String, nonce: String
    ) {
        var location: String?
        try await app.test(
            .GET, "/api/v1/auth/oidc/login/\(providerId)",
            afterResponse: { res async throws in
                #expect(res.status == .seeOther)
                location = res.headers.first(name: .location)
            })

        let state = try #require(
            location.flatMap { URLComponents(string: $0) }?.queryItems?.first(where: {
                $0.name == "state"
            })?.value)
        let nonce = try #require(
            location.flatMap { URLComponents(string: $0) }?.queryItems?.first(where: {
                $0.name == "nonce"
            })?.value)
        return (state, nonce)
    }

    private func fragment(from location: String?) -> [String: String] {
        guard let location, let hashRange = location.range(of: "#") else { return [:] }
        let fragment = String(location[hashRange.upperBound...])
        var result: [String: String] = [:]
        for pair in fragment.split(separator: "&") {
            let parts = pair.split(separator: "=", maxSplits: 1)
            if parts.count == 2 {
                result[String(parts[0])] = String(parts[1])
            }
        }
        return result
    }

    // MARK: - /providers

    @Test("providers is empty when none are configured")
    func providersEmptyWhenNoneConfigured() async throws {
        try await withApp { app, _ in
            try await app.test(
                .GET, "/api/v1/auth/oidc/providers",
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let body = try res.content.decode([OIDCProvider.PublicDTO].self)
                    #expect(body.isEmpty)
                })
        }
    }

    @Test("providers lists enabled providers, excluding disabled ones")
    func providersListsOnlyEnabled() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider, enabled: true) { app, providerId in
            let disabled = OIDCProvider(
                name: "Disabled SSO", issuerURL: "https://example.com", clientId: "x",
                clientSecret: "y", enabled: false)
            try await disabled.save(on: app.db)

            try await app.test(
                .GET, "/api/v1/auth/oidc/providers",
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let body = try res.content.decode([OIDCProvider.PublicDTO].self)
                    #expect(body.count == 1)
                    #expect(body.first?.id == providerId)
                    #expect(body.first?.name == "Test SSO")
                })
        }
    }

    // MARK: - /login

    @Test("login 404s for an unknown provider id")
    func loginUnknownProvider() async throws {
        try await withApp { app, _ in
            try await app.test(
                .GET, "/api/v1/auth/oidc/login/\(UUID())",
                afterResponse: { res async throws in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("login 404s for a disabled provider")
    func loginDisabledProvider() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider, enabled: false) { app, providerId in
            let id = try #require(providerId)
            try await app.test(
                .GET, "/api/v1/auth/oidc/login/\(id)",
                afterResponse: { res async throws in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("login redirects to the authorization endpoint with the required parameters")
    func loginRedirectsWithParams() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            var location: String?
            try await app.test(
                .GET, "/api/v1/auth/oidc/login/\(id)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    location = res.headers.first(name: .location)
                })

            let components = try #require(location.flatMap { URLComponents(string: $0) })
            #expect(components.string?.hasPrefix("\(provider.baseURL)/authorize") == true)

            let items = try #require(components.queryItems)
            func value(_ name: String) -> String? {
                items.first(where: { $0.name == name })?.value
            }

            #expect(value("client_id") == "test-client-id")
            #expect(value("redirect_uri") == InternalAuthOIDCController.redirectURL)
            #expect(value("response_type") == "code")
            #expect(value("scope") == "openid email profile")
            #expect(value("code_challenge_method") == "S256")
            #expect(!(value("state") ?? "").isEmpty)
            #expect(!(value("nonce") ?? "").isEmpty)
            #expect(!(value("code_challenge") ?? "").isEmpty)
        }
    }

    @Test("login rejects a discovery document whose self-reported issuer doesn't match the configured issuer URL")
    func loginRejectsDiscoveryIssuerMismatch() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }
        await provider.state.set(discoveryIssuerOverride: "https://attacker.example.com")

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            try await app.test(
                .GET, "/api/v1/auth/oidc/login/\(id)",
                afterResponse: { res async throws in
                    #expect(res.status == .badGateway)
                })
        }
    }

    // MARK: - /callback

    @Test("callback rejects a missing code or state")
    func callbackMissingCodeOrState() async throws {
        try await withApp { app, _ in
            try await app.test(
                .GET, "/api/v1/auth/oidc/callback",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    #expect(fragment(from: location)["error"] == "missing_code_or_state")
                })
        }
    }

    @Test("callback rejects an unknown or expired state")
    func callbackUnknownState() async throws {
        try await withApp { app, _ in
            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=never-issued",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    #expect(fragment(from: location)["error"] == "invalid_or_expired_state")
                })
        }
    }

    @Test("callback surfaces a token exchange failure")
    func callbackTokenExchangeFailure() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }
        await provider.state.set(tokenStatus: .internalServerError)

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            let (state, _) = try await performLoginRedirect(app, providerId: id)

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    #expect(fragment(from: location)["error"] == "token_exchange_failed")
                })
        }
    }

    @Test("callback rejects an expired ID token")
    func callbackExpiredIDToken() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }
        await provider.state.set(expired: true)

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            let (state, nonce) = try await performLoginRedirect(app, providerId: id)
            await provider.state.set(email: "person@example.com", nonce: nonce)

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    #expect(fragment(from: location)["error"] == "invalid_id_token")
                })
        }
    }

    @Test("callback rejects an audience mismatch")
    func callbackAudienceMismatch() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }
        await provider.state.set(audience: "some-other-client-id")

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            let (state, nonce) = try await performLoginRedirect(app, providerId: id)
            await provider.state.set(email: "person@example.com", nonce: nonce)

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    #expect(fragment(from: location)["error"] == "audience_mismatch")
                })
        }
    }

    @Test("callback rejects a nonce mismatch")
    func callbackNonceMismatch() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            let (state, _) = try await performLoginRedirect(app, providerId: id)
            await provider.state.set(
                email: "person@example.com", nonce: "a-completely-different-nonce")

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    #expect(fragment(from: location)["error"] == "nonce_mismatch")
                })
        }
    }

    @Test("callback rejects a missing email claim")
    func callbackMissingEmail() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            let (state, nonce) = try await performLoginRedirect(app, providerId: id)
            await provider.state.set(email: .some(nil), nonce: nonce)

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    #expect(fragment(from: location)["error"] == "missing_email_claim")
                })
        }
    }

    @Test("callback rejects first-login linking when the email claim is not verified")
    func callbackRejectsUnverifiedEmailOnFirstLogin() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            _ = try await createUser(app, username: "victim@example.com")

            let (state, nonce) = try await performLoginRedirect(app, providerId: id)
            await provider.state.set(
                sub: "attacker-sub", email: "victim@example.com", emailVerified: .some(false),
                nonce: nonce)

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    #expect(fragment(from: location)["error"] == "email_not_verified")
                })

            // Confirm the account was genuinely not touched/linked.
            let victim = try await User.query(on: app.db)
                .filter(\.$username == "victim@example.com")
                .first()
            #expect(victim?.oidcSubject == nil)
        }
    }

    @Test("callback allows a previously-linked user to log in even if a later token's email is unverified")
    func callbackAllowsExistingLinkRegardlessOfEmailVerification() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            let user = try await createUser(app, username: "already-linked@example.com")
            user.oidcProviderId = id
            user.oidcSubject = "already-linked-sub"
            try await user.save(on: app.db)

            let (state, nonce) = try await performLoginRedirect(app, providerId: id)
            // The (provider, sub) link is the trust anchor here, not the email - this must
            // still succeed even though emailVerified is false on this particular token.
            await provider.state.set(
                sub: "already-linked-sub", email: "already-linked@example.com",
                emailVerified: .some(false), nonce: nonce)

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    let token = try #require(fragment(from: location)["token"])

                    let sessionToken = try await app.jwt.keys.verify(token, as: SessionToken.self)
                    #expect(sessionToken.userId == user.id)
                })
        }
    }

    @Test("callback rejects login when no local account matches (no auto-provisioning)")
    func callbackNoMatchingAccount() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            let (state, nonce) = try await performLoginRedirect(app, providerId: id)
            await provider.state.set(sub: "sub-nobody", email: "nobody@example.com", nonce: nonce)

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    #expect(fragment(from: location)["error"] == "no_matching_account")
                })

            let users = try await User.query(on: app.db)
                .filter(\.$username == "nobody@example.com")
                .all()
            #expect(users.isEmpty)
        }
    }

    @Test("callback links and logs in an existing user on first OIDC login, by username == email")
    func callbackFirstLoginLinksByEmail() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            let user = try await createUser(app, username: "linked@example.com")
            #expect(user.oidcSubject == nil)

            let (state, nonce) = try await performLoginRedirect(app, providerId: id)
            await provider.state.set(
                sub: "sub-linked-user", email: "linked@example.com", nonce: nonce)

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    let token = try #require(fragment(from: location)["token"])
                    #expect(!token.isEmpty)

                    let sessionToken = try await app.jwt.keys.verify(token, as: SessionToken.self)
                    #expect(sessionToken.userId == user.id)
                })

            let reloaded = try #require(try await User.find(user.id, on: app.db))
            #expect(reloaded.oidcSubject == "sub-linked-user")
            #expect(reloaded.oidcProviderId == id)
        }
    }

    @Test("callback logs in a previously-linked user directly by (provider, sub), even if username changed")
    func callbackSubsequentLoginMatchesByProviderAndSubject() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)
            let user = try await createUser(app, username: "renamed-locally@example.com")
            user.oidcProviderId = id
            user.oidcSubject = "sub-already-linked"
            try await user.save(on: app.db)

            let (state, nonce) = try await performLoginRedirect(app, providerId: id)
            // Email claim no longer matches the (since-renamed) local username - matching by
            // (provider, sub) must still succeed without needing another email match.
            await provider.state.set(
                sub: "sub-already-linked", email: "different-email-now@example.com", nonce: nonce)

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    let token = try #require(fragment(from: location)["token"])

                    let sessionToken = try await app.jwt.keys.verify(token, as: SessionToken.self)
                    #expect(sessionToken.userId == user.id)
                })
        }
    }

    @Test("callback does not match a subject linked to a different provider")
    func callbackDoesNotMatchSubjectFromDifferentProvider() async throws {
        let provider = try await FakeOIDCProvider.start()
        defer { Task { try? await provider.shutdown() } }

        try await withApp(oidc: provider) { app, providerId in
            let id = try #require(providerId)

            // A user linked to some OTHER provider, with the SAME subject value the fake
            // provider is about to issue - subjects are only unique within one provider's
            // namespace, so this must not match.
            let otherProviderId = UUID()
            let user = try await createUser(app, username: "collision@example.com")
            user.oidcProviderId = otherProviderId
            user.oidcSubject = "shared-subject-value"
            try await user.save(on: app.db)

            let (state, nonce) = try await performLoginRedirect(app, providerId: id)
            await provider.state.set(
                sub: "shared-subject-value", email: "nobody-else@example.com", nonce: nonce)

            try await app.test(
                .GET, "/api/v1/auth/oidc/callback?code=abc&state=\(state)",
                afterResponse: { res async throws in
                    #expect(res.status == .seeOther)
                    let location = res.headers.first(name: .location)
                    #expect(fragment(from: location)["error"] == "no_matching_account")
                })
        }
    }
}
