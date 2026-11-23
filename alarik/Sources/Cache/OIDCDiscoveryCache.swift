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
import JWTKit
import Vapor

struct OIDCDiscoveryDocument: Content {
    let issuer: String
    let authorizationEndpoint: String
    let tokenEndpoint: String
    let jwksURI: String
    /// Only consulted to word the error when the JWKS turns out empty (see `validateJWKS`).
    let idTokenSigningAlgValuesSupported: [String]?

    enum CodingKeys: String, CodingKey {
        case issuer
        case authorizationEndpoint = "authorization_endpoint"
        case tokenEndpoint = "token_endpoint"
        case jwksURI = "jwks_uri"
        case idTokenSigningAlgValuesSupported = "id_token_signing_alg_values_supported"
    }
}

/// Caches the IdP's `.well-known/openid-configuration` discovery document and the
/// `JWTKeyCollection` built from its JWKS, so every OIDC login/callback doesn't refetch them.
/// This is a separate key collection from `app.jwt.keys` (which signs Alarik's own session
/// tokens) - mixing the IdP's public keys into that collection would let a token signed by the
/// IdP be mistaken for a request to verify against Alarik's own HMAC key, or vice versa.
/// Not a `StoreBackedCache`: this caches documents fetched over HTTP from an external identity
/// provider, not a projection of `MetadataStore`, so there is no local authoritative record to
/// fall back to on a miss - the fallback is simply re-fetching from the provider.
final actor OIDCDiscoveryCache {

    public static let shared = OIDCDiscoveryCache()

    private struct Entry {
        let discovery: OIDCDiscoveryDocument
        let keys: JWTKeyCollection
        let fetchedAt: Date
    }

    // Keyed by issuer URL rather than a single global slot: a provider's issuer URL can be
    // edited by an admin at any time (no restart), so a deployment that changes it must not be
    // served another issuer's stale discovery doc/keys - and tests spinning up a fresh fake IdP
    // per test need the same isolation.
    private var entries: [String: Entry] = [:]

    private let ttl: TimeInterval = 3600

    /// Fetches (or returns the cached) discovery document + signing keys for `issuerURL`.
    /// Pass `forceRefresh: true` after a `kid` lookup miss to pick up rotated IdP keys before
    /// failing outright.
    func resolve(issuerURL: String, client: any Client, forceRefresh: Bool = false) async throws
        -> (discovery: OIDCDiscoveryDocument, keys: JWTKeyCollection)
    {
        if !forceRefresh, let entry = entries[issuerURL],
            Date().timeIntervalSince(entry.fetchedAt) < ttl
        {
            return (entry.discovery, entry.keys)
        }

        let trimmedIssuer = Self.canonicalIssuer(issuerURL)
        try Self.rejectMetadataHost(of: trimmedIssuer)

        let discoveryResponse = try await client.get(
            URI(string: "\(trimmedIssuer)/.well-known/openid-configuration")
        ).get()
        guard discoveryResponse.status == .ok else {
            throw Abort(.badGateway, reason: "OIDC discovery document request failed.")
        }
        let discoveryDoc = try discoveryResponse.content.decode(OIDCDiscoveryDocument.self)

        // Per the OIDC Discovery spec, the issuer value the discovery document reports MUST
        // match the issuer URL it was fetched from - otherwise a token's `iss` claim is only
        // ever compared against a value the doc itself supplied, which proves nothing.
        //
        // Compared in canonical form on BOTH sides. Providers differ on whether their issuer
        // carries a trailing slash (Authentik's does, Google's doesn't), and an admin pastes it
        // either way - but the slash is not what makes the check meaningful. What matters is
        // that the document came from the host and path the admin configured, and a trailing
        // slash changes neither. Comparing the raw document value against a one-slash-trimmed
        // config value made a correctly-configured Authentik issuer impossible to match, since
        // trimming was applied to only one side (issue #14).
        guard Self.canonicalIssuer(discoveryDoc.issuer) == trimmedIssuer else {
            throw Abort(
                .badGateway,
                reason: """
                    OIDC discovery document's issuer does not match the configured issuer URL. \
                    Configured: '\(issuerURL)', but \(trimmedIssuer)/.well-known/openid-configuration \
                    reports issuer '\(discoveryDoc.issuer)'. Set the provider's issuer URL to \
                    exactly the value the document reports.
                    """
            )
        }

        // The discovery doc's URLs are attacker-controlled if the issuer is ever compromised or
        // misconfigured - re-check them too, not just the issuer URL itself, before fetching.
        try Self.rejectMetadataHost(of: discoveryDoc.jwksURI)

        let jwksResponse = try await client.get(URI(string: discoveryDoc.jwksURI)).get()
        guard jwksResponse.status == .ok, let jwksBody = jwksResponse.body else {
            throw Abort(.badGateway, reason: "OIDC JWKS request failed.")
        }
        let jwksJSON = String(buffer: jwksBody)
        try Self.validateJWKS(jwksJSON, discovery: discoveryDoc)
        let keyCollection = try await JWTKeyCollection().add(jwksJSON: jwksJSON)

        entries[issuerURL] = Entry(discovery: discoveryDoc, keys: keyCollection, fetchedAt: Date())

        return (discoveryDoc, keyCollection)
    }

    /// Verifies a signed ID token against the cached (or freshly-fetched) keys for `issuerURL`,
    /// retrying once with a forced JWKS refresh on the first failure - covers the IdP having
    /// rotated its signing keys since the last cache fill. A failure on the retry (whether the
    /// refresh itself failed, e.g. the IdP is unreachable, or the token still doesn't verify) is
    /// genuinely the caller's error to handle and propagates rather than being swallowed here.
    func verifyIDToken(_ token: String, issuerURL: String, client: any Client) async throws
        -> OIDCIDToken
    {
        let (_, keys) = try await resolve(issuerURL: issuerURL, client: client)
        do {
            return try await keys.verify(token, as: OIDCIDToken.self)
        } catch {
            let (_, refreshedKeys) = try await resolve(
                issuerURL: issuerURL, client: client, forceRefresh: true)
            return try await refreshedKeys.verify(token, as: OIDCIDToken.self)
        }
    }

    private struct JWKSEnvelope: Decodable {
        struct Key: Decodable {}
        let keys: [Key]?
    }

    /// Authentik serves a bare `{}` when the provider has no Signing Key (HS256 via client
    /// secret); JWTKit then fails with the opaque "No such key 'keys' at path ''" (issue #30).
    /// Alarik only verifies against asymmetric JWKS keys, so name the real cause instead.
    static func validateJWKS(_ json: String, discovery: OIDCDiscoveryDocument) throws {
        let envelope = try? JSONDecoder().decode(JWKSEnvelope.self, from: Data(json.utf8))
        if let keys = envelope?.keys, !keys.isEmpty { return }

        let algs = discovery.idTokenSigningAlgValuesSupported ?? []
        let hmacOnly = !algs.isEmpty && algs.allSatisfy { $0.uppercased().hasPrefix("HS") }
        let hint: String
        if hmacOnly {
            hint = """
                The provider advertises only symmetric ID token signing \
                (\(algs.joined(separator: ", "))), which Alarik does not support. Configure an \
                asymmetric signing key on the provider (Authentik: select a Signing Key on the \
                OAuth2/OIDC provider) so it signs with RS256/ES256 and publishes the public key.
                """
        } else {
            hint = """
                Alarik verifies ID tokens against the public keys published there, so the provider \
                must be configured with an asymmetric signing key (RS256/ES256).
                """
        }
        throw Abort(
            .badGateway,
            reason: "OIDC provider's JWKS at \(discovery.jwksURI) contains no keys. \(hint)")
    }

    /// Drops any cached discovery/keys for `issuerURL` - call this when an admin edits a
    /// provider's issuer URL, so the previous (now-orphaned) issuer's entry doesn't linger in
    /// memory for up to an hour after nothing references it anymore. Not calling this isn't
    /// unsafe (a changed issuer URL is simply never looked up under its old key again), just a
    /// minor memory-hygiene cleanup.
    func invalidate(issuerURL: String) {
        entries.removeValue(forKey: issuerURL)
    }

    /// An issuer URL with every trailing slash removed - the form used both to build the
    /// `.well-known` URL and to compare the configured issuer against the one the discovery
    /// document reports.
    ///
    /// Strips *all* trailing slashes, not one: admins hitting issue #14 were told to work around
    /// it by configuring a doubled slash (`.../alarik//`), and that config must keep working
    /// after this fix rather than breaking a second time. It also stops the doubled slash from
    /// producing a `.../alarik//.well-known/openid-configuration` request, which some providers
    /// reject outright.
    static func canonicalIssuer(_ issuerURL: String) -> String {
        var canonical = issuerURL
        while canonical.hasSuffix("/") { canonical.removeLast() }
        return canonical
    }

    /// Best-effort SSRF defense-in-depth: blocks the cloud metadata service address
    /// (169.254.169.254, used identically by AWS/GCP/Azure/Alibaba) and the wider IPv4/IPv6
    /// link-local ranges it lives in. Deliberately does NOT block general private ranges
    /// (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16) or loopback - those commonly host a
    /// legitimate, same-network IdP in self-hosted/containerized deployments, and this is
    /// already admin-gated (only admins can register a provider), unlike a feature exposed to
    /// arbitrary users. This is a string-level check on the literal host, not full DNS-rebinding
    /// protection. Not private: also called at provider create/edit time (see
    /// `InternalAdminOIDCProviderController`) for immediate feedback, rather than only failing
    /// lazily the first time someone attempts to log in through a bad provider.
    static func rejectMetadataHost(of urlString: String) throws {
        guard let host = URL(string: urlString)?.host?.lowercased() else {
            throw Abort(.badRequest, reason: "Invalid OIDC provider URL.")
        }
        let bare = host.hasPrefix("[") && host.hasSuffix("]") ? String(host.dropFirst().dropLast()) : host
        if let bytes = WebhookURLValidator.parseIPv6(bare) {
            // Link-local, or the metadata address inside a transition form (`::ffff:169.254.169.254`).
            if bytes[0] == 0xfe, bytes[1] & 0xc0 == 0x80 {
                throw Abort(.badRequest, reason: "OIDC provider URL points to a blocked address.")
            }
            if let embedded = WebhookURLValidator.embeddedIPv4(in: bytes), (embedded >> 16) == 0xA9FE {
                throw Abort(.badRequest, reason: "OIDC provider URL points to a blocked address.")
            }
        } else if bare.hasPrefix("fe80:") {
            throw Abort(.badRequest, reason: "OIDC provider URL points to a blocked address.")
        }
        // A plain prefix check only catches "169.254.x.x" written out in dotted-decimal - the
        // same address reachable via "2852039166" (decimal), "0xA9FEA9FE" (hex), or "169.254.1"
        // (short form) would slip through. Reuse WebhookURLValidator's numeric-form parser so
        // every equivalent spelling of the metadata address is caught the same way.
        if let address = WebhookURLValidator.parseNumericIPv4(bare), (address >> 16) == 0xA9FE {
            throw Abort(.badRequest, reason: "OIDC provider URL points to a blocked address.")
        }
    }
}
