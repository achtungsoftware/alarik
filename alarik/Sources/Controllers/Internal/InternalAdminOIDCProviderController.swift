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

import Vapor

/// Admin-only CRUD for OIDC SSO providers - see `OIDCProvider+DTO.swift` for why this is
/// admin-managed rather than per-user, and `InternalAuthOIDCController` for the public
/// login/callback flow that uses these.
struct InternalAdminOIDCProviderController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("admin").grouped("oidcProviders").get(use: self.listProviders)
        routes.grouped("admin").grouped("oidcProviders").post(use: self.createProvider)
        routes.grouped("admin").grouped("oidcProviders").put(use: self.editProvider)
        routes.grouped("admin").grouped("oidcProviders").grouped(":providerId")
            .delete(use: self.deleteProvider)
    }

    /// Fetches an OIDC provider by ID or throws the standard 404 - the same fetch-or-404 shape
    /// needed by both `editProvider` and `deleteProvider`.
    private func requireProvider(req: Request, id: UUID) async throws -> OIDCProvider {
        guard let provider = try await OIDCProvider.find(app: req.application, id: id) else {
            throw Abort(.notFound, reason: "OIDC provider not found")
        }
        return provider
    }

    @Sendable
    func listProviders(req: Request) async throws -> Page<OIDCProvider.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let providers = await OIDCProvider.all(app: req.application)
            .sorted { $0.createdAt > $1.createdAt }

        return try providers.paginated(for: req).map { $0.toResponseDTO() }
    }

    @Sendable
    func createProvider(req: Request) async throws -> OIDCProvider.ResponseDTO {
        try OIDCProvider.Create.validate(content: req)

        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let create: OIDCProvider.Create = try req.content.decode(OIDCProvider.Create.self)

        // Fail fast with a clear 400 here, rather than only discovering a blocked address the
        // first time someone attempts to log in through this provider.
        try OIDCDiscoveryCache.rejectMetadataHost(of: create.issuerURL)

        let provider = OIDCProvider(
            name: create.name,
            issuerURL: create.issuerURL,
            clientId: create.clientId,
            clientSecret: create.clientSecret,
            enabled: create.enabled
        )

        try await provider.save(app: req.application)

        return provider.toResponseDTO()
    }

    @Sendable
    func editProvider(req: Request) async throws -> OIDCProvider.ResponseDTO {
        try OIDCProvider.Edit.validate(content: req)

        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        let edit: OIDCProvider.Edit = try req.content.decode(OIDCProvider.Edit.self)
        try OIDCDiscoveryCache.rejectMetadataHost(of: edit.issuerURL)

        let provider = try await requireProvider(req: req, id: edit.id)
        let previousIssuerURL = provider.issuerURL

        provider.name = edit.name
        provider.issuerURL = edit.issuerURL
        provider.clientId = edit.clientId
        provider.enabled = edit.enabled
        if let newSecret = edit.clientSecret, !newSecret.isEmpty {
            provider.clientSecret = newSecret
        }

        try await provider.save(app: req.application)

        // The discovery/JWKS cache is keyed by issuer URL - if it changed, drop the old entry
        // immediately rather than leaving it to expire on its own TTL (up to an hour), which
        // would otherwise leave logins resolving against the stale, no-longer-configured issuer.
        if previousIssuerURL != provider.issuerURL {
            await OIDCDiscoveryCache.shared.invalidate(issuerURL: previousIssuerURL)
        }

        return provider.toResponseDTO()
    }

    @Sendable
    func deleteProvider(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)
        try auth.requireAdmin()

        guard let providerId = req.parameters.get("providerId", as: UUID.self) else {
            throw Abort(.badRequest, reason: "Invalid provider ID")
        }

        let provider = try await requireProvider(req: req, id: providerId)

        // Unlink, don't block: any users signed in via this provider keep their local account,
        // they just lose the SSO link and fall back to local login (or re-link to a re-created
        // provider later) - matches this codebase's general "clean up, don't block" deletion
        // style elsewhere (e.g. deleting a bucket force-deletes its contents rather than
        // refusing).
        let linkedUsers = await MetadataListingService.list(
            app: req.application, collection: MetadataCollections.users
        )
        .compactMap { try? JSONDecoder().decode(User.self, from: $0.value) }
        .filter { $0.oidcProviderId == providerId }

        for user in linkedUsers {
            user.oidcProviderId = nil
            user.oidcSubject = nil
            try await user.save(app: req.application)
        }

        await OIDCDiscoveryCache.shared.invalidate(issuerURL: provider.issuerURL)
        try await provider.delete(app: req.application)

        return .noContent
    }
}
