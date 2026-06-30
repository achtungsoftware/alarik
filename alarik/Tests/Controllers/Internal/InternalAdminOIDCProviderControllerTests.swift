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
import Testing
import Vapor
import VaporTesting

@testable import Alarik

@Suite("InternalAdminOIDCProviderController tests", .serialized)
struct InternalAdminOIDCProviderControllerTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()
            try await test(app)
            try await app.autoRevert()
        } catch {
            try? StorageHelper.cleanStorage()
            try? await app.autoRevert()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    @discardableResult
    private func createProvider(
        _ app: Application, name: String = "Test Provider", enabled: Bool = true
    ) async throws -> OIDCProvider {
        let provider = OIDCProvider(
            name: name, issuerURL: "https://example.com", clientId: "client-id",
            clientSecret: "original-secret", enabled: enabled)
        try await provider.save(on: app.db)
        return provider
    }

    @Test("List providers as admin - should pass")
    func testListProviders() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)
            try await createProvider(app, name: "Provider A")
            try await createProvider(app, name: "Provider B")

            try await app.test(
                .GET, "/api/v1/admin/oidcProviders",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let page = try res.content.decode(Page<OIDCProvider.ResponseDTO>.self)
                    #expect(page.items.count == 2)
                })
        }
    }

    @Test("List providers as non admin - should fail")
    func testListProvidersAsNonAdmin() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createProvider(app)

            try await app.test(
                .GET, "/api/v1/admin/oidcProviders",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Create provider as admin - should pass, response omits the client secret")
    func testCreateProvider() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            let createDTO = OIDCProvider.Create(
                name: "Google", issuerURL: "https://accounts.google.com", clientId: "abc",
                clientSecret: "shh", enabled: true)

            try await app.test(
                .POST, "/api/v1/admin/oidcProviders",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let body = try res.content.decode(OIDCProvider.ResponseDTO.self)
                    #expect(body.name == "Google")
                    #expect(body.issuerURL == "https://accounts.google.com")
                    #expect(body.enabled == true)
                })

            let providers = try await OIDCProvider.query(on: app.db).all()
            #expect(providers.count == 1)
            #expect(providers.first?.clientSecret == "shh")
        }
    }

    @Test("Create provider as non admin - should fail")
    func testCreateProviderAsNonAdmin() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            let createDTO = OIDCProvider.Create(
                name: "Google", issuerURL: "https://accounts.google.com", clientId: "abc",
                clientSecret: "shh", enabled: true)

            try await app.test(
                .POST, "/api/v1/admin/oidcProviders",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .unauthorized)
                })

            let providers = try await OIDCProvider.query(on: app.db).all()
            #expect(providers.isEmpty)
        }
    }

    @Test("Create provider rejects an issuer URL pointing at the cloud metadata address")
    func testCreateProviderRejectsMetadataHost() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            let createDTO = OIDCProvider.Create(
                name: "Malicious", issuerURL: "http://169.254.169.254/", clientId: "abc",
                clientSecret: "shh", enabled: true)

            try await app.test(
                .POST, "/api/v1/admin/oidcProviders",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .badRequest)
                })

            let providers = try await OIDCProvider.query(on: app.db).all()
            #expect(providers.isEmpty)
        }
    }

    @Test("Edit provider as admin - should pass")
    func testEditProvider() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)
            let provider = try await createProvider(app)

            let editDTO = OIDCProvider.Edit(
                id: try provider.requireID(), name: "Renamed", issuerURL: "https://new.example.com",
                clientId: "new-client-id", clientSecret: nil, enabled: false)

            try await app.test(
                .PUT, "/api/v1/admin/oidcProviders",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let body = try res.content.decode(OIDCProvider.ResponseDTO.self)
                    #expect(body.name == "Renamed")
                    #expect(body.issuerURL == "https://new.example.com")
                    #expect(body.enabled == false)
                })

            // Blank clientSecret means "keep the existing one" - must be untouched.
            let reloaded = try #require(try await OIDCProvider.find(provider.id, on: app.db))
            #expect(reloaded.clientSecret == "original-secret")
        }
    }

    @Test("Edit provider can replace the client secret when one is provided")
    func testEditProviderReplacesSecret() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)
            let provider = try await createProvider(app)

            let editDTO = OIDCProvider.Edit(
                id: try provider.requireID(), name: provider.name, issuerURL: provider.issuerURL,
                clientId: provider.clientId, clientSecret: "rotated-secret", enabled: true)

            try await app.test(
                .PUT, "/api/v1/admin/oidcProviders",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                })

            let reloaded = try #require(try await OIDCProvider.find(provider.id, on: app.db))
            #expect(reloaded.clientSecret == "rotated-secret")
        }
    }

    @Test("Edit provider as non admin - should fail")
    func testEditProviderAsNonAdmin() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            let provider = try await createProvider(app)

            let editDTO = OIDCProvider.Edit(
                id: try provider.requireID(), name: "Renamed", issuerURL: provider.issuerURL,
                clientId: provider.clientId, clientSecret: nil, enabled: true)

            try await app.test(
                .PUT, "/api/v1/admin/oidcProviders",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Delete provider as admin - should pass")
    func testDeleteProvider() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)
            let provider = try await createProvider(app)

            try await app.test(
                .DELETE, "/api/v1/admin/oidcProviders/\(try provider.requireID())",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .noContent)
                })

            let providers = try await OIDCProvider.query(on: app.db).all()
            #expect(providers.isEmpty)
        }
    }

    @Test("Delete provider as non admin - should fail")
    func testDeleteProviderAsNonAdmin() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            let provider = try await createProvider(app)

            try await app.test(
                .DELETE, "/api/v1/admin/oidcProviders/\(try provider.requireID())",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .unauthorized)
                })

            let providers = try await OIDCProvider.query(on: app.db).all()
            #expect(providers.count == 1)
        }
    }

    @Test("Delete provider unlinks users without deleting their accounts")
    func testDeleteProviderUnlinksUsers() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)
            let provider = try await createProvider(app)
            let providerId = try provider.requireID()

            let user = User(
                name: "Linked User", username: "linked@example.com",
                passwordHash: try Bcrypt.hash("TestPass123!"), isAdmin: false)
            user.oidcProviderId = providerId
            user.oidcSubject = "some-subject"
            try await user.save(on: app.db)

            try await app.test(
                .DELETE, "/api/v1/admin/oidcProviders/\(providerId)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .noContent)
                })

            let reloaded = try #require(try await User.find(user.id, on: app.db))
            #expect(reloaded.oidcProviderId == nil)
            #expect(reloaded.oidcSubject == nil)
        }
    }
}
