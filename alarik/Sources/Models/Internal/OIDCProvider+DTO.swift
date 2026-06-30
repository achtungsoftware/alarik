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

import struct Foundation.UUID

/// An admin-configured OIDC identity provider that users can sign in with - see
/// `InternalAuthOIDCController` for the login/callback flow and
/// `InternalAdminOIDCProviderController` for admin CRUD. Modeled after how Gitea lets admins
/// register multiple OAuth2/OIDC "Authentication Sources": this is deployment/admin-level
/// configuration, not a per-user setting - the client credentials identify the whole Alarik
/// instance to the provider, not an individual user.
final class OIDCProvider: Model, @unchecked Sendable {
    static let schema = "oidc_providers"

    @ID(key: .id)
    var id: UUID?

    @Field(key: "name")
    var name: String

    @Field(key: "issuer_url")
    var issuerURL: String

    @Field(key: "client_id")
    var clientId: String

    @Field(key: "client_secret")
    var clientSecret: String

    @Field(key: "enabled")
    var enabled: Bool

    @Field(key: "created_at")
    var createdAt: Date

    init() {}

    init(
        id: UUID? = nil,
        name: String,
        issuerURL: String,
        clientId: String,
        clientSecret: String,
        enabled: Bool = true,
        createdAt: Date = Date()
    ) {
        self.id = id
        self.name = name
        self.issuerURL = issuerURL
        self.clientId = clientId
        self.clientSecret = clientSecret
        self.enabled = enabled
        self.createdAt = createdAt
    }

    func toResponseDTO() -> OIDCProvider.ResponseDTO {
        .init(
            id: self.id,
            name: self.name,
            issuerURL: self.issuerURL,
            clientId: self.clientId,
            enabled: self.enabled
        )
    }
}

extension OIDCProvider {
    struct Create: Content {
        var name: String
        var issuerURL: String
        var clientId: String
        var clientSecret: String
        var enabled: Bool
    }

    struct Edit: Content {
        var id: UUID
        var name: String
        var issuerURL: String
        var clientId: String
        /// Blank/omitted means "keep the existing secret" - the client secret is write-only,
        /// admins never read it back via `ResponseDTO`, so there's nothing to diff against.
        var clientSecret: String?
        var enabled: Bool
    }

    /// Deliberately omits `clientSecret` - same write-only treatment as a password hash.
    struct ResponseDTO: Content {
        var id: UUID?
        var name: String?
        var issuerURL: String?
        var clientId: String?
        var enabled: Bool?
    }

    /// What the public, unauthenticated login page needs to render provider buttons.
    struct PublicDTO: Content {
        var id: UUID
        var name: String
    }
}

extension OIDCProvider.Create: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("name", as: String.self, is: !.empty)
        validations.add("issuerURL", as: String.self, is: !.empty)
        validations.add("clientId", as: String.self, is: !.empty)
        validations.add("clientSecret", as: String.self, is: !.empty)
    }
}

extension OIDCProvider.Edit: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("id", as: UUID.self)
        validations.add("name", as: String.self, is: !.empty)
        validations.add("issuerURL", as: String.self, is: !.empty)
        validations.add("clientId", as: String.self, is: !.empty)
    }
}
