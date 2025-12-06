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

final class User: Model, @unchecked Sendable {
    static let schema = "users"

    @ID(key: .id)
    var id: UUID?

    @Field(key: "name")
    var name: String

    @Field(key: "username")
    var username: String

    @Field(key: "password_hash")
    var passwordHash: String

    @Field(key: "is_admin")
    var isAdmin: Bool

    init() {}

    init(id: UUID? = nil, name: String, username: String, passwordHash: String, isAdmin: Bool) {
        self.id = id
        self.name = name
        self.username = username
        self.passwordHash = passwordHash
        self.isAdmin = isAdmin
    }

    func toResponseDTO() -> User.ResponseDTO {
        .init(
            id: self.id,
            name: self.$name.value,
            username: self.$username.value,
            isAdmin: self.$isAdmin.value,
        )
    }
}

extension User {
    struct FormCreate: Content {
        var name: String
        var username: String
        var password: String
    }

    struct Create: Content {
        var name: String
        var username: String
        var password: String
        var isAdmin: Bool
    }

    struct ResponseDTO: Content {
        var id: UUID?
        var name: String?
        var username: String?
        var isAdmin: Bool?

        func toModel() -> User {
            let model = User()

            model.id = self.id
            if let name = self.name {
                model.name = name
            }
            if let username = self.username {
                model.username = username
            }
            if let isAdmin = self.isAdmin {
                model.isAdmin = isAdmin
            }
            return model
        }
    }
}

extension User.Create: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("name", as: String.self, is: !.empty)
        validations.add("username", as: String.self, is: !.empty)
        validations.add("password", as: String.self, is: !.empty)
    }
}

extension User.FormCreate: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("name", as: String.self, is: !.empty)
        validations.add("username", as: String.self, is: !.empty)
        validations.add("password", as: String.self, is: !.empty)
    }
}

extension User: ModelAuthenticatable, ModelCredentialsAuthenticatable {
    static var usernameKey: KeyPath<User, FluentKit.FieldProperty<User, String>> {
        \User.$username
    }

    static var passwordHashKey: KeyPath<User, FluentKit.FieldProperty<User, String>> {
        \User.$passwordHash
    }

    func verify(password: String) throws -> Bool {
        try Bcrypt.verify(password, created: self.passwordHash)
    }
}