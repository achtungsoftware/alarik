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

final class AccessKey: Model, @unchecked Sendable {
    static let schema = "access_keys"

    @ID(key: .id)
    var id: UUID?

    @Parent(key: "user_id")
    var user: User

    @Field(key: "access_key")
    var accessKey: String

    @Field(key: "secret_key")
    var secretKey: String

    @Field(key: "created_at")
    var createdAt: Date

    init() {}

    init(
        id: UUID? = nil, userId: UUID, accessKey: String, secretKey: String,
        createdAt: Date = Date()
    ) {
        self.id = id
        self.$user.id = userId
        self.accessKey = accessKey
        self.secretKey = secretKey
        self.createdAt = createdAt
    }
}

extension AccessKey {
    struct Create: Content {
        var accessKey: String
        var secretKey: String
    }

    struct ResponseDTO: Content {
        var id: UUID?
        var accessKey: String?
        var createdAt: Date?
    }

    func toResponseDTO() -> AccessKey.ResponseDTO {
        .init(
            id: self.id,
            accessKey: self.$accessKey.value,
            createdAt: self.$createdAt.value,
        )
    }
}

extension AccessKey.Create: Validatable {
    static func validations(_ validations: inout Validations) {
        validations.add("accessKey", as: String.self, is: !.empty)
        validations.add("secretKey", as: String.self, is: !.empty)
    }
}