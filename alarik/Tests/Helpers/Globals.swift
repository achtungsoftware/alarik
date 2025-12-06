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
import Testing
import Vapor
import VaporTesting

@testable import Alarik

public func createUserAndLogin(_ app: Application) async throws -> String {
    let createDTO = User.Create(
        name: "Test User",
        username: "test@example.com",
        password: "TestPass123!",
        isAdmin: false
    )

    try await app.test(
        .POST, "/api/v1/users",
        beforeRequest: { req in
            try req.content.encode(createDTO)
        })

    let loginDTO = ["username": "test@example.com", "password": "TestPass123!"]
    var token = ""

    try await app.test(
        .POST, "/api/v1/users/login",
        beforeRequest: { req in
            try req.content.encode(loginDTO)
        },
        afterResponse: { res async throws in
            let tokenResponse = try res.content.decode(ClientTokenResponse.self)
            token = tokenResponse.token
        })

    return token
}
