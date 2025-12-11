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

/// Creates a non-admin user with random username
public func createRandomUser(_ app: Application) async throws {
    let createDTO = User.Create(
        name: "Test User",
        username: UUID().uuidString,
        password: "TestPass123!",
        isAdmin: false
    )

    try await app.test(
        .POST, "/api/v1/users",
        beforeRequest: { req in
            try req.content.encode(createDTO)
        },
        afterResponse: { res async throws in
        })
}

/// Uses the already existing admin user alarik
public func loginDefaultAdminUser(_ app: Application) async throws -> String {
    // We are testing, so there should be a default admin user.
    var token = ""
    try await app.test(
        .POST, "/api/v1/users/login",
        beforeRequest: { req in
            try req.content.encode(["username": "alarik", "password": "alarik"])
        },
        afterResponse: { res async throws in
            let tokenResponse = try res.content.decode(ClientTokenResponse.self)
            token = tokenResponse.token
        })

    return token
}

public func createUserAndLogin(_ app: Application, username: String = "test@example.com") async throws -> String {
    let createDTO = User.Create(
        name: "Test User",
        username: username,
        password: "TestPass123!",
        isAdmin: false
    )

    try await app.test(
        .POST, "/api/v1/users",
        beforeRequest: { req in
            try req.content.encode(createDTO)
        })

    let loginDTO = ["username": username, "password": "TestPass123!"]
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
