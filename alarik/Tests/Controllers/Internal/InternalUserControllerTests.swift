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

struct LoginCredentials: Content {
    let username: String
    let password: String
}

@Suite("UserController tests", .serialized)
struct UserControllerTests {
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

    @Test("Create user with valid data")
    func createUserSuccess() async throws {
        try await withApp { app in
            let createDTO = User.Create(
                name: "John Doe",
                username: "john@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let user = try res.content.decode(User.ResponseDTO.self)
                    #expect(user.name == "John Doe")
                    #expect(user.username == "john@example.com")
                    #expect(user.id != nil)
                })
        }
    }

    @Test("Create user with duplicate username fails")
    func createUserDuplicateUsername() async throws {
        try await withApp { app in
            let createDTO = User.Create(
                name: "John Doe",
                username: "john@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            // Create first user
            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO)
                })

            // Try to create second user with same username
            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .conflict)
                })
        }
    }

    @Test("Create user with missing fields fails")
    func createUserMissingFields() async throws {
        try await withApp { app in
            let incompleteDTO = ["name": "John Doe"]

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(incompleteDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest || res.status == .unprocessableEntity)
                })
        }
    }

    @Test("Login with valid credentials")
    func loginSuccess() async throws {
        try await withApp { app in
            // Create user first
            let createDTO = User.Create(
                name: "Jane Smith",
                username: "jane@example.com",
                password: "Password123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO)
                })

            // Login
            let loginDTO = LoginCredentials(
                username: "jane@example.com",
                password: "Password123!"
            )

            try await app.test(
                .POST, "api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let tokenResponse = try res.content.decode(ClientTokenResponse.self)
                    #expect(!tokenResponse.token.isEmpty)
                })
        }
    }

    @Test("Login with invalid password fails")
    func loginInvalidPassword() async throws {
        try await withApp { app in
            // Create user first
            let createDTO = User.Create(
                name: "Jane Smith",
                username: "jane@example.com",
                password: "Password123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO)
                })

            // Try login with wrong password
            let loginDTO = LoginCredentials(
                username: "jane@example.com",
                password: "WrongPassword"
            )

            try await app.test(
                .POST, "api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Login with non-existent user fails")
    func loginNonExistentUser() async throws {
        try await withApp { app in
            let loginDTO = LoginCredentials(
                username: "nonexistent@example.com",
                password: "SomePassword"
            )

            try await app.test(
                .POST, "api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Login without credentials fails")
    func loginNoCredentials() async throws {
        try await withApp { app in
            try await app.test(
                .POST, "api/v1/users/login",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Auth with valid token")
    func authSuccess() async throws {
        try await withApp { app in
            // Create user
            let createDTO = User.Create(
                name: "Bob Johnson",
                username: "bob@example.com",
                password: "BobPass123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO)
                })

            // Login to get token
            let loginDTO = LoginCredentials(
                username: "bob@example.com",
                password: "BobPass123!"
            )

            var token = ""
            try await app.test(
                .POST, "api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO)
                },
                afterResponse: { res async throws in
                    let tokenResponse = try res.content.decode(ClientTokenResponse.self)
                    token = tokenResponse.token
                })

            // Use token for auth
            try await app.test(
                .POST, "api/v1/users/auth",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let user = try res.content.decode(User.ResponseDTO.self)
                    #expect(user.name == "Bob Johnson")
                    #expect(user.username == "bob@example.com")
                })
        }
    }

    @Test("Auth with invalid token fails")
    func authInvalidToken() async throws {
        try await withApp { app in
            try await app.test(
                .POST, "api/v1/users/auth",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(
                        token: "invalid.token.here")
                },
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Auth without token fails")
    func authNoToken() async throws {
        try await withApp { app in
            try await app.test(
                .POST, "api/v1/users/auth",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Auth with token for deleted user fails")
    func authDeletedUser() async throws {
        try await withApp { app in
            // Create user
            let createDTO = User.Create(
                name: "Temp User",
                username: "temp@example.com",
                password: "TempPass123!",
                isAdmin: false
            )

            var userId: UUID?
            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    let user = try res.content.decode(User.ResponseDTO.self)
                    userId = user.id
                })

            // Login to get token
            let loginDTO = LoginCredentials(
                username: "temp@example.com",
                password: "TempPass123!"
            )

            var token = ""
            try await app.test(
                .POST, "api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO)
                },
                afterResponse: { res async throws in
                    let tokenResponse = try res.content.decode(ClientTokenResponse.self)
                    token = tokenResponse.token
                })

            // Delete user manually from database
            if let id = userId {
                let user = try await User.find(id, on: app.db)
                try await user?.delete(on: app.db)
            }

            // Try to use token after user deletion
            try await app.test(
                .POST, "api/v1/users/auth",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    private func createKey(_ app: Application, token: String, accessKey: String, secretKey: String)
        async throws
    {
        let createDTO = AccessKey.Create(accessKey: accessKey, secretKey: secretKey)

        try await app.test(
            .POST, "/api/v1/users/accessKeys",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(createDTO)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    @Test("Create already existing key - should fail")
    func testCreateAlreadyExistingKey() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            // Create key first
            try await createKey(app, token: token, accessKey: "test1", secretKey: "test2")

            // expect conflict since key already exists
            let createDTO = AccessKey.Create(accessKey: "test1", secretKey: "fds")
            try await app.test(
                .POST, "/api/v1/users/accessKeys",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res in
                    #expect(res.status == .conflict)
                })
        }
    }

    @Test("List keys - Should return user's keys")
    func testListAccessKeys() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            // Create keys first
            try await createKey(app, token: token, accessKey: "test1", secretKey: "test2")
            try await createKey(app, token: token, accessKey: "test3", secretKey: "test4")

            try await app.test(
                .GET, "/api/v1/users/accessKeys",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<AccessKey.ResponseDTO>.self)
                    #expect(page.items.count == 2)
                })
        }
    }

    @Test("Edit user with valid data")
    func editUserSuccess() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            let editDTO = User.Edit(
                name: "Updated Name",
                username: "updated@example.com",
                currentPassword: nil,
                newPassword: nil
            )

            try await app.test(
                .PUT, "/api/v1/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let user = try res.content.decode(User.ResponseDTO.self)
                    #expect(user.name == "Updated Name")
                    #expect(user.username == "updated@example.com")
                })
        }
    }

    @Test("Edit user without token fails")
    func editUserNoToken() async throws {
        try await withApp { app in
            let editDTO = User.Edit(
                name: "Updated Name",
                username: "updated@example.com",
                currentPassword: nil,
                newPassword: nil
            )

            try await app.test(
                .PUT, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Edit user with duplicate username fails")
    func editUserDuplicateUsername() async throws {
        try await withApp { app in
            // Create first user and get token
            let token = try await createUserAndLogin(app, username: "first@example.com")

            // Create second user
            let createDTO = User.Create(
                name: "Second User",
                username: "second@example.com",
                password: "SecondPass123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO)
                })

            // Try to change first user's username to second user's username
            let editDTO = User.Edit(
                name: "First User",
                username: "second@example.com",
                currentPassword: nil,
                newPassword: nil
            )

            try await app.test(
                .PUT, "/api/v1/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .conflict)
                })
        }
    }

    @Test("Change password with valid current password")
    func changePasswordSuccess() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            let editDTO = User.Edit(
                name: "Test User",
                username: "test@example.com",
                currentPassword: "TestPass123!",
                newPassword: "NewPassword456!"
            )

            try await app.test(
                .PUT, "/api/v1/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .ok)
                })

            // Verify new password works by logging in
            let loginDTO = LoginCredentials(
                username: "test@example.com",
                password: "NewPassword456!"
            )

            try await app.test(
                .POST, "/api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("Change password with incorrect current password fails")
    func changePasswordWrongCurrentPassword() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            let editDTO = User.Edit(
                name: "Test User",
                username: "test@example.com",
                currentPassword: "WrongPassword!",
                newPassword: "NewPassword456!"
            )

            try await app.test(
                .PUT, "/api/v1/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Edit user without password change keeps old password")
    func editUserKeepsPassword() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            // Edit without password fields
            let editDTO = User.Edit(
                name: "Updated Name",
                username: "test@example.com",
                currentPassword: nil,
                newPassword: nil
            )

            try await app.test(
                .PUT, "/api/v1/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .ok)
                })

            // Verify old password still works
            let loginDTO = LoginCredentials(
                username: "test@example.com",
                password: "TestPass123!"
            )

            try await app.test(
                .POST, "/api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("Delete user with valid token")
    func deleteUserSuccess() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .DELETE, "/api/v1/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify user is deleted by trying to auth
            try await app.test(
                .POST, "/api/v1/users/auth",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Delete user without token fails")
    func deleteUserNoToken() async throws {
        try await withApp { app in
            try await app.test(
                .DELETE, "/api/v1/users",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Delete user also deletes access keys")
    func deleteUserDeletesAccessKeys() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            // Create access keys
            try await createKey(app, token: token, accessKey: "testkey1", secretKey: "testsecret1")
            try await createKey(app, token: token, accessKey: "testkey2", secretKey: "testsecret2")

            // Verify keys exist
            try await app.test(
                .GET, "/api/v1/users/accessKeys",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let page = try res.content.decode(Page<AccessKey.ResponseDTO>.self)
                    #expect(page.items.count == 2)
                })

            // Delete user
            try await app.test(
                .DELETE, "/api/v1/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify user's access keys are deleted from database
            let accessKeys = try await AccessKey.query(on: app.db)
                .filter(\.$accessKey == "testkey1")
                .all()
            #expect(accessKeys.isEmpty)

            let accessKeys2 = try await AccessKey.query(on: app.db)
                .filter(\.$accessKey == "testkey2")
                .all()
            #expect(accessKeys2.isEmpty)
        }
    }

    @Test("Delete user also deletes buckets")
    func deleteUserDeletesBuckets() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            // Create access key first
            try await createKey(app, token: token, accessKey: "testkey", secretKey: "testsecret")

            // Get the user ID and access key to create bucket directly in database
            let accessKey = try await AccessKey.query(on: app.db)
                .filter(\.$accessKey == "testkey")
                .first()

            guard let userId = accessKey?.$user.id else {
                Issue.record("Access key or user not found")
                return
            }

            // Create bucket directly in database
            let bucket = Bucket(name: "testbucket", userId: userId)
            try await bucket.save(on: app.db)

            // Create bucket directory
            try BucketHandler.create(name: "testbucket")

            // Verify bucket exists
            let bucketsBefore = try await Bucket.query(on: app.db).all()
            #expect(bucketsBefore.count == 1)

            // Delete user
            try await app.test(
                .DELETE, "/api/v1/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify buckets are deleted from database
            let bucketsAfter = try await Bucket.query(on: app.db).all()
            #expect(bucketsAfter.isEmpty)
        }
    }
}
