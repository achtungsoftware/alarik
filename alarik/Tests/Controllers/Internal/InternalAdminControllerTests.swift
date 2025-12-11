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

@Suite("InternalAdminController tests", .serialized)
struct InternalAdminControllerTests {
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

    @Test("List users as admin - should pass")
    func testListUsers() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            // Create 3 users
            try await createRandomUser(app)
            try await createRandomUser(app)
            try await createRandomUser(app)

            try await app.test(
                .GET, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let page = try res.content.decode(Page<User.ResponseDTO>.self)
                    #expect(res.status == .ok)
                    #expect(page.items.count == 4)
                })
        }
    }

    @Test("List users as non admin - should fail")
    func testListUsersAsNonAdmin() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            // Create 3 users
            try await createRandomUser(app)
            try await createRandomUser(app)
            try await createRandomUser(app)

            try await app.test(
                .GET, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Create user as admin - should pass")
    func testCreateUser() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            let createDTO = User.Create(
                name: "John Doe",
                username: "john@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            try await app.test(
                .GET, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("Create user as non admin - should fail")
    func testCreateUserAsNonAdmin() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            let createDTO = User.Create(
                name: "John Doe",
                username: "john@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            try await app.test(
                .GET, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Edit user as admin - should pass")
    func testEditUser() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            // Create a user to edit
            let createDTO = User.Create(
                name: "Original Name",
                username: "original@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            var userId: UUID?
            try await app.test(
                .POST, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let user = try res.content.decode(User.ResponseDTO.self)
                    userId = user.id
                })

            // Edit the user
            let editDTO = User.EditAdmin(
                id: userId!,
                name: "Updated Name",
                username: "updated@example.com",
                isAdmin: true
            )

            try await app.test(
                .PUT, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    let user = try res.content.decode(User.ResponseDTO.self)
                    #expect(user.name == "Updated Name")
                    #expect(user.username == "updated@example.com")
                    #expect(user.isAdmin == true)
                })
        }
    }

    @Test("Edit user as non admin - should fail")
    func testEditUserAsNonAdmin() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            let editDTO = User.EditAdmin(
                id: UUID(),
                name: "Updated Name",
                username: "updated@example.com",
                isAdmin: true
            )

            try await app.test(
                .PUT, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Edit user with duplicate username - should fail")
    func testEditUserDuplicateUsername() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            // Create first user
            let createDTO1 = User.Create(
                name: "User One",
                username: "user1@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            var user1Id: UUID?
            try await app.test(
                .POST, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO1)
                },
                afterResponse: { res async throws in
                    let user = try res.content.decode(User.ResponseDTO.self)
                    user1Id = user.id
                })

            // Create second user
            let createDTO2 = User.Create(
                name: "User Two",
                username: "user2@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO2)
                })

            // Try to edit user1 to have user2's username
            let editDTO = User.EditAdmin(
                id: user1Id!,
                name: "User One",
                username: "user2@example.com",
                isAdmin: false
            )

            try await app.test(
                .PUT, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(editDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .conflict)
                })
        }
    }

    @Test("Delete user as admin - should pass")
    func testDeleteUser() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            // Create a user to delete
            let createDTO = User.Create(
                name: "To Delete",
                username: "delete@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            var userId: UUID?
            try await app.test(
                .POST, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    let user = try res.content.decode(User.ResponseDTO.self)
                    userId = user.id
                })

            // Delete the user
            try await app.test(
                .DELETE, "/api/v1/admin/users/\(userId!.uuidString)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .noContent)
                })

            // Verify user is deleted
            let deletedUser = try await User.find(userId!, on: app.db)
            #expect(deletedUser == nil)
        }
    }

    @Test("Delete user as non admin - should fail")
    func testDeleteUserAsNonAdmin() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .DELETE, "/api/v1/admin/users/\(UUID().uuidString)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Delete yourself - should fail")
    func testDeleteSelf() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            // Get admin user ID
            let adminUser = try await User.query(on: app.db)
                .filter(\.$username == "alarik")
                .first()

            try await app.test(
                .DELETE, "/api/v1/admin/users/\(adminUser!.id!.uuidString)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .forbidden)
                })
        }
    }

    @Test("Delete non-existent user - should fail")
    func testDeleteNonExistentUser() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            try await app.test(
                .DELETE, "/api/v1/admin/users/\(UUID().uuidString)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Delete user with buckets - should delete buckets from disk")
    func testDeleteUserWithBuckets() async throws {
        try await withApp { app in
            let adminToken = try await loginDefaultAdminUser(app)

            // Create a user
            let createDTO = User.Create(
                name: "User With Buckets",
                username: "bucketuser@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            var userId: UUID?
            try await app.test(
                .POST, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: adminToken)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    let user = try res.content.decode(User.ResponseDTO.self)
                    userId = user.id
                })

            // Create buckets for this user directly in DB and on disk
            let bucket1 = Bucket(name: "user-bucket-1", userId: userId!)
            let bucket2 = Bucket(name: "user-bucket-2", userId: userId!)
            try await bucket1.save(on: app.db)
            try await bucket2.save(on: app.db)
            try BucketHandler.create(name: "user-bucket-1")
            try BucketHandler.create(name: "user-bucket-2")

            // Add files to buckets
            let bucket1URL = BucketHandler.bucketURL(for: "user-bucket-1")
            let bucket2URL = BucketHandler.bucketURL(for: "user-bucket-2")
            try "content".write(
                to: bucket1URL.appendingPathComponent("test.txt"),
                atomically: true,
                encoding: .utf8
            )
            try "content".write(
                to: bucket2URL.appendingPathComponent("test.txt"),
                atomically: true,
                encoding: .utf8
            )

            // Verify buckets exist on disk
            #expect(FileManager.default.fileExists(atPath: bucket1URL.path))
            #expect(FileManager.default.fileExists(atPath: bucket2URL.path))

            // Delete the user
            try await app.test(
                .DELETE, "/api/v1/admin/users/\(userId!.uuidString)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: adminToken)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .noContent)
                })

            // Verify buckets are deleted from disk
            #expect(!FileManager.default.fileExists(atPath: bucket1URL.path))
            #expect(!FileManager.default.fileExists(atPath: bucket2URL.path))

            // Verify buckets are deleted from DB (cascade)
            let remainingBuckets = try await Bucket.query(on: app.db)
                .filter(\.$user.$id == userId!)
                .all()
            #expect(remainingBuckets.isEmpty)
        }
    }

    @Test("Delete user with access keys - should clean caches")
    func testDeleteUserWithAccessKeys() async throws {
        try await withApp { app in
            let adminToken = try await loginDefaultAdminUser(app)

            // Create a user
            let createDTO = User.Create(
                name: "User With Keys",
                username: "keyuser@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            var userId: UUID?
            try await app.test(
                .POST, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: adminToken)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    let user = try res.content.decode(User.ResponseDTO.self)
                    userId = user.id
                })

            // Create access keys for this user
            let accessKey1 = AccessKey(userId: userId!, accessKey: "TESTKEY1", secretKey: "secret1")
            let accessKey2 = AccessKey(userId: userId!, accessKey: "TESTKEY2", secretKey: "secret2")
            try await accessKey1.save(on: app.db)
            try await accessKey2.save(on: app.db)

            // Add to caches
            await AccessKeyUserMapCache.shared.add(accessKey: "TESTKEY1", userId: userId!)
            await AccessKeyUserMapCache.shared.add(accessKey: "TESTKEY2", userId: userId!)

            // Verify keys are in cache
            let cachedUserId1 = await AccessKeyUserMapCache.shared.userId(for: "TESTKEY1")
            let cachedUserId2 = await AccessKeyUserMapCache.shared.userId(for: "TESTKEY2")
            #expect(cachedUserId1 == userId)
            #expect(cachedUserId2 == userId)

            // Delete the user
            try await app.test(
                .DELETE, "/api/v1/admin/users/\(userId!.uuidString)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: adminToken)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .noContent)
                })

            // Verify keys are removed from cache
            let removedUserId1 = await AccessKeyUserMapCache.shared.userId(for: "TESTKEY1")
            let removedUserId2 = await AccessKeyUserMapCache.shared.userId(for: "TESTKEY2")
            #expect(removedUserId1 == nil)
            #expect(removedUserId2 == nil)

            // Verify keys are deleted from DB (cascade)
            let remainingKeys = try await AccessKey.query(on: app.db)
                .filter(\.$user.$id == userId!)
                .all()
            #expect(remainingKeys.isEmpty)
        }
    }
}
