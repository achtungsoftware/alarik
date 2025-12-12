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

@Suite("InternalBucketController tests", .serialized)
struct InternalBucketControllerTests {
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

    private func createBucket(_ app: Application, token: String, name: String) async throws {
        let createDTO = Bucket.Create(name: name, versioningEnabled: false)

        try await app.test(
            .POST, "/api/v1/buckets",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(createDTO)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func putObject(_ app: Application, bucketName: String, key: String, content: String)
        async throws
    {
        let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
        let data = content.data(using: .utf8)!
        let meta = ObjectMeta(
            bucketName: bucketName,
            key: key,
            size: data.count,
            contentType: "text/plain",
            etag: Insecure.MD5.hash(data: data).hex,
            updatedAt: Date()
        )
        try ObjectFileHandler.write(metadata: meta, data: data, to: path)
    }

    @Test("List buckets - Should return user's buckets")
    func testListBuckets() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            // Create test buckets
            try await createBucket(app, token: token, name: "bucket1")
            try await createBucket(app, token: token, name: "bucket2")
            try await createBucket(app, token: token, name: "bucket3")

            try await app.test(
                .GET, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<Bucket>.self)
                    #expect(page.items.count == 3)
                    #expect(page.metadata.total == 3)

                    let bucketNames = page.items.map { $0.name }
                    #expect(bucketNames.contains("bucket1"))
                    #expect(bucketNames.contains("bucket2"))
                    #expect(bucketNames.contains("bucket3"))
                })
        }
    }

    @Test("List buckets - Empty list for new user")
    func testListBucketsEmpty() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .GET, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<Bucket>.self)
                    #expect(page.items.count == 0)
                    #expect(page.metadata.total == 0)
                })
        }
    }

    @Test("List buckets - Without auth fails")
    func testListBucketsUnauthorized() async throws {
        try await withApp { app in
            try await app.test(
                .GET, "/api/v1/buckets",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("List buckets - With pagination")
    func testListBucketsPagination() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            // Create 5 buckets
            for i in 1...5 {
                try await createBucket(app, token: token, name: "bucket\(i)")
            }

            // Page 1 with 2 items
            try await app.test(
                .GET, "/api/v1/buckets?page=1&per=2",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<Bucket>.self)
                    #expect(page.items.count == 2)
                    #expect(page.metadata.total == 5)
                    #expect(page.metadata.page == 1)
                    #expect(page.metadata.per == 2)
                })

            // Page 2 with 2 items
            try await app.test(
                .GET, "/api/v1/buckets?page=2&per=2",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<Bucket>.self)
                    #expect(page.items.count == 2)
                    #expect(page.metadata.page == 2)
                })
        }
    }

    @Test("Create bucket - Success")
    func testCreateBucketSuccess() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            let createDTO = Bucket.Create(name: "my-new-bucket", versioningEnabled: false)

            try await app.test(
                .POST, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let bucket = try res.content.decode(Bucket.ResponseDTO.self)
                    #expect(bucket.name == "my-new-bucket")
                    #expect(bucket.id != nil)
                })
        }
    }

    @Test("Create bucket - Without auth fails")
    func testCreateBucketUnauthorized() async throws {
        try await withApp { app in
            let createDTO = Bucket.Create(name: "test-bucket", versioningEnabled: false)

            try await app.test(
                .POST, "/api/v1/buckets",
                beforeRequest: { req in
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Create bucket - Invalid name fails validation")
    func testCreateBucketInvalidName() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            let createDTO = Bucket.Create(name: "Invalid_Bucket_Name!", versioningEnabled: false)

            try await app.test(
                .POST, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest || res.status == .unprocessableEntity)
                })
        }
    }

    @Test("Delete bucket - Success")
    func testDeleteBucketSuccess() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "delete-me-bucket")

            // Verify bucket exists
            try await app.test(
                .GET, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let page = try res.content.decode(Page<Bucket>.self)
                    #expect(page.items.count == 1)
                })

            // Delete bucket
            try await app.test(
                .DELETE, "/api/v1/buckets/delete-me-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify bucket is deleted
            try await app.test(
                .GET, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let page = try res.content.decode(Page<Bucket>.self)
                    #expect(page.items.count == 0)
                })
        }
    }

    @Test("Delete bucket - Without auth fails")
    func testDeleteBucketUnauthorized() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "protected-bucket")

            try await app.test(
                .DELETE, "/api/v1/buckets/protected-bucket",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })

            // Verify bucket still exists
            try await app.test(
                .GET, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let page = try res.content.decode(Page<Bucket>.self)
                    #expect(page.items.count == 1)
                })
        }
    }

    @Test("Delete bucket - Non-existent bucket fails")
    func testDeleteBucketNonExistent() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .DELETE, "/api/v1/buckets/nonexistent-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Delete bucket - User cannot delete other user's bucket")
    func testDeleteBucketUserIsolation() async throws {
        try await withApp { app in
            // Create first user and bucket
            let token1 = try await createUserAndLogin(app)
            try await createBucket(app, token: token1, name: "user1-bucket")

            // Create second user
            let createDTO2 = User.Create(
                name: "User 2",
                username: "user2@example.com",
                password: "Pass123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO2)
                })

            let loginDTO2 = ["username": "user2@example.com", "password": "Pass123!"]
            var token2 = ""

            try await app.test(
                .POST, "/api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO2)
                },
                afterResponse: { res async throws in
                    let tokenResponse = try res.content.decode(ClientTokenResponse.self)
                    token2 = tokenResponse.token
                })

            // User 2 tries to delete User 1's bucket
            try await app.test(
                .DELETE, "/api/v1/buckets/user1-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token2)
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })

            // Verify bucket still exists for user 1
            try await app.test(
                .GET, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token1)
                },
                afterResponse: { res async throws in
                    let page = try res.content.decode(Page<Bucket>.self)
                    #expect(page.items.count == 1)
                    #expect(page.items.first?.name == "user1-bucket")
                })
        }
    }

    @Test("Delete bucket - Multiple buckets deletes only specified one")
    func testDeleteBucketSelectiveDelete() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "bucket-a")
            try await createBucket(app, token: token, name: "bucket-b")
            try await createBucket(app, token: token, name: "bucket-c")

            // Delete bucket-b
            try await app.test(
                .DELETE, "/api/v1/buckets/bucket-b",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify only bucket-b is deleted
            try await app.test(
                .GET, "/api/v1/buckets",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let page = try res.content.decode(Page<Bucket>.self)
                    #expect(page.items.count == 2)

                    let bucketNames = page.items.map { $0.name }
                    #expect(bucketNames.contains("bucket-a"))
                    #expect(!bucketNames.contains("bucket-b"))
                    #expect(bucketNames.contains("bucket-c"))
                })
        }
    }

    @Test("List objects - Without bucket param fails")
    func testListObjectsMissingBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .GET, "/api/v1/objects",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("List objects - Non-existent bucket fails")
    func testListObjectsNonExistentBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .GET, "/api/v1/objects?bucket=nonexistent",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("List objects - Empty bucket")
    func testListObjectsEmpty() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "empty-bucket")

            try await app.test(
                .GET, "/api/v1/objects?bucket=empty-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 0)
                    #expect(page.metadata.total == 0)
                })
        }
    }

    @Test("List objects - With files")
    func testListObjectsWithFiles() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            // Create test files
            try await putObject(
                app, bucketName: "test-bucket", key: "file1.txt", content: "content1")
            try await putObject(
                app, bucketName: "test-bucket", key: "file2.txt", content: "content2")
            try await putObject(
                app, bucketName: "test-bucket", key: "file3.txt", content: "content3")

            try await app.test(
                .GET, "/api/v1/objects?bucket=test-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 3)
                    #expect(page.metadata.total == 3)

                    let keys = page.items.map { $0.key }
                    #expect(keys.contains("file1.txt"))
                    #expect(keys.contains("file2.txt"))
                    #expect(keys.contains("file3.txt"))

                    // Verify all are files (not folders)
                    for item in page.items {
                        #expect(item.isFolder == false)
                    }
                })
        }
    }

    @Test("List objects - With folders and files (delimiter)")
    func testListObjectsWithFolders() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            // Create nested structure
            try await putObject(app, bucketName: "test-bucket", key: "root.txt", content: "root")
            try await putObject(
                app, bucketName: "test-bucket", key: "docs/file1.txt", content: "content1")
            try await putObject(
                app, bucketName: "test-bucket", key: "docs/file2.txt", content: "content2")
            try await putObject(
                app, bucketName: "test-bucket", key: "images/photo.jpg", content: "photo")

            try await app.test(
                .GET, "/api/v1/objects?bucket=test-bucket&delimiter=/",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)

                    // Should have: root.txt (file), docs/ (folder), images/ (folder)
                    #expect(page.items.count == 3)

                    let folders = page.items.filter { $0.isFolder }
                    let files = page.items.filter { !$0.isFolder }

                    #expect(folders.count == 2)
                    #expect(files.count == 1)

                    let folderKeys = folders.map { $0.key }
                    #expect(folderKeys.contains("docs/"))
                    #expect(folderKeys.contains("images/"))

                    #expect(files.first?.key == "root.txt")
                })
        }
    }

    @Test("List objects - With prefix filter")
    func testListObjectsWithPrefix() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            try await putObject(
                app, bucketName: "test-bucket", key: "docs/api/file1.txt", content: "content1")
            try await putObject(
                app, bucketName: "test-bucket", key: "docs/api/file2.txt", content: "content2")
            try await putObject(
                app, bucketName: "test-bucket", key: "docs/guides/guide1.txt", content: "guide1")
            try await putObject(
                app, bucketName: "test-bucket", key: "images/photo.jpg", content: "photo")

            try await app.test(
                .GET, "/api/v1/objects?bucket=test-bucket&prefix=docs/&delimiter=/",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)

                    // Should have: docs/api/ (folder), docs/guides/ (folder)
                    #expect(page.items.count == 2)

                    let folderKeys = page.items.filter { $0.isFolder }.map { $0.key }
                    #expect(folderKeys.contains("docs/api/"))
                    #expect(folderKeys.contains("docs/guides/"))

                    // Should not include images/
                    #expect(!page.items.contains { $0.key.starts(with: "images/") })
                })
        }
    }

    @Test("List objects - Folders sorted before files")
    func testListObjectsSorting() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            try await putObject(app, bucketName: "test-bucket", key: "zebra.txt", content: "z")
            try await putObject(app, bucketName: "test-bucket", key: "apple.txt", content: "a")
            try await putObject(
                app, bucketName: "test-bucket", key: "folder/file.txt", content: "f")

            try await app.test(
                .GET, "/api/v1/objects?bucket=test-bucket&delimiter=/",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 3)

                    // First item should be the folder
                    #expect(page.items[0].isFolder == true)
                    #expect(page.items[0].key == "folder/")

                    // Then files sorted alphabetically
                    #expect(page.items[1].isFolder == false)
                    #expect(page.items[1].key == "apple.txt")

                    #expect(page.items[2].isFolder == false)
                    #expect(page.items[2].key == "zebra.txt")
                })
        }
    }

    @Test("List objects - With pagination")
    func testListObjectsPagination() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            // Create 5 files
            for i in 1...5 {
                try await putObject(
                    app, bucketName: "test-bucket", key: "file\(i).txt", content: "content\(i)")
            }

            // Page 1
            try await app.test(
                .GET, "/api/v1/objects?bucket=test-bucket&page=1&per=2",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 2)
                    #expect(page.metadata.total == 5)
                    #expect(page.metadata.page == 1)
                })

            // Page 2
            try await app.test(
                .GET, "/api/v1/objects?bucket=test-bucket&page=2&per=2",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 2)
                    #expect(page.metadata.page == 2)
                })
        }
    }

    @Test("List objects - Without auth fails")
    func testListObjectsUnauthorized() async throws {
        try await withApp { app in
            try await app.test(
                .GET, "/api/v1/objects?bucket=test",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("List objects - User can only see their own buckets")
    func testListObjectsUserIsolation() async throws {
        try await withApp { app in
            // Create first user and bucket
            let token1 = try await createUserAndLogin(app)
            try await createBucket(app, token: token1, name: "user1-bucket")

            // Create second user
            let createDTO2 = User.Create(
                name: "User 2",
                username: "user2@example.com",
                password: "Pass123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO2)
                })

            let loginDTO2 = ["username": "user2@example.com", "password": "Pass123!"]
            var token2 = ""

            try await app.test(
                .POST, "/api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO2)
                },
                afterResponse: { res async throws in
                    let tokenResponse = try res.content.decode(ClientTokenResponse.self)
                    token2 = tokenResponse.token
                })

            // User 2 tries to access User 1's bucket
            try await app.test(
                .GET, "/api/v1/objects?bucket=user1-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token2)
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Upload object - Success")
    func testUploadObjectSuccess() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "upload-bucket")

            let fileContent = "Hello, World!"
            let fileData = fileContent.data(using: .utf8)!
            let fileName = "test.txt"

            try await app.test(
                .POST, "/api/v1/objects?bucket=upload-bucket&prefix=",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    req.headers.contentType = .formData

                    let boundary = "----WebKitFormBoundary\(UUID().uuidString)"
                    req.headers.replaceOrAdd(
                        name: .contentType, value: "multipart/form-data; boundary=\(boundary)")

                    var body = ""
                    body += "--\(boundary)\r\n"
                    body +=
                        "Content-Disposition: form-data; name=\"data\"; filename=\"\(fileName)\"\r\n"
                    body += "Content-Type: text/plain\r\n\r\n"
                    body += fileContent
                    body += "\r\n--\(boundary)--\r\n"

                    req.body = ByteBuffer(string: body)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let response = try res.content.decode(ObjectMeta.ResponseDTO.self)
                    #expect(response.key == "test.txt")
                    #expect(response.size == fileData.count)
                    #expect(response.contentType.starts(with: "text/plain"))
                    #expect(response.isFolder == false)
                })
        }
    }

    @Test("Upload object - With prefix")
    func testUploadObjectWithPrefix() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "upload-bucket")

            let fileContent = "File in folder"
            let fileName = "document.txt"

            try await app.test(
                .POST, "/api/v1/objects?bucket=upload-bucket&prefix=documents/",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)

                    let boundary = "----WebKitFormBoundary\(UUID().uuidString)"
                    req.headers.replaceOrAdd(
                        name: .contentType, value: "multipart/form-data; boundary=\(boundary)")

                    var body = ""
                    body += "--\(boundary)\r\n"
                    body +=
                        "Content-Disposition: form-data; name=\"data\"; filename=\"\(fileName)\"\r\n"
                    body += "Content-Type: text/plain\r\n\r\n"
                    body += fileContent
                    body += "\r\n--\(boundary)--\r\n"

                    req.body = ByteBuffer(string: body)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let response = try res.content.decode(ObjectMeta.ResponseDTO.self)
                    #expect(response.key == "documents/document.txt")
                    #expect(response.isFolder == false)
                })
        }
    }

    @Test("Upload object - Without bucket param fails")
    func testUploadObjectMissingBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .POST, "/api/v1/objects",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("Upload object - Non-existent bucket fails")
    func testUploadObjectNonExistentBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            let fileContent = "test"
            let fileName = "test.txt"

            try await app.test(
                .POST, "/api/v1/objects?bucket=nonexistent-bucket&prefix=",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)

                    let boundary = "----WebKitFormBoundary\(UUID().uuidString)"
                    req.headers.replaceOrAdd(
                        name: .contentType, value: "multipart/form-data; boundary=\(boundary)")

                    var body = ""
                    body += "--\(boundary)\r\n"
                    body +=
                        "Content-Disposition: form-data; name=\"data\"; filename=\"\(fileName)\"\r\n"
                    body += "Content-Type: text/plain\r\n\r\n"
                    body += fileContent
                    body += "\r\n--\(boundary)--\r\n"

                    req.body = ByteBuffer(string: body)
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Upload object - Without auth fails")
    func testUploadObjectUnauthorized() async throws {
        try await withApp { app in
            try await app.test(
                .POST, "/api/v1/objects?bucket=test-bucket&prefix=",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Upload object - Empty filename fails")
    func testUploadObjectEmptyFilename() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "upload-bucket")

            let fileContent = "content"

            try await app.test(
                .POST, "/api/v1/objects?bucket=upload-bucket&prefix=",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)

                    let boundary = "----WebKitFormBoundary\(UUID().uuidString)"
                    req.headers.replaceOrAdd(
                        name: .contentType, value: "multipart/form-data; boundary=\(boundary)")

                    var body = ""
                    body += "--\(boundary)\r\n"
                    body += "Content-Disposition: form-data; name=\"data\"; filename=\"\"\r\n"
                    body += "Content-Type: text/plain\r\n\r\n"
                    body += fileContent
                    body += "\r\n--\(boundary)--\r\n"

                    req.body = ByteBuffer(string: body)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("Upload object - User cannot upload to other user's bucket")
    func testUploadObjectUserIsolation() async throws {
        try await withApp { app in
            // Create first user and bucket
            let token1 = try await createUserAndLogin(app)
            try await createBucket(app, token: token1, name: "user1-bucket")

            // Create second user
            let createDTO2 = User.Create(
                name: "User 2",
                username: "user2@example.com",
                password: "Pass123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO2)
                })

            let loginDTO2 = ["username": "user2@example.com", "password": "Pass123!"]
            var token2 = ""

            try await app.test(
                .POST, "/api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO2)
                },
                afterResponse: { res async throws in
                    let tokenResponse = try res.content.decode(ClientTokenResponse.self)
                    token2 = tokenResponse.token
                })

            let fileContent = "unauthorized upload"
            let fileName = "hack.txt"

            // User 2 tries to upload to User 1's bucket
            try await app.test(
                .POST, "/api/v1/objects?bucket=user1-bucket&prefix=",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token2)

                    let boundary = "----WebKitFormBoundary\(UUID().uuidString)"
                    req.headers.replaceOrAdd(
                        name: .contentType, value: "multipart/form-data; boundary=\(boundary)")

                    var body = ""
                    body += "--\(boundary)\r\n"
                    body +=
                        "Content-Disposition: form-data; name=\"data\"; filename=\"\(fileName)\"\r\n"
                    body += "Content-Type: text/plain\r\n\r\n"
                    body += fileContent
                    body += "\r\n--\(boundary)--\r\n"

                    req.body = ByteBuffer(string: body)
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Upload object - File is retrievable after upload")
    func testUploadObjectAndRetrieve() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "upload-bucket")

            let fileContent = "Test file content"
            let fileName = "retrievable.txt"

            // Upload file
            try await app.test(
                .POST, "/api/v1/objects?bucket=upload-bucket&prefix=",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)

                    let boundary = "----WebKitFormBoundary\(UUID().uuidString)"
                    req.headers.replaceOrAdd(
                        name: .contentType, value: "multipart/form-data; boundary=\(boundary)")

                    var body = ""
                    body += "--\(boundary)\r\n"
                    body +=
                        "Content-Disposition: form-data; name=\"data\"; filename=\"\(fileName)\"\r\n"
                    body += "Content-Type: text/plain\r\n\r\n"
                    body += fileContent
                    body += "\r\n--\(boundary)--\r\n"

                    req.body = ByteBuffer(string: body)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                })

            // Verify file appears in list
            try await app.test(
                .GET, "/api/v1/objects?bucket=upload-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 1)
                    #expect(page.items.first?.key == fileName)
                    #expect(page.items.first?.size == fileContent.utf8.count)
                })
        }
    }

    @Test("Delete object - Success")
    func testDeleteObjectSuccess() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "delete-bucket")

            // Upload a file first
            try await putObject(
                app, bucketName: "delete-bucket", key: "deleteme.txt", content: "content")

            // Delete the file
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=delete-bucket&key=deleteme.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify file is no longer in list
            try await app.test(
                .GET, "/api/v1/objects?bucket=delete-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 0)
                })
        }
    }

    @Test("Delete object - With prefix path")
    func testDeleteObjectWithPrefix() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "delete-bucket")

            // Upload a file in a folder
            try await putObject(
                app, bucketName: "delete-bucket", key: "folder/subfolder/file.txt",
                content: "content")

            // Delete the file
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=delete-bucket&key=folder/subfolder/file.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify file is deleted
            try await app.test(
                .GET, "/api/v1/objects?bucket=delete-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 0)
                })
        }
    }

    @Test("Delete object - Missing bucket param fails")
    func testDeleteObjectMissingBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .DELETE, "/api/v1/objects?key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("Delete object - Missing key param fails")
    func testDeleteObjectMissingKey() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "delete-bucket")

            try await app.test(
                .DELETE, "/api/v1/objects?bucket=delete-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("Delete object - Non-existent bucket fails")
    func testDeleteObjectNonExistentBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .DELETE, "/api/v1/objects?bucket=nonexistent-bucket&key=file.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Delete object - Non-existent object succeeds (idempotent)")
    func testDeleteObjectNonExistent() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "delete-bucket")

            // Delete non-existent file (should succeed per S3 behavior)
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=delete-bucket&key=nonexistent.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })
        }
    }

    @Test("Delete object - Without auth fails")
    func testDeleteObjectUnauthorized() async throws {
        try await withApp { app in
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=test-bucket&key=file.txt",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Delete object - User cannot delete from other user's bucket")
    func testDeleteObjectUserIsolation() async throws {
        try await withApp { app in
            // Create first user and bucket
            let token1 = try await createUserAndLogin(app)
            try await createBucket(app, token: token1, name: "user1-bucket")
            try await putObject(
                app, bucketName: "user1-bucket", key: "private.txt", content: "secret")

            // Create second user
            let createDTO2 = User.Create(
                name: "User 2",
                username: "user2@example.com",
                password: "Pass123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO2)
                })

            let loginDTO2 = ["username": "user2@example.com", "password": "Pass123!"]
            var token2 = ""

            try await app.test(
                .POST, "/api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO2)
                },
                afterResponse: { res async throws in
                    let tokenResponse = try res.content.decode(ClientTokenResponse.self)
                    token2 = tokenResponse.token
                })

            // User 2 tries to delete User 1's file
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=user1-bucket&key=private.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token2)
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })

            // Verify file still exists for user 1
            try await app.test(
                .GET, "/api/v1/objects?bucket=user1-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token1)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 1)
                    #expect(page.items.first?.key == "private.txt")
                })
        }
    }

    @Test("Delete object - Path traversal attack fails")
    func testDeleteObjectPathTraversalAttack() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "safe-bucket")

            // Create a file in the safe bucket
            try await putObject(
                app, bucketName: "safe-bucket", key: "safe-file.txt", content: "safe")

            // Try to delete with path traversal
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=safe-bucket&key=../../etc/passwd",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    // Should not crash or delete system files, should just return noContent
                    // (file won't exist but that's OK - idempotent behavior)
                    #expect(res.status == .noContent)
                })

            // Verify original file still exists
            try await app.test(
                .GET, "/api/v1/objects?bucket=safe-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 1)
                    #expect(page.items.first?.key == "safe-file.txt")
                })
        }
    }

    @Test("Delete object - Null byte injection fails")
    func testDeleteObjectNullByteInjection() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            try await putObject(
                app, bucketName: "test-bucket", key: "important.txt", content: "important")

            // Try to inject null byte
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=test-bucket&key=test.txt%00.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify file still exists
            try await app.test(
                .GET, "/api/v1/objects?bucket=test-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 1)
                })
        }
    }

    @Test("Delete object - Absolute path injection fails")
    func testDeleteObjectAbsolutePath() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            try await putObject(app, bucketName: "test-bucket", key: "file.txt", content: "data")

            // Try to use absolute path
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=test-bucket&key=/etc/passwd",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify original file still exists
            try await app.test(
                .GET, "/api/v1/objects?bucket=test-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 1)
                })
        }
    }

    @Test("Delete folder - Path traversal in prefix fails")
    func testDeleteFolderPathTraversal() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            // Create files in legitimate folder
            try await putObject(
                app, bucketName: "test-bucket", key: "data/file1.txt", content: "data1")
            try await putObject(
                app, bucketName: "test-bucket", key: "data/file2.txt", content: "data2")

            // Try to delete with path traversal in folder prefix - should fail
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=test-bucket&key=../../",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    // Should fail with internal server error due to invalid prefix
                    #expect(res.status == .internalServerError)
                })

            // Verify files still exist
            try await app.test(
                .GET, "/api/v1/objects?bucket=test-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    // Should have at least 1 item (the folder or files)
                    #expect(page.items.count >= 1)

                    // Verify the data files are still there
                    let keys = page.items.map { $0.key }
                    #expect(keys.contains("data/file1.txt") || keys.contains("data/"))
                })
        }
    }

    @Test("Delete folder - Normal folder deletion works")
    func testDeleteFolderSuccess() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "delete-bucket")

            // Create files in folder
            try await putObject(
                app, bucketName: "delete-bucket", key: "folder/file1.txt", content: "data1")
            try await putObject(
                app, bucketName: "delete-bucket", key: "folder/file2.txt", content: "data2")
            try await putObject(
                app, bucketName: "delete-bucket", key: "folder/subfolder/file3.txt",
                content: "data3")
            try await putObject(
                app, bucketName: "delete-bucket", key: "other.txt", content: "keep")

            // Delete the folder
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=delete-bucket&key=folder/",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify folder contents are deleted but other file remains
            try await app.test(
                .GET, "/api/v1/objects?bucket=delete-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 1)
                    #expect(page.items.first?.key == "other.txt")
                })
        }
    }

    @Test("Delete folder - Nested folder deletion works")
    func testDeleteNestedFolder() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "delete-bucket")

            // Create nested structure
            try await putObject(
                app, bucketName: "delete-bucket", key: "a/b/c/file.txt", content: "data")
            try await putObject(
                app, bucketName: "delete-bucket", key: "a/b/file2.txt", content: "data2")
            try await putObject(
                app, bucketName: "delete-bucket", key: "a/file3.txt", content: "data3")

            // Delete middle folder
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=delete-bucket&key=a/b/",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify only a/file3.txt remains (or a/ folder with the file inside)
            try await app.test(
                .GET, "/api/v1/objects?bucket=delete-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    // May be 1 item (a/ folder) or 1 item (a/file3.txt) depending on delimiter
                    #expect(page.items.count >= 1)

                    // Check that a/file3.txt still exists (either directly or within a/ folder)
                    let keys = page.items.map { $0.key }
                    #expect(keys.contains("a/file3.txt") || keys.contains("a/"))

                    // Verify a/b/ files are gone
                    #expect(!keys.contains("a/b/c/file.txt"))
                    #expect(!keys.contains("a/b/file2.txt"))
                })
        }
    }

    @Test("Delete folder - Special characters in folder name")
    func testDeleteFolderSpecialCharacters() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "delete-bucket")

            // Create files with special characters
            try await putObject(
                app, bucketName: "delete-bucket", key: "test folder/file.txt", content: "data")
            try await putObject(
                app, bucketName: "delete-bucket", key: "test folder/file2.txt", content: "data2")

            // Delete folder with space
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=delete-bucket&key=test%20folder/",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify all files deleted
            try await app.test(
                .GET, "/api/v1/objects?bucket=delete-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 0)
                })
        }
    }

    @Test("Download single file - Success")
    func testDownloadSingleFileSuccess() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "download-bucket")

            let fileContent = "Hello, World!"
            try await putObject(
                app, bucketName: "download-bucket", key: "test.txt", content: fileContent)

            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        DownloadRequestDTO(bucket: "download-bucket", keys: ["test.txt"]))
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .plainText)

                    let data = Data(buffer: res.body)
                    let content = String(data: data, encoding: .utf8)
                    #expect(content == fileContent)

                    // Verify Content-Disposition header
                    let disposition = res.headers.first(name: "Content-Disposition")
                    #expect(disposition?.contains("attachment") == true)
                    #expect(disposition?.contains("test.txt") == true)
                })
        }
    }

    @Test("Download single file - Non-existent file fails")
    func testDownloadSingleFileNonExistent() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "download-bucket")

            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        DownloadRequestDTO(bucket: "download-bucket", keys: ["nonexistent.txt"]))
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Download multiple files - Creates ZIP")
    func testDownloadMultipleFilesZip() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "download-bucket")

            // Create test files
            try await putObject(
                app, bucketName: "download-bucket", key: "folder/file1.txt", content: "content1")
            try await putObject(
                app, bucketName: "download-bucket", key: "folder/file2.txt", content: "content2")

            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        DownloadRequestDTO(
                            bucket: "download-bucket",
                            keys: ["folder/file1.txt", "folder/file2.txt"]))
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .zip)

                    // Verify Content-Disposition header contains .zip
                    let disposition = res.headers.first(name: "Content-Disposition")
                    #expect(disposition?.contains("attachment") == true)
                    #expect(disposition?.contains(".zip") == true)

                    // Verify we got data back
                    let data = Data(buffer: res.body)
                    #expect(data.count > 0)
                })
        }
    }

    @Test("Download folder - Creates ZIP with folder contents")
    func testDownloadFolderZip() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "download-bucket")

            // Create nested structure
            try await putObject(
                app, bucketName: "download-bucket", key: "docs/readme.txt", content: "readme")
            try await putObject(
                app, bucketName: "download-bucket", key: "docs/guide.txt", content: "guide")
            try await putObject(
                app, bucketName: "download-bucket", key: "docs/subfolder/nested.txt",
                content: "nested")

            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        DownloadRequestDTO(bucket: "download-bucket", keys: ["docs/"]))
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .zip)

                    let data = Data(buffer: res.body)
                    #expect(data.count > 0)
                })
        }
    }

    @Test("Download - Empty keys array fails")
    func testDownloadEmptyKeys() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "download-bucket")

            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(DownloadRequestDTO(bucket: "download-bucket", keys: []))
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("Download - Non-existent bucket fails")
    func testDownloadNonExistentBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        DownloadRequestDTO(bucket: "nonexistent-bucket", keys: ["file.txt"]))
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Download - Without auth fails")
    func testDownloadUnauthorized() async throws {
        try await withApp { app in
            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    try req.content.encode(
                        DownloadRequestDTO(bucket: "test-bucket", keys: ["file.txt"]))
                },
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Download - User cannot download from other user's bucket")
    func testDownloadUserIsolation() async throws {
        try await withApp { app in
            // Create first user and bucket
            let token1 = try await createUserAndLogin(app)
            try await createBucket(app, token: token1, name: "user1-bucket")
            try await putObject(
                app, bucketName: "user1-bucket", key: "private.txt", content: "secret")

            // Create second user
            let createDTO2 = User.Create(
                name: "User 2",
                username: "user2@example.com",
                password: "Pass123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO2)
                })

            let loginDTO2 = ["username": "user2@example.com", "password": "Pass123!"]
            var token2 = ""

            try await app.test(
                .POST, "/api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO2)
                },
                afterResponse: { res async throws in
                    let tokenResponse = try res.content.decode(ClientTokenResponse.self)
                    token2 = tokenResponse.token
                })

            // User 2 tries to download from User 1's bucket
            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token2)
                    try req.content.encode(
                        DownloadRequestDTO(bucket: "user1-bucket", keys: ["private.txt"]))
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Download - No files found returns error")
    func testDownloadNoFilesFound() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "download-bucket")

            // Try to download multiple non-existent files
            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        DownloadRequestDTO(
                            bucket: "download-bucket", keys: ["missing1.txt", "missing2.txt"]))
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Download single file - Content-Length header set correctly")
    func testDownloadContentLength() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "download-bucket")

            let fileContent = "Test content with known length"
            try await putObject(
                app, bucketName: "download-bucket", key: "test.txt", content: fileContent)

            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        DownloadRequestDTO(bucket: "download-bucket", keys: ["test.txt"]))
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let contentLength = res.headers.first(name: .contentLength)
                    #expect(contentLength == String(fileContent.utf8.count))
                })
        }
    }

    @Test("Download - Mixed files and folders")
    func testDownloadMixedFilesAndFolders() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "download-bucket")

            // Create structure
            try await putObject(
                app, bucketName: "download-bucket", key: "root-file.txt", content: "root")
            try await putObject(
                app, bucketName: "download-bucket", key: "folder1/file1.txt", content: "file1")
            try await putObject(
                app, bucketName: "download-bucket", key: "folder1/file2.txt", content: "file2")
            try await putObject(
                app, bucketName: "download-bucket", key: "folder2/file3.txt", content: "file3")

            // Download root file and folder1
            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        DownloadRequestDTO(
                            bucket: "download-bucket", keys: ["root-file.txt", "folder1/"]))
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                    #expect(res.headers.contentType == .zip)

                    let data = Data(buffer: res.body)
                    #expect(data.count > 0)
                })
        }
    }

    // MARK: - Versioning Tests

    private func createVersionedBucket(_ app: Application, token: String, name: String)
        async throws
    {
        let createDTO = Bucket.Create(name: name, versioningEnabled: true)

        try await app.test(
            .POST, "/api/v1/buckets",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)
                try req.content.encode(createDTO)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })

        // Load caches to ensure versioning is active
        let loadCacheLifecycle = LoadCacheLifecycle()
        try await loadCacheLifecycle.didBootAsync(app)
    }

    private func uploadFileViaAPI(
        _ app: Application, token: String, bucketName: String, fileName: String, content: String
    ) async throws -> ObjectMeta.ResponseDTO? {
        var result: ObjectMeta.ResponseDTO?

        try await app.test(
            .POST, "/api/v1/objects?bucket=\(bucketName)&prefix=",
            beforeRequest: { req in
                req.headers.bearerAuthorization = BearerAuthorization(token: token)

                let boundary = "----WebKitFormBoundary\(UUID().uuidString)"
                req.headers.replaceOrAdd(
                    name: .contentType, value: "multipart/form-data; boundary=\(boundary)")

                var body = ""
                body += "--\(boundary)\r\n"
                body +=
                    "Content-Disposition: form-data; name=\"data\"; filename=\"\(fileName)\"\r\n"
                body += "Content-Type: text/plain\r\n\r\n"
                body += content
                body += "\r\n--\(boundary)--\r\n"

                req.body = ByteBuffer(string: body)
            },
            afterResponse: { res async throws in
                #expect(res.status == .ok)
                result = try res.content.decode(ObjectMeta.ResponseDTO.self)
            })

        return result
    }

    @Test("Get versioning - Disabled by default")
    func testGetVersioningDisabledByDefault() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-versioning-bucket")

            try await app.test(
                .GET, "/api/v1/buckets/test-versioning-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let status = try res.content.decode(
                        InternalBucketController.VersioningStatusDTO.self)
                    #expect(status.status == "Disabled")
                })
        }
    }

    @Test("Get versioning - Enabled when created with versioning")
    func testGetVersioningEnabled() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token, name: "versioned-bucket")

            try await app.test(
                .GET, "/api/v1/buckets/versioned-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let status = try res.content.decode(
                        InternalBucketController.VersioningStatusDTO.self)
                    #expect(status.status == "Enabled")
                })
        }
    }

    @Test("Get versioning - Non-existent bucket fails")
    func testGetVersioningNonExistentBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .GET, "/api/v1/buckets/nonexistent-bucket/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Get versioning - Without auth fails")
    func testGetVersioningUnauthorized() async throws {
        try await withApp { app in
            try await app.test(
                .GET, "/api/v1/buckets/test-bucket/versioning",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Set versioning - Enable versioning")
    func testSetVersioningEnable() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-versioning")

            // Enable versioning
            try await app.test(
                .PUT, "/api/v1/buckets/test-versioning/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        InternalBucketController.VersioningStatusDTO(status: "Enabled"))
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let status = try res.content.decode(
                        InternalBucketController.VersioningStatusDTO.self)
                    #expect(status.status == "Enabled")
                })

            // Verify it's enabled
            try await app.test(
                .GET, "/api/v1/buckets/test-versioning/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let status = try res.content.decode(
                        InternalBucketController.VersioningStatusDTO.self)
                    #expect(status.status == "Enabled")
                })
        }
    }

    @Test("Set versioning - Suspend versioning")
    func testSetVersioningSuspend() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token, name: "test-versioning")

            // Suspend versioning
            try await app.test(
                .PUT, "/api/v1/buckets/test-versioning/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        InternalBucketController.VersioningStatusDTO(status: "Suspended"))
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let status = try res.content.decode(
                        InternalBucketController.VersioningStatusDTO.self)
                    #expect(status.status == "Suspended")
                })
        }
    }

    @Test("Set versioning - Invalid status fails")
    func testSetVersioningInvalidStatus() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-versioning")

            try await app.test(
                .PUT, "/api/v1/buckets/test-versioning/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        InternalBucketController.VersioningStatusDTO(status: "Invalid"))
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("Set versioning - Non-existent bucket fails")
    func testSetVersioningNonExistentBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .PUT, "/api/v1/buckets/nonexistent/versioning",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        InternalBucketController.VersioningStatusDTO(status: "Enabled"))
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("Set versioning - Without auth fails")
    func testSetVersioningUnauthorized() async throws {
        try await withApp { app in
            try await app.test(
                .PUT, "/api/v1/buckets/test-bucket/versioning",
                beforeRequest: { req in
                    try req.content.encode(
                        InternalBucketController.VersioningStatusDTO(status: "Enabled"))
                },
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Upload to versioned bucket - Returns version ID")
    func testUploadVersionedReturnsVersionId() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token, name: "versioned-bucket")

            let result = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Version 1")

            #expect(result != nil)
            #expect(result?.versionId != nil)
            #expect(result?.versionId != "null")
            #expect(result?.versionId?.count == 32)
            #expect(result?.isLatest == true)
        }
    }

    @Test("Upload to non-versioned bucket - No version ID")
    func testUploadNonVersionedNoVersionId() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "non-versioned-bucket")

            let result = try await uploadFileViaAPI(
                app, token: token, bucketName: "non-versioned-bucket",
                fileName: "test.txt", content: "Content")

            #expect(result != nil)
            #expect(result?.versionId == nil)
        }
    }

    @Test("Upload multiple versions - Creates unique version IDs")
    func testUploadMultipleVersions() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token, name: "versioned-bucket")

            let result1 = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Version 1")

            let result2 = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Version 2")

            let result3 = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Version 3")

            #expect(result1?.versionId != nil)
            #expect(result2?.versionId != nil)
            #expect(result3?.versionId != nil)
            #expect(result1?.versionId != result2?.versionId)
            #expect(result2?.versionId != result3?.versionId)
            #expect(result1?.versionId != result3?.versionId)
        }
    }

    @Test("List object versions - Returns all versions")
    func testListObjectVersions() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token, name: "versioned-bucket")

            // Upload multiple versions
            let v1 = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Version 1")

            let v2 = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Version 2")

            let v3 = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Version 3")

            // List versions
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(versions.count == 3)

                    let versionIds = versions.compactMap { $0.versionId }
                    #expect(versionIds.contains(v1?.versionId ?? ""))
                    #expect(versionIds.contains(v2?.versionId ?? ""))
                    #expect(versionIds.contains(v3?.versionId ?? ""))

                    // Only one should be latest
                    let latestCount = versions.filter { $0.isLatest == true }.count
                    #expect(latestCount == 1)
                })
        }
    }

    @Test("List object versions - Empty for non-existent key")
    func testListObjectVersionsEmpty() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket-empty")

            // Don't upload any object - query for a key that doesn't exist
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=test-bucket-empty&key=non-existent.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(versions.count == 0)
                })
        }
    }

    @Test("List object versions - Non-versioned bucket returns single version")
    func testListObjectVersionsNonVersionedBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "non-versioned-bucket")

            // Upload object to non-versioned bucket
            try await putObject(
                app, bucketName: "non-versioned-bucket", key: "test.txt", content: "content")

            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=non-versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    // Non-versioned bucket still returns the current object as a single "version"
                    #expect(versions.count == 1)
                    #expect(versions[0].key == "test.txt")
                })
        }
    }

    @Test("List object versions - Missing bucket param fails")
    func testListObjectVersionsMissingBucket() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            try await app.test(
                .GET, "/api/v1/objects/versions?key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("List object versions - Missing key param fails")
    func testListObjectVersionsMissingKey() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=test-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("List object versions - Without auth fails")
    func testListObjectVersionsUnauthorized() async throws {
        try await withApp { app in
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=test&key=test.txt",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Delete object version - Success")
    func testDeleteObjectVersionSuccess() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token, name: "versioned-bucket")

            // Upload two versions
            let v1 = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Version 1")

            let v2 = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Version 2")

            // Delete first version
            try await app.test(
                .DELETE,
                "/api/v1/objects/version?bucket=versioned-bucket&key=test.txt&versionId=\(v1!.versionId!)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // Verify only one version remains
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(versions.count == 1)
                    #expect(versions.first?.versionId == v2?.versionId)
                })
        }
    }

    @Test("Delete object version - Missing params fails")
    func testDeleteObjectVersionMissingParams() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createBucket(app, token: token, name: "test-bucket")

            // Missing bucket
            try await app.test(
                .DELETE, "/api/v1/objects/version?key=test.txt&versionId=abc",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })

            // Missing key
            try await app.test(
                .DELETE, "/api/v1/objects/version?bucket=test-bucket&versionId=abc",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })

            // Missing versionId
            try await app.test(
                .DELETE, "/api/v1/objects/version?bucket=test-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .badRequest)
                })
        }
    }

    @Test("Delete object version - Without auth fails")
    func testDeleteObjectVersionUnauthorized() async throws {
        try await withApp { app in
            try await app.test(
                .DELETE, "/api/v1/objects/version?bucket=test&key=test.txt&versionId=abc",
                afterResponse: { res async in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Delete object version - User isolation")
    func testDeleteObjectVersionUserIsolation() async throws {
        try await withApp { app in
            // Create first user with versioned bucket
            let token1 = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token1, name: "user1-versioned")

            let v1 = try await uploadFileViaAPI(
                app, token: token1, bucketName: "user1-versioned",
                fileName: "test.txt", content: "Secret content")

            // Create second user
            let createDTO2 = User.Create(
                name: "User 2",
                username: "user2@example.com",
                password: "Pass123!",
                isAdmin: false
            )

            try await app.test(
                .POST, "/api/v1/users",
                beforeRequest: { req in
                    try req.content.encode(createDTO2)
                })

            let loginDTO2 = ["username": "user2@example.com", "password": "Pass123!"]
            var token2 = ""

            try await app.test(
                .POST, "/api/v1/users/login",
                beforeRequest: { req in
                    try req.content.encode(loginDTO2)
                },
                afterResponse: { res async throws in
                    let tokenResponse = try res.content.decode(ClientTokenResponse.self)
                    token2 = tokenResponse.token
                })

            // User 2 tries to delete User 1's version
            try await app.test(
                .DELETE,
                "/api/v1/objects/version?bucket=user1-versioned&key=test.txt&versionId=\(v1!.versionId!)",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token2)
                },
                afterResponse: { res async in
                    #expect(res.status == .notFound)
                })

            // Verify version still exists for user 1
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=user1-versioned&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token1)
                },
                afterResponse: { res async throws in
                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(versions.count == 1)
                    #expect(versions.first?.versionId == v1?.versionId)
                })
        }
    }

    @Test("Download specific version - Success")
    func testDownloadSpecificVersion() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token, name: "versioned-bucket")

            // Upload two versions with different content
            let v1 = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "First version content")

            _ = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Second version content")

            // Download first version
            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        DownloadRequestDTO(
                            bucket: "versioned-bucket",
                            keys: ["test.txt"],
                            versionId: v1?.versionId
                        ))
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let data = Data(buffer: res.body)
                    let content = String(data: data, encoding: .utf8)
                    #expect(content == "First version content")
                })
        }
    }

    @Test("Download latest version - Returns latest content")
    func testDownloadLatestVersion() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token, name: "versioned-bucket")

            // Upload multiple versions
            _ = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "First version")

            _ = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Second version")

            _ = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Latest version")

            // Download without specifying version (should get latest)
            try await app.test(
                .POST, "/api/v1/objects/download",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(
                        DownloadRequestDTO(bucket: "versioned-bucket", keys: ["test.txt"]))
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)

                    let data = Data(buffer: res.body)
                    let content = String(data: data, encoding: .utf8)
                    #expect(content == "Latest version")
                })
        }
    }

    @Test("Delete versioned object - Creates delete marker")
    func testDeleteVersionedObjectCreatesMarker() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token, name: "versioned-bucket")

            // Upload object
            let v1 = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "test.txt", content: "Content to delete")

            // Delete object (without specifying version)
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // List versions - should show both original and delete marker
            try await app.test(
                .GET, "/api/v1/objects/versions?bucket=versioned-bucket&key=test.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let versions = try res.content.decode([ObjectMeta.ResponseDTO].self)
                    #expect(versions.count == 2)

                    // One should be delete marker (latest)
                    let deleteMarkers = versions.filter { $0.isDeleteMarker == true }
                    #expect(deleteMarkers.count == 1)
                    #expect(deleteMarkers.first?.isLatest == true)

                    // Original version should still exist
                    let originalVersions = versions.filter { $0.isDeleteMarker != true }
                    #expect(originalVersions.count == 1)
                    #expect(originalVersions.first?.versionId == v1?.versionId)
                })
        }
    }

    @Test("List objects - Deleted versioned object not shown")
    func testListObjectsDeletedVersionedNotShown() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)
            try await createVersionedBucket(app, token: token, name: "versioned-bucket")

            // Upload objects
            _ = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "keep.txt", content: "Keep this")

            _ = try await uploadFileViaAPI(
                app, token: token, bucketName: "versioned-bucket",
                fileName: "delete.txt", content: "Delete this")

            // Delete one object
            try await app.test(
                .DELETE, "/api/v1/objects?bucket=versioned-bucket&key=delete.txt",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async in
                    #expect(res.status == .noContent)
                })

            // List objects - should only show keep.txt
            try await app.test(
                .GET, "/api/v1/objects?bucket=versioned-bucket",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let page = try res.content.decode(Page<ObjectMeta.ResponseDTO>.self)
                    #expect(page.items.count == 1)
                    #expect(page.items.first?.key == "keep.txt")
                })
        }
    }
}
