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

import Crypto
import Foundation
import NIOCore
import NIOHTTP1
import SotoCore
import SotoS3
import SotoSignerV4
import Testing
import Vapor
import VaporTesting

@testable import Alarik

@Suite("S3 Tagging tests", .serialized)
struct S3TaggingTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()
            let loadCacheLifecycle = LoadCacheLifecycle()
            try await loadCacheLifecycle.didBootAsync(app)
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

    private func signedHeaders(
        for method: HTTPMethod,
        path: String,
        query: String? = nil,
        body: Data? = nil,
        additionalHeaders: [String: String] = [:]
    ) -> HTTPHeaders {
        var fullPath = path
        if let query = query, !query.isEmpty {
            fullPath += "?\(query)"
        }

        let urlString = "http://\(host)\(fullPath)"
        guard let url = URL(string: urlString) else {
            Issue.record("Invalid URL: \(urlString)")
            return HTTPHeaders()
        }

        let signer = AWSSigner(
            credentials: StaticCredential(accessKeyId: accessKey, secretAccessKey: secretKey),
            name: "s3",
            region: region
        )

        var headers: [(String, String)] = [("host", host)]
        for (key, value) in additionalHeaders {
            headers.append((key, value))
        }

        let signed = signer.signHeaders(
            url: url,
            method: method,
            headers: HTTPHeaders(headers),
            body: body != nil ? .data(body!) : .none
        )

        return signed
    }

    private func createBucket(_ app: Application, bucketName: String) async throws {
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)")
        try await app.test(
            .PUT, "/\(bucketName)",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func enableVersioning(_ app: Application, bucketName: String) async throws {
        let body = """
            <?xml version="1.0" encoding="UTF-8"?>
            <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Enabled</Status>
            </VersioningConfiguration>
            """
        let bodyData = Data(body.utf8)
        let signed = signedHeaders(
            for: .PUT, path: "/\(bucketName)", query: "versioning", body: bodyData)
        try await app.test(
            .PUT, "/\(bucketName)?versioning",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: bodyData)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func putObject(_ app: Application, bucketName: String, key: String, content: String)
        async throws
    {
        let data = content.data(using: .utf8)!
        let signed = signedHeaders(for: .PUT, path: "/\(bucketName)/\(key)", body: data)

        try await app.test(
            .PUT, "/\(bucketName)/\(key)",
            beforeRequest: { req in
                req.headers.add(contentsOf: signed)
                req.body = ByteBuffer(data: data)
            },
            afterResponse: { res in
                #expect(res.status == .ok)
            })
    }

    private func taggingXML(_ tags: [String: String]) -> String {
        let tagElements = tags.map { "<Tag><Key>\($0.key)</Key><Value>\($0.value)</Value></Tag>" }
            .joined()
        return """
            <?xml version="1.0" encoding="UTF-8"?>
            <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <TagSet>\(tagElements)</TagSet>
            </Tagging>
            """
    }

    // MARK: - Bucket tagging

    @Test("PUT/GET/DELETE BucketTagging round-trip")
    func testBucketTaggingRoundTrip() async throws {
        let bucketName = "test-bucket-tagging-roundtrip"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let xml = taggingXML(["env": "prod", "team": "storage"])
            let bodyData = Data(xml.utf8)
            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)", query: "tagging", body: bodyData)
            try await app.test(
                .PUT, "/\(bucketName)?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "tagging")
            try await app.test(
                .GET, "/\(bucketName)?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<Key>env</Key><Value>prod</Value>"))
                    #expect(res.body.string.contains("<Key>team</Key><Value>storage</Value>"))
                })

            let deleteSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)", query: "tagging")
            try await app.test(
                .DELETE, "/\(bucketName)?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            try await app.test(
                .GET, "/\(bucketName)?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    #expect(res.body.string.contains("<Code>NoSuchTagSet</Code>"))
                })
        }
    }

    @Test("GetBucketTagging 404s when never configured")
    func testGetBucketTaggingNotConfigured() async throws {
        let bucketName = "test-bucket-tagging-unset"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "tagging")
            try await app.test(
                .GET, "/\(bucketName)?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("PutBucketTagging overwrites entirely rather than merging")
    func testPutBucketTaggingOverwritesEntirely() async throws {
        let bucketName = "test-bucket-tagging-overwrite"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let firstXML = taggingXML(["a": "1"])
            let firstBody = Data(firstXML.utf8)
            let firstSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)", query: "tagging", body: firstBody)
            try await app.test(
                .PUT, "/\(bucketName)?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: firstSigned)
                    req.body = ByteBuffer(data: firstBody)
                })

            let secondXML = taggingXML(["b": "2"])
            let secondBody = Data(secondXML.utf8)
            let secondSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)", query: "tagging", body: secondBody)
            try await app.test(
                .PUT, "/\(bucketName)?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: secondSigned)
                    req.body = ByteBuffer(data: secondBody)
                })

            let getSigned = signedHeaders(for: .GET, path: "/\(bucketName)", query: "tagging")
            try await app.test(
                .GET, "/\(bucketName)?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(!res.body.string.contains("<Key>a</Key>"))
                    #expect(res.body.string.contains("<Key>b</Key><Value>2</Value>"))
                })
        }
    }

    // MARK: - Object tagging

    @Test("PUT/GET/DELETE ObjectTagging round-trip")
    func testObjectTaggingRoundTrip() async throws {
        let bucketName = "test-object-tagging-roundtrip"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "file.txt", content: "data")

            let xml = taggingXML(["project": "alarik"])
            let bodyData = Data(xml.utf8)
            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/file.txt", query: "tagging", body: bodyData)
            try await app.test(
                .PUT, "/\(bucketName)/file.txt?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/file.txt", query: "tagging")
            try await app.test(
                .GET, "/\(bucketName)/file.txt?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<Key>project</Key><Value>alarik</Value>"))
                })

            let deleteSigned = signedHeaders(
                for: .DELETE, path: "/\(bucketName)/file.txt", query: "tagging")
            try await app.test(
                .DELETE, "/\(bucketName)/file.txt?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            try await app.test(
                .GET, "/\(bucketName)/file.txt?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    // Unlike bucket tagging, object tagging always 200s, even with no tags
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<TagSet></TagSet>") || res.body.string.contains("<TagSet/>"))
                })

            // The object itself must still be intact (not corrupted by the metadata rewrite)
            let objGetSigned = signedHeaders(for: .GET, path: "/\(bucketName)/file.txt")
            try await app.test(
                .GET, "/\(bucketName)/file.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: objGetSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string == "data")
                })
        }
    }

    @Test("GetObjectTagging 200s with an empty TagSet when never tagged, not a 404")
    func testGetObjectTaggingNeverTaggedReturns200() async throws {
        let bucketName = "test-object-tagging-never-tagged"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "file.txt", content: "data")

            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/file.txt", query: "tagging")
            try await app.test(
                .GET, "/\(bucketName)/file.txt?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<Tagging"))
                })
        }
    }

    @Test("PutObjectTagging with more than 10 tags is rejected")
    func testPutObjectTaggingTooManyTagsRejected() async throws {
        let bucketName = "test-object-tagging-too-many"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "file.txt", content: "data")

            var tags: [String: String] = [:]
            for i in 0..<11 {
                tags["key\(i)"] = "value\(i)"
            }
            let xml = taggingXML(tags)
            let bodyData = Data(xml.utf8)
            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/file.txt", query: "tagging", body: bodyData)
            try await app.test(
                .PUT, "/\(bucketName)/file.txt?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .badRequest)
                    #expect(res.body.string.contains("<Code>InvalidTag</Code>"))
                })
        }
    }

    @Test("x-amz-tagging header on PutObject sets tags inline at upload time")
    func testXAmzTaggingHeaderOnPutObject() async throws {
        let bucketName = "test-object-tagging-header"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let data = Data("hello".utf8)
            let signed = signedHeaders(
                for: .PUT, path: "/\(bucketName)/file.txt", body: data,
                additionalHeaders: ["x-amz-tagging": "env=prod&team=storage"])
            try await app.test(
                .PUT, "/\(bucketName)/file.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: data)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/file.txt", query: "tagging")
            try await app.test(
                .GET, "/\(bucketName)/file.txt?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                    #expect(res.body.string.contains("<Key>env</Key><Value>prod</Value>"))
                    #expect(res.body.string.contains("<Key>team</Key><Value>storage</Value>"))
                })
        }
    }

    @Test("x-amz-tagging-count appears on GetObject/HeadObject only when tags exist")
    func testXAmzTaggingCountHeader() async throws {
        let bucketName = "test-object-tagging-count"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)

            let data = Data("hello".utf8)
            let signed = signedHeaders(
                for: .PUT, path: "/\(bucketName)/tagged.txt", body: data,
                additionalHeaders: ["x-amz-tagging": "a=1&b=2"])
            try await app.test(
                .PUT, "/\(bucketName)/tagged.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: signed)
                    req.body = ByteBuffer(data: data)
                })
            try await putObject(
                app, bucketName: bucketName, key: "untagged.txt", content: "hello")

            let taggedGetSigned = signedHeaders(for: .GET, path: "/\(bucketName)/tagged.txt")
            try await app.test(
                .GET, "/\(bucketName)/tagged.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: taggedGetSigned)
                },
                afterResponse: { res in
                    #expect(res.headers.first(name: "x-amz-tagging-count") == "2")
                })

            let untaggedGetSigned = signedHeaders(for: .GET, path: "/\(bucketName)/untagged.txt")
            try await app.test(
                .GET, "/\(bucketName)/untagged.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: untaggedGetSigned)
                },
                afterResponse: { res in
                    #expect(res.headers.first(name: "x-amz-tagging-count") == nil)
                })

            let taggedHeadSigned = signedHeaders(for: .HEAD, path: "/\(bucketName)/tagged.txt")
            try await app.test(
                .HEAD, "/\(bucketName)/tagged.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: taggedHeadSigned)
                },
                afterResponse: { res in
                    #expect(res.headers.first(name: "x-amz-tagging-count") == "2")
                })
        }
    }

    @Test("Object tagging targets a specific version, leaving other versions untouched")
    func testObjectTaggingScopedToVersion() async throws {
        let bucketName = "test-object-tagging-versioned"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)

            var firstVersionId = ""
            let firstData = Data("v1".utf8)
            let firstSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/file.txt", body: firstData)
            try await app.test(
                .PUT, "/\(bucketName)/file.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: firstSigned)
                    req.body = ByteBuffer(data: firstData)
                },
                afterResponse: { res in
                    firstVersionId = res.headers.first(name: "x-amz-version-id") ?? ""
                })

            var secondVersionId = ""
            let secondData = Data("v2".utf8)
            let secondSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/file.txt", body: secondData)
            try await app.test(
                .PUT, "/\(bucketName)/file.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: secondSigned)
                    req.body = ByteBuffer(data: secondData)
                },
                afterResponse: { res in
                    secondVersionId = res.headers.first(name: "x-amz-version-id") ?? ""
                })

            #expect(!firstVersionId.isEmpty)
            #expect(!secondVersionId.isEmpty)
            #expect(firstVersionId != secondVersionId)

            // Tag only the first (older) version
            let xml = taggingXML(["only-on-v1": "true"])
            let bodyData = Data(xml.utf8)
            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/file.txt",
                query: "tagging&versionId=\(firstVersionId)", body: bodyData)
            try await app.test(
                .PUT, "/\(bucketName)/file.txt?tagging&versionId=\(firstVersionId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .ok)
                })

            // First version has the tag
            let getFirstSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/file.txt",
                query: "tagging&versionId=\(firstVersionId)")
            try await app.test(
                .GET, "/\(bucketName)/file.txt?tagging&versionId=\(firstVersionId)",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getFirstSigned)
                },
                afterResponse: { res in
                    #expect(res.body.string.contains("<Key>only-on-v1</Key>"))
                })

            // Second (current) version is untouched
            let getSecondSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/file.txt", query: "tagging")
            try await app.test(
                .GET, "/\(bucketName)/file.txt?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSecondSigned)
                },
                afterResponse: { res in
                    #expect(!res.body.string.contains("<Key>only-on-v1</Key>"))
                })

            // Tagging must not have created a third version
            let listSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)", query: "versions")
            try await app.test(
                .GET, "/\(bucketName)?versions",
                beforeRequest: { req in
                    req.headers.add(contentsOf: listSigned)
                },
                afterResponse: { res in
                    let versionCount = res.body.string.components(separatedBy: "<Version>").count - 1
                    #expect(versionCount == 2)
                })
        }
    }

    @Test("GetObjectTagging/PutObjectTagging 404 on a key whose latest version is a delete marker")
    func testObjectTaggingOnDeleteMarkerReturns404() async throws {
        let bucketName = "test-object-tagging-delete-marker"
        try await withApp { app in
            try await createBucket(app, bucketName: bucketName)
            try await enableVersioning(app, bucketName: bucketName)
            try await putObject(app, bucketName: bucketName, key: "file.txt", content: "data")

            // Deleting in a versioned bucket creates a delete marker as the new latest version
            let deleteSigned = signedHeaders(for: .DELETE, path: "/\(bucketName)/file.txt")
            try await app.test(
                .DELETE, "/\(bucketName)/file.txt",
                beforeRequest: { req in
                    req.headers.add(contentsOf: deleteSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .noContent)
                })

            let getSigned = signedHeaders(
                for: .GET, path: "/\(bucketName)/file.txt", query: "tagging")
            try await app.test(
                .GET, "/\(bucketName)/file.txt?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: getSigned)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })

            let xml = taggingXML(["a": "1"])
            let bodyData = Data(xml.utf8)
            let putSigned = signedHeaders(
                for: .PUT, path: "/\(bucketName)/file.txt", query: "tagging", body: bodyData)
            try await app.test(
                .PUT, "/\(bucketName)/file.txt?tagging",
                beforeRequest: { req in
                    req.headers.add(contentsOf: putSigned)
                    req.body = ByteBuffer(data: bodyData)
                },
                afterResponse: { res in
                    #expect(res.status == .notFound)
                })
        }
    }

    @Test("An object written before the tags field existed (missing key in the JSON) decodes fine")
    func testBackwardCompatibilityWithObjectsMissingTagsKey() throws {
        try StorageHelper.cleanStorage()
        defer { try? StorageHelper.cleanStorage() }

        let bucketName = "test-tags-backward-compat"
        let key = "legacy.txt"
        let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
        let data = Data("legacy content".utf8)

        // Hand-construct the on-disk format with metadata JSON that has no "tags" key at all,
        // simulating a file written before this field was introduced.
        let legacyMetadataJSON = """
            {"bucketName":"\(bucketName)","key":"\(key)","size":\(data.count),"contentType":"text/plain","etag":"abc123","metadata":{},"updatedAt":0,"isLatest":true,"isDeleteMarker":false}
            """
        let jsonData = Data(legacyMetadataJSON.utf8)
        var fileData = Data()
        var length = UInt32(jsonData.count).bigEndian
        withUnsafeBytes(of: &length) { fileData.append(contentsOf: $0) }
        fileData.append(jsonData)
        fileData.append(data)

        let folderURL = URL(fileURLWithPath: path).deletingLastPathComponent()
        try FileManager.default.createDirectory(at: folderURL, withIntermediateDirectories: true)
        try fileData.write(to: URL(fileURLWithPath: path))

        let (meta, readData) = try ObjectFileHandler.read(from: path, loadData: true)
        #expect(meta.tags == nil)
        #expect(readData == data)
    }
}
