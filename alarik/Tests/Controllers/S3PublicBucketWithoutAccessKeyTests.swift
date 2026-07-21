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

/// Regression coverage for issue #16: a bucket whose owner has never created an S3 access key -
/// the normal state for one created and managed entirely through the web console - was reported
/// as `NoSuchBucket` to everyone, and anonymous public-read was therefore impossible.
///
/// The cause was that bucket existence was answered from `AccessKeyBucketMapCache`, which only
/// ever contains buckets some access key can reach. These tests deliberately create the bucket
/// through the model/service layer (as the console does, with a JWT) rather than the S3 API,
/// because using the S3 API would require the owner to have exactly the access key whose absence
/// is the bug.
@Suite("Public bucket reachable without an owner access key", .serialized)
struct S3PublicBucketWithoutAccessKeyTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            let loadCacheLifecycle = LoadCacheLifecycle()
            try await loadCacheLifecycle.didBootAsync(app)
            try await test(app)
        } catch {
            try? StorageHelper.cleanStorage()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    /// A user with no access key whatsoever, owning `bucketName` with `policyJSON` applied.
    private func makeKeylessOwnerBucket(
        _ app: Application, bucketName: String, policyJSON: String?
    ) async throws {
        let user = User(
            name: "Console Only User", username: UUID().uuidString,
            passwordHash: try Bcrypt.hash("TestPass123!"), isAdmin: false)
        try await user.create(app: app)

        try await BucketService.create(app: app, bucketName: bucketName, userId: user.id)

        guard let policyJSON else { return }
        guard let bucket = try await Bucket.find(app: app, name: bucketName) else {
            Issue.record("bucket was not created")
            return
        }
        bucket.policy = policyJSON
        try await bucket.save(app: app)

        let parsed = try BucketPolicy.parseAndValidate(
            rawJSON: policyJSON, bucketName: bucketName, requestId: "test")
        await BucketPolicyCache.shared.setPolicy(for: bucketName, policy: parsed)

        // The owner must genuinely have no access key - otherwise this test would pass for the
        // wrong reason, since the old access-key-derived existence check would find the bucket.
        let ownedKeys = try await AccessKey.findAll(app: app, userId: user.id)
        #expect(ownedKeys.isEmpty)
    }

    private func publicReadPolicy(bucketName: String) -> String {
        """
        {
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "PublicRead",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::\(bucketName)/*"
            }]
        }
        """
    }

    private func publicListPolicy(bucketName: String) -> String {
        """
        {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::\(bucketName)"
            }]
        }
        """
    }

    @Test("anonymous GET of a missing key reports NoSuchKey, not NoSuchBucket")
    func anonymousObjectGetSeesTheBucket() async throws {
        let bucketName = "issue16-public-read-bucket"
        try await withApp { app in
            try await makeKeylessOwnerBucket(
                app, bucketName: bucketName, policyJSON: publicReadPolicy(bucketName: bucketName))

            // The object doesn't exist, so a 404 is expected either way - what matters is WHICH
            // 404. `NoSuchBucket` here means the bucket itself was invisible (the bug);
            // `NoSuchKey` means the bucket resolved and only the key was missing (correct).
            try await app.test(
                .GET, "/\(bucketName)/never-uploaded.txt",
                afterResponse: { res in
                    let body = res.body.string
                    #expect(!body.contains("NoSuchBucket"), "bucket was invisible: \(body)")
                    #expect(body.contains("NoSuchKey"), "unexpected response: \(body)")
                })
        }
    }

    @Test("anonymous ListBucket on a public bucket succeeds without any owner access key")
    func anonymousListSucceeds() async throws {
        let bucketName = "issue16-public-list-bucket"
        try await withApp { app in
            try await makeKeylessOwnerBucket(
                app, bucketName: bucketName, policyJSON: publicListPolicy(bucketName: bucketName))

            try await app.test(
                .GET, "/\(bucketName)",
                afterResponse: { res in
                    #expect(res.status == .ok, "body: \(res.body.string)")
                    #expect(!res.body.string.contains("NoSuchBucket"))
                })
        }
    }

    @Test("a bucket with no public policy is still found - it is denied, not reported missing")
    func privateBucketIsDeniedNotMissing() async throws {
        let bucketName = "issue16-private-bucket"
        try await withApp { app in
            try await makeKeylessOwnerBucket(app, bucketName: bucketName, policyJSON: nil)

            // Existence must not leak as authorization: the bucket exists, so anonymous access is
            // refused with AccessDenied rather than pretending the bucket isn't there.
            try await app.test(
                .GET, "/\(bucketName)",
                afterResponse: { res in
                    #expect(res.status == .forbidden, "body: \(res.body.string)")
                })
        }
    }

    @Test("a genuinely nonexistent bucket still reports NoSuchBucket")
    func missingBucketStillReportsNoSuchBucket() async throws {
        try await withApp { app in
            try await app.test(
                .GET, "/issue16-never-created-bucket/some-key.txt",
                afterResponse: { res in
                    #expect(res.status == .notFound)
                    #expect(res.body.string.contains("NoSuchBucket"), "body: \(res.body.string)")
                })
        }
    }
}

/// The console half of the same report (PR #17): the bucket DTOs did not return `policy`, so the
/// policy editor opened blank even when a policy was saved, and the admin list badged every
/// bucket "private" regardless of what its policy actually said. An editor that cannot see the
/// current policy is worse than inconvenient - saving from it silently overwrites the real one.
@Suite("Bucket DTOs expose the saved policy", .serialized)
struct BucketPolicyDTOTests {
    private func bucket(policy: String?) -> Bucket {
        let bucket = Bucket(name: "dto-policy-test", userId: UUID())
        bucket.policy = policy
        return bucket
    }

    @Test("toResponseDTO carries the saved policy through verbatim")
    func responseDTOCarriesPolicy() {
        let raw = """
            {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",            "Action":"s3:GetObject","Resource":"arn:aws:s3:::dto-policy-test/*"}]}
            """
        #expect(bucket(policy: raw).toResponseDTO().policy == raw)
    }

    @Test("a bucket with no policy reports nil, not an empty string")
    func responseDTOReportsNilWhenUnset() {
        // The console distinguishes "no policy" from "a policy I wasn't told about"; collapsing
        // nil to "" would make an unset bucket look like it has an unparseable one.
        #expect(bucket(policy: nil).toResponseDTO().policy == nil)
    }

    @Test("the DTO survives a JSON encode/decode round trip with the policy intact")
    func responseDTORoundTrips() throws {
        let raw = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
        let encoded = try JSONEncoder().encode(bucket(policy: raw).toResponseDTO())
        let decoded = try JSONDecoder().decode(Bucket.ResponseDTO.self, from: encoded)
        #expect(decoded.policy == raw)
    }
}
