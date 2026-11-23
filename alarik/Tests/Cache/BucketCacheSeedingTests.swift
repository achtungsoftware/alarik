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

/// One bucket cache miss warms every bucket cache from the record it just read.
@Suite("Bucket cache seeding", .serialized)
struct BucketCacheSeedingTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await test(app)
        } catch {
            try? StorageHelper.cleanStorage()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    @Test("a versioning miss fills the policy and notification caches from the same record")
    func missSeedsSiblings() async throws {
        try await withApp { app in
            let admin = try await User.findByUsername(app: app, username: "alarik")!
            let bucket = Bucket(name: "seed-bucket", userId: admin.id)
            bucket.versioningStatus = VersioningStatus.enabled.rawValue
            bucket.policy = """
                {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::seed-bucket/*"}]}
                """
            bucket.notificationConfig = NotificationConfiguration(rules: [
                NotificationRule(id: UUID(), url: "https://example.com/hook", secret: nil, events: ["s3:ObjectCreated:*"], prefix: nil, suffix: nil, enabled: true)
            ])
            try await bucket.save(app: app)

            // Cold: nothing cached for the bucket.
            await BucketVersioningCache.shared.removeBucket("seed-bucket")
            await BucketPolicyCache.shared.removePolicy(for: "seed-bucket")
            await NotificationConfigCache.shared.removeBucket("seed-bucket")
            #expect(await BucketPolicyCache.shared.cachedValue(for: "seed-bucket") == nil)
            #expect(await NotificationConfigCache.shared.cachedValue(for: "seed-bucket") == nil)

            // One miss on the versioning cache...
            #expect(await BucketVersioningCache.shared.resolve(app: app, key: "seed-bucket") == .enabled)
            // ...and the siblings know the bucket without a read of their own.
            #expect(await BucketPolicyCache.shared.cachedValue(for: "seed-bucket")?.policy != nil)
            #expect(await NotificationConfigCache.shared.cachedValue(for: "seed-bucket")?.rules.count == 1)
        }
    }
}
