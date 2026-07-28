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

import Vapor

/// Creates the buckets named in the `DEFAULT_BUCKETS` environment variable (comma-separated) at
/// boot, owned by the seeded admin - handy for development and for reproducible deployments where
/// the same buckets should always exist. Idempotent: a name that already exists is left untouched,
/// so this is safe to run on every boot. Runs only on the designated seeder node in cluster mode
/// (the same gate as `CreateDefaultUser`), and every creation goes through `BucketService.create`,
/// so a seeded bucket is indistinguishable from one made via the S3 API - ownership, access-key
/// mapping, caches, and cluster propagation are all identical.
enum CreateDefaultBuckets {
    /// Splits a `DEFAULT_BUCKETS` value into trimmed, non-empty, de-duplicated bucket names, in
    /// order. Kept separate so the parsing is unit-testable without booting an application.
    static func parseBucketNames(_ raw: String) -> [String] {
        var seen = Set<String>()
        var names: [String] = []
        for name in raw.split(separator: ",").map({ $0.trimmingCharacters(in: .whitespaces) }) {
            guard !name.isEmpty, seen.insert(name).inserted else { continue }
            names.append(name)
        }
        return names
    }

    static func run(app: Application) async throws {
        guard let raw = Environment.sanitizedGet("DEFAULT_BUCKETS") else { return }
        let names = parseBucketNames(raw)
        guard !names.isEmpty else { return }

        // The buckets are owned by the seeded admin - resolve its username exactly as
        // `CreateDefaultUser` does, then look it up to get the owner id.
        #if DEBUG
            let adminUsername = "alarik"
        #else
            let adminUsername = Environment.sanitizedGet("ADMIN_USERNAME") ?? "alarik"
        #endif
        guard let admin = try await User.findByUsername(app: app, username: adminUsername) else {
            app.logger.warning(
                "DEFAULT_BUCKETS is set but the seeded admin '\(adminUsername)' was not found - skipping bucket seeding."
            )
            return
        }

        for name in names {
            if Validator.bucketName.validate(name).isFailure {
                app.logger.warning(
                    "DEFAULT_BUCKETS: '\(name)' is not a valid bucket name - skipping.")
                continue
            }
            // Idempotent: leave an already-existing bucket exactly as it is (created on a prior
            // boot, or owned by another user). Single-seeder gating means there is no concurrent
            // creation to race with here.
            if try await Bucket.find(app: app, name: name) != nil {
                continue
            }
            do {
                try await BucketService.create(
                    app: app, bucketName: name, userId: admin.id)
                app.logger.notice("Seeded default bucket '\(name)' owned by '\(adminUsername)'.")
            } catch {
                app.logger.warning("DEFAULT_BUCKETS: failed to create bucket '\(name)': \(error)")
            }
        }
    }
}
