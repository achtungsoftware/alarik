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

/// Seeds a default admin `User`. In DEBUG builds this is always username/password "alarik". In
/// release builds, the username defaults to "alarik" but is overridable via `ADMIN_USERNAME`,
/// and the password comes from `ADMIN_PASSWORD` if set, or a freshly generated random one
/// otherwise - never a fixed fallback shared by every unconfigured deployment. A boot-time step,
/// not a `Migration`: idempotency comes from `User.create`'s `putIfAbsent` semantics directly.
enum CreateDefaultUser {
    /// 24 random bytes (192 bits), hex-encoded - far more entropy than needed, but cheap, and
    /// safe to print to a log line without worrying about non-printable characters.
    static func generateRandomPassword() -> String {
        Data((0..<24).map { _ in UInt8.random(in: 0...255) }).hexString()
    }

    static func run(app: Application) async throws {
        #if DEBUG
            let user = User(
                name: "Admin User", username: "alarik",
                passwordHash: try Bcrypt.hash("alarik"), isAdmin: true)
            do {
                try await user.create(app: app)
            } catch is User.UserError {
                return
            }

            let accessKey = AccessKey(
                userId: user.id, accessKey: "AKIAIOSFODNN7EXAMPLE",
                secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
            if try await accessKey.create(app: app) {
                await populateAccessKeyCaches(app: app, accessKey: accessKey)
            }
        #else
            let username = Environment.sanitizedGet("ADMIN_USERNAME") ?? "alarik"
            // A fixed fallback password here would mean every unconfigured production
            // deployment ships with the same guessable admin credentials. Generating and
            // prominently logging a random one instead means an operator who forgets
            // ADMIN_PASSWORD still gets a genuinely unguessable credential, discoverable only
            // from the node's own boot log.
            let configuredPassword = Environment.sanitizedGet("ADMIN_PASSWORD")
            let password = configuredPassword ?? Self.generateRandomPassword()
            let user = User(
                name: "Admin User", username: username,
                passwordHash: try Bcrypt.hash(password), isAdmin: true)

            do {
                try await user.create(app: app)
            } catch is User.UserError {
                // Already seeded on a previous boot.
                return
            }

            if configuredPassword == nil {
                Logger(label: "codes.vapor.application").critical(
                    "No ADMIN_PASSWORD was set - generated a random password for the seeded admin account '\(username)': \(password). Log in and change it, or set ADMIN_PASSWORD and recreate the deployment."
                )
            }

            // The user can provide default keys via .env - this is optional
            if let accessKey = Environment.sanitizedGet("DEFAULT_ACCESS_KEY"),
                let secretKey = Environment.sanitizedGet("DEFAULT_SECRET_KEY")
            {
                let key = AccessKey(userId: user.id, accessKey: accessKey, secretKey: secretKey)
                if try await key.create(app: app) {
                    await populateAccessKeyCaches(app: app, accessKey: key)
                }
            }
        #endif
    }

    /// Mirrors what `InternalUserController.createAccessKey` does after a successful
    /// `AccessKey.create` - populate this node's own in-memory caches immediately, then broadcast
    /// so every other currently-known node does too. Without this, the boot-seeded admin key
    /// stays invisible to SigV4 auth until the next bulk cache reload. Only called when `create`
    /// actually created the record, never on "already existed from a prior boot."
    private static func populateAccessKeyCaches(app: Application, accessKey: AccessKey) async {
        await AccessKeySecretKeyMapCache.shared.add(
            accessKey: accessKey.accessKey, secretKey: accessKey.secretKey)
        CacheInvalidationService.notify(
            app: app, cache: "accessKeySecret", op: .upsert, key: accessKey.accessKey)
        await AccessKeyUserMapCache.shared.add(
            accessKey: accessKey.accessKey, userId: accessKey.userId)
        CacheInvalidationService.notify(
            app: app, cache: "accessKeyUser", op: .upsert, key: accessKey.accessKey)
    }
}
