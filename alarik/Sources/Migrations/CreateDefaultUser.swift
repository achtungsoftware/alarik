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
import Vapor

/// Creates a default admin User. In DEBUG builds this is always username/password "alarik" (relied
/// on throughout the test suite and `cluster_tests.sh`). In release builds, the username defaults
/// to "alarik" but is overridable via `ADMIN_USERNAME`, and the password comes from `ADMIN_PASSWORD`
/// if set, or a freshly generated random one (logged once at startup) otherwise - never a fixed
/// fallback password, which would make every unconfigured deployment share the same credential.
struct CreateDefaultUser: AsyncMigration {
    /// 24 random bytes (192 bits), hex-encoded - far more entropy than needed, but cheap, and
    /// safe to print to a log line without worrying about non-printable characters. Internal
    /// (not private) so it's directly unit-testable regardless of build configuration - the
    /// `#if DEBUG`/`#else` split below means only one of the two branches actually compiles into
    /// any given test run, but this generator is shared code, outside that split.
    static func generateRandomPassword() -> String {
        Data((0..<24).map { _ in UInt8.random(in: 0...255) }).hexString()
    }

    func prepare(on database: any Database) async throws {
        try await database.transaction { db in

            #if DEBUG
                let user = User(
                    id: UUID(),
                    name: "Admin User",
                    username: "alarik",
                    passwordHash: try Bcrypt.hash("alarik"),
                    isAdmin: true
                )

                try await user.save(on: db)

                let accessKey = AccessKey(
                    id: UUID(),
                    userId: user.id ?? UUID(),
                    accessKey: "AKIAIOSFODNN7EXAMPLE",
                    secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
                )

                try await accessKey.save(on: db)
            #else
                // A fixed fallback password here would mean every unconfigured production
                // deployment ships with the same guessable admin/admin credentials - the
                // "alarik"/"alarik" pair used to be exactly that. Generating and prominently
                // logging a random one instead means an operator who forgets ADMIN_PASSWORD
                // still gets a genuinely unguessable credential, discoverable only from the
                // node's own boot log.
                let configuredPassword = Environment.sanitizedGet("ADMIN_PASSWORD")
                let password = configuredPassword ?? Self.generateRandomPassword()
                let user = User(
                    id: UUID(),
                    name: "Admin User",
                    username: Environment.sanitizedGet("ADMIN_USERNAME") ?? "alarik",
                    passwordHash: try Bcrypt.hash(password),
                    isAdmin: true
                )

                try await user.save(on: db)

                if configuredPassword == nil {
                    Logger(label: "codes.vapor.application").critical(
                        "No ADMIN_PASSWORD was set - generated a random password for the seeded admin account '\(user.username)': \(password). Log in and change it, or set ADMIN_PASSWORD and recreate the deployment."
                    )
                }

                // The user can provide default keys via .env
                // this is optional
                if let accessKey = Environment.sanitizedGet("DEFAULT_ACCESS_KEY"),
                    let secretKey = Environment.sanitizedGet("DEFAULT_SECRET_KEY")
                {
                    let accessKey = AccessKey(
                        id: UUID(),
                        userId: user.id ?? UUID(),
                        accessKey: accessKey,
                        secretKey: secretKey
                    )

                    try await accessKey.save(on: db)
                }
            #endif

        }
    }

    func revert(on database: any Database) async throws {
        #if DEBUG
            let username = "alarik"
        #else
            let username = Environment.sanitizedGet("ADMIN_USERNAME") ?? "alarik"
        #endif
        try await User.query(on: database)
            .filter(\.$username == username)
            .delete()
    }
}
