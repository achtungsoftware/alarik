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

/// Creates a default User (username: alarik, password: alarik)
struct CreateDefaultUser: AsyncMigration {
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
                let user = User(
                    id: UUID(),
                    name: "Admin User",
                    username: Environment.get("ADMIN_USERNAME") ?? "alarik",
                    passwordHash: try Bcrypt.hash(Environment.get("ADMIN_PASSWORD") ?? "alarik"),
                    isAdmin: true
                )

                try await user.save(on: db)

                // The user can provide default keys via .env
                // this is optional
                if let accessKey = Environment.get("DEFAULT_ACCESS_KEY"),
                    let secretKey = Environment.get("DEFAULT_SECRET_KEY")
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
        try await User.query(on: database)
            .filter(\.$username == "alarik")
            .delete()
    }
}
