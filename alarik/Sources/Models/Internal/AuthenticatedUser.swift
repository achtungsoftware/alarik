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

/// Represents a user authenticated via any supported method (JWT or Access Key)
struct AuthenticatedUser: Authenticatable {

    enum AuthMethod {
        case jwt
        case accessKey
    }

    let user: User
    let authMethod: AuthMethod

    var userId: UUID {
        user.id
    }

    var isAdmin: Bool {
        user.isAdmin
    }

    /// Require that the authenticated user is an admin, otherwise throw unauthorized
    func requireAdmin() throws {
        guard isAdmin else {
            throw Abort(.unauthorized, reason: "User not admin")
        }
    }
}
