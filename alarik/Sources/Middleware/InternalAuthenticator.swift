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

import JWT
import Vapor

/// Authenticator that supports both JWT (SessionToken) and Access Key authentication.
///
/// Tries JWT Bearer token (`Authorization: Bearer <token>`) first, then falls back to Access Key
/// headers (`X-Access-Key`/`X-Secret-Key`). On success, an `AuthenticatedUser` is stored in
/// `req.auth`.
struct InternalAuthenticator: AsyncRequestAuthenticator {

    func authenticate(request: Request) async throws {
        // Try JWT first
        if let authenticatedUser = try await authenticateJWT(request: request) {
            request.auth.login(authenticatedUser)
            return
        }

        // Try Access Key headers
        if let authenticatedUser = try await authenticateAccessKey(request: request) {
            request.auth.login(authenticatedUser)
            return
        }
    }

    /// Attempts JWT authentication via Bearer token
    private func authenticateJWT(request: Request) async throws -> AuthenticatedUser? {
        // Check for Bearer token
        guard request.headers.bearerAuthorization != nil else {
            return nil
        }

        do {
            let sessionToken = try await request.jwt.verify(as: SessionToken.self)

            guard let user = try await User.find(app: request.application, id: sessionToken.userId)
            else {
                return nil
            }

            return AuthenticatedUser(user: user, authMethod: .jwt)
        } catch {
            // JWT verification failed, return nil to try next method
            return nil
        }
    }

    /// Attempts Access Key authentication via custom headers
    /// Headers: `X-Access-Key` and `X-Secret-Key`
    private func authenticateAccessKey(request: Request) async throws -> AuthenticatedUser? {
        guard
            let accessKeyId = request.headers.first(name: "X-Access-Key"),
            let secretKey = request.headers.first(name: "X-Secret-Key")
        else {
            return nil
        }

        guard let accessKey = try await AccessKey.find(app: request.application, accessKey: accessKeyId)
        else {
            return nil
        }

        // Check expiration
        if let expirationDate = accessKey.expirationDate, expirationDate < Date() {
            return nil
        }

        // Verify secret key using constant-time comparison
        guard secretKey.constantTimeCompare(to: accessKey.secretKey) else {
            return nil
        }

        guard let user = try await User.find(app: request.application, id: accessKey.userId) else {
            return nil
        }

        return AuthenticatedUser(user: user, authMethod: .accessKey)
    }
}
