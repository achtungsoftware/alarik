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
import FluentSQLiteDriver
import JWTKit
import NIOSSL
import Vapor

public func configure(_ app: Application) async throws {

    // TODO: make this configurable ?
    app.routes.defaultMaxBodySize = "5tb"
    app.http.server.configuration.supportPipelining = true

    #if DEBUG
        let consoleBaseUrl = "http://localhost:3000"

        // In debug, test & profiling - store the db relative to the work dir
        app.databases.use(.sqlite(.file("db.sqlite")), as: .sqlite)
    #else
        let consoleBaseUrl = Environment.get("CONSOLE_BASE_URL") ?? "http://localhost:3000"

        try FileManager.default.createDirectory(
            atPath: "Storage/buckets",
            withIntermediateDirectories: true
        )
        app.databases.use(
            DatabaseConfigurationFactory.sqlite(.file("Storage/db.sqlite")), as: .sqlite)
    #endif

    app.migrations.add(CreateUser())
    app.migrations.add(CreateAccessKey())
    app.migrations.add(CreateBucket())

    app.migrations.add(CreateDefaultUser())

    let cors: CORSMiddleware = CORSMiddleware(
        configuration: .init(
            allowedOrigin: .any([consoleBaseUrl]),
            allowedMethods: [.GET, .POST, .PUT, .OPTIONS, .DELETE, .PATCH],
            allowedHeaders: [
                .accept,
                .authorization,
                .contentType,
                .origin,
                .xRequestedWith,
                .init(stringLiteral: "amz-sdk-invocation-id"),
            ]
        )
    )

    app.middleware.use(cors)
    app.middleware.use(S3ErrorMiddleware())

    if let jwt = Environment.get("JWT") {
        await app.jwt.keys.add(hmac: HMACKey(from: jwt), digestAlgorithm: .sha256)
    } else {
        app.logger.error(
            "No JWT key provided in environment variable 'JWT'. Falling back to an insecure default key. Please set a secure JWT key before deploying to production."
        )
        await app.jwt.keys.add(hmac: "super-secret-key", digestAlgorithm: .sha256)
    }

    app.lifecycle.use(LoadCacheLifecycle())

    try await app.autoMigrate()

    try routes(app)

    if app.environment != .testing {
        app.eventLoopGroup.next().scheduleRepeatedTask(
            initialDelay: .zero,
            delay: .minutes(1)
        ) { task in
            Task {
                do {
                    let expiredAccessKeys = try await AccessKey.query(on: app.db)
                        .filter(\.$expirationDate <= Date.now)
                        .all()

                    for accessKey in expiredAccessKeys {
                        try await AccessKeyService.delete(
                            on: app.db, accessKey: accessKey.accessKey)
                    }
                } catch {
                    app.logger.error("Failed to invalidate expired access keys: \(error)")
                }
            }
        }
    }
}
