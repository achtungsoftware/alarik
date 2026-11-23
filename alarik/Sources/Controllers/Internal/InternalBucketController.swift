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
import Vapor

/// The console's bucket endpoints: the caller's own buckets and their versioning, policy and
/// tags. Objects, shared links, notifications and replication each have their own controller.
struct InternalBucketController: RouteCollection, ConsoleBucketRoutes {
    struct VersioningStatusDTO: Content {
        let status: String
    }

    struct PolicyDTO: Content {
        let policy: String?
    }

    struct TagsDTO: Content {
        let tags: [String: String]
    }

    struct LifecycleDTO: Content {
        let rules: [LifecycleRule]
    }

    struct CORSDTO: Content {
        let rules: [CORSRule]
    }

    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("buckets").get(use: self.listBuckets)
        routes.grouped("buckets").post(use: self.createBucket)
        routes.grouped("buckets").grouped(":bucketName").delete(use: self.deleteBucket)
        routes.grouped("buckets").grouped(":bucketName").grouped("versioning").get(
            use: self.getVersioning)
        routes.grouped("buckets").grouped(":bucketName").grouped("versioning")
            .on(.PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit), use: self.setVersioning)
        routes.grouped("buckets").grouped(":bucketName").grouped("policy").get(
            use: self.getPolicy)
        routes.grouped("buckets").grouped(":bucketName").grouped("policy")
            .on(.PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit), use: self.setPolicy)
        routes.grouped("buckets").grouped(":bucketName").grouped("policy").delete(
            use: self.deletePolicy)
        routes.grouped("buckets").grouped(":bucketName").grouped("tags").get(
            use: self.getBucketTags)
        routes.grouped("buckets").grouped(":bucketName").grouped("tags")
            .on(.PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit), use: self.setBucketTags)
        routes.grouped("buckets").grouped(":bucketName").grouped("tags").delete(
            use: self.deleteBucketTags)
        routes.grouped("buckets").grouped(":bucketName").grouped("lifecycle").get(
            use: self.getLifecycle)
        routes.grouped("buckets").grouped(":bucketName").grouped("lifecycle")
            .on(.PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit), use: self.setLifecycle)
        routes.grouped("buckets").grouped(":bucketName").grouped("lifecycle").delete(
            use: self.deleteLifecycle)
        routes.grouped("buckets").grouped(":bucketName").grouped("cors").get(use: self.getCORS)
        routes.grouped("buckets").grouped(":bucketName").grouped("cors")
            .on(.PUT, body: .collect(maxSize: Self.subresourceBodySizeLimit), use: self.setCORS)
        routes.grouped("buckets").grouped(":bucketName").grouped("cors").delete(
            use: self.deleteCORS)
    }

    /// Lists the caller's own buckets. An optional `?search=` narrows to names containing that
    /// substring (case-insensitive, server-side) - needed so UI like a bucket picker can search
    /// as the user types instead of ever having to load every bucket up front.
    @Sendable
    func listBuckets(req: Request) async throws -> Page<Bucket.ResponseDTO> {
        let auth = try req.auth.require(AuthenticatedUser.self)
        let search = req.query[String.self, at: "search"]?.trimmingCharacters(in: .whitespaces)

        var buckets = await Bucket.all(app: req.application).filter { $0.userId == auth.userId }
        buckets.sort { ($0.creationDate ?? .distantPast) > ($1.creationDate ?? .distantPast) }

        if let search, !search.isEmpty {
            buckets = buckets.filter { $0.name.localizedCaseInsensitiveContains(search) }
        }

        // Never the raw model - a bucket the caller doesn't own but that happens to match
        // `search` would never reach here (already scoped to `auth.userId` above), but the raw
        // `Bucket` model doesn't carry anything sensitive by itself either way; this is purely
        // about not silently starting to leak the moment someone adds an eager-loaded relation
        // (like `.with(\.$user)`) to this query later, the same mistake the admin bucket list
        // endpoint made.
        let page = try buckets.paginated(for: req)
        return page.map { $0.toResponseDTO() }
    }

    @Sendable
    func createBucket(req: Request) async throws -> Bucket.ResponseDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        try Bucket.Create.validate(content: req)

        let create: Bucket.Create = try req.content.decode(Bucket.Create.self)

        if try await Bucket.find(app: req.application, name: create.name) != nil {
            throw Abort(.conflict, reason: "The requested bucket name is not available.")
        }

        try await BucketService.create(
            app: req.application, bucketName: create.name, userId: auth.userId,
            versioningEnabled: create.versioningEnabled)

        // Read the created bucket back from the store to get the ID
        guard let bucket = try await Bucket.find(app: req.application, name: create.name),
            bucket.userId == auth.userId
        else {
            throw Abort(.internalServerError, reason: "Failed to retrieve created bucket")
        }

        return bucket.toResponseDTO()
    }

    @Sendable
    func deleteBucket(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        // Verify bucket exists and belongs to user
        try await requireOwnedBucketExists(req: req, bucketName: bucketName, userId: auth.userId)

        try await BucketService.delete(
            req: req, bucketName: bucketName, userId: auth.userId, force: true)

        return .noContent
    }

    @Sendable
    func getVersioning(req: Request) async throws -> VersioningStatusDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        // Verify bucket exists and belongs to user
        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return VersioningStatusDTO(status: bucket.versioningStatus)
    }

    /// S3 never lets a client set `Disabled` via PutBucketVersioning - only `Enabled` or
    /// `Suspended` are valid request values (verified against the API reference); `Disabled`
    /// only ever describes a bucket that has never had versioning touched at all. Once
    /// versioning has been enabled, the only way "off" is `Suspended`.
    @Sendable
    func setVersioning(req: Request) async throws -> VersioningStatusDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(VersioningStatusDTO.self)

        guard let newStatus = VersioningStatus(rawValue: input.status),
            newStatus != .disabled
        else {
            throw Abort(
                .badRequest,
                reason: "Invalid versioning status. Use 'Enabled' or 'Suspended'.")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        bucket.versioningStatus = newStatus.rawValue
        try await bucket.save(app: req.application)

        await BucketVersioningCache.shared.setStatus(for: bucketName, status: newStatus)
        CacheInvalidationService.notify(app: req.application, cache: "bucketVersioning", op: .upsert, key: bucketName)

        return VersioningStatusDTO(status: newStatus.rawValue)
    }

    @Sendable
    func getPolicy(req: Request) async throws -> PolicyDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return Self.policyResponse(for: bucket)
    }

    @Sendable
    func setPolicy(req: Request) async throws -> PolicyDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let rawJSON = try Self.requirePolicyBody(req: req)

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return try await Self.setPolicy(
            req: req, bucket: bucket, bucketName: bucketName, rawJSON: rawJSON)
    }

    @Sendable
    func deletePolicy(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        try await Self.deletePolicy(req: req, bucket: bucket, bucketName: bucketName)

        return .noContent
    }

    // MARK: - Shared policy logic (also used by InternalAdminController, which resolves the
    // bucket differently - any bucket, not just owned ones - but shares everything else)

    static func policyResponse(for bucket: Bucket) -> PolicyDTO {
        PolicyDTO(policy: bucket.policy)
    }

    static func requirePolicyBody(req: Request) throws -> String {
        let input = try req.content.decode(PolicyDTO.self)
        guard let rawJSON = input.policy else {
            throw Abort(.badRequest, reason: "Missing policy")
        }
        return rawJSON
    }

    static func setPolicy(req: Request, bucket: Bucket, bucketName: String, rawJSON: String)
        async throws -> PolicyDTO
    {
        if await BucketPolicyCache.shared.publicAccessBlock(for: bucketName)?.blockPublicPolicy
            == true
        {
            throw Abort(
                .forbidden,
                reason:
                    "Bucket policies cannot be set while BlockPublicPolicy is enabled in this bucket's Public Access Block configuration."
            )
        }

        let policy: BucketPolicy
        do {
            policy = try BucketPolicy.parseAndValidate(
                rawJSON: rawJSON, bucketName: bucketName, requestId: req.id)
        } catch let error as S3Error {
            throw Abort(.badRequest, reason: error.message)
        }

        bucket.policy = rawJSON
        try await bucket.save(app: req.application)

        await BucketPolicyCache.shared.setPolicy(for: bucketName, policy: policy)
        CacheInvalidationService.notify(app: req.application, cache: "bucketPolicy", op: .upsert, key: bucketName)

        return PolicyDTO(policy: rawJSON)
    }

    static func deletePolicy(req: Request, bucket: Bucket, bucketName: String) async throws {
        bucket.policy = nil
        try await bucket.save(app: req.application)

        await BucketPolicyCache.shared.removePolicy(for: bucketName)
        CacheInvalidationService.notify(app: req.application, cache: "bucketPolicy", op: .remove, key: bucketName)
    }

    @Sendable
    func getBucketTags(req: Request) async throws -> TagsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return TagsDTO(tags: bucket.tags ?? [:])
    }

    /// Sets the bucket's tags, overwriting any existing tags entirely - matches the
    /// S3-protocol PutBucketTagging semantics (no merge).
    @Sendable
    func setBucketTags(req: Request) async throws -> TagsDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(TagsDTO.self)

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        bucket.tags = input.tags
        try await bucket.save(app: req.application)

        return TagsDTO(tags: input.tags)
    }

    @Sendable
    func deleteBucketTags(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        bucket.tags = nil
        try await bucket.save(app: req.application)

        return .noContent
    }

    // MARK: - Lifecycle

    /// No configuration reads as an empty rule set: the console shows an editor either way.
    @Sendable
    func getLifecycle(req: Request) async throws -> LifecycleDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return LifecycleDTO(rules: bucket.lifecycleRules ?? [])
    }

    /// Replaces the whole configuration, like PutBucketLifecycleConfiguration. Rules sent
    /// without an id get one, as the XML path does.
    @Sendable
    func setLifecycle(req: Request) async throws -> LifecycleDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(LifecycleDTO.self)
        let rules = input.rules.map { rule -> LifecycleRule in
            var rule = rule
            rule.id = rule.id.trimmingCharacters(in: .whitespaces)
            if rule.id.isEmpty { rule.id = UUID().uuidString }
            return rule
        }
        do {
            try LifecycleConfiguration.validate(rules)
        } catch let error as LifecycleConfiguration.ValidationError {
            throw Abort(.badRequest, reason: error.message)
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        bucket.lifecycleRules = rules
        try await bucket.save(app: req.application)

        return LifecycleDTO(rules: rules)
    }

    @Sendable
    func deleteLifecycle(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        bucket.lifecycleRules = nil
        try await bucket.save(app: req.application)

        return .noContent
    }

    // MARK: - CORS

    @Sendable
    func getCORS(req: Request) async throws -> CORSDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        return CORSDTO(rules: bucket.corsRules ?? [])
    }

    /// Replaces the whole configuration, like PutBucketCors. Methods are normalised to upper
    /// case and blank entries dropped before the shared validation runs.
    @Sendable
    func setCORS(req: Request) async throws -> CORSDTO {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(CORSDTO.self)
        let rules = input.rules.map { rule -> CORSRule in
            var rule = rule
            let trimmed = { (values: [String]) in
                values.map { $0.trimmingCharacters(in: .whitespaces) }.filter { !$0.isEmpty }
            }
            rule.id = rule.id.flatMap { id -> String? in
                let trimmed = id.trimmingCharacters(in: .whitespaces)
                return trimmed.isEmpty ? nil : trimmed
            }
            rule.allowedOrigins = trimmed(rule.allowedOrigins)
            rule.allowedMethods = trimmed(rule.allowedMethods).map { $0.uppercased() }
            rule.allowedHeaders = trimmed(rule.allowedHeaders)
            rule.exposeHeaders = trimmed(rule.exposeHeaders)
            return rule
        }
        do {
            try CORSConfiguration.validate(rules)
        } catch let error as CORSConfiguration.ValidationError {
            throw Abort(.badRequest, reason: error.message)
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        try await S3Controller.storeCORS(rules, on: bucket, req: req)

        return CORSDTO(rules: rules)
    }

    @Sendable
    func deleteCORS(req: Request) async throws -> HTTPStatus {
        let auth = try req.auth.require(AuthenticatedUser.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let bucket = try await requireOwnedBucket(
            req: req, bucketName: bucketName, userId: auth.userId)

        try await S3Controller.storeCORS(nil, on: bucket, req: req)

        return .noContent
    }
}
