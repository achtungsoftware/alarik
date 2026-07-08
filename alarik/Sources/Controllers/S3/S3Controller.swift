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
import XMLCoder

struct S3Controller: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        routes.get(use: self.listBuckets)

        // S3 "Path Style" routes: /:bucketName
        let bucketRoute = routes.grouped(":bucketName")

        // Bucket Operations
        bucketRoute.put(use: self.handleBucketPut)
        bucketRoute.delete(use: self.handleBucketDelete)
        bucketRoute.on(.HEAD, use: self.handleBucketHead)
        bucketRoute.get(use: self.handleBucketGet)
        bucketRoute.post(use: self.handleBucketPost)

        bucketRoute.on(.HEAD, "**", use: self.handleObjectHead)
        bucketRoute.get("**", use: self.handleObjectGet)
        // `body: .stream`: object payloads are spooled to disk by StreamingBodySpooler with
        // bounded memory instead of being buffered whole by Vapor. Subresource PUTs routed
        // through the same handler (tagging etc.) still buffer via collectBody*, which also
        // performs the payload-hash check the SigV4 validator defers for unbuffered bodies.
        bucketRoute.on(.PUT, "**", body: .stream, use: self.handleObjectPut)
        bucketRoute.post("**", use: self.handleObjectPost)
        bucketRoute.delete("**", use: self.handleObjectDelete)
    }

    /// Fetches a bucket by name or throws the standard `NoSuchBucket` error - the same
    /// 404 shape needed by every bucket subresource handler (policy/tagging/lifecycle/public
    /// access block) once authentication has already established the caller may act on it.
    private func fetchBucket(req: Request, bucketName: String) async throws -> Bucket {
        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .first()
        else {
            throw S3Error(
                status: .notFound, code: "NoSuchBucket",
                message: "The specified bucket does not exist.", requestId: req.id)
        }
        return bucket
    }

    @Sendable
    func handleBucketGet(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)

        try await S3Service.verifyBucketExists(bucketName, requestId: req.id)

        let query = req.url.query ?? ""
        let lowerQuery = query.lowercased()

        // Handle ?uploads - list multipart uploads (always requires strict auth - not in the
        // public-access whitelist)
        if lowerQuery.contains("uploads") && !lowerQuery.contains("uploadid") {
            _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)
            return try handleListMultipartUploads(req: req, bucketName: bucketName)
        }

        // Handle ?versions - list object versions (always requires strict auth)
        // Use word boundary check: "versions" but not "versioning"
        let isListVersions = lowerQuery.contains("versions") && !lowerQuery.contains("versioning")
        if isListVersions {
            _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)
            return try handleListVersions(req: req, bucketName: bucketName)
        }

        // Handle subresource queries (location, policy, versioning config) - always requires
        // strict auth. The bucket is only fetched here (not unconditionally up front) since
        // it's only needed by the ?versioning/?policy responses - no need to hit the DB for
        // every plain ListObjects call, anonymous or not.
        let shouldHandle = S3Service.shouldHandleSubresource(query: query)
        if shouldHandle {
            _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .first()
            if let response = try await S3Service.handleSubresourceQuery(
                query: query, req: req, bucket: bucket)
            {
                return response
            }
        }

        // Plain ListObjects/ListObjectsV2 - the only bucket-level action eligible for anonymous
        // access, via a bucket policy granting s3:ListBucket
        _ = try await S3Service.authenticateOrAuthorizePublic(
            req: req, bucketName: bucketName, action: .listBucket, key: nil)

        let params = S3Service.parseListObjectsParams(from: req, bucketName: bucketName)

        let (objects, commonPrefixes, isTruncated, nextMarker) = try ObjectFileHandler.listObjects(
            bucketName: params.bucketName,
            prefix: params.prefix,
            delimiter: params.delimiter,
            maxKeys: params.maxKeys,
            marker: params.marker
        )

        // listObjects already returns the latest version of each object and filters delete markers
        let xmlData = try S3Service.buildListObjectsResponse(
            params: params,
            objects: objects,
            commonPrefixes: commonPrefixes,
            isTruncated: isTruncated,
            nextMarker: nextMarker
        )

        return S3Service.buildXMLResponse(data: xmlData)
    }

    /// Handles GET /:bucketName?versions - list all object versions
    @Sendable
    private func handleListVersions(req: Request, bucketName: String) throws -> Response {
        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let delimiter = req.query[String.self, at: "delimiter"]
        let keyMarker = req.query[String.self, at: "key-marker"]
        let versionIdMarker = req.query[String.self, at: "version-id-marker"]
        let maxKeys = req.query[Int.self, at: "max-keys"] ?? 1000

        let (
            versions, deleteMarkers, commonPrefixes, isTruncated, nextKeyMarker, nextVersionIdMarker
        ) =
            try ObjectFileHandler.listAllVersions(
                bucketName: bucketName,
                prefix: prefix,
                delimiter: delimiter,
                keyMarker: keyMarker,
                versionIdMarker: versionIdMarker,
                maxKeys: maxKeys
            )

        let xmlData = try S3Service.buildListVersionsResponse(
            bucketName: bucketName,
            prefix: prefix,
            delimiter: delimiter,
            keyMarker: keyMarker,
            versionIdMarker: versionIdMarker,
            maxKeys: maxKeys,
            versions: versions,
            deleteMarkers: deleteMarkers,
            commonPrefixes: commonPrefixes,
            isTruncated: isTruncated,
            nextKeyMarker: nextKeyMarker,
            nextVersionIdMarker: nextVersionIdMarker
        )

        return S3Service.buildXMLResponse(data: xmlData)
    }

    @Sendable
    func handleBucketHead(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        try await S3Service.verifyBucketExists(bucketName, requestId: req.id)
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)
        let response = S3Service.buildStandardResponse(status: .ok, requestId: req.id)
        // S3 always reports the bucket's region via this header on HeadBucket, regardless
        // of the LocationConstraint XML quirk for us-east-1 (verified against the
        // HeadBucket/GetBucketLocation API references).
        response.headers.replaceOrAdd(name: "x-amz-bucket-region", value: AlarikRegion.resolve())
        return response
    }

    @Sendable
    func handleObjectHead(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let keyPath = S3Service.extractObjectKey(from: req)

        try await S3Service.verifyBucketExists(bucketName, requestId: req.id)

        // Check for versionId query parameter
        let versionId = req.query[String.self, at: "versionId"]

        _ = try await S3Service.authenticateOrAuthorizePublic(
            req: req, bucketName: bucketName,
            action: versionId != nil ? .getObjectVersion : .getObject, key: keyPath)

        // Read object (versioned or non-versioned)
        let (meta, _): (ObjectMeta, Data?)
        do {
            (meta, _) = try ObjectFileHandler.readVersion(
                bucketName: bucketName,
                key: keyPath,
                versionId: versionId,
                loadData: false
            )
        } catch {
            // Fallback to non-versioned path for backwards compatibility
            let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
            guard ObjectFileHandler.keyExists(for: bucketName, key: keyPath, path: path) else {
                throw S3Error(
                    status: .notFound, code: "NoSuchKey",
                    message: "The specified key does not exist.", requestId: req.id)
            }
            (meta, _) = try ObjectFileHandler.read(from: path, loadData: false)
        }

        // Check if latest version is a delete marker (object is "deleted")
        if meta.isDeleteMarker && versionId == nil {
            throw S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: req.id)
        }

        // Validate conditional request headers
        try S3Service.validateConditionalHeaders(req: req, meta: meta)

        return S3Service.buildVersionedObjectMetadataResponse(meta: meta)
    }

    // GET / (List all buckets)
    @Sendable
    func listBuckets(req: Request) async throws -> Response {
        let key = try await S3Service.parseAndAuthenticateWithDB(req: req)

        guard let userId = key.user.id else {
            throw S3Error(status: .forbidden, code: "AccessDenied", message: "Access Denied")
        }

        let buckets = try await Bucket.query(on: req.db)
            .filter(\.$user.$id == userId)
            .all()

        let xmlData: Data = try ListAllMyBucketsResultDTO.s3XMLContainer(buckets)
        return S3Service.buildXMLResponse(data: xmlData)
    }

    // PUT /:bucketName
    @Sendable
    func handleBucketPut(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let query = req.url.query ?? ""

        // Handle PUT ?versioning
        if query.lowercased().contains("versioning") {
            return try await handleVersioningPut(req: req, bucketName: bucketName)
        }

        // Handle PUT ?policy
        if query.lowercased().contains("policy") {
            return try await handleBucketPolicyPut(req: req, bucketName: bucketName)
        }

        // Handle PUT ?publicAccessBlock
        if query.lowercased().contains("publicaccessblock") {
            return try await handlePublicAccessBlockPut(req: req, bucketName: bucketName)
        }

        // Handle PUT ?tagging
        if query.lowercased().contains("tagging") {
            return try await handleBucketTaggingPut(req: req, bucketName: bucketName)
        }

        // Handle PUT ?lifecycle
        if query.lowercased().contains("lifecycle") {
            return try await handleLifecyclePut(req: req, bucketName: bucketName)
        }

        // PUT ?notification - Alarik's webhook rules target http(s) URLs, not the SNS/SQS/Lambda
        // ARNs the S3 XML format carries, so there's nothing meaningful an S3 client could PUT
        // here. Managed via the console / internal API instead (GET ?notification still works).
        if query.lowercased().contains("notification") {
            _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)
            throw S3Error(
                status: .notImplemented, code: "NotImplemented",
                message:
                    "Configuring bucket notifications via the S3 API is not supported. Manage webhooks through the Alarik console or internal API.",
                requestId: req.id)
        }

        // PUT ?replication - AWS's ReplicationConfiguration XML has no field for target
        // credentials, which Alarik's model requires (see ReplicationTarget) - so there's
        // nothing meaningful an S3 client could PUT here. Managed via the console / internal
        // API instead (GET ?replication still works).
        if query.lowercased().contains("replication") {
            _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)
            throw S3Error(
                status: .notImplemented, code: "NotImplemented",
                message:
                    "Configuring bucket replication via the S3 API is not supported. Manage replication through the Alarik console or internal API.",
                requestId: req.id)
        }

        if Validator.bucketName.validate(bucketName).isFailure {
            throw S3Error(
                status: .badRequest,
                code: "InvalidBucketName",
                message: "The specified bucket is not valid.", requestId: req.id
            )
        }

        // Authenticate BEFORE the existence check - answering 409 to unsigned requests
        // would let anyone enumerate bucket names without credentials
        let key = try await S3Service.parseAndAuthenticateWithDB(req: req)

        if let existing = try await Bucket.query(on: req.db).filter(\.$name == bucketName).first()
        {
            if existing.$user.id == key.user.id {
                // S3 only no-ops re-creating your own bucket as a 200 OK in us-east-1,
                // for legacy compatibility - every other region returns 409
                // BucketAlreadyOwnedByYou instead (verified against the CreateBucket API
                // reference).
                guard AlarikRegion.resolve() == AlarikRegion.default else {
                    throw S3Error(
                        status: .conflict,
                        code: "BucketAlreadyOwnedByYou",
                        message: "Your previous request to create the named bucket succeeded and you already own it."
                    )
                }
                let response = S3Service.buildStandardResponse(status: .ok, requestId: req.id)
                response.headers.replaceOrAdd(name: "Location", value: "/\(bucketName)")
                return response
            }
            throw S3Error(
                status: .conflict,
                code: "BucketAlreadyExists",
                message: "The requested bucket name is not available."
            )
        }

        try await BucketService.create(on: req.db, bucketName: bucketName, userId: key.user.id!)

        let response = S3Service.buildStandardResponse(status: .ok, requestId: req.id)
        response.headers.replaceOrAdd(name: "Location", value: "/\(bucketName)")
        return response
    }

    /// Handles PUT /:bucketName?versioning - set bucket versioning configuration. S3's
    /// `VersioningConfiguration.Status` schema only ever accepts `Enabled` or `Suspended` -
    /// there is no way to PUT your way back to `Disabled` (verified against the
    /// PutBucketVersioning API reference); `Disabled` only ever describes a bucket that has
    /// never had this operation called on it. Any other value, including a well-formed but
    /// unrecognized one, is the same `MalformedXML` 400 S3 returns.
    @Sendable
    private func handleVersioningPut(req: Request, bucketName: String) async throws -> Response {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let bucket = try await fetchBucket(req: req, bucketName: bucketName)

        let bodyString = try await S3Service.collectBodyString(req: req)

        let newStatus: VersioningStatus
        if bodyString.contains("<Status>Enabled</Status>") {
            newStatus = .enabled
        } else if bodyString.contains("<Status>Suspended</Status>") {
            newStatus = .suspended
        } else {
            throw S3Error(
                status: .badRequest, code: "MalformedXML",
                message:
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                requestId: req.id)
        }

        bucket.versioningStatus = newStatus.rawValue
        try await bucket.save(on: req.db)

        await BucketVersioningCache.shared.setStatus(for: bucketName, status: newStatus)
        CacheInvalidationService.notify(on: req.db, cache: "bucketVersioning", op: .upsert, key: bucketName)

        return S3Service.buildStandardResponse(status: .ok, requestId: req.id)
    }

    /// Handles PUT /:bucketName?policy - set the bucket policy. Only the bucket owner can set
    /// their own bucket's policy, so this always requires strict authentication, never policy.
    @Sendable
    private func handleBucketPolicyPut(req: Request, bucketName: String) async throws -> Response {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let bucket = try await fetchBucket(req: req, bucketName: bucketName)

        // BlockPublicPolicy rejects PutBucketPolicy outright - every policy this system can
        // store grants Principal "*" access (see BucketPolicy.parseAndValidate), so there's no
        // policy content to inspect; any policy at all is a public-access grant.
        if await BucketPolicyCache.shared.publicAccessBlock(for: bucketName)?.blockPublicPolicy
            == true
        {
            throw S3Error(
                status: .forbidden, code: "AccessDenied",
                message:
                    "Bucket policies cannot be set while BlockPublicPolicy is enabled in this bucket's Public Access Block configuration.",
                requestId: req.id)
        }

        let rawJSON = try await S3Service.collectBodyString(req: req)

        let policy = try BucketPolicy.parseAndValidate(
            rawJSON: rawJSON, bucketName: bucketName, requestId: req.id)

        bucket.policy = rawJSON
        try await bucket.save(on: req.db)

        await BucketPolicyCache.shared.setPolicy(for: bucketName, policy: policy)
        CacheInvalidationService.notify(on: req.db, cache: "bucketPolicy", op: .upsert, key: bucketName)

        return S3Service.buildStandardResponse(status: .noContent, requestId: req.id)
    }

    /// Handles PUT /:bucketName?publicAccessBlock - set the bucket's Public Access Block
    /// configuration.
    @Sendable
    private func handlePublicAccessBlockPut(req: Request, bucketName: String) async throws
        -> Response
    {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let bucket = try await fetchBucket(req: req, bucketName: bucketName)

        let xml = try await S3Service.collectBodyString(req: req)
        let configuration = PublicAccessBlockConfiguration.parse(xml: xml)

        bucket.blockPublicAcls = configuration.blockPublicAcls
        bucket.ignorePublicAcls = configuration.ignorePublicAcls
        bucket.blockPublicPolicy = configuration.blockPublicPolicy
        bucket.restrictPublicBuckets = configuration.restrictPublicBuckets
        try await bucket.save(on: req.db)

        await BucketPolicyCache.shared.setPublicAccessBlock(
            for: bucketName, configuration: configuration)
        CacheInvalidationService.notify(
            on: req.db, cache: "bucketPublicAccessBlock", op: .upsert, key: bucketName)

        return S3Service.buildStandardResponse(status: .ok, requestId: req.id)
    }

    /// Handles DELETE /:bucketName?publicAccessBlock - removes the bucket's Public Access Block
    /// configuration (resetting all 4 flags to false). S3 returns 204 No Content.
    @Sendable
    private func handlePublicAccessBlockDelete(req: Request, bucketName: String) async throws
        -> HTTPStatus
    {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let bucket = try await fetchBucket(req: req, bucketName: bucketName)

        bucket.blockPublicAcls = false
        bucket.ignorePublicAcls = false
        bucket.blockPublicPolicy = false
        bucket.restrictPublicBuckets = false
        try await bucket.save(on: req.db)

        await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)
        CacheInvalidationService.notify(
            on: req.db, cache: "bucketPublicAccessBlock", op: .remove, key: bucketName)

        return .noContent
    }

    /// Handles PUT /:bucketName?tagging - sets the bucket's tag-set, overwriting any existing
    /// tags entirely (S3 does not merge - verified against the PutBucketTagging API
    /// reference). S3 returns 204 No Content.
    @Sendable
    private func handleBucketTaggingPut(req: Request, bucketName: String) async throws
        -> Response
    {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let bucket = try await fetchBucket(req: req, bucketName: bucketName)

        let xml = try await S3Service.collectBodyString(req: req)
        let tagging = try Tagging.parse(xml: xml, requestId: req.id)

        bucket.tags = tagging.toJSON()
        try await bucket.save(on: req.db)

        return S3Service.buildStandardResponse(status: .noContent, requestId: req.id)
    }

    /// Handles DELETE /:bucketName?tagging - removes all of the bucket's tags. S3 returns
    /// 204 No Content.
    @Sendable
    private func handleBucketTaggingDelete(req: Request, bucketName: String) async throws
        -> HTTPStatus
    {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let bucket = try await fetchBucket(req: req, bucketName: bucketName)

        bucket.tags = nil
        try await bucket.save(on: req.db)

        return .noContent
    }

    /// Handles PUT /:bucketName?lifecycle - sets the bucket's lifecycle configuration,
    /// overwriting any existing configuration entirely. S3 returns 200 OK with an empty
    /// body (verified against the PutBucketLifecycleConfiguration API reference).
    @Sendable
    private func handleLifecyclePut(req: Request, bucketName: String) async throws -> Response {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let bucket = try await fetchBucket(req: req, bucketName: bucketName)

        let xml = try await S3Service.collectBodyString(req: req)
        let configuration = try LifecycleConfiguration.parse(xml: xml, requestId: req.id)

        bucket.lifecycleRules = configuration.toJSON()
        try await bucket.save(on: req.db)

        return S3Service.buildStandardResponse(status: .ok, requestId: req.id)
    }

    /// Handles DELETE /:bucketName?lifecycle - removes the bucket's lifecycle configuration.
    /// S3 returns 204 No Content.
    @Sendable
    private func handleLifecycleDelete(req: Request, bucketName: String) async throws
        -> HTTPStatus
    {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let bucket = try await fetchBucket(req: req, bucketName: bucketName)

        bucket.lifecycleRules = nil
        try await bucket.save(on: req.db)

        return .noContent
    }

    /// Handles DELETE /:bucketName?replication - clears the bucket's whole replication
    /// configuration (targets and rules). Unlike PUT ?replication, this needs no target
    /// credentials, so - matching S3's DeleteBucketReplication - it's fully supported, not
    /// a 501.
    @Sendable
    private func handleReplicationDelete(req: Request, bucketName: String) async throws
        -> HTTPStatus
    {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let bucket = try await fetchBucket(req: req, bucketName: bucketName)

        bucket.replicationConfig = nil
        try await bucket.save(on: req.db)

        await ReplicationConfigCache.shared.removeBucket(bucketName)
        CacheInvalidationService.notify(on: req.db, cache: "replicationConfig", op: .remove, key: bucketName)

        return .noContent
    }

    // DELETE /:bucketName
    @Sendable
    func handleBucketDelete(req: Request) async throws -> HTTPStatus {
        let bucketName = try S3Service.extractBucketName(from: req)
        let query = req.url.query ?? ""

        // Handle DELETE ?publicAccessBlock
        if query.lowercased().contains("publicaccessblock") {
            return try await handlePublicAccessBlockDelete(req: req, bucketName: bucketName)
        }

        // Handle DELETE ?tagging
        if query.lowercased().contains("tagging") {
            return try await handleBucketTaggingDelete(req: req, bucketName: bucketName)
        }

        // Handle DELETE ?lifecycle
        if query.lowercased().contains("lifecycle") {
            return try await handleLifecycleDelete(req: req, bucketName: bucketName)
        }

        // Handle DELETE ?policy
        if query.lowercased().contains("policy") {
            return try await handleBucketPolicyDelete(req: req, bucketName: bucketName)
        }

        // Handle DELETE ?replication
        if query.lowercased().contains("replication") {
            return try await handleReplicationDelete(req: req, bucketName: bucketName)
        }

        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .with(\.$user)
                .first()
        else {
            throw S3Error(
                status: .notFound,
                code: "NoSuchBucket",
                message: "The specified bucket does not exist."
            )
        }

        let key = try await S3Service.parseAndAuthenticateWithDB(req: req)

        guard let userId = key.user.id else {
            throw S3Error(status: .forbidden, code: "AccessDenied", message: "Access Denied")
        }

        if bucket.user.id != userId {
            throw S3Error(status: .forbidden, code: "AccessDenied", message: "Access Denied")
        }

        if ObjectFileHandler.hasBucketObjects(bucketName: bucketName) {
            throw S3Error(
                status: .conflict,
                code: "BucketNotEmpty",
                message: "The bucket you tried to delete is not empty."
            )
        }

        try await BucketService.delete(on: req.db, bucketName: bucketName, userId: bucket.user.id!)

        return .noContent
    }

    /// Handles DELETE /:bucketName?policy - removes the bucket policy. Always requires strict
    /// authentication, just like setting it.
    @Sendable
    private func handleBucketPolicyDelete(req: Request, bucketName: String) async throws
        -> HTTPStatus
    {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let bucket = try await fetchBucket(req: req, bucketName: bucketName)

        bucket.policy = nil
        try await bucket.save(on: req.db)

        await BucketPolicyCache.shared.removePolicy(for: bucketName)
        CacheInvalidationService.notify(on: req.db, cache: "bucketPolicy", op: .remove, key: bucketName)

        return .noContent
    }

    // PUT /:bucketName/*key
    @Sendable
    func handleObjectPut(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let keyPath = S3Service.extractObjectKey(from: req)

        guard !keyPath.isEmpty else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Invalid argument", requestId: req.id)
        }

        let authInfo = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        // Check if this is an UploadPart request (PUT with partNumber & uploadId)
        if let partNumberStr = req.query[String.self, at: "partNumber"],
            let partNumber = Int(partNumberStr),
            let uploadId = req.query[String.self, at: "uploadId"]
        {
            // UploadPartCopy - same query params, but copies the part from another object
            if let copySource = try S3Service.parseCopySource(from: req) {
                return try await handleUploadPartCopy(
                    req: req,
                    destinationBucket: bucketName,
                    uploadId: uploadId,
                    partNumber: partNumber,
                    copySource: copySource
                )
            }

            return try await handleUploadPart(
                req: req,
                bucketName: bucketName,
                key: keyPath,
                uploadId: uploadId,
                partNumber: partNumber,
                authInfo: authInfo
            )
        }

        // PUT ?tagging - set the tag-set of an existing object/version, distinct from a plain
        // body PUT
        if req.url.query?.lowercased().contains("tagging") == true {
            return try await handleObjectTaggingPut(req: req, bucketName: bucketName, key: keyPath)
        }

        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        // Check if this is a copy operation
        if let copySource = try S3Service.parseCopySource(from: req) {
            return try await handleCopyObject(
                req: req,
                destinationBucket: bucketName,
                destinationKey: keyPath,
                copySource: copySource,
                versioningStatus: versioningStatus
            )
        }

        // Conditional writes (If-Match/If-None-Match) - only worth the extra disk lookup when
        // either header is actually present, so plain unconditional PUTs (the overwhelming
        // majority) pay zero extra cost. Checked before collecting the body so a request that's
        // going to be rejected doesn't pay for reading/decoding the upload first.
        if req.headers.first(name: "If-Match") != nil
            || req.headers.first(name: "If-None-Match") != nil
        {
            let existing = try ObjectFileHandler.readCurrentObject(
                bucketName: bucketName, key: keyPath, loadData: false)
            try S3Service.validateConditionalPutHeaders(req: req, existingMeta: existing?.meta)
        }

        // Stream the body to a spool file with bounded memory - never buffered whole. The
        // spooler decodes aws-chunked framing (verifying every chunk signature), computes
        // MD5/SHA256 incrementally, and enforces the declared x-amz-content-sha256. S3 allows
        // zero-byte objects; a missing body spools to an empty file.
        let spooled = try await StreamingBodySpooler.spool(req: req, authInfo: authInfo)
        defer { spooled.cleanup() }

        // Validate Content-MD5 if provided
        try S3Service.validateContentMD5(req: req, spooled: spooled)

        let etag = spooled.md5Hex
        var meta = ObjectMeta(
            bucketName: bucketName,
            key: keyPath,
            size: spooled.size,
            contentType: req.headers.contentType?.description ?? "application/octet-stream",
            etag: etag,
            updatedAt: Date()
        )

        for (name, value) in req.headers {
            if name.lowercased().hasPrefix("x-amz-meta-") {
                let metaKey = String(name.dropFirst("x-amz-meta-".count)).lowercased()
                meta.metadata[metaKey] = value
            }
        }

        // x-amz-tagging sets tags inline at upload time - URL query-string encoded (verified
        // against the PutObject API reference)
        if let taggingHeader = req.headers.first(name: "x-amz-tagging") {
            let tagging = Tagging.parseHeaderValue(taggingHeader)
            guard tagging.tags.count <= Tagging.maxTagCount else {
                throw S3Error(
                    status: .badRequest, code: "InvalidTag",
                    message: "Object tags cannot be greater than \(Tagging.maxTagCount).",
                    requestId: req.id)
            }
            meta.tags = tagging.tags
        }

        var headers = HTTPHeaders()
        headers.add(name: "ETag", value: S3Service.quoteETag(etag))

        // Write with versioning support. Small bodies arrive in memory and take the direct
        // write; large ones were spooled to disk and are copied into the final .obj in fixed
        // windows. Both are fsynced by AtomicObjectWriter before the PUT is acknowledged -
        // real blocking syscalls, so the whole write+fsync+rename sequence is offloaded to
        // the blocking-IO thread pool rather than tying up the async executor.
        let storage = spooled.storage
        let finalMeta = meta
        let writtenVersionId: String? = try await S3Service.offloadBlockingIO(req) {
            switch storage {
            case .memory(let data):
                if versioningStatus != .disabled {
                    return try ObjectFileHandler.writeVersioned(
                        metadata: finalMeta,
                        data: data,
                        bucketName: bucketName,
                        key: keyPath,
                        versioningStatus: versioningStatus
                    )
                } else {
                    let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
                    try ObjectFileHandler.write(metadata: finalMeta, data: data, to: path)
                    return nil
                }
            case .file(let spoolPath):
                let spoolSource = [(path: spoolPath, offset: 0, size: spooled.size)]
                if versioningStatus != .disabled {
                    return try ObjectFileHandler.writeVersionedStreamed(
                        metadata: finalMeta,
                        payloadSources: spoolSource,
                        bucketName: bucketName,
                        key: keyPath,
                        versioningStatus: versioningStatus
                    )
                } else {
                    let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
                    try ObjectFileHandler.writeStreamed(
                        metadata: finalMeta, payloadSources: spoolSource, to: path)
                    return nil
                }
            }
        }
        if let writtenVersionId {
            headers.add(name: "x-amz-version-id", value: writtenVersionId)
        }

        await NotificationService.emit(
            event: .objectCreatedPut, bucketName: bucketName, key: keyPath,
            size: spooled.size, etag: etag, versionId: writtenVersionId,
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, on: req.db)
        await ReplicationService.enqueuePut(
            bucketName: bucketName, key: keyPath, versionId: writtenVersionId, on: req.db)

        return Response(status: .ok, headers: headers)
    }

    /// Handles PUT /:bucket/:key?tagging - sets the tag-set of a specific object version (or
    /// the current one if no `versionId` is given). Modifies the existing version's metadata in
    /// place - does not create a new version. S3 returns 200 with x-amz-version-id
    /// (verified against the PutObjectTagging API reference). Auth is already done by the
    /// caller (`handleObjectPut`) before dispatching here.
    @Sendable
    private func handleObjectTaggingPut(req: Request, bucketName: String, key: String)
        async throws -> Response
    {
        let versionId = req.query[String.self, at: "versionId"]
        guard
            let path = try ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: versionId)
        else {
            throw S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: req.id)
        }

        let xml = try await S3Service.collectBodyString(req: req)
        let tagging = try Tagging.parse(xml: xml, requestId: req.id)

        // Metadata-only rewrite - the payload is window-copied from the file to itself, never
        // buffered, so this doesn't cost a full read+rewrite of a potentially huge object.
        let updatedMeta = try await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.rewriteMetadata(at: path) { $0.tags = tagging.tags }
        }

        var headers = HTTPHeaders()
        if let versionId = updatedMeta.versionId {
            headers.add(name: "x-amz-version-id", value: versionId)
        }
        return Response(status: .ok, headers: headers)
    }

    /// Handles GET /:bucket/:key?tagging - returns the tag-set of a specific object version (or
    /// the current one). Always 200, even with no tags (verified against the GetObjectTagging
    /// API reference - unlike bucket tagging, which 404s when unset).
    @Sendable
    private func handleObjectTaggingGet(req: Request, bucketName: String, key: String)
        async throws -> Response
    {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let versionId = req.query[String.self, at: "versionId"]
        guard
            let path = try ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: versionId)
        else {
            throw S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: req.id)
        }

        let (meta, _) = try ObjectFileHandler.read(from: path, loadData: false)
        let tagging = Tagging(tags: meta.tags ?? [:])

        let response = S3Service.buildXMLResponse(data: Data(tagging.toXML().utf8))
        if let versionId = meta.versionId {
            response.headers.add(name: "x-amz-version-id", value: versionId)
        }
        return response
    }

    /// Handles DELETE /:bucket/:key?tagging - removes all tags from a specific object version
    /// (or the current one). S3 returns 204 No Content. Auth is already done by the
    /// caller (`handleObjectDelete`) before dispatching here.
    @Sendable
    private func handleObjectTaggingDelete(req: Request, bucketName: String, key: String)
        async throws -> Response
    {
        let versionId = req.query[String.self, at: "versionId"]
        guard
            let path = try ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: versionId)
        else {
            throw S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: req.id)
        }

        let updatedMeta = try await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.rewriteMetadata(at: path) { $0.tags = nil }
        }

        var headers = HTTPHeaders()
        if let versionId = updatedMeta.versionId {
            headers.add(name: "x-amz-version-id", value: versionId)
        }
        return Response(status: .noContent, headers: headers)
    }

    // Helper method to handle copy operations
    @Sendable
    private func handleCopyObject(
        req: Request,
        destinationBucket: String,
        destinationKey: String,
        copySource: CopySource,
        versioningStatus: VersioningStatus
    ) async throws -> Response {
        try await S3Service.verifyBucketExists(copySource.bucketName, requestId: req.id)

        // Authenticate access to source bucket
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: copySource.bucketName)

        // Resolve the source straight to its on-disk file - the payload is streamed
        // file-to-file in fixed windows, never buffered whole
        let (sourceMeta, sourcePath, sourcePayloadOffset, sourcePayloadSize) =
            try await S3Service.offloadBlockingIO(req) {
                try S3Service.resolveObjectForCopy(
                    bucketName: copySource.bucketName,
                    key: copySource.key,
                    versionId: copySource.versionId,
                    requestId: req.id
                )
            }

        // Validate copy conditions (if-match, if-none-match, etc.)
        try S3Service.validateCopyConditions(req: req, sourceMeta: sourceMeta)

        // Determine metadata handling. x-amz-metadata-directive COPY (the default) carries
        // the source's Content-Type AND user metadata over; REPLACE takes both from this
        // request's headers instead (verified against the CopyObject API reference - metadata
        // is all-or-nothing per directive, never merged).
        let replaceMetadata = S3Service.shouldReplaceMetadata(req: req)

        let contentType: String
        var userMetadata: [String: String]
        if replaceMetadata {
            contentType = req.headers.contentType?.description ?? sourceMeta.contentType
            userMetadata = [:]
            for (name, value) in req.headers {
                if name.lowercased().hasPrefix("x-amz-meta-") {
                    let metaKey = String(name.dropFirst("x-amz-meta-".count)).lowercased()
                    userMetadata[metaKey] = value
                }
            }
        } else {
            contentType = sourceMeta.contentType
            userMetadata = sourceMeta.metadata
        }

        // Tags follow their own directive: x-amz-tagging-directive COPY (the default) carries
        // the source's tag-set over; REPLACE takes it from this request's x-amz-tagging header
        // (verified against the CopyObject API reference).
        let tags: [String: String]?
        if req.headers.first(name: "x-amz-tagging-directive")?.uppercased() == "REPLACE" {
            let tagging = Tagging.parseHeaderValue(
                req.headers.first(name: "x-amz-tagging") ?? "")
            guard tagging.tags.count <= Tagging.maxTagCount else {
                throw S3Error(
                    status: .badRequest, code: "InvalidTag",
                    message: "Object tags cannot be greater than \(Tagging.maxTagCount).",
                    requestId: req.id)
            }
            tags = tagging.tags.isEmpty ? nil : tagging.tags
        } else {
            tags = sourceMeta.tags
        }

        // The destination's ETag is the plain MD5 of the copied bytes. A single-part source's
        // ETag already is exactly that; a multipart source ("-N" suffix) needs one streaming
        // hash pass over the payload.
        let etag: String
        if sourceMeta.etag.contains("-") {
            etag = try await S3Service.offloadBlockingIO(req) {
                try ObjectFileHandler.md5HexOfFileRegion(
                    path: sourcePath, offset: sourcePayloadOffset, size: sourcePayloadSize)
            }
        } else {
            etag = sourceMeta.etag
        }

        let destinationMeta = ObjectMeta(
            bucketName: destinationBucket,
            key: destinationKey,
            size: sourcePayloadSize,
            contentType: contentType,
            etag: etag,
            metadata: userMetadata,
            updatedAt: Date(),
            tags: tags
        )

        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/xml")

        // Write with versioning support - payload window-copied from the source file. Real
        // blocking file IO (open/read/write/fsync/rename), so it's offloaded rather than
        // tying up the async executor for the whole copy.
        let copySources = [
            (path: sourcePath, offset: sourcePayloadOffset, size: sourcePayloadSize)
        ]
        let versionId: String? = try await S3Service.offloadBlockingIO(req) {
            if versioningStatus != .disabled {
                return try ObjectFileHandler.writeVersionedStreamed(
                    metadata: destinationMeta,
                    payloadSources: copySources,
                    bucketName: destinationBucket,
                    key: destinationKey,
                    versioningStatus: versioningStatus
                )
            } else {
                let destinationPath = ObjectFileHandler.storagePath(
                    for: destinationBucket, key: destinationKey)
                try ObjectFileHandler.writeStreamed(
                    metadata: destinationMeta, payloadSources: copySources, to: destinationPath)
                return nil
            }
        }
        if let versionId {
            headers.add(name: "x-amz-version-id", value: versionId)
        }

        // Add source version ID if present
        if let sourceVersionId = sourceMeta.versionId {
            headers.add(name: "x-amz-copy-source-version-id", value: sourceVersionId)
        }

        await NotificationService.emit(
            event: .objectCreatedCopy, bucketName: destinationBucket, key: destinationKey,
            size: destinationMeta.size, etag: etag, versionId: versionId,
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, on: req.db)
        await ReplicationService.enqueuePut(
            bucketName: destinationBucket, key: destinationKey, versionId: versionId, on: req.db)

        // Build copy result response (S3 returns XML for copy operations)
        let copyResult = """
            <?xml version="1.0" encoding="UTF-8"?>
            <CopyObjectResult>
                <LastModified>\(ISO8601DateFormatter().string(from: destinationMeta.updatedAt))</LastModified>
                <ETag>"\(etag)"</ETag>
            </CopyObjectResult>
            """

        return Response(status: .ok, headers: headers, body: .init(string: copyResult))
    }

    // GET /:bucketName/*key
    @Sendable
    func handleObjectGet(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let keyPath = S3Service.extractObjectKey(from: req)

        try await S3Service.verifyBucketExists(bucketName, requestId: req.id)

        // ListParts (GET with uploadId) always requires strict auth - it isn't in the
        // public-access whitelist, so it's checked before any anonymous-access decision.
        if let uploadId = req.query[String.self, at: "uploadId"] {
            _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)
            return try handleListParts(
                req: req,
                bucketName: bucketName,
                key: keyPath,
                uploadId: uploadId
            )
        }

        // GetObjectTagging - a different permission than GetObject in S3, not covered by
        // the bucket-policy public-access whitelist, so it always requires strict auth.
        if req.url.query?.lowercased().contains("tagging") == true {
            return try await handleObjectTaggingGet(req: req, bucketName: bucketName, key: keyPath)
        }

        // Check for versionId query parameter
        let versionId = req.query[String.self, at: "versionId"]

        _ = try await S3Service.authenticateOrAuthorizePublic(
            req: req, bucketName: bucketName,
            action: versionId != nil ? .getObjectVersion : .getObject, key: keyPath)

        // Resolve to a single on-disk path up front - the meta, conditional checks, range
        // math, and body all come off that one file, and large payloads stream from it
        // directly instead of ever being buffered whole in memory.
        var path = try ObjectFileHandler.resolvePath(
            bucketName: bucketName, key: keyPath, versionId: versionId)
        if path == nil, versionId == "null" {
            // The "null" version of a never-versioned key lives at the plain path (mirrors
            // the same fallback in deleteVersion - listings report it as VersionId "null")
            let plainPath = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
            if ObjectFileHandler.keyExists(for: bucketName, key: keyPath, path: plainPath) {
                path = plainPath
            }
        }
        guard let path else {
            throw S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: req.id)
        }

        let (meta, payloadOffset, payloadSize) = try ObjectFileHandler.payloadLocation(path: path)

        // A latest-version delete marker means "deleted" (resolvePath already nils this for
        // versionId == nil - kept as a belt-and-suspenders check)
        if meta.isDeleteMarker && versionId == nil {
            throw S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: req.id)
        }

        try S3Service.validateConditionalHeaders(req: req, meta: meta)

        let byteRange: ByteRange? =
            req.headers.first(name: .range) != nil
            ? try S3RangeParser.parseRange(from: req, fileSize: meta.size)
            : nil
        let bodyLength = byteRange?.length ?? payloadSize

        if bodyLength > Constants.streamingThreshold {
            return S3Service.buildStreamingObjectResponse(
                req: req, meta: meta, path: path, payloadOffset: payloadOffset, range: byteRange)
        }

        // Small payloads: one buffered read is cheaper than a threadpool round trip. The
        // response is built from THIS read's metadata (not the earlier lookup's), so headers
        // and body always describe the same snapshot of the file even under a concurrent
        // overwrite - the streaming branch gets the same guarantee from its ETag check.
        let (freshMeta, data) = try ObjectFileHandler.read(
            from: path, loadData: true,
            range: byteRange.map { ($0.start, $0.end) })
        guard let data else {
            throw S3Error(
                status: .internalServerError, code: "InternalError",
                message: "We encountered an internal error. Please try again.",
                requestId: req.id)
        }
        return S3Service.buildVersionedObjectMetadataResponse(
            meta: freshMeta, includeBody: true, data: data, range: byteRange)
    }

    // DELETE /:bucketName/*key
    @Sendable
    func handleObjectDelete(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let keyPath = S3Service.extractObjectKey(from: req)

        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        // Check if this is AbortMultipartUpload (DELETE with uploadId, no versionId)
        if let uploadId = req.query[String.self, at: "uploadId"],
            req.query[String.self, at: "versionId"] == nil
        {
            return try await handleAbortMultipartUpload(
                req: req,
                bucketName: bucketName,
                key: keyPath,
                uploadId: uploadId
            )
        }

        // DeleteObjectTagging - distinct from deleting the object/version itself
        if req.url.query?.lowercased().contains("tagging") == true {
            return try await handleObjectTaggingDelete(
                req: req, bucketName: bucketName, key: keyPath)
        }

        let versionId = req.query[String.self, at: "versionId"]
        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        let outcome = try await S3Service.offloadBlockingIO(req) {
            try S3Service.deleteObject(
                bucketName: bucketName,
                key: keyPath,
                versionId: versionId,
                versioningStatus: versioningStatus
            )
        }

        var headers = HTTPHeaders()
        if let resultVersionId = outcome.versionId {
            headers.add(name: "x-amz-version-id", value: resultVersionId)
        }
        if outcome.isDeleteMarker {
            headers.add(name: "x-amz-delete-marker", value: "true")
        }

        await NotificationService.emit(
            event: outcome.isDeleteMarker ? .objectRemovedDeleteMarkerCreated : .objectRemovedDelete,
            bucketName: bucketName, key: keyPath, size: nil, etag: nil,
            versionId: outcome.versionId,
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, on: req.db)
        // A client-specified versionId permanently prunes one historical version - there's no
        // matching version on the replication target to remove (see ReplicationClient.replicateDelete),
        // so only replicate when this deleted the *current* object.
        if versionId == nil {
            await ReplicationService.enqueueDelete(
                bucketName: bucketName, key: keyPath, versionId: outcome.versionId, on: req.db)
        }

        return Response(status: .noContent, headers: headers)
    }

    // POST /:bucketName (no key) - currently only used for ?delete (Multi-Object Delete)
    @Sendable
    func handleBucketPost(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let query = req.url.query ?? ""

        try await S3Service.verifyBucketExists(bucketName, requestId: req.id)
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        if query.lowercased().contains("delete") {
            return try await handleDeleteObjects(req: req, bucketName: bucketName)
        }

        throw S3Error(
            status: .badRequest, code: "InvalidRequest",
            message: "Invalid POST request", requestId: req.id)
    }

    /// Multi-Object Delete - POST /:bucket?delete
    @Sendable
    private func handleDeleteObjects(req: Request, bucketName: String) async throws -> Response {
        let bodyString = try await S3Service.collectBodyString(req: req)

        let (objects, quiet) = try parseDeleteObjectsBody(bodyString, requestId: req.id)

        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        var deleted: [DeletedEntry] = []
        var errors: [DeleteErrorEntry] = []

        for object in objects {
            guard !object.key.isEmpty else {
                errors.append(
                    DeleteErrorEntry(
                        key: object.key, code: "InvalidArgument",
                        message: "The Key element of an Object cannot be empty."))
                continue
            }

            do {
                let outcome = try await S3Service.offloadBlockingIO(req) {
                    try S3Service.deleteObject(
                        bucketName: bucketName,
                        key: object.key,
                        versionId: object.versionId,
                        versioningStatus: versioningStatus
                    )
                }

                deleted.append(
                    DeletedEntry(
                        key: object.key,
                        versionId: outcome.isDeleteMarker ? nil : outcome.versionId,
                        deleteMarker: outcome.isDeleteMarker ? true : nil,
                        deleteMarkerVersionId: outcome.isDeleteMarker ? outcome.versionId : nil
                    ))

                await NotificationService.emit(
                    event: outcome.isDeleteMarker
                        ? .objectRemovedDeleteMarkerCreated : .objectRemovedDelete,
                    bucketName: bucketName, key: object.key, size: nil, etag: nil,
                    versionId: outcome.versionId,
                    requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, on: req.db)
                // See the single-object DELETE handler: a client-specified versionId prunes one
                // historical version, which has no replicable equivalent on the target.
                if object.versionId == nil {
                    await ReplicationService.enqueueDelete(
                        bucketName: bucketName, key: object.key, versionId: outcome.versionId,
                        on: req.db)
                }
            } catch {
                errors.append(
                    DeleteErrorEntry(
                        key: object.key, code: "InternalError",
                        message: "We encountered an internal error. Please try again."))
            }
        }

        let xmlData = try S3Service.buildDeleteObjectsResponse(
            deleted: quiet ? [] : deleted, errors: errors)
        return S3Service.buildXMLResponse(data: xmlData)
    }

    /// Parses the Multi-Object Delete request XML body:
    /// `<Delete><Quiet>true</Quiet><Object><Key>k</Key><VersionId>v</VersionId></Object>...</Delete>`
    private func parseDeleteObjectsBody(
        _ body: String,
        requestId: String
    ) throws -> (objects: [DeleteObjectRequestEntry], quiet: Bool) {
        let quiet = body.contains("<Quiet>true</Quiet>")

        let objectBlockPattern = #"<Object>(.*?)</Object>"#
        let objectBlockRegex = try NSRegularExpression(
            pattern: objectBlockPattern, options: [.dotMatchesLineSeparators])
        let range = NSRange(body.startIndex..., in: body)

        let objectBlocks = objectBlockRegex.matches(in: body, options: [], range: range)

        guard !objectBlocks.isEmpty else {
            throw S3Error(
                status: .badRequest, code: "MalformedXML",
                message: "The XML you provided was not well-formed.", requestId: requestId)
        }

        guard objectBlocks.count <= 1000 else {
            throw S3Error(
                status: .badRequest, code: "MalformedXML",
                message: "The request contains more keys than are permitted in a single request.",
                requestId: requestId)
        }

        let keyPattern = #"<Key>(.*?)</Key>"#
        let versionIdPattern = #"<VersionId>(.*?)</VersionId>"#
        let keyRegex = try NSRegularExpression(
            pattern: keyPattern, options: [.dotMatchesLineSeparators])
        let versionIdRegex = try NSRegularExpression(
            pattern: versionIdPattern, options: [.dotMatchesLineSeparators])

        var objects: [DeleteObjectRequestEntry] = []

        for objectBlock in objectBlocks {
            guard let blockRange = Range(objectBlock.range(at: 1), in: body) else {
                continue
            }
            let blockContent = String(body[blockRange])
            let blockNSRange = NSRange(blockContent.startIndex..., in: blockContent)

            guard
                let keyMatch = keyRegex.firstMatch(
                    in: blockContent, options: [], range: blockNSRange),
                let keyRange = Range(keyMatch.range(at: 1), in: blockContent)
            else {
                continue
            }
            let key = String(blockContent[keyRange]).xmlUnescaped

            var versionId: String? = nil
            if let versionIdMatch = versionIdRegex.firstMatch(
                in: blockContent, options: [], range: blockNSRange),
                let versionIdRange = Range(versionIdMatch.range(at: 1), in: blockContent)
            {
                versionId = String(blockContent[versionIdRange]).xmlUnescaped
            }

            objects.append(DeleteObjectRequestEntry(key: key, versionId: versionId))
        }

        return (objects, quiet)
    }

    /// Handles POST /:bucketName/*key
    /// - ?uploads → CreateMultipartUpload
    /// - ?uploadId=X → CompleteMultipartUpload
    @Sendable
    func handleObjectPost(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let keyPath = S3Service.extractObjectKey(from: req)
        let query = req.url.query ?? ""

        try await S3Service.verifyBucketExists(bucketName, requestId: req.id)
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        // POST ?uploads → CreateMultipartUpload
        if query.lowercased().contains("uploads") && !query.contains("uploadId") {
            return try handleCreateMultipartUpload(req: req, bucketName: bucketName, key: keyPath)
        }

        // POST ?uploadId=X → CompleteMultipartUpload
        if let uploadId = req.query[String.self, at: "uploadId"] {
            return try await handleCompleteMultipartUpload(
                req: req, bucketName: bucketName, key: keyPath, uploadId: uploadId)
        }

        throw S3Error(
            status: .badRequest, code: "InvalidRequest",
            message: "Invalid POST request", requestId: req.id)
    }

    /// CreateMultipartUpload - POST /:bucket/:key?uploads
    @Sendable
    private func handleCreateMultipartUpload(
        req: Request,
        bucketName: String,
        key: String
    ) throws -> Response {
        guard !key.isEmpty else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Invalid argument", requestId: req.id)
        }

        let contentType = req.headers.contentType?.description ?? "application/octet-stream"

        // Extract custom metadata headers
        var metadata: [String: String] = [:]
        for (name, value) in req.headers {
            if name.lowercased().hasPrefix("x-amz-meta-") {
                let metaKey = String(name.dropFirst("x-amz-meta-".count)).lowercased()
                metadata[metaKey] = value
            }
        }

        let uploadId = try MultipartFileHandler.createUpload(
            bucketName: bucketName,
            key: key,
            contentType: contentType,
            metadata: metadata
        )

        let xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Bucket>\(bucketName)</Bucket>
                <Key>\(key.xmlEscaped)</Key>
                <UploadId>\(uploadId)</UploadId>
            </InitiateMultipartUploadResult>
            """

        return S3Service.buildXMLResponse(data: Data(xml.utf8))
    }

    /// CompleteMultipartUpload - POST /:bucket/:key?uploadId=X
    @Sendable
    private func handleCompleteMultipartUpload(
        req: Request,
        bucketName: String,
        key: String,
        uploadId: String
    ) async throws -> Response {
        // Verify upload exists
        guard MultipartFileHandler.uploadExists(bucketName: bucketName, uploadId: uploadId) else {
            throw S3Error(
                status: .notFound, code: "NoSuchUpload",
                message: "The specified upload does not exist.", requestId: req.id)
        }

        // Parse the CompleteMultipartUpload XML body
        let bodyString = try await S3Service.collectBodyString(req: req)

        let parts = try S3Service.parseCompleteMultipartBody(bodyString, requestId: req.id)

        // Get versioning status
        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        // Complete the upload - streams every part into the final .obj in fixed windows and
        // fsyncs before returning, all real blocking file IO, so it's offloaded to the
        // blocking-IO thread pool rather than tying up the async executor for the whole
        // (potentially multi-gigabyte) assembly.
        let etag: String
        let versionId: String?
        let finalSize: Int
        do {
            let result = try await S3Service.offloadBlockingIO(req) {
                try MultipartFileHandler.completeUpload(
                    bucketName: bucketName,
                    uploadId: uploadId,
                    parts: parts,
                    versioningStatus: versioningStatus
                )
            }
            etag = result.etag
            versionId = result.versionId
            finalSize = result.size
        } catch let error as NSError {
            // Convert NSError to S3Error
            let code = error.domain == "InvalidPartOrder" ? "InvalidPartOrder" : "InvalidPart"
            throw S3Error(
                status: .badRequest, code: code,
                message: error.localizedDescription, requestId: req.id)
        }

        await NotificationService.emit(
            event: .objectCreatedCompleteMultipartUpload, bucketName: bucketName, key: key,
            size: finalSize, etag: etag, versionId: versionId,
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, on: req.db)
        await ReplicationService.enqueuePut(
            bucketName: bucketName, key: key, versionId: versionId, on: req.db)

        // Build response headers
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/xml")
        if let versionId = versionId {
            headers.add(name: "x-amz-version-id", value: versionId)
        }

        // Build XML response
        let location = "/\(bucketName)/\(key)"
        let xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Location>\(location)</Location>
                <Bucket>\(bucketName)</Bucket>
                <Key>\(key.xmlEscaped)</Key>
                <ETag>"\(etag)"</ETag>
            </CompleteMultipartUploadResult>
            """

        return Response(status: .ok, headers: headers, body: .init(string: xml))
    }


    /// UploadPart - handled in handleObjectPut when partNumber & uploadId are present
    @Sendable
    private func handleUploadPart(
        req: Request,
        bucketName: String,
        key: String,
        uploadId: String,
        partNumber: Int,
        authInfo: S3AuthInfo
    ) async throws -> Response {
        // Verify upload exists
        guard MultipartFileHandler.uploadExists(bucketName: bucketName, uploadId: uploadId) else {
            throw S3Error(
                status: .notFound, code: "NoSuchUpload",
                message: "The specified upload does not exist.", requestId: req.id)
        }

        // Validate part number
        guard partNumber >= 1 && partNumber <= 10000 else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Part number must be between 1 and 10000.", requestId: req.id)
        }

        // Stream the part body to a spool file (bounded memory, incremental digests and
        // chunk-signature verification - same treatment as a plain PutObject body)
        let spooled = try await StreamingBodySpooler.spool(req: req, authInfo: authInfo)
        defer { spooled.cleanup() }

        // Unlike PutObject, an empty UploadPart is always an error
        guard spooled.size > 0 else {
            throw S3Error(
                status: .badRequest, code: "MissingRequestBodyError",
                message: "Request body is empty.", requestId: req.id)
        }

        // Validate Content-MD5 if provided
        try S3Service.validateContentMD5(req: req, spooled: spooled)

        // Small parts write directly from memory; large ones were spooled to disk and the
        // spool file becomes the part file via rename - no copy at all. Real blocking file
        // IO either way, so it's offloaded to the blocking-IO thread pool.
        let storage = spooled.storage
        let etag: String = try await S3Service.offloadBlockingIO(req) {
            switch storage {
            case .memory(let data):
                return try MultipartFileHandler.writePart(
                    bucketName: bucketName,
                    uploadId: uploadId,
                    partNumber: partNumber,
                    data: data
                )
            case .file(let spoolPath):
                return try MultipartFileHandler.writePartStreamed(
                    bucketName: bucketName,
                    uploadId: uploadId,
                    partNumber: partNumber,
                    spoolPath: spoolPath,
                    etag: spooled.md5Hex,
                    size: spooled.size
                )
            }
        }

        var headers = HTTPHeaders()
        headers.add(name: "ETag", value: S3Service.quoteETag(etag))

        return Response(status: .ok, headers: headers)
    }

    /// UploadPartCopy - PUT /:bucket/:key?partNumber=X&uploadId=Y with an x-amz-copy-source header.
    /// Copies (a range of) another object's data into a part of an in-progress multipart upload.
    @Sendable
    private func handleUploadPartCopy(
        req: Request,
        destinationBucket: String,
        uploadId: String,
        partNumber: Int,
        copySource: CopySource
    ) async throws -> Response {
        guard MultipartFileHandler.uploadExists(bucketName: destinationBucket, uploadId: uploadId)
        else {
            throw S3Error(
                status: .notFound, code: "NoSuchUpload",
                message: "The specified upload does not exist.", requestId: req.id)
        }

        guard partNumber >= 1 && partNumber <= 10000 else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Part number must be between 1 and 10000.", requestId: req.id)
        }

        try await S3Service.verifyBucketExists(copySource.bucketName, requestId: req.id)

        // Authenticate access to the source bucket
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: copySource.bucketName)

        // Resolve the source straight to its on-disk file - the copied region is streamed
        // file-to-file in fixed windows, never buffered whole
        let (sourceMeta, sourcePath, sourcePayloadOffset, sourcePayloadSize) =
            try await S3Service.offloadBlockingIO(req) {
                try S3Service.resolveObjectForCopy(
                    bucketName: copySource.bucketName,
                    key: copySource.key,
                    versionId: copySource.versionId,
                    requestId: req.id
                )
            }

        // Validate copy conditions (x-amz-copy-source-if-match, etc.)
        try S3Service.validateCopyConditions(req: req, sourceMeta: sourceMeta)

        // Optional partial copy via x-amz-copy-source-range: "bytes=start-end"
        let copyOffset: Int
        let copySize: Int
        if let rangeHeader = req.headers.first(name: "x-amz-copy-source-range") {
            guard
                let range = S3RangeParser.parseRangeHeader(rangeHeader, fileSize: sourceMeta.size)
            else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument",
                    message: "The x-amz-copy-source-range value is invalid.", requestId: req.id)
            }
            copyOffset = sourcePayloadOffset + range.start
            copySize = range.length
        } else {
            copyOffset = sourcePayloadOffset
            copySize = sourcePayloadSize
        }

        let etag = try await S3Service.offloadBlockingIO(req) {
            try MultipartFileHandler.writePartFromFile(
                bucketName: destinationBucket,
                uploadId: uploadId,
                partNumber: partNumber,
                sourcePath: sourcePath,
                sourceOffset: copyOffset,
                sourceSize: copySize
            )
        }

        let xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <CopyPartResult>
                <LastModified>\(sourceMeta.updatedAt.iso8601String)</LastModified>
                <ETag>"\(etag)"</ETag>
            </CopyPartResult>
            """

        let response = S3Service.buildXMLResponse(data: Data(xml.utf8))
        if let sourceVersionId = sourceMeta.versionId {
            response.headers.add(name: "x-amz-copy-source-version-id", value: sourceVersionId)
        }
        return response
    }

    /// AbortMultipartUpload - DELETE /:bucket/:key?uploadId=X
    @Sendable
    private func handleAbortMultipartUpload(
        req: Request,
        bucketName: String,
        key: String,
        uploadId: String
    ) async throws -> Response {
        // Verify upload exists
        guard MultipartFileHandler.uploadExists(bucketName: bucketName, uploadId: uploadId) else {
            throw S3Error(
                status: .notFound, code: "NoSuchUpload",
                message: "The specified upload does not exist.", requestId: req.id)
        }

        // Deletes every part file in the upload directory - a real (if usually small) batch
        // of unlink syscalls, offloaded like every other write/delete path here.
        try await S3Service.offloadBlockingIO(req) {
            try MultipartFileHandler.abortUpload(bucketName: bucketName, uploadId: uploadId)
        }

        return Response(status: .noContent)
    }

    /// ListParts - GET /:bucket/:key?uploadId=X
    @Sendable
    private func handleListParts(
        req: Request,
        bucketName: String,
        key: String,
        uploadId: String
    ) throws -> Response {
        // Verify upload exists - a missing/aborted upload must surface as NoSuchUpload,
        // not as whatever the file layer throws (which would become a 500)
        guard MultipartFileHandler.uploadExists(bucketName: bucketName, uploadId: uploadId) else {
            throw S3Error(
                status: .notFound, code: "NoSuchUpload",
                message: "The specified upload does not exist.", requestId: req.id)
        }

        let maxParts = req.query[Int.self, at: "max-parts"] ?? 1000
        let partNumberMarker = req.query[Int.self, at: "part-number-marker"] ?? 0

        let (parts, isTruncated, nextPartNumberMarker) = try MultipartFileHandler.listParts(
            bucketName: bucketName,
            uploadId: uploadId,
            maxParts: maxParts,
            partNumberMarker: partNumberMarker
        )

        // Build XML response
        var partsXml = ""
        for part in parts {
            partsXml += """
                    <Part>
                        <PartNumber>\(part.partNumber)</PartNumber>
                        <LastModified>\(part.lastModified.iso8601String)</LastModified>
                        <ETag>"\(part.etag)"</ETag>
                        <Size>\(part.size)</Size>
                    </Part>
                """
        }

        let xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Bucket>\(bucketName)</Bucket>
                <Key>\(key.xmlEscaped)</Key>
                <UploadId>\(uploadId)</UploadId>
                <PartNumberMarker>\(partNumberMarker)</PartNumberMarker>
                <NextPartNumberMarker>\(nextPartNumberMarker ?? 0)</NextPartNumberMarker>
                <MaxParts>\(maxParts)</MaxParts>
                <IsTruncated>\(isTruncated)</IsTruncated>
            \(partsXml)
            </ListPartsResult>
            """

        return S3Service.buildXMLResponse(data: Data(xml.utf8))
    }

    /// ListMultipartUploads - GET /:bucket?uploads
    @Sendable
    private func handleListMultipartUploads(req: Request, bucketName: String) throws -> Response {
        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let keyMarker = req.query[String.self, at: "key-marker"]
        let uploadIdMarker = req.query[String.self, at: "upload-id-marker"]
        let maxUploads = req.query[Int.self, at: "max-uploads"] ?? 1000

        let (uploads, isTruncated, nextKeyMarker, nextUploadIdMarker) =
            try MultipartFileHandler.listUploads(
                bucketName: bucketName,
                prefix: prefix,
                keyMarker: keyMarker,
                uploadIdMarker: uploadIdMarker,
                maxUploads: maxUploads
            )

        var uploadsXml = ""
        for upload in uploads {
            uploadsXml += """
                    <Upload>
                        <Key>\(upload.key.xmlEscaped)</Key>
                        <UploadId>\(upload.uploadId)</UploadId>
                        <Initiated>\(upload.initiated.iso8601String)</Initiated>
                    </Upload>
                """
        }

        let xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Bucket>\(bucketName)</Bucket>
                <KeyMarker>\(keyMarker ?? "")</KeyMarker>
                <UploadIdMarker>\(uploadIdMarker ?? "")</UploadIdMarker>
                <NextKeyMarker>\(nextKeyMarker ?? "")</NextKeyMarker>
                <NextUploadIdMarker>\(nextUploadIdMarker ?? "")</NextUploadIdMarker>
                <MaxUploads>\(maxUploads)</MaxUploads>
                <IsTruncated>\(isTruncated)</IsTruncated>
                <Prefix>\(prefix)</Prefix>
            \(uploadsXml)
            </ListMultipartUploadsResult>
            """

        return S3Service.buildXMLResponse(data: Data(xml.utf8))
    }
}
