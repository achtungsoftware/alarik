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
        guard let bucket = try await Bucket.find(app: req.application, name: bucketName) else {
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
        let queryNames = S3Service.queryParameterNames(from: query)

        // Handle ?uploads - list multipart uploads (always requires strict auth - not in the
        // public-access whitelist). Exact parameter-name match: a plain ListObjects call with
        // e.g. ?prefix=uploads/2024/photo.jpg must never be misrouted here just because the
        // *value* of an unrelated parameter contains the word "uploads".
        if queryNames.contains("uploads") {
            _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)
            return try await handleListMultipartUploads(req: req, bucketName: bucketName)
        }

        // Handle ?versions - list object versions (always requires strict auth)
        let isListVersions = queryNames.contains("versions")
        if isListVersions {
            _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)
            return try await handleListVersions(req: req, bucketName: bucketName)
        }

        // Handle subresource queries (location, policy, versioning config) - always requires
        // strict auth. The bucket is only fetched here (not unconditionally up front) since
        // it's only needed by the ?versioning/?policy responses - no need to hit the DB for
        // every plain ListObjects call, anonymous or not.
        let shouldHandle = S3Service.shouldHandleSubresource(query: query)
        if shouldHandle {
            _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)
            let bucket = try await Bucket.find(app: req.application, name: bucketName)
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

        let (objects, commonPrefixes, isTruncated, nextMarker) =
            try await ClusterListingService.listObjects(
                req: req,
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
    private func handleListVersions(req: Request, bucketName: String) async throws -> Response {
        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let delimiter = req.query[String.self, at: "delimiter"]
        let keyMarker = req.query[String.self, at: "key-marker"]
        let versionIdMarker = req.query[String.self, at: "version-id-marker"]
        let maxKeys = req.query[Int.self, at: "max-keys"] ?? 1000

        let (
            versions, deleteMarkers, commonPrefixes, isTruncated, nextKeyMarker, nextVersionIdMarker
        ) =
            try await ClusterListingService.listAllVersions(
                req: req,
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

    /// Read-only cluster routing check for handlers that only ever need the forward-or-not
    /// answer (no local write, so no `peers` to carry forward for replication) - HEAD, tagging
    /// GET, plain object GET, and (with `requirePrimary: true`) ListParts. Returns the forwarded
    /// response when this node isn't responsible for `key`; `nil` means serve locally.
    private func forwardIfNeeded(
        req: Request, bucketName: String, key: String, requirePrimary: Bool = false
    ) async throws
        -> Response?
    {
        let decision =
            requirePrimary
            ? await ObjectRoutingService.multipartRoutingDecision(
                req: req, bucketName: bucketName, key: key)
            : await ObjectRoutingService.routingDecision(req: req, bucketName: bucketName, key: key)
        guard case .forward(let candidates) = decision else {
            return nil
        }
        return try await ClusterForwardingClient.forward(req: req, candidates: candidates)
    }

    /// The on-disk path of *whatever* EC shard this node actually holds for (bucketName, key,
    /// effective version) - `nil` when EC isn't configured or this node holds no shard for it.
    /// Crucially index-agnostic: after any membership change a node can hold a shard whose index
    /// no longer equals its current HRW rank, so this discovers what's on disk
    /// (`locallyHeldShardIndices`) rather than computing an own-rank path that would miss a
    /// drifted shard and wrongly report the object as absent.
    private static func localHeldShardPath(
        req: Request, bucketName: String, key: String, versionId: String?
    ) -> String? {
        guard req.application.storage[ClusterErasureCodingConfigKey.self] != nil else { return nil }

        let effectiveVersionId: String?
        if let versionId {
            effectiveVersionId = versionId
        } else if ObjectFileHandler.isVersioned(bucketName: bucketName, key: key) {
            effectiveVersionId = try? ObjectFileHandler.getLatestVersionId(
                bucketName: bucketName, key: key)
        } else {
            effectiveVersionId = nil
        }

        let held = ErasureCodedObjectHandler.locallyHeldShardIndices(
            bucketName: bucketName, key: key, versionId: effectiveVersionId)
        guard let index = held.first else { return nil }

        return ErasureCodedObjectHandler.shardPath(
            bucketName: bucketName, key: key, versionId: effectiveVersionId, shardIndex: index)
    }

    /// Whether this node holds a local EC shard for (bucketName, key, versionId) right now -
    /// used by read-side handlers to decide whether the legacy top-3 forward check even applies
    /// (a real shard holder outside the legacy top-3 - always possible once k+m > 3 - must never
    /// be redirected away from data it actually has).
    static func hasLocalECShard(
        req: Request, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo]
    ) async -> Bool {
        localHeldShardPath(req: req, bucketName: bucketName, key: key, versionId: versionId) != nil
    }

    /// Whether any responsible peer holds a shard for (bucketName, key, effectiveVersionId) -
    /// cluster-wide EC detection for the narrow window where this coordinating node is responsible
    /// but doesn't itself hold a shard yet (mid-reindex, or it just gained a rank). Without this,
    /// such a GET would fall through to the plain path and wrongly 404 an object that plainly
    /// exists on peers. Probed in parallel; only invoked on a local miss, so a plain object (which
    /// resolves locally or forwards via the top-3 path) never pays for it.
    static func anyPeerHoldsShard(
        req: Request, responsible: [ClusterNodeInfo], selfNodeId: UUID,
        bucketName: String, key: String, versionId: String?
    ) async -> Bool {
        await withTaskGroup(of: Bool.self) { group in
            for node in responsible where node.id != selfNodeId {
                group.addTask {
                    let held = await ClusterReplicationClient.heldShards(
                        app: req.application, node: node, bucketName: bucketName, key: key,
                        versionId: versionId)
                    return !(held ?? []).isEmpty
                }
            }
            for await holds in group where holds {
                group.cancelAll()
                return true
            }
            return false
        }
    }

    /// Resolves an object's `ObjectMeta` for HEAD-shaped handlers, EC-aware: reads whatever EC
    /// shard this node holds (header only, no stripe data touched), falling through to the plain
    /// `.obj` path unchanged when the target isn't erasure-coded here. Throws `NoSuchKey` when
    /// found by neither format. `static`, not instance-private - reused by
    /// `InternalBucketController`/`SharedLinkController` (`S3Controller` has no stored state).
    static func resolveObjectMetaEitherFormat(
        req: Request, bucketName: String, key: String, versionId: String?,
        responsible: [ClusterNodeInfo]
    ) async throws -> ObjectMeta {
        if let localShardPath = localHeldShardPath(
            req: req, bucketName: bucketName, key: key, versionId: versionId)
        {
            return try await S3Service.offloadBlockingIO(req) {
                try ErasureCodedShardReader(path: localShardPath).header.objectMeta
            }
        }

        do {
            let (meta, _) = try ObjectFileHandler.readVersion(
                bucketName: bucketName, key: key, versionId: versionId, loadData: false)
            return meta
        } catch {
            let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
            guard ObjectFileHandler.keyExists(for: bucketName, key: key, path: path) else {
                // Nothing local in either format - before concluding 404, ask responsible peers
                // for a shard header (cheap, header-only). Covers the fresh-write straggler
                // window: this node is responsible but hasn't received its shard yet, while peers
                // already hold the object. Parallel, first hit wins.
                if let clusterConfig = req.application.storage[ClusterConfigurationKey.self],
                    req.application.storage[ClusterErasureCodingConfigKey.self] != nil,
                    let peerMeta = await withTaskGroup(
                        of: ObjectMeta?.self, returning: ObjectMeta?.self, body: { group in
                        for node in responsible where node.id != clusterConfig.nodeId {
                            group.addTask {
                                await ClusterReplicationClient.fetchShardMeta(
                                    app: req.application, node: node, bucketName: bucketName,
                                    key: key, versionId: versionId)
                            }
                        }
                        for await meta in group where meta != nil {
                            group.cancelAll()
                            return meta
                        }
                        return nil
                    })
                {
                    return peerMeta
                }
                throw S3Error(
                    status: .notFound, code: "NoSuchKey",
                    message: "The specified key does not exist.", requestId: req.id)
            }
            let (meta, _) = try ObjectFileHandler.read(from: path, loadData: false)
            return meta
        }
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

        // EC-aware locality - see handleObjectGet's identical reasoning; replaces
        // forwardIfNeeded's plain top-3 check entirely for this handler.
        let (isLocal, candidates, responsible) =
            await ObjectRoutingService.erasureCodedReadPlacement(
                req: req, bucketName: bucketName, key: keyPath)
        if !isLocal {
            return try await ClusterForwardingClient.forward(req: req, candidates: candidates)
        }

        // isLocal above only proves membership in the *wider* top-(k+m) set - when k+m > 3 a
        // node can be in that set without holding a legacy plain replica, so a plain (non-EC)
        // object with no local EC shard still needs the legacy top-3 forward. Only forward when
        // there's genuinely no local EC shard to serve.
        if !(await Self.hasLocalECShard(
            req: req, bucketName: bucketName, key: keyPath, versionId: versionId,
            responsible: responsible)),
            let forwarded = try await forwardIfNeeded(req: req, bucketName: bucketName, key: keyPath)
        {
            return forwarded
        }

        let meta = try await Self.resolveObjectMetaEitherFormat(
            req: req, bucketName: bucketName, key: keyPath, versionId: versionId,
            responsible: responsible)

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
        let userId = key.userId

        let buckets = try await Bucket.all(app: req.application).filter { $0.userId == userId }

        let xmlData: Data = try ListAllMyBucketsResultDTO.s3XMLContainer(buckets)
        return S3Service.buildXMLResponse(data: xmlData)
    }

    // PUT /:bucketName
    @Sendable
    func handleBucketPut(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let query = req.url.query ?? ""
        let queryNames = S3Service.queryParameterNames(from: query)

        // Handle PUT ?versioning
        if queryNames.contains("versioning") {
            return try await handleVersioningPut(req: req, bucketName: bucketName)
        }

        // Handle PUT ?policy
        if queryNames.contains("policy") {
            return try await handleBucketPolicyPut(req: req, bucketName: bucketName)
        }

        // Handle PUT ?publicAccessBlock
        if queryNames.contains("publicaccessblock") {
            return try await handlePublicAccessBlockPut(req: req, bucketName: bucketName)
        }

        // Handle PUT ?tagging
        if queryNames.contains("tagging") {
            return try await handleBucketTaggingPut(req: req, bucketName: bucketName)
        }

        // Handle PUT ?lifecycle
        if queryNames.contains("lifecycle") {
            return try await handleLifecyclePut(req: req, bucketName: bucketName)
        }

        // PUT ?notification - Alarik's webhook rules target http(s) URLs, not the SNS/SQS/Lambda
        // ARNs the S3 XML format carries, so there's nothing meaningful an S3 client could PUT
        // here. Managed via the console / internal API instead (GET ?notification still works).
        if queryNames.contains("notification") {
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
        if queryNames.contains("replication") {
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

        if let existing = try await Bucket.find(app: req.application, name: bucketName) {
            if existing.userId == key.userId {
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

        try await BucketService.create(
            app: req.application, bucketName: bucketName, userId: key.userId)

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
        try await bucket.save(app: req.application)

        await BucketVersioningCache.shared.setStatus(for: bucketName, status: newStatus)
        CacheInvalidationService.notify(app: req.application, cache: "bucketVersioning", op: .upsert, key: bucketName)

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
        try await bucket.save(app: req.application)

        await BucketPolicyCache.shared.setPolicy(for: bucketName, policy: policy)
        CacheInvalidationService.notify(app: req.application, cache: "bucketPolicy", op: .upsert, key: bucketName)

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
        try await bucket.save(app: req.application)

        await BucketPolicyCache.shared.setPublicAccessBlock(
            for: bucketName, configuration: configuration)
        CacheInvalidationService.notify(
            app: req.application, cache: "bucketPublicAccessBlock", op: .upsert, key: bucketName)

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
        try await bucket.save(app: req.application)

        await BucketPolicyCache.shared.removePublicAccessBlock(for: bucketName)
        CacheInvalidationService.notify(
            app: req.application, cache: "bucketPublicAccessBlock", op: .remove, key: bucketName)

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
        try await bucket.save(app: req.application)

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
        try await bucket.save(app: req.application)

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
        try await bucket.save(app: req.application)

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
        try await bucket.save(app: req.application)

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
        try await bucket.save(app: req.application)

        await ReplicationConfigCache.shared.removeBucket(bucketName)
        CacheInvalidationService.notify(app: req.application, cache: "replicationConfig", op: .remove, key: bucketName)

        return .noContent
    }

    // DELETE /:bucketName
    @Sendable
    func handleBucketDelete(req: Request) async throws -> HTTPStatus {
        let bucketName = try S3Service.extractBucketName(from: req)
        let query = req.url.query ?? ""
        let queryNames = S3Service.queryParameterNames(from: query)

        // Handle DELETE ?publicAccessBlock
        if queryNames.contains("publicaccessblock") {
            return try await handlePublicAccessBlockDelete(req: req, bucketName: bucketName)
        }

        // Handle DELETE ?tagging
        if queryNames.contains("tagging") {
            return try await handleBucketTaggingDelete(req: req, bucketName: bucketName)
        }

        // Handle DELETE ?lifecycle
        if queryNames.contains("lifecycle") {
            return try await handleLifecycleDelete(req: req, bucketName: bucketName)
        }

        // Handle DELETE ?policy
        if queryNames.contains("policy") {
            return try await handleBucketPolicyDelete(req: req, bucketName: bucketName)
        }

        // Handle DELETE ?replication
        if queryNames.contains("replication") {
            return try await handleReplicationDelete(req: req, bucketName: bucketName)
        }

        guard let bucket = try await Bucket.find(app: req.application, name: bucketName) else {
            throw S3Error(
                status: .notFound,
                code: "NoSuchBucket",
                message: "The specified bucket does not exist."
            )
        }

        let key = try await S3Service.parseAndAuthenticateWithDB(req: req)

        if bucket.userId != key.userId {
            throw S3Error(status: .forbidden, code: "AccessDenied", message: "Access Denied")
        }

        let notEmpty: Bool
        do {
            notEmpty = try await ClusterListingService.hasBucketObjects(
                req: req, bucketName: bucketName)
        } catch {
            throw S3Error(
                status: .serviceUnavailable,
                code: "ServiceUnavailable",
                message:
                    "Could not verify the bucket is empty across every cluster node; refusing to delete."
            )
        }
        if notEmpty {
            throw S3Error(
                status: .conflict,
                code: "BucketNotEmpty",
                message: "The bucket you tried to delete is not empty."
            )
        }

        // force: true - the cluster-wide emptiness check above already ran authoritatively, so
        // BucketHandler.delete's own redundant *local-only* re-check is skipped rather than left
        // in as a second, incomplete gate.
        try await BucketService.delete(
            req: req, bucketName: bucketName, userId: bucket.userId, force: true)

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
        try await bucket.save(app: req.application)

        await BucketPolicyCache.shared.removePolicy(for: bucketName)
        CacheInvalidationService.notify(app: req.application, cache: "bucketPolicy", op: .remove, key: bucketName)

        return .noContent
    }

    // PUT /:bucketName/*key
    @Sendable
    func handleObjectPut(req: Request) async throws -> Response {
        // Every early-throw path below (routing/admission-control rejection, auth failure, a
        // conditional-header precondition failure) can fire before the body is ever read - see
        // `StreamingBodySpooler.withGuaranteedBodyDrain`'s doc comment for why that would
        // otherwise crash the process.
        try await StreamingBodySpooler.withGuaranteedBodyDrain(req: req) {
        let bucketName = try S3Service.extractBucketName(from: req)
        let keyPath = S3Service.extractObjectKey(from: req)

        guard !keyPath.isEmpty else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Invalid argument", requestId: req.id)
        }

        let authInfo = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        // Computed once and reused for both the routing decision below and the dispatch check
        // further down, so the two can never disagree on whether `partNumber` actually parses -
        // a non-numeric `partNumber` must return InvalidArgument, never silently fall through to
        // a plain-PUT overwrite of the destination object.
        let partNumberStr = req.query[String.self, at: "partNumber"]
        let uploadIdParam = req.query[String.self, at: "uploadId"]
        let isUploadPart = partNumberStr != nil && uploadIdParam != nil
        let hasTaggingParam =
            S3Service.queryParameterNames(from: req.url.query ?? "").contains("tagging")

        // A plain, single-shot PUT or CopyObject destination (no upload-part/tagging) is
        // erasure-coded end to end - routed independently of the rest of this handler's dispatch
        // tree, straight to rank-0 (`erasureCodedRoutingDecision`'s own pinning), never through
        // the top-3 `routeForWrite` path the other branches below still use. UploadPart/tagging
        // PUT remain on plain 3x replication for now (multipart's own EC integration is
        // separate, larger work).
        let usesECDestinationRouting = !isUploadPart && !hasTaggingParam
        let ecConfig = req.application.storage[ClusterErasureCodingConfigKey.self]

        // UploadPart/UploadPartCopy must pin to the exact node coordinating this upload's
        // Create, not "any responsible node" - see multipartRoutingDecision's doc comment.
        // Cluster routing: one check covers this whole handler's dispatch tree (UploadPart,
        // UploadPartCopy, tagging PUT, CopyObject, plain PUT) - all key off this same
        // (bucketName, keyPath).
        let peers: [ClusterNodeInfo]
        var ecWriteFanOut: (peers: [ClusterNodeInfo], config: ClusterErasureCodingConfig)?
        if usesECDestinationRouting, let ecConfig {
            switch try await ObjectRoutingService.routeForErasureCodedWrite(
                req: req, bucketName: bucketName, key: keyPath)
            {
            case .notClustered:
                // Unreachable (ecConfig != nil implies cluster mode is on), but fall through to
                // the plain non-EC write below exactly like a genuinely non-clustered node.
                peers = []
            case .local(let localPeers):
                peers = []
                ecWriteFanOut = (localPeers, ecConfig)
            case .forwarded(let response):
                return response
            }
        } else {
            switch try await ObjectRoutingService.routeForWrite(
                req: req, bucketName: bucketName, key: keyPath, requirePrimary: isUploadPart)
            {
            case .local(let localPeers):
                peers = localPeers
            case .forwarded(let response):
                return response
            }
        }

        // Check if this is an UploadPart request (PUT with partNumber & uploadId)
        if isUploadPart {
            guard let partNumberStr, let partNumber = Int(partNumberStr) else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument",
                    message: "Part number must be a valid integer.", requestId: req.id)
            }
            let uploadId = uploadIdParam!

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
        if S3Service.queryParameterNames(from: req.url.query ?? "").contains("tagging") {
            return try await handleObjectTaggingPut(
                req: req, bucketName: bucketName, key: keyPath, peers: peers)
        }

        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        // Check if this is a copy operation
        if let copySource = try S3Service.parseCopySource(from: req) {
            return try await handleCopyObject(
                req: req,
                destinationBucket: bucketName,
                destinationKey: keyPath,
                copySource: copySource,
                versioningStatus: versioningStatus,
                peers: peers,
                ecWriteFanOut: ecWriteFanOut
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

        let writtenVersionId: String?
        if let ecWriteFanOut {
            // Erasure-coded write: mint the version id the same way the plain versioned-write
            // path does (tags/listing behave identically either way), but encode+place shards
            // instead of writing a single `.obj` file.
            let (versionId, versionedMeta, priorLatestVersionId) = try prepareErasureCodedVersionedWrite(
                metadata: meta, bucketName: bucketName, key: keyPath,
                versioningStatus: versioningStatus)

            let payloadSources: [(path: String, offset: Int, size: Int)]
            var ecTempSourcePath: String?
            switch spooled.storage {
            case .memory(let data):
                let tempPath = Constants.spoolDirectory + ".ec-source-" + UUID().uuidString
                try await S3Service.offloadBlockingIO(req) {
                    try FileManager.default.createDirectory(
                        atPath: Constants.spoolDirectory, withIntermediateDirectories: true)
                    try data.write(to: URL(fileURLWithPath: tempPath))
                }
                ecTempSourcePath = tempPath
                payloadSources = [(path: tempPath, offset: 0, size: data.count)]
            case .file(let spoolPath):
                payloadSources = [(path: spoolPath, offset: 0, size: spooled.size)]
            }
            defer {
                if let ecTempSourcePath { _ = POSIXFile.unlink(ecTempSourcePath) }
            }

            do {
                try await ErasureCodedWriteCoordinator.write(
                    app: req.application, bucketName: bucketName, key: keyPath,
                    objectMeta: versionedMeta, payloadSources: payloadSources,
                    peers: ecWriteFanOut.peers,
                    ecConfig: (ecWriteFanOut.config.dataShards, ecWriteFanOut.config.parityShards),
                    priorLatestVersionId: priorLatestVersionId)
            } catch let error as ErasureCodedCoordinatorError {
                throw S3Error(
                    status: .serviceUnavailable, code: "ServiceUnavailable",
                    message: "\(error)", requestId: req.id)
            }

            if let versionId {
                try await S3Service.offloadBlockingIO(req) {
                    try ObjectFileHandler.updateLatestPointer(
                        bucketName: bucketName, key: keyPath, versionId: versionId)
                }
            }
            writtenVersionId = versionId
        } else {
            // Write with versioning support. Small bodies arrive in memory and take the direct
            // write; large ones were spooled to disk and are copied into the final .obj in fixed
            // windows. Both are fsynced by AtomicObjectWriter before the PUT is acknowledged -
            // real blocking syscalls, so the whole write+fsync+rename sequence is offloaded to
            // the blocking-IO thread pool rather than tying up the async executor.
            let storage = spooled.storage
            let finalMeta = meta
            writtenVersionId = try await S3Service.offloadBlockingIO(req) {
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
        }
        if let writtenVersionId {
            headers.add(name: "x-amz-version-id", value: writtenVersionId)
        }

        await NotificationService.emit(
            event: .objectCreatedPut, bucketName: bucketName, key: keyPath,
            size: spooled.size, etag: etag, versionId: writtenVersionId,
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)
        await ReplicationService.enqueuePut(
            app: req.application, bucketName: bucketName, key: keyPath,
            versionId: writtenVersionId)
        if ecWriteFanOut == nil {
            await ClusterReplicationService.replicateWrite(
                app: req.application, bucketName: bucketName, key: keyPath,
                versionId: writtenVersionId, operation: .put, peers: peers)
        }

        return Response(status: .ok, headers: headers)
        }
    }

    /// Like `ObjectFileHandler.prepareVersionedWrite`, but correct for EC path selection: leaves
    /// `versionId` as true Swift `nil` for `.disabled` buckets instead of the `"null"`-string
    /// sentinel, since EC shard path selection keys off `versionId == nil`. `.suspended` is
    /// unaffected - it genuinely uses versioned storage under the literal name `"null"`.
    private func prepareErasureCodedVersionedWrite(
        metadata: ObjectMeta, bucketName: String, key: String, versioningStatus: VersioningStatus
    ) throws -> (versionId: String?, versionedMeta: ObjectMeta, priorLatestVersionId: String?) {
        guard versioningStatus != .disabled else {
            return (nil, metadata, nil)
        }
        // Capture the version `.latest` pointed at BEFORE the demote below, so a failed-quorum
        // rollback can restore it (see ErasureCodedWriteCoordinator.rollback).
        let priorLatestVersionId = try? ObjectFileHandler.getLatestVersionId(
            bucketName: bucketName, key: key)
        let (versionId, _, versionedMeta) = try ObjectFileHandler.prepareVersionedWrite(
            metadata: metadata, bucketName: bucketName, key: key, versioningStatus: versioningStatus)
        ErasureCodedObjectHandler.markAllLocalShardsNotLatest(bucketName: bucketName, key: key)
        return (versionId, versionedMeta, priorLatestVersionId)
    }

    /// Shared in-place metadata rewrite for tagging PUT/DELETE and the admin console's metadata
    /// editor (`InternalBucketController.setObjectMetadata`): EC-aware first (checked directly
    /// against local disk, like GET/DELETE's own EC detection), falling through to the plain
    /// `.obj` `ObjectFileHandler.rewriteMetadata` + best-effort peer push unchanged when the
    /// target isn't erasure-coded. `static`, not instance-private - `S3Controller` has no stored
    /// state, so this is safely callable from other controllers too.
    static func rewriteObjectMetadata(
        req: Request, bucketName: String, key: String, versionId: String?,
        peers: [ClusterNodeInfo], transform: @escaping @Sendable (inout ObjectMeta) -> Void
    ) async throws -> ObjectMeta {
        if let placement = await ErasureCodedDeleteCoordinator.ecPlacement(
            app: req.application, bucketName: bucketName, key: key),
            let selfRank = placement.responsible.firstIndex(where: { $0.id == placement.selfNodeId }),
            ErasureCodedDeleteCoordinator.localShardExists(
                bucketName: bucketName, key: key, versionId: versionId, selfRank: selfRank)
        {
            return try await ErasureCodedWriteCoordinator.rewriteMetadata(
                app: req.application, bucketName: bucketName, key: key, versionId: versionId,
                responsible: placement.responsible, selfNodeId: placement.selfNodeId,
                transform: transform)
        }

        guard
            let path = try ObjectFileHandler.resolvePath(
                bucketName: bucketName, key: key, versionId: versionId)
        else {
            throw S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: req.id)
        }

        // Metadata-only rewrite - the payload is window-copied from the file to itself, never
        // buffered, so this doesn't cost a full read+rewrite of a potentially huge object.
        let updatedMeta = try await S3Service.offloadBlockingIO(req) {
            try ObjectFileHandler.rewriteMetadata(at: path, transform: transform)
        }

        // Cluster peers physically hold the exact same version file, so a change must be pushed
        // to them too - an in-place metadata edit has no outbox task backing it the way an
        // object write/delete does, so without this a peer would silently serve stale metadata
        // forever rather than just temporarily lagging behind.
        await ClusterReplicationService.replicateWrite(
            app: req.application, bucketName: bucketName, key: key,
            versionId: updatedMeta.versionId, operation: .put, peers: peers)

        return updatedMeta
    }

    /// Handles PUT /:bucket/:key?tagging - sets the tag-set of a specific object version (or
    /// the current one if no `versionId` is given). Modifies the existing version's metadata in
    /// place - does not create a new version. S3 returns 200 with x-amz-version-id
    /// (verified against the PutObjectTagging API reference). Auth is already done by the
    /// caller (`handleObjectPut`) before dispatching here.
    @Sendable
    private func handleObjectTaggingPut(
        req: Request, bucketName: String, key: String, peers: [ClusterNodeInfo]
    )
        async throws -> Response
    {
        let versionId = req.query[String.self, at: "versionId"]
        let xml = try await S3Service.collectBodyString(req: req)
        let tagging = try Tagging.parse(xml: xml, requestId: req.id)

        let updatedMeta = try await Self.rewriteObjectMetadata(
            req: req, bucketName: bucketName, key: key, versionId: versionId, peers: peers
        ) { $0.tags = tagging.tags }

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

        let (isLocal, candidates, responsible) =
            await ObjectRoutingService.erasureCodedReadPlacement(
                req: req, bucketName: bucketName, key: key)
        if !isLocal {
            return try await ClusterForwardingClient.forward(req: req, candidates: candidates)
        }

        let versionId = req.query[String.self, at: "versionId"]
        // See handleObjectHead's identical fallthrough: isLocal only proves membership in the
        // wider top-(k+m) set, not the legacy top-3 a plain (non-EC) object actually replicates
        // to.
        if !(await Self.hasLocalECShard(
            req: req, bucketName: bucketName, key: key, versionId: versionId,
            responsible: responsible)),
            let forwarded = try await forwardIfNeeded(req: req, bucketName: bucketName, key: key)
        {
            return forwarded
        }
        let meta = try await Self.resolveObjectMetaEitherFormat(
            req: req, bucketName: bucketName, key: key, versionId: versionId,
            responsible: responsible)
        // A key whose latest version is a delete marker has no current tag-set - 404, matching
        // GetObject/HeadObject. `resolveObjectMetaEitherFormat` returns the marker's own meta
        // (unlike the plain `resolvePath`, which nils delete markers), so the guard lives here.
        if meta.isDeleteMarker && versionId == nil {
            throw S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: req.id)
        }
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
    private func handleObjectTaggingDelete(
        req: Request, bucketName: String, key: String, peers: [ClusterNodeInfo]
    )
        async throws -> Response
    {
        let versionId = req.query[String.self, at: "versionId"]

        let updatedMeta = try await Self.rewriteObjectMetadata(
            req: req, bucketName: bucketName, key: key, versionId: versionId, peers: peers
        ) { $0.tags = nil }

        var headers = HTTPHeaders()
        if let versionId = updatedMeta.versionId {
            headers.add(name: "x-amz-version-id", value: versionId)
        }
        return Response(status: .noContent, headers: headers)
    }

    // Helper method to handle copy operations
    @Sendable
    /// Resolves a CopyObject/UploadPartCopy source - locally if this node holds it, otherwise
    /// fetches it from a responsible peer into a local temp file first. Source and destination
    /// of a copy can be arbitrary, unrelated buckets/keys, so the source may live elsewhere even
    /// though this node is correctly responsible for the destination. Callers must call `cleanup`
    /// once done reading (a no-op when the source was local).
    private func resolveCopySource(
        req: Request, bucketName: String, key: String, versionId: String?
    ) async throws -> (
        meta: ObjectMeta, path: String, payloadOffset: Int, payloadSize: Int, cleanup: () -> Void
    ) {
        let (isLocal, candidates, responsible) =
            await ObjectRoutingService.erasureCodedReadPlacement(
                req: req, bucketName: bucketName, key: key)

        // EC-aware: a copy source can be any unrelated bucket/key, so it may be erasure-coded
        // even when the destination isn't (or vice versa) - checked directly against disk
        // (locally, or via one network probe to rank-0), never assumed from the destination's
        // own routing.
        if let clusterConfig = req.application.storage[ClusterConfigurationKey.self],
            req.application.storage[ClusterErasureCodingConfigKey.self] != nil,
            !responsible.isEmpty
        {
            let effectiveVersionId: String?
            if let versionId {
                effectiveVersionId = versionId
            } else if isLocal, ObjectFileHandler.isVersioned(bucketName: bucketName, key: key) {
                effectiveVersionId = try ObjectFileHandler.getLatestVersionId(
                    bucketName: bucketName, key: key)
            } else if !isLocal {
                effectiveVersionId = await ClusterReplicationClient.resolveLatestVersionId(
                    app: req.application, candidates: responsible, bucketName: bucketName, key: key)
            } else {
                effectiveVersionId = nil
            }

            if await isSourceErasureCoded(
                req: req, responsible: responsible, bucketName: bucketName, key: key,
                versionId: effectiveVersionId)
            {
                let (meta, stream) = try await ErasureCodedReadCoordinator.read(
                    app: req.application, bucketName: bucketName, key: key,
                    versionId: effectiveVersionId, responsible: responsible,
                    selfNodeId: clusterConfig.nodeId, requestId: req.id)
                let tempPath = try await Self.drainToTempFile(stream: stream, app: req.application)
                return (meta, tempPath, 0, meta.size, { _ = POSIXFile.unlink(tempPath) })
            }
        }

        // isLocal only proves membership in the wider top-(k+m) set - the EC check above already
        // confirmed this source isn't erasure-coded, so a genuine local read is only valid when
        // this node also holds the legacy top-3 plain replica. Once k+m > 3 a node can be local
        // under the wide check without ever having received this plain object at all.
        if let clusterConfig = req.application.storage[ClusterConfigurationKey.self],
            isLocal,
            !ObjectRoutingService.isLegacyReplica(
                responsible: responsible, selfNodeId: clusterConfig.nodeId)
        {
            let (tempPath, meta) = try await ClusterReplicationClient.fetchObjectToTempFile(
                app: req.application, candidates: Array(responsible.prefix(PlacementService.replicationFactor)),
                bucketName: bucketName, key: key, versionId: versionId, requestId: req.id)
            return (meta, tempPath, 0, meta.size, { _ = POSIXFile.unlink(tempPath) })
        }

        if isLocal {
            let (meta, path, offset, size) = try await S3Service.offloadBlockingIO(req) {
                try S3Service.resolveObjectForCopy(
                    bucketName: bucketName, key: key, versionId: versionId, requestId: req.id)
            }
            return (meta, path, offset, size, {})
        }
        let (tempPath, meta) = try await ClusterReplicationClient.fetchObjectToTempFile(
            app: req.application, candidates: candidates, bucketName: bucketName, key: key,
            versionId: versionId, requestId: req.id)
        return (meta, tempPath, 0, meta.size, { _ = POSIXFile.unlink(tempPath) })
    }

    /// Single rank-0 probe, matching the plan's "rank-0 always self-describes" design: rank-0
    /// physically holds either the plain `.obj` or shard index 0 for every version, so asking it
    /// alone (locally if it's us, one network call otherwise) is enough to know the format -
    /// no need to probe all `k+m` candidates just to answer "is this EC or not".
    private func isSourceErasureCoded(
        req: Request, responsible: [ClusterNodeInfo], bucketName: String, key: String,
        versionId: String?
    ) async -> Bool {
        guard let rank0 = responsible.first else { return false }
        if let config = req.application.storage[ClusterConfigurationKey.self],
            rank0.id == config.nodeId
        {
            let path =
                versionId != nil
                ? ErasureCodedObjectHandler.versionedShardPath(
                    bucketName: bucketName, key: key, versionId: versionId!, shardIndex: 0)
                : ErasureCodedObjectHandler.shardPath(bucketName: bucketName, key: key, shardIndex: 0)
            return FileManager.default.fileExists(atPath: path)
        }
        return await ClusterReplicationClient.shardExists(
            app: req.application, node: rank0, bucketName: bucketName, key: key,
            versionId: versionId, shardIndex: 0)
    }

    /// Drains an `ErasureCodedReadCoordinator.read` stream into a local temp file - CopyObject's
    /// downstream logic (ETag computation, metadata merge, writing the destination) already
    /// expects a plain `(path, offset, size)` source, the same shape every other copy-source
    /// resolution in this handler produces. Caller must unlink the returned path once done.
    /// `static`, not instance-private - reused by `InternalBucketController`'s console download.
    static func drainToTempFile(
        stream: AsyncThrowingStream<ByteBuffer, any Error>, app: Application
    ) async throws -> String {
        let threadPool = app.threadPool
        let tempPath = Constants.spoolDirectory + ".ec-copy-source-" + UUID().uuidString
        let fd = try await threadPool.runIfActive { () -> Int32 in
            var fd = POSIXFile.openWrite(tempPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            if fd < 0 && errno == ENOENT {
                try FileManager.default.createDirectory(
                    atPath: Constants.spoolDirectory, withIntermediateDirectories: true)
                fd = POSIXFile.openWrite(tempPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
            }
            guard fd >= 0 else {
                throw S3Error(
                    status: .internalServerError, code: "InternalError",
                    message: "Failed to open EC copy-source temp file")
            }
            return fd
        }
        do {
            for try await buffer in stream {
                let chunk = buffer
                try await threadPool.runIfActive {
                    try chunk.withUnsafeReadableBytes { raw in
                        try StreamingIOLoops.writeFully(fd: fd, raw)
                    }
                }
            }
            _ = try await threadPool.runIfActive { POSIXFile.close(fd) }
        } catch {
            _ = try? await threadPool.runIfActive { POSIXFile.close(fd) }
            _ = POSIXFile.unlink(tempPath)
            throw error
        }
        return tempPath
    }

    private func handleCopyObject(
        req: Request,
        destinationBucket: String,
        destinationKey: String,
        copySource: CopySource,
        versioningStatus: VersioningStatus,
        peers: [ClusterNodeInfo] = [],
        ecWriteFanOut: (peers: [ClusterNodeInfo], config: ClusterErasureCodingConfig)? = nil
    ) async throws -> Response {
        try await S3Service.verifyBucketExists(copySource.bucketName, requestId: req.id)

        // Authenticate access to source bucket
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: copySource.bucketName)

        // Resolve the source straight to its on-disk file (fetching it from a peer first if
        // this node doesn't hold it) - the payload is streamed file-to-file in fixed windows,
        // never buffered whole.
        let (sourceMeta, sourcePath, sourcePayloadOffset, sourcePayloadSize, cleanupSource) =
            try await resolveCopySource(
                req: req, bucketName: copySource.bucketName, key: copySource.key,
                versionId: copySource.versionId)
        defer { cleanupSource() }

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

        let versionId: String?
        if let ecWriteFanOut {
            let (mintedVersionId, versionedMeta, priorLatestVersionId) = try prepareErasureCodedVersionedWrite(
                metadata: destinationMeta, bucketName: destinationBucket, key: destinationKey,
                versioningStatus: versioningStatus)

            do {
                try await ErasureCodedWriteCoordinator.write(
                    app: req.application, bucketName: destinationBucket, key: destinationKey,
                    objectMeta: versionedMeta, payloadSources: copySources,
                    peers: ecWriteFanOut.peers,
                    ecConfig: (ecWriteFanOut.config.dataShards, ecWriteFanOut.config.parityShards),
                    priorLatestVersionId: priorLatestVersionId)
            } catch let error as ErasureCodedCoordinatorError {
                throw S3Error(
                    status: .serviceUnavailable, code: "ServiceUnavailable",
                    message: "\(error)", requestId: req.id)
            }

            if let mintedVersionId {
                try await S3Service.offloadBlockingIO(req) {
                    try ObjectFileHandler.updateLatestPointer(
                        bucketName: destinationBucket, key: destinationKey,
                        versionId: mintedVersionId)
                }
            }
            versionId = mintedVersionId
        } else {
            versionId = try await S3Service.offloadBlockingIO(req) {
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
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)
        await ReplicationService.enqueuePut(
            app: req.application, bucketName: destinationBucket, key: destinationKey,
            versionId: versionId)
        if ecWriteFanOut == nil {
            await ClusterReplicationService.replicateWrite(
                app: req.application, bucketName: destinationBucket, key: destinationKey,
                versionId: versionId, operation: .put, peers: peers)
        }

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
            if let forwarded = try await forwardIfNeeded(
                req: req, bucketName: bucketName, key: keyPath, requirePrimary: true)
            {
                return forwarded
            }
            return try handleListParts(
                req: req,
                bucketName: bucketName,
                key: keyPath,
                uploadId: uploadId
            )
        }

        // GetObjectTagging - a different permission than GetObject in S3, not covered by
        // the bucket-policy public-access whitelist, so it always requires strict auth. Exact
        // parameter-name match: a plain GetObject with e.g.
        // ?response-content-disposition=...tagging-report.csv must never be misrouted here just
        // because that unrelated parameter's *value* contains the word "tagging".
        if S3Service.queryParameterNames(from: req.url.query ?? "").contains("tagging") {
            return try await handleObjectTaggingGet(req: req, bucketName: bucketName, key: keyPath)
        }

        // Check for versionId query parameter
        let versionId = req.query[String.self, at: "versionId"]

        _ = try await S3Service.authenticateOrAuthorizePublic(
            req: req, bucketName: bucketName,
            action: versionId != nil ? .getObjectVersion : .getObject, key: keyPath)

        // EC-aware locality: any of the wider top-(k+m) responsible nodes can serve a read
        // (no pinning needed, unlike writes) - a strict superset of the plain top-3 check
        // `forwardIfNeeded` does, so this replaces it entirely for this handler rather than
        // running both.
        let (isLocal, candidates, responsible) =
            await ObjectRoutingService.erasureCodedReadPlacement(
                req: req, bucketName: bucketName, key: keyPath)
        if !isLocal {
            return try await ClusterForwardingClient.forward(req: req, candidates: candidates)
        }

        // Resolve the effective version id the same way every version-aware lookup in this
        // codebase does - works identically for EC and plain versions, since both share the
        // same `.latest` pointer / `.versions` directory convention; only the payload format
        // (shards vs a single file) differs.
        let effectiveVersionId: String?
        if let versionId {
            effectiveVersionId = versionId
        } else if ObjectFileHandler.isVersioned(bucketName: bucketName, key: keyPath) {
            effectiveVersionId = try ObjectFileHandler.getLatestVersionId(
                bucketName: bucketName, key: keyPath)
        } else {
            effectiveVersionId = nil
        }

        // Is this an erasure-coded object? Discover it location-independently: this node holds a
        // shard (fast path, no network), or - only when there's no local plain object either, so
        // plain reads never pay for it - a responsible peer holds one (the mid-reindex window
        // where this coordinating node hasn't received its own shard yet). Either way the read is
        // a gather-and-decode across the responsible set; falling through to the plain path is
        // reserved for genuinely plain-replicated objects and true 404s.
        if let clusterConfig = req.application.storage[ClusterConfigurationKey.self],
            req.application.storage[ClusterErasureCodingConfigKey.self] != nil,
            responsible.contains(where: { $0.id == clusterConfig.nodeId })
        {
            let localHeld = ErasureCodedObjectHandler.locallyHeldShardIndices(
                bucketName: bucketName, key: keyPath, versionId: effectiveVersionId)
            var isErasureCoded = !localHeld.isEmpty
            if !isErasureCoded {
                let plainResolvesLocally =
                    ((try? ObjectFileHandler.resolvePath(
                        bucketName: bucketName, key: keyPath, versionId: versionId)) ?? nil) != nil
                if !plainResolvesLocally {
                    isErasureCoded = await Self.anyPeerHoldsShard(
                        req: req, responsible: responsible, selfNodeId: clusterConfig.nodeId,
                        bucketName: bucketName, key: keyPath, versionId: effectiveVersionId)
                }
            }

            if isErasureCoded {
                // Gather + resolve metadata first (no body streamed yet), so the delete-marker
                // 404, conditional-header checks, and Range parsing/validation all happen before
                // committing to decode - and a ranged request only reconstructs the stripes it
                // actually needs.
                let prepared = try await ErasureCodedReadCoordinator.prepare(
                    app: req.application, bucketName: bucketName, key: keyPath,
                    versionId: effectiveVersionId, responsible: responsible,
                    selfNodeId: clusterConfig.nodeId, requestId: req.id)
                let meta = prepared.meta

                if meta.isDeleteMarker && versionId == nil {
                    prepared.discard()
                    throw S3Error(
                        status: .notFound, code: "NoSuchKey",
                        message: "The specified key does not exist.", requestId: req.id)
                }
                do {
                    try S3Service.validateConditionalHeaders(req: req, meta: meta)
                } catch {
                    prepared.discard()
                    throw error
                }

                let byteRange: ByteRange?
                do {
                    byteRange =
                        req.headers.first(name: .range) != nil
                        ? try S3RangeParser.parseRange(from: req, fileSize: meta.size) : nil
                } catch {
                    prepared.discard()
                    throw error
                }

                let body = ErasureCodedReadCoordinator.streamBody(
                    app: req.application, prepared: prepared,
                    range: byteRange.map { ($0.start, $0.end) })

                let response = S3Service.buildObjectMetadataResponse(
                    meta: meta, includeBody: false, data: nil, range: nil)
                let bodyLength = byteRange?.length ?? meta.size
                response.status = byteRange != nil ? .partialContent : .ok
                response.headers.replaceOrAdd(name: .contentLength, value: String(bodyLength))
                if let byteRange {
                    response.headers.replaceOrAdd(
                        name: "Content-Range", value: byteRange.contentRange(fileSize: meta.size))
                }
                S3Service.addVersionHeaders(to: response, meta: meta)
                if let tagCount = meta.tags?.count, tagCount > 0 {
                    response.headers.add(name: "x-amz-tagging-count", value: String(tagCount))
                }
                response.body = Response.Body(
                    managedAsyncStream: { writer in
                        for try await chunk in body {
                            try await writer.writeBuffer(chunk)
                        }
                    }, count: bodyLength)
                return response
            }
        }

        // No local EC shard - the target is either plain-format or doesn't exist. Top-3 being a
        // *prefix* of the wider top-(k+m) list `isLocal` above passed doesn't mean this node is
        // also IN that top-3: a plain object replicated only to ranks 0-2 must still forward when
        // this node is rank 3+. Reusing the legacy top-3 check here restores that forwarding.
        if let forwarded = try await forwardIfNeeded(req: req, bucketName: bucketName, key: keyPath)
        {
            return forwarded
        }

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

        // AbortMultipartUpload must pin to the exact node coordinating this upload's Create,
        // not "any responsible node" - see multipartRoutingDecision's doc comment.
        let isAbortMultipart =
            req.query[String.self, at: "uploadId"] != nil
            && req.query[String.self, at: "versionId"] == nil
        let isTaggingDelete = S3Service.queryParameterNames(from: req.url.query ?? "").contains("tagging")

        // Only a DELETE that will actually *create* a fresh EC delete marker needs the
        // rank-0-pinned, admission-gated EC write routing, since marker creation places a new
        // shard. Every other DELETE - a specific-version removal, a non-versioned/suspended
        // removal, or a plain legacy object - places no new shards and must NOT be blocked by EC
        // admission when the cluster is temporarily below k+m; those route via plain `routeForWrite`.
        let deleteVersionId = req.query[String.self, at: "versionId"]
        let deleteVersioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)
        let createsErasureCodedMarker =
            deleteVersionId == nil && deleteVersioningStatus == .enabled
        let usesECDestinationRouting =
            !isAbortMultipart && !isTaggingDelete && createsErasureCodedMarker
        let ecConfig = req.application.storage[ClusterErasureCodingConfigKey.self]

        // Cluster routing: one check covers this handler's dispatch tree (AbortMultipartUpload,
        // tagging DELETE, plain object DELETE) - all key off this same (bucketName, keyPath).
        let peers: [ClusterNodeInfo]
        if usesECDestinationRouting, ecConfig != nil {
            switch try await ObjectRoutingService.routeForErasureCodedWrite(
                req: req, bucketName: bucketName, key: keyPath)
            {
            case .notClustered:
                peers = []
            case .local(let localPeers):
                peers = localPeers
            case .forwarded(let response):
                return response
            }
        } else {
            switch try await ObjectRoutingService.routeForWrite(
                req: req, bucketName: bucketName, key: keyPath, requirePrimary: isAbortMultipart)
            {
            case .local(let localPeers):
                peers = localPeers
            case .forwarded(let response):
                return response
            }
        }

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
        if isTaggingDelete {
            return try await handleObjectTaggingDelete(
                req: req, bucketName: bucketName, key: keyPath, peers: peers)
        }

        let versionId = deleteVersionId
        let versioningStatus = deleteVersioningStatus

        // Unlike external replication (below), cluster peers physically hold the exact same
        // version files as this node, so an explicit historical-version delete DOES need to
        // propagate - always fan out, not just for current-object deletes.
        let outcome = try await ClusterReplicationService.coordinateDelete(
            app: req.application, bucketName: bucketName, key: keyPath, versionId: versionId,
            versioningStatus: versioningStatus, peers: peers)

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
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)
        // A client-specified versionId permanently prunes one historical version - there's no
        // matching version on the replication target to remove (see ReplicationClient.replicateDelete),
        // so only replicate when this deleted the *current* object.
        if versionId == nil {
            await ReplicationService.enqueueDelete(
                app: req.application, bucketName: bucketName, key: keyPath,
                versionId: outcome.versionId)
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

        if S3Service.queryParameterNames(from: query).contains("delete") {
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
                let outcome = try await ClusterReplicationService.deleteObjectClusterWide(
                    req: req, bucketName: bucketName, key: object.key,
                    versionId: object.versionId, versioningStatus: versioningStatus)

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
                    requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)
                // See the single-object DELETE handler: a client-specified versionId prunes one
                // historical version, which has no replicable equivalent on the target.
                if object.versionId == nil {
                    await ReplicationService.enqueueDelete(
                        app: req.application, bucketName: bucketName, key: object.key,
                        versionId: outcome.versionId)
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

        // Cluster routing: this whole handler is multipart create/complete, so every request
        // through it pins to the primary responsible node specifically (multipartRoutingDecision,
        // not routingDecision) - forwarding a create/uploadPart/complete/abort sequence to
        // different-but-each-individually-"responsible" nodes would split a single upload's
        // parts across nodes with no way to reassemble them, so the whole lifecycle must land on
        // the identical node throughout.
        let peers: [ClusterNodeInfo]
        switch try await ObjectRoutingService.routeForWrite(
            req: req, bucketName: bucketName, key: keyPath, requirePrimary: true)
        {
        case .local(let localPeers):
            peers = localPeers
        case .forwarded(let response):
            return response
        }

        // POST ?uploads → CreateMultipartUpload
        if S3Service.queryParameterNames(from: query).contains("uploads") {
            return try handleCreateMultipartUpload(req: req, bucketName: bucketName, key: keyPath)
        }

        // POST ?uploadId=X → CompleteMultipartUpload
        if let uploadId = req.query[String.self, at: "uploadId"] {
            return try await handleCompleteMultipartUpload(
                req: req, bucketName: bucketName, key: keyPath, uploadId: uploadId, peers: peers)
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
                <Bucket>\(bucketName.xmlEscaped)</Bucket>
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
        uploadId: String,
        peers: [ClusterNodeInfo] = []
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

        // Complete the upload - streams every part into the final object and fsyncs before
        // returning, real blocking file IO, so it's offloaded to the blocking-IO thread pool
        // rather than tying up the async executor for a potentially multi-gigabyte assembly.
        // Every request in this upload's lifecycle already landed on the identical (rank-0) node,
        // so EC eligibility can be checked fresh here without any extra forwarding.
        var usedEC = false
        let etag: String
        let versionId: String?
        let finalSize: Int
        do {
            if let ecConfig = req.application.storage[ClusterErasureCodingConfigKey.self],
                case .local(let ecPeers) = try await ObjectRoutingService.erasureCodedRoutingDecision(
                    req: req, bucketName: bucketName, key: key)
            {
                let plan = try await S3Service.offloadBlockingIO(req) {
                    try MultipartFileHandler.prepareCompletion(
                        bucketName: bucketName, uploadId: uploadId, parts: parts)
                }
                let (mintedVersionId, versionedMeta, priorLatestVersionId) = try prepareErasureCodedVersionedWrite(
                    metadata: plan.objectMeta, bucketName: bucketName, key: key,
                    versioningStatus: versioningStatus)
                do {
                    try await ErasureCodedWriteCoordinator.write(
                        app: req.application, bucketName: bucketName, key: key,
                        objectMeta: versionedMeta, payloadSources: plan.payloadSources,
                        peers: ecPeers, ecConfig: (ecConfig.dataShards, ecConfig.parityShards),
                        priorLatestVersionId: priorLatestVersionId)
                } catch let error as ErasureCodedCoordinatorError {
                    throw S3Error(
                        status: .serviceUnavailable, code: "ServiceUnavailable",
                        message: "\(error)", requestId: req.id)
                }
                if let mintedVersionId {
                    try await S3Service.offloadBlockingIO(req) {
                        try ObjectFileHandler.updateLatestPointer(
                            bucketName: bucketName, key: key, versionId: mintedVersionId)
                    }
                }
                versionId = mintedVersionId
                try await S3Service.offloadBlockingIO(req) {
                    try MultipartFileHandler.abortUpload(bucketName: bucketName, uploadId: uploadId)
                }
                etag = plan.etag
                finalSize = plan.totalSize
                usedEC = true
            } else {
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
            }
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
            requestId: req.id, sourceIP: req.remoteAddress?.ipAddress, app: req.application)
        await ReplicationService.enqueuePut(
            app: req.application, bucketName: bucketName, key: key, versionId: versionId)
        if !usedEC {
            await ClusterReplicationService.replicateWrite(
                app: req.application, bucketName: bucketName, key: key, versionId: versionId,
                operation: .put, peers: peers)
        }

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
                <Location>\(location.xmlEscaped)</Location>
                <Bucket>\(bucketName.xmlEscaped)</Bucket>
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

        // Resolve the source straight to its on-disk file (fetching it from a peer first if
        // this node doesn't hold it) - the copied region is streamed file-to-file in fixed
        // windows, never buffered whole.
        let (sourceMeta, sourcePath, sourcePayloadOffset, sourcePayloadSize, cleanupSource) =
            try await resolveCopySource(
                req: req, bucketName: copySource.bucketName, key: copySource.key,
                versionId: copySource.versionId)
        defer { cleanupSource() }

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
                <Bucket>\(bucketName.xmlEscaped)</Bucket>
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
    private func handleListMultipartUploads(req: Request, bucketName: String) async throws
        -> Response
    {
        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let keyMarker = req.query[String.self, at: "key-marker"]
        let uploadIdMarker = req.query[String.self, at: "upload-id-marker"]
        let maxUploads = req.query[Int.self, at: "max-uploads"] ?? 1000

        let (uploads, isTruncated, nextKeyMarker, nextUploadIdMarker) =
            try await ClusterListingService.listUploads(
                req: req,
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
                <Bucket>\(bucketName.xmlEscaped)</Bucket>
                <KeyMarker>\((keyMarker ?? "").xmlEscaped)</KeyMarker>
                <UploadIdMarker>\((uploadIdMarker ?? "").xmlEscaped)</UploadIdMarker>
                <NextKeyMarker>\((nextKeyMarker ?? "").xmlEscaped)</NextKeyMarker>
                <NextUploadIdMarker>\((nextUploadIdMarker ?? "").xmlEscaped)</NextUploadIdMarker>
                <MaxUploads>\(maxUploads)</MaxUploads>
                <IsTruncated>\(isTruncated)</IsTruncated>
                <Prefix>\(prefix.xmlEscaped)</Prefix>
            \(uploadsXml)
            </ListMultipartUploadsResult>
            """

        return S3Service.buildXMLResponse(data: Data(xml.utf8))
    }
}
