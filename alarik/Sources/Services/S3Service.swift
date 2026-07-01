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

import Crypto
import Fluent
import Foundation
import Vapor
import XMLCoder

struct CopySource {
    let bucketName: String
    let key: String
    let versionId: String?
}

struct ListObjectsParams {
    let bucketName: String
    let prefix: String
    let delimiter: String?
    let maxKeys: Int
    let marker: String?
    let isV2: Bool
    let startAfter: String?
    let continuationToken: String?
}

struct S3Service {
    static func extractBucketName(from req: Request) throws -> String {
        guard let bucketName = req.parameters.get("bucketName") else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Invalid argument", requestId: req.id)
        }
        return bucketName
    }

    static func extractObjectKey(from req: Request) -> String {
        let key = req.parameters.getCatchall().joined(separator: "/")
        // URL-decode the key in case the client sent encoded characters
        return key.removingPercentEncoding ?? key
    }

    static func verifyBucketExists(_ bucketName: String, requestId: String) async throws {
        guard await AccessKeyBucketMapCache.shared.bucket(for: bucketName) != nil else {
            throw S3Error(
                status: .notFound, code: "NoSuchBucket",
                message: "The specified bucket does not exist.", requestId: requestId)
        }
    }

    static func buildXMLResponse(data: Data, status: HTTPStatus = .ok) -> Response {
        let response = Response(status: status, body: .init(data: data))
        response.headers.contentType = .xml
        return response
    }

    static func buildStandardResponse(status: HTTPStatus = .ok, requestId: String? = nil)
        -> Response
    {
        let response = Response(status: status)
        if let requestId = requestId {
            response.headers.add(name: "x-amz-request-id", value: requestId)
        }
        return response
    }

    static func buildObjectMetadataResponse(
        meta: ObjectMeta,
        status: HTTPStatus = .ok,
        includeBody: Bool = false,
        data: Data? = nil,
        range: ByteRange? = nil
    ) -> Response {
        let body: Response.Body
        if includeBody, let data = data {
            body = Response.Body(data: data)
        } else {
            body = Response.Body.empty
        }

        let responseStatus = range != nil ? .partialContent : status
        let response = Response(status: responseStatus, body: body)

        if let contentType = HTTPMediaType.from(string: meta.contentType) {
            response.headers.contentType = contentType
        }

        response.headers.add(name: "ETag", value: "\"\(meta.etag)\"")
        response.headers.add(name: "Last-Modified", value: meta.updatedAt.rfc1123String)
        response.headers.add(name: "Accept-Ranges", value: "bytes")

        for (key, value) in meta.metadata {
            response.headers.add(name: "x-amz-meta-\(key)", value: value)
        }

        if let range = range {
            response.headers.add(
                name: "Content-Range", value: range.contentRange(fileSize: meta.size))
            response.headers.replaceOrAdd(name: .contentLength, value: String(range.length))
        } else if !includeBody {
            response.headers.replaceOrAdd(name: .contentLength, value: String(meta.size))
        }

        return response
    }

    static func parseListObjectsParams(from req: Request, bucketName: String) -> ListObjectsParams {
        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let delimiter = req.query[String.self, at: "delimiter"]
        let maxKeys = req.query[Int.self, at: "max-keys"] ?? 1000
        let listType = req.query[String.self, at: "list-type"]
        let isV2 = listType == "2"
        let startAfter = req.query[String.self, at: "start-after"]
        let continuationToken = req.query[String.self, at: "continuation-token"]
        let marker = isV2 ? (startAfter ?? continuationToken) : req.query[String.self, at: "marker"]

        return ListObjectsParams(
            bucketName: bucketName,
            prefix: prefix,
            delimiter: delimiter,
            maxKeys: maxKeys,
            marker: marker,
            isV2: isV2,
            startAfter: startAfter,
            continuationToken: continuationToken
        )
    }

    static func buildObjectEntries(from objects: [ObjectMeta]) -> [ObjectEntry] {
        return objects.map { meta in
            ObjectEntry(
                key: meta.key,
                lastModified: meta.updatedAt.iso8601String,
                etag: "\"\(meta.etag)\"",
                size: meta.size,
                storageClass: "STANDARD"
            )
        }
    }

    static func buildCommonPrefixEntries(from prefixes: [String]) -> [CommonPrefix] {
        prefixes.map { CommonPrefix(prefix: $0) }
    }

    static func buildListObjectsResponse(
        params: ListObjectsParams,
        objects: [ObjectMeta],
        commonPrefixes: [String],
        isTruncated: Bool,
        nextMarker: String?
    ) throws -> Data {
        let encoder = XMLEncoder()
        encoder.outputFormatting = .prettyPrinted

        let objectEntries = buildObjectEntries(from: objects)
        let commonPrefixEntries = buildCommonPrefixEntries(from: commonPrefixes)

        if params.isV2 {
            let result = ListBucketResultV2(
                name: params.bucketName,
                prefix: params.prefix,
                startAfter: params.startAfter,
                continuationToken: params.continuationToken,
                nextContinuationToken: isTruncated ? (nextMarker ?? "") : nil,
                keyCount: objectEntries.count + commonPrefixEntries.count,
                maxKeys: params.maxKeys,
                isTruncated: isTruncated,
                contents: objectEntries,
                commonPrefixes: commonPrefixEntries
            )
            return try encoder.encode(
                result, withRootKey: "ListBucketResult",
                rootAttributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"])
        } else {
            let result = ListBucketResult(
                name: params.bucketName,
                prefix: params.prefix,
                marker: params.marker,
                nextMarker: isTruncated ? nextMarker : nil,
                maxKeys: params.maxKeys,
                isTruncated: isTruncated,
                contents: objectEntries,
                commonPrefixes: commonPrefixEntries
            )
            return try encoder.encode(
                result, withRootKey: "ListBucketResult",
                rootAttributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"])
        }
    }

    /// Parses the x-amz-copy-source header
    /// Format: /source-bucket/source-key or source-bucket/source-key, optionally
    /// suffixed with ?versionId=xxx to copy from a specific source object version.
    static func parseCopySource(from req: Request) throws -> CopySource? {
        guard let copySourceHeader = req.headers.first(name: "x-amz-copy-source") else {
            return nil
        }

        // Split off the query string before percent-decoding: a literal "?" only ever
        // appears as the query separator, since a "?" that's part of the key itself
        // would already be percent-encoded as %3F by a well-behaved client.
        let rawPath: Substring
        let rawQuery: Substring?
        if let queryIndex = copySourceHeader.firstIndex(of: "?") {
            rawPath = copySourceHeader[..<queryIndex]
            rawQuery = copySourceHeader[copySourceHeader.index(after: queryIndex)...]
        } else {
            rawPath = Substring(copySourceHeader)
            rawQuery = nil
        }

        // URL decode the header value
        guard let decoded = String(rawPath).removingPercentEncoding else {
            throw S3Error(
                status: .badRequest,
                code: "InvalidArgument",
                message: "Invalid copy source format",
                requestId: req.id
            )
        }

        // Remove leading slash if present
        let normalized = decoded.hasPrefix("/") ? String(decoded.dropFirst()) : decoded

        // Split into bucket and key
        guard let slashIndex = normalized.firstIndex(of: "/") else {
            throw S3Error(
                status: .badRequest,
                code: "InvalidArgument",
                message: "Copy source must be in format: /source-bucket/source-key",
                requestId: req.id
            )
        }

        let bucketName = String(normalized[..<slashIndex])
        let key = String(normalized[normalized.index(after: slashIndex)...])

        guard !bucketName.isEmpty && !key.isEmpty else {
            throw S3Error(
                status: .badRequest,
                code: "InvalidArgument",
                message: "Copy source bucket and key cannot be empty",
                requestId: req.id
            )
        }

        var versionId: String? = nil
        if let rawQuery = rawQuery {
            for param in rawQuery.split(separator: "&") {
                guard let eqIndex = param.firstIndex(of: "=") else { continue }
                let paramKey = param[..<eqIndex]
                guard paramKey == "versionId" else { continue }
                let rawValue = String(param[param.index(after: eqIndex)...])
                versionId = rawValue.removingPercentEncoding ?? rawValue
            }
        }

        return CopySource(bucketName: bucketName, key: key, versionId: versionId)
    }

    /// Validates conditional copy headers against source object metadata
    static func validateCopyConditions(req: Request, sourceMeta: ObjectMeta) throws {
        let headers = req.headers

        // x-amz-copy-source-if-match: Copy only if source ETag matches
        if let ifMatch = headers.first(name: "x-amz-copy-source-if-match") {
            let matches = matchesETag(ifMatch, etag: sourceMeta.etag)
            if !matches {
                throw S3Error(
                    status: .preconditionFailed,
                    code: "PreconditionFailed",
                    message: "At least one of the pre-conditions you specified did not hold",
                    requestId: req.id
                )
            }
        }

        // x-amz-copy-source-if-none-match: Copy only if source ETag doesn't match
        if let ifNoneMatch = headers.first(name: "x-amz-copy-source-if-none-match") {
            let matches = matchesETag(ifNoneMatch, etag: sourceMeta.etag)
            if matches {
                throw S3Error(
                    status: .preconditionFailed,
                    code: "PreconditionFailed",
                    message: "At least one of the pre-conditions you specified did not hold",
                    requestId: req.id
                )
            }
        }

        // x-amz-copy-source-if-modified-since: Copy only if source modified after date
        if let ifModifiedSince = headers.first(name: "x-amz-copy-source-if-modified-since") {
            if let sinceDate = Date.fromHTTPDateString(ifModifiedSince) {
                if sourceMeta.updatedAt.timeIntervalSince1970 <= sinceDate.timeIntervalSince1970 {
                    throw S3Error(
                        status: .preconditionFailed,
                        code: "PreconditionFailed",
                        message: "At least one of the pre-conditions you specified did not hold",
                        requestId: req.id
                    )
                }
            }
        }

        // x-amz-copy-source-if-unmodified-since: Copy only if source not modified after date
        if let ifUnmodifiedSince = headers.first(name: "x-amz-copy-source-if-unmodified-since") {
            if let sinceDate = Date.fromHTTPDateString(ifUnmodifiedSince) {
                if sourceMeta.updatedAt.timeIntervalSince1970 > sinceDate.timeIntervalSince1970 {
                    throw S3Error(
                        status: .preconditionFailed,
                        code: "PreconditionFailed",
                        message: "At least one of the pre-conditions you specified did not hold",
                        requestId: req.id
                    )
                }
            }
        }
    }

    /// Determines whether to replace or copy metadata
    /// Returns true if metadata should be replaced, false if copied from source
    static func shouldReplaceMetadata(req: Request) -> Bool {
        guard let directive = req.headers.first(name: "x-amz-metadata-directive") else {
            return false  // Default is COPY
        }
        return directive.uppercased() == "REPLACE"
    }

    /// Checks if the provided ETag matches the object's ETag.
    /// Handles quoted, unquoted, entity-encoded, and wildcard (*) forms.
    private static func matchesETag(_ headerValue: String, etag: String) -> Bool {
        let trimmed = headerValue.trimmingCharacters(in: .whitespaces)
        if trimmed == "*" { return true }
        return normalizeETag(trimmed) == normalizeETag(etag)
    }

    /// Validates conditional request headers against object metadata
    /// Returns true if the request should proceed, throws S3Error if precondition fails
    static func validateConditionalHeaders(req: Request, meta: ObjectMeta) throws {
        let headers = req.headers

        // If-Match: Return object only if ETag matches
        if let ifMatch = headers.first(name: "If-Match") {
            let matches = Self.matchesETag(ifMatch, etag: meta.etag)
            if !matches {
                throw S3Error(
                    status: .preconditionFailed,
                    code: "PreconditionFailed",
                    message: "At least one of the pre-conditions you specified did not hold",
                    requestId: req.id
                )
            }
        }

        // If-None-Match: Return object only if ETag doesn't match
        if let ifNoneMatch = headers.first(name: "If-None-Match") {
            let matches = Self.matchesETag(ifNoneMatch, etag: meta.etag)
            if matches {
                throw S3Error(
                    status: .notModified,
                    code: "NotModified",
                    message: "Not Modified",
                    requestId: req.id
                )
            }
        }

        // If-Modified-Since: Return object only if modified after this date
        if let ifModifiedSince = headers.first(name: "If-Modified-Since") {
            if let sinceDate = Date.fromHTTPDateString(ifModifiedSince) {
                // S3 uses truncated seconds comparison
                if meta.updatedAt.timeIntervalSince1970 <= sinceDate.timeIntervalSince1970 {
                    throw S3Error(
                        status: .notModified,
                        code: "NotModified",
                        message: "Not Modified",
                        requestId: req.id
                    )
                }
            }
        }

        // If-Unmodified-Since: Return object only if not modified after this date
        if let ifUnmodifiedSince = headers.first(name: "If-Unmodified-Since") {
            if let sinceDate = Date.fromHTTPDateString(ifUnmodifiedSince) {
                // S3 uses truncated seconds comparison
                if meta.updatedAt.timeIntervalSince1970 > sinceDate.timeIntervalSince1970 {
                    throw S3Error(
                        status: .preconditionFailed,
                        code: "PreconditionFailed",
                        message: "At least one of the pre-conditions you specified did not hold",
                        requestId: req.id
                    )
                }
            }
        }
    }

    /// Validates conditional write headers (`If-Match`/`If-None-Match`) for PutObject, against
    /// the *current* object if one exists (nil if it doesn't). Unlike the GET-side
    /// `validateConditionalHeaders`, a failed precondition is always `412 PreconditionFailed`
    /// here - `304 Not Modified` only makes sense for a read, never a write.
    static func validateConditionalPutHeaders(req: Request, existingMeta: ObjectMeta?) throws {
        if let ifMatch = req.headers.first(name: "If-Match") {
            guard let meta = existingMeta, Self.matchesETag(ifMatch, etag: meta.etag) else {
                throw S3Error(
                    status: .preconditionFailed,
                    code: "PreconditionFailed",
                    message: "At least one of the pre-conditions you specified did not hold",
                    requestId: req.id
                )
            }
        }

        if let ifNoneMatch = req.headers.first(name: "If-None-Match") {
            if let meta = existingMeta, Self.matchesETag(ifNoneMatch, etag: meta.etag) {
                throw S3Error(
                    status: .preconditionFailed,
                    code: "PreconditionFailed",
                    message: "At least one of the pre-conditions you specified did not hold",
                    requestId: req.id
                )
            }
        }
    }

    /// Validates Content-MD5 header if present
    /// Throws S3Error if the MD5 doesn't match the data
    static func validateContentMD5(req: Request, data: Data) throws {
        guard let contentMD5 = req.headers.first(name: "Content-MD5") else {
            return
        }

        // Compute MD5 hash of the data
        let computedMD5 = Insecure.MD5.hash(data: data)
        let computedBase64 = Data(computedMD5).base64EncodedString()

        if contentMD5 != computedBase64 {
            throw S3Error(
                status: .badRequest,
                code: "BadDigest",
                message: "The Content-MD5 you specified did not match what we received.",
                requestId: req.id
            )
        }
    }

    static func handleLocationQuery(req: Request) -> Response {
        let region = "us-east-1"
        let xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\(region)</LocationConstraint>
            """
        return buildXMLResponse(data: Data(xml.utf8))
    }

    /// Handles GET ?policy - returns the bucket's policy as raw JSON, matching real S3
    /// (GetBucketPolicy is the one bucket subresource that responds with JSON, not XML)
    static func handlePolicyQuery(bucket: Bucket?, requestId: String) throws -> Response {
        guard let rawPolicy = bucket?.policy else {
            throw S3Error(
                status: .notFound, code: "NoSuchBucketPolicy",
                message: "The specified bucket does not have a bucket policy.",
                requestId: requestId)
        }

        let response = Response(status: .ok, body: .init(string: rawPolicy))
        response.headers.contentType = .json
        return response
    }

    static func shouldHandleSubresource(query: String) -> Bool {
        let lowerQuery = query.lowercased()
        return lowerQuery.contains("location")
            || lowerQuery.contains("policy")
            || lowerQuery.contains("versioning")
            || lowerQuery.contains("versions")
            || lowerQuery.contains("publicaccessblock")
            || lowerQuery.contains("tagging")
            || lowerQuery.contains("lifecycle")
    }

    static func handleSubresourceQuery(query: String, req: Request, bucket: Bucket?) async throws
        -> Response?
    {
        let lowerQuery = query.lowercased()

        if lowerQuery.contains("location") {
            return handleLocationQuery(req: req)
        }

        if lowerQuery.contains("publicaccessblock") {
            return try handlePublicAccessBlockGet(bucket: bucket, requestId: req.id)
        }

        if lowerQuery.contains("tagging") {
            return try handleBucketTaggingGet(bucket: bucket, requestId: req.id)
        }

        if lowerQuery.contains("lifecycle") {
            return try handleLifecycleGet(bucket: bucket, requestId: req.id)
        }

        if lowerQuery.contains("policy") {
            return try handlePolicyQuery(bucket: bucket, requestId: req.id)
        }

        // Handle versioning configuration (GET only - PUT handled in controller)
        if lowerQuery.contains("versioning") && !lowerQuery.contains("versions") {
            return handleVersioningGet(bucket: bucket)
        }

        // Note: ?versions is handled in the controller for list versions

        return nil
    }

    /// Handles GET ?lifecycle on a bucket. Matches real S3: a 404 NoSuchLifecycleConfiguration
    /// if none has ever been set (verified against the GetBucketLifecycleConfiguration API
    /// reference).
    static func handleLifecycleGet(bucket: Bucket?, requestId: String) throws -> Response {
        guard let rawRules = bucket?.lifecycleRules else {
            throw S3Error(
                status: .notFound, code: "NoSuchLifecycleConfiguration",
                message: "The lifecycle configuration does not exist.", requestId: requestId)
        }
        return buildXMLResponse(data: Data(LifecycleConfiguration.fromJSON(rawRules).toXML().utf8))
    }

    /// Handles GET ?tagging on a bucket. Matches real S3: a 404 NoSuchTagSet if no tag set has
    /// ever been configured (verified against the GetBucketTagging API reference) - unlike
    /// object tagging, which always returns 200 with a possibly-empty TagSet.
    static func handleBucketTaggingGet(bucket: Bucket?, requestId: String) throws -> Response {
        guard let rawTags = bucket?.tags else {
            throw S3Error(
                status: .notFound, code: "NoSuchTagSet",
                message: "There is no tag set associated with the bucket.",
                requestId: requestId)
        }
        return buildXMLResponse(data: Data(Tagging.fromJSON(rawTags).toXML().utf8))
    }

    /// Handles GET ?publicAccessBlock - returns the bucket's Public Access Block configuration.
    /// Matches real S3: a 404 NoSuchPublicAccessBlockConfiguration if none has ever been set,
    /// same shape as GetBucketPolicy's NoSuchBucketPolicy.
    static func handlePublicAccessBlockGet(bucket: Bucket?, requestId: String) throws -> Response
    {
        guard let bucket = bucket,
            bucket.blockPublicAcls || bucket.ignorePublicAcls || bucket.blockPublicPolicy
                || bucket.restrictPublicBuckets
        else {
            throw S3Error(
                status: .notFound, code: "NoSuchPublicAccessBlockConfiguration",
                message: "The public access block configuration was not found.",
                requestId: requestId)
        }
        return buildXMLResponse(data: Data(bucket.publicAccessBlock.toXML().utf8))
    }

    /// Handles GET ?versioning - returns bucket versioning configuration
    static func handleVersioningGet(bucket: Bucket?) -> Response {
        let status = bucket?.versioningStatus ?? VersioningStatus.disabled.rawValue

        // S3 returns empty VersioningConfiguration for buckets that have never had versioning enabled
        let statusElement: String
        if status == VersioningStatus.disabled.rawValue {
            statusElement = ""
        } else {
            statusElement = "<Status>\(status)</Status>"
        }

        let xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\(statusElement)</VersioningConfiguration>"
        return buildXMLResponse(data: Data(xml.utf8))
    }

    /// Builds the ListVersionsResult XML response
    static func buildListVersionsResponse(
        bucketName: String,
        prefix: String,
        delimiter: String?,
        keyMarker: String?,
        versionIdMarker: String?,
        maxKeys: Int,
        versions: [ObjectMeta],
        deleteMarkers: [ObjectMeta],
        commonPrefixes: [String],
        isTruncated: Bool,
        nextKeyMarker: String?,
        nextVersionIdMarker: String?
    ) throws -> Data {
        let encoder = XMLEncoder()
        encoder.outputFormatting = .prettyPrinted

        let versionEntries = versions.map { VersionEntry(from: $0) }
        let deleteMarkerEntries = deleteMarkers.map { DeleteMarkerEntry(from: $0) }
        let commonPrefixEntries = commonPrefixes.map { CommonPrefix(prefix: $0) }

        let result = ListVersionsResult(
            name: bucketName,
            prefix: prefix,
            delimiter: delimiter,
            keyMarker: keyMarker,
            versionIdMarker: versionIdMarker,
            nextKeyMarker: isTruncated ? nextKeyMarker : nil,
            nextVersionIdMarker: isTruncated ? nextVersionIdMarker : nil,
            maxKeys: maxKeys,
            isTruncated: isTruncated,
            versions: versionEntries,
            deleteMarkers: deleteMarkerEntries,
            commonPrefixes: commonPrefixEntries
        )

        return try encoder.encode(
            result, withRootKey: "ListVersionsResult",
            rootAttributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"])
    }

    /// Adds version headers to a response
    static func addVersionHeaders(to response: Response, meta: ObjectMeta) {
        if let versionId = meta.versionId {
            response.headers.add(name: "x-amz-version-id", value: versionId)
        }
        if meta.isDeleteMarker {
            response.headers.add(name: "x-amz-delete-marker", value: "true")
        }
    }

    /// Builds object metadata response with version headers
    static func buildVersionedObjectMetadataResponse(
        meta: ObjectMeta,
        status: HTTPStatus = .ok,
        includeBody: Bool = false,
        data: Data? = nil,
        range: ByteRange? = nil
    ) -> Response {
        let response = buildObjectMetadataResponse(
            meta: meta,
            status: status,
            includeBody: includeBody,
            data: data,
            range: range
        )

        addVersionHeaders(to: response, meta: meta)

        // x-amz-tagging-count - only present when the object actually has tags (verified
        // against the GetObject API reference)
        if let tagCount = meta.tags?.count, tagCount > 0 {
            response.headers.add(name: "x-amz-tagging-count", value: String(tagCount))
        }

        return response
    }

    static func authenticateWithCache(
        req: Request,
        bucketName: String
    ) async throws -> S3AuthInfo {
        let authInfo: S3AuthInfo = try await SigV4Validator.authenticateRequest(for: req)

        guard
            await AccessKeyBucketMapCache.shared.canAccess(
                accessKey: authInfo.accessKey, bucket: bucketName)
        else {
            throw S3Error(status: .forbidden, code: "AccessDenied", message: "Access Denied")
        }

        return authInfo
    }

    /// Authenticates the request if any credentials are present (header or query), exactly like
    /// `authenticateWithCache` - a malformed/expired signature always fails here and is never
    /// treated as anonymous. Only a true absence of credentials falls through to the bucket
    /// policy, and only for actions in the small public-access whitelist (`S3PolicyAction`).
    /// Returns nil when the request was authorized anonymously via policy.
    static func authenticateOrAuthorizePublic(
        req: Request,
        bucketName: String,
        action: S3PolicyAction,
        key: String?
    ) async throws -> S3AuthInfo? {
        let hasCredentials =
            req.headers.first(name: "authorization") != nil
            || req.query[String.self, at: "X-Amz-Algorithm"] != nil

        if hasCredentials {
            return try await authenticateWithCache(req: req, bucketName: bucketName)
        }

        // RestrictPublicBuckets blocks anonymous access outright, regardless of what the bucket
        // policy says - it only ever affects this no-credentials branch, never authenticated
        // (owner/SigV4) requests.
        if await BucketPolicyCache.shared.publicAccessBlock(for: bucketName)?.restrictPublicBuckets
            == true
        {
            throw S3Error(status: .forbidden, code: "AccessDenied", message: "Access Denied")
        }

        guard
            let policy = await BucketPolicyCache.shared.policy(for: bucketName),
            policy.allowsAnonymous(action: action, bucketName: bucketName, key: key)
        else {
            throw S3Error(status: .forbidden, code: "AccessDenied", message: "Access Denied")
        }

        return nil
    }

    static func authenticateWithDB(
        req: Request,
        authInfo: S3AuthInfo
    ) async throws -> AccessKey {
        guard
            let key =
                try await AccessKey
                .query(on: req.db)
                .filter(\.$accessKey == authInfo.accessKey)
                .with(\.$user)
                .first()
        else {
            throw S3Error(status: .forbidden, code: "AccessDenied", message: "Access Denied")
        }

        let validator = SigV4Validator(secretKey: key.secretKey)
        guard try validator.validate(request: req, authInfo: authInfo) else {
            throw S3Error(status: .forbidden, code: "AccessDenied", message: "Access Denied")
            // HIER HIER
        }

        return key
    }

    static func parseAndAuthenticateWithDB(req: Request) async throws -> AccessKey {
        let authInfo = try S3AuthParser.parse(request: req)
        return try await authenticateWithDB(req: req, authInfo: authInfo)
    }

    /// Outcome of deleting a single object, used to build both the single-object
    /// DELETE response headers and the multi-object DeleteResult XML entries.
    struct ObjectDeleteOutcome {
        let versionId: String?
        let isDeleteMarker: Bool
    }

    /// Deletes a single object, honoring versionId (permanent delete of a specific version)
    /// and bucket versioning status (delete marker vs. permanent delete), matching S3 semantics.
    /// Deleting a key that doesn't exist is treated as success, like real S3.
    static func deleteObject(
        bucketName: String,
        key: String,
        versionId: String?,
        versioningStatus: VersioningStatus
    ) throws -> ObjectDeleteOutcome {
        if let versionId = versionId {
            do {
                try ObjectFileHandler.deleteVersion(
                    bucketName: bucketName, key: key, versionId: versionId)
            } catch {
                // Version might not exist - S3 returns success anyway
            }
            return ObjectDeleteOutcome(versionId: versionId, isDeleteMarker: false)
        }

        if versioningStatus == .enabled {
            let deleteMarker = try ObjectFileHandler.createDeleteMarker(
                bucketName: bucketName, key: key)
            return ObjectDeleteOutcome(versionId: deleteMarker.versionId, isDeleteMarker: true)
        }

        // Versioning disabled or suspended - permanent delete
        if ObjectFileHandler.isVersioned(bucketName: bucketName, key: key) {
            let versions = try ObjectFileHandler.listVersions(bucketName: bucketName, key: key)
            for version in versions {
                if let vid = version.versionId {
                    try? ObjectFileHandler.deleteVersion(
                        bucketName: bucketName, key: key, versionId: vid)
                }
            }
        }

        let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
        if FileManager.default.fileExists(atPath: path) {
            try FileManager.default.removeItem(atPath: path)
        }
        return ObjectDeleteOutcome(versionId: nil, isDeleteMarker: false)
    }

    /// Builds the DeleteResult XML response for a Multi-Object Delete request
    static func buildDeleteObjectsResponse(
        deleted: [DeletedEntry],
        errors: [DeleteErrorEntry]
    ) throws -> Data {
        let encoder = XMLEncoder()
        encoder.outputFormatting = .prettyPrinted

        let result = DeleteObjectsResult(deleted: deleted, errors: errors)
        return try encoder.encode(
            result, withRootKey: "DeleteResult",
            rootAttributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"])
    }

    /// Resolves and reads a copy-source object (CopyObject / UploadPartCopy), preferring
    /// versioned storage - either a specific versionId or the current latest version - and
    /// falling back to the legacy non-versioned path for objects written before the source
    /// bucket ever had versioning enabled.
    /// Throws NoSuchKey if the object doesn't exist, or if its latest version is a delete
    /// marker and no explicit versionId was requested (matching GetObject semantics).
    // MARK: – ETag utilities

    /// Computes the MD5-based S3 ETag for a data blob.
    static func computeETag(_ data: Data) -> String {
        Insecure.MD5.hash(data: data).hex
    }

    /// Wraps a bare ETag hex string in the AWS wire-format quotes (`"<hash>"`).
    static func quoteETag(_ etag: String) -> String {
        "\"\(etag)\""
    }

    /// Normalises an ETag value received from a client over the wire.
    /// Handles literal surrounding `"`, XML entity-encoded quotes (`&#34;`, `&quot;`),
    /// and leading/trailing whitespace.  Returns the bare lowercase hex string.
    static func normalizeETag(_ raw: String) -> String {
        raw
            .replacingOccurrences(of: "&#34;", with: "\"")
            .replacingOccurrences(of: "&quot;", with: "\"")
            .trimmingCharacters(in: .whitespaces)
            .replacingOccurrences(of: "\"", with: "")
    }

    // MARK: – Body collection

    /// Collects the request body as a `String`.
    static func collectBodyString(req: Request) async throws -> String {
        let buffer = try await req.body.collect().get() ?? ByteBuffer()
        return String(buffer: buffer)
    }

    /// Collects the request body and, when the request uses AWS chunked transfer encoding,
    /// strips the chunk framing to return only the actual content bytes.
    /// Returns empty `Data` for requests with no body (valid for PutObject zero-byte objects).
    static func collectBodyData(req: Request) async throws -> Data {
        let maxBodySize = req.application.routes.defaultMaxBodySize.value
        let bodyBuffer = try await req.body.collect(max: maxBodySize).get()
        var buffer = bodyBuffer ?? ByteBuffer()
        let isChunked =
            req.headers.first(name: "Content-Encoding")?.contains("aws-chunked") ?? false
        let hasChunkedHeader =
            req.headers.first(name: "x-amz-content-sha256") == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
        if isChunked || hasChunkedHeader {
            return try ChunkedDataDecoder.decode(buffer: &buffer)
        }
        return Data(buffer.readableBytesView)
    }

    // MARK: – Multipart XML parsing

    /// Parses the `<CompleteMultipartUpload>` XML body and returns an ordered list of
    /// `(partNumber, etag)` pairs with normalised (bare hex, no quotes) ETags.
    static func parseCompleteMultipartBody(
        _ body: String, requestId: String
    ) throws -> [(partNumber: Int, etag: String)] {
        var parts: [(partNumber: Int, etag: String)] = []

        let partBlockRegex = try NSRegularExpression(
            pattern: #"<Part>(.*?)</Part>"#, options: [.dotMatchesLineSeparators])
        let partNumberRegex = try NSRegularExpression(
            pattern: #"<PartNumber>\s*(\d+)\s*</PartNumber>"#, options: [])
        // Capture everything between <ETag> and </ETag>; entity-decoding is done below.
        let etagRegex = try NSRegularExpression(
            pattern: #"<ETag>\s*([^<]+?)\s*</ETag>"#, options: [])

        let fullRange = NSRange(body.startIndex..., in: body)
        for block in partBlockRegex.matches(in: body, options: [], range: fullRange) {
            guard let blockRange = Range(block.range(at: 1), in: body) else { continue }
            let content = String(body[blockRange])
            let contentRange = NSRange(content.startIndex..., in: content)

            guard
                let pnMatch = partNumberRegex.firstMatch(
                    in: content, options: [], range: contentRange),
                let pnRange = Range(pnMatch.range(at: 1), in: content),
                let partNumber = Int(content[pnRange]),
                let etagMatch = etagRegex.firstMatch(
                    in: content, options: [], range: contentRange),
                let etagRange = Range(etagMatch.range(at: 1), in: content)
            else { continue }

            parts.append((partNumber: partNumber, etag: normalizeETag(String(content[etagRange]))))
        }

        if parts.isEmpty {
            throw S3Error(
                status: .badRequest, code: "MalformedXML",
                message: "The XML you provided was not well-formed.", requestId: requestId)
        }
        return parts
    }

    static func readVersionedObjectForCopy(
        bucketName: String,
        key: String,
        versionId: String?,
        loadData: Bool,
        range: (start: Int, end: Int)? = nil,
        requestId: String
    ) throws -> (meta: ObjectMeta, data: Data?) {
        let meta: ObjectMeta
        let data: Data?
        do {
            (meta, data) = try ObjectFileHandler.readVersion(
                bucketName: bucketName, key: key, versionId: versionId,
                loadData: loadData, range: range)
        } catch {
            // Fallback to non-versioned path for backwards compatibility
            let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
            guard ObjectFileHandler.keyExists(for: bucketName, key: key, path: path) else {
                throw S3Error(
                    status: .notFound, code: "NoSuchKey",
                    message: "The specified key does not exist.", requestId: requestId)
            }
            (meta, data) = try ObjectFileHandler.read(from: path, loadData: loadData, range: range)
        }

        if meta.isDeleteMarker && versionId == nil {
            throw S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: requestId)
        }

        return (meta, data)
    }
}
