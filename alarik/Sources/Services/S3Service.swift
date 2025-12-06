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
        req.parameters.getCatchall().joined(separator: "/")
    }

    static func verifyBucketExists(_ bucketName: String, requestId: String) async throws {
        guard await AccessKeyBucketMapCache.shared.bucket(for: bucketName) != nil else {
            throw S3Error(
                status: .notFound, code: "NoSuchBucket",
                message: "The specified bucket does not exist.", requestId: requestId)
        }
    }

    static func verifyObjectExists(
        bucketName: String,
        key: String,
        path: String,
        requestId: String
    ) throws {
        guard ObjectFileHandler.keyExists(for: bucketName, key: key, path: path) else {
            throw S3Error(
                status: .notFound, code: "NoSuchKey",
                message: "The specified key does not exist.", requestId: requestId)
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
    /// Format: /source-bucket/source-key or source-bucket/source-key
    static func parseCopySource(from req: Request) throws -> CopySource? {
        guard let copySourceHeader = req.headers.first(name: "x-amz-copy-source") else {
            return nil
        }

        // URL decode the header value
        guard let decoded = copySourceHeader.removingPercentEncoding else {
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

        return CopySource(bucketName: bucketName, key: key)
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

    /// Checks if the provided ETag matches the object's ETag
    /// Handles both quoted ("etag") and unquoted (etag) formats, and wildcard (*)
    private static func matchesETag(_ headerValue: String, etag: String) -> Bool {
        let trimmed = headerValue.trimmingCharacters(in: .whitespaces)

        // Handle wildcard
        if trimmed == "*" {
            return true
        }

        // Remove quotes from both values for comparison
        let normalizedHeader = trimmed.replacingOccurrences(of: "\"", with: "")
        let normalizedETag = etag.replacingOccurrences(of: "\"", with: "")

        return normalizedHeader == normalizedETag
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

    static func handlePolicyQuery(req: Request) throws -> Response {
        throw S3Error(
            status: .notFound, code: "NoSuchBucketPolicy",
            message: "The specified bucket does not have a bucket policy.", requestId: req.id)
    }

    static func shouldHandleSubresource(query: String) -> Bool {
        query.lowercased().contains("location") || query.contains("policy")
    }

    static func handleSubresourceQuery(query: String, req: Request) throws -> Response? {
        if query.lowercased().contains("location") {
            return handleLocationQuery(req: req)
        }

        if query.contains("policy") {
            return try handlePolicyQuery(req: req)
        }

        return nil
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
}
