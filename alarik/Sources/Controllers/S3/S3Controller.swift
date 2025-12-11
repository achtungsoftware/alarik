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

        // Object Operations
        bucketRoute.on(.HEAD, "**", use: self.handleObjectHead)
        bucketRoute.get("**", use: self.handleObjectGet)
        bucketRoute.put("**", use: self.handleObjectPut)
        bucketRoute.delete("**", use: self.handleObjectDelete)
    }

    @Sendable
    func handleBucketGet(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)

        try await S3Service.verifyBucketExists(bucketName, requestId: req.id)
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        // Get bucket for versioning info
        let bucket = try await Bucket.query(on: req.db)
            .filter(\.$name == bucketName)
            .first()

        let query = req.url.query ?? ""
        let lowerQuery = query.lowercased()

        // Handle ?versions - list object versions
        // Use word boundary check: "versions" but not "versioning"
        let isListVersions = lowerQuery.contains("versions") && !lowerQuery.contains("versioning")
        if isListVersions {
            return try handleListVersions(req: req, bucketName: bucketName)
        }

        // Handle subresource queries (location, policy, versioning config)
        let shouldHandle = S3Service.shouldHandleSubresource(query: query)
        if shouldHandle {
            if let response = try await S3Service.handleSubresourceQuery(
                query: query, req: req, bucket: bucket)
            {
                return response
            }
        }

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
        return S3Service.buildStandardResponse(status: .ok, requestId: req.id)
    }

    @Sendable
    func handleObjectHead(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let keyPath = S3Service.extractObjectKey(from: req)

        try await S3Service.verifyBucketExists(bucketName, requestId: req.id)
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        // Check for versionId query parameter
        let versionId = req.query[String.self, at: "versionId"]

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

        if Validator.bucketName.validate(bucketName).isFailure {
            throw S3Error(
                status: .badRequest,
                code: "InvalidBucketName",
                message: "The specified bucket is not valid.", requestId: req.id
            )
        }

        if (try await Bucket.query(on: req.db).filter(\.$name == bucketName).first()) != nil {
            throw S3Error(
                status: .conflict,
                code: "BucketAlreadyExists",
                message: "The requested bucket name is not available."
            )
        }

        let key = try await S3Service.parseAndAuthenticateWithDB(req: req)
        try await BucketService.create(on: req.db, bucketName: bucketName, userId: key.user.id!)

        let response = S3Service.buildStandardResponse(status: .ok, requestId: req.id)
        response.headers.replaceOrAdd(name: "Location", value: "/\(bucketName)")
        return response
    }

    /// Handles PUT /:bucketName?versioning - set bucket versioning configuration
    @Sendable
    private func handleVersioningPut(req: Request, bucketName: String) async throws -> Response {
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .first()
        else {
            throw S3Error(
                status: .notFound, code: "NoSuchBucket",
                message: "The specified bucket does not exist.", requestId: req.id)
        }

        // Parse XML body for versioning configuration
        let bodyData = try await req.body.collect().get() ?? ByteBuffer()
        let bodyString = String(buffer: bodyData)

        // Simple XML parsing for versioning status
        var newStatus = VersioningStatus.disabled

        if bodyString.contains("<Status>Enabled</Status>") {
            newStatus = .enabled
        } else if bodyString.contains("<Status>Suspended</Status>") {
            newStatus = .suspended
        }

        // Update bucket versioning status in database
        bucket.versioningStatus = newStatus.rawValue
        try await bucket.save(on: req.db)

        // Update cache
        await BucketVersioningCache.shared.setStatus(for: bucketName, status: newStatus)

        return S3Service.buildStandardResponse(status: .ok, requestId: req.id)
    }

    // DELETE /:bucketName
    @Sendable
    func handleBucketDelete(req: Request) async throws -> HTTPStatus {
        let bucketName = try S3Service.extractBucketName(from: req)

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

        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

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

        let maxBodySize = req.application.routes.defaultMaxBodySize.value
        let bodyBuffer = try await req.body.collect(max: maxBodySize).get()
        guard var buffer = bodyBuffer else {
            throw S3Error(
                status: .badRequest, code: "MissingRequestBodyError",
                message: "Request body is empty.", requestId: req.id)
        }

        let isChunked =
            req.headers.first(name: "Content-Encoding")?.contains("aws-chunked") ?? false
        let hasChunkedHeader =
            req.headers.first(name: "x-amz-content-sha256") == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
        let dataToWrite: Data =
            if isChunked || hasChunkedHeader {
                try ChunkedDataDecoder.decode(buffer: &buffer)
            } else {
                Data(buffer.readableBytesView)
            }

        // Validate Content-MD5 if provided
        try S3Service.validateContentMD5(req: req, data: dataToWrite)

        let etag = Insecure.MD5.hash(data: dataToWrite).hex
        var meta = ObjectMeta(
            bucketName: bucketName,
            key: keyPath,
            size: dataToWrite.count,
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

        var headers = HTTPHeaders()
        headers.add(name: "ETag", value: "\"\(etag)\"")

        // Write with versioning support
        if versioningStatus != .disabled {
            let versionId = try ObjectFileHandler.writeVersioned(
                metadata: meta,
                data: dataToWrite,
                bucketName: bucketName,
                key: keyPath,
                versioningStatus: versioningStatus
            )
            headers.add(name: "x-amz-version-id", value: versionId)
        } else {
            // Non-versioned write
            let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
            try ObjectFileHandler.write(metadata: meta, data: dataToWrite, to: path)
        }

        return Response(status: .ok, headers: headers)
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

        // Get source object path and verify it exists
        let sourcePath = ObjectFileHandler.storagePath(
            for: copySource.bucketName, key: copySource.key)
        try S3Service.verifyObjectExists(
            bucketName: copySource.bucketName,
            key: copySource.key,
            path: sourcePath,
            requestId: req.id
        )

        // Read source object metadata and data
        let (sourceMeta, sourceData) = try ObjectFileHandler.read(from: sourcePath, loadData: true)
        guard let data = sourceData else {
            throw S3Error(
                status: .internalServerError,
                code: "InternalError",
                message: "Could not read source object",
                requestId: req.id
            )
        }

        // Validate copy conditions (if-match, if-none-match, etc.)
        try S3Service.validateCopyConditions(req: req, sourceMeta: sourceMeta)

        // Determine metadata handling
        let replaceMetadata = S3Service.shouldReplaceMetadata(req: req)

        // Create destination metadata
        let contentType: String
        if replaceMetadata {
            contentType = req.headers.contentType?.description ?? sourceMeta.contentType
        } else {
            contentType = sourceMeta.contentType
        }

        let etag = Insecure.MD5.hash(data: data).hex
        let destinationMeta = ObjectMeta(
            bucketName: destinationBucket,
            key: destinationKey,
            size: data.count,
            contentType: contentType,
            etag: etag,
            updatedAt: Date()
        )

        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/xml")

        // Write with versioning support
        var versionId: String? = nil
        if versioningStatus != .disabled {
            versionId = try ObjectFileHandler.writeVersioned(
                metadata: destinationMeta,
                data: data,
                bucketName: destinationBucket,
                key: destinationKey,
                versioningStatus: versioningStatus
            )
            headers.add(name: "x-amz-version-id", value: versionId!)
        } else {
            let destinationPath = ObjectFileHandler.storagePath(
                for: destinationBucket, key: destinationKey)
            try ObjectFileHandler.write(metadata: destinationMeta, data: data, to: destinationPath)
        }

        // Add source version ID if present
        if let sourceVersionId = sourceMeta.versionId {
            headers.add(name: "x-amz-copy-source-version-id", value: sourceVersionId)
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
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        // Check for versionId query parameter
        let versionId = req.query[String.self, at: "versionId"]

        let meta: ObjectMeta
        let objectData: Data

        // Check if Range header is present
        let hasRangeHeader = req.headers.first(name: .range) != nil

        do {
            if !hasRangeHeader {
                // Full read
                let (m, fullData) = try ObjectFileHandler.readVersion(
                    bucketName: bucketName,
                    key: keyPath,
                    versionId: versionId,
                    loadData: true
                )
                meta = m

                // Check if latest version is a delete marker
                if meta.isDeleteMarker && versionId == nil {
                    throw S3Error(
                        status: .notFound, code: "NoSuchKey",
                        message: "The specified key does not exist.", requestId: req.id)
                }

                // Validate conditional request headers
                try S3Service.validateConditionalHeaders(req: req, meta: meta)

                guard let data = fullData else {
                    throw S3Error(
                        status: .internalServerError, code: "InternalError",
                        message: "We encountered an internal error. Please try again.",
                        requestId: req.id)
                }

                return S3Service.buildVersionedObjectMetadataResponse(
                    meta: meta, includeBody: true, data: data, range: nil)
            }

            // For range requests, read metadata first
            let (m, _) = try ObjectFileHandler.readVersion(
                bucketName: bucketName,
                key: keyPath,
                versionId: versionId,
                loadData: false
            )
            meta = m

            // Check if latest version is a delete marker
            if meta.isDeleteMarker && versionId == nil {
                throw S3Error(
                    status: .notFound, code: "NoSuchKey",
                    message: "The specified key does not exist.", requestId: req.id)
            }

            // Validate conditional request headers
            try S3Service.validateConditionalHeaders(req: req, meta: meta)

            // Parse range header
            let byteRange = S3RangeParser.parseRange(from: req, fileSize: meta.size)

            if let range = byteRange {
                let (_, rangeData) = try ObjectFileHandler.readVersion(
                    bucketName: bucketName,
                    key: keyPath,
                    versionId: versionId,
                    loadData: true,
                    range: (range.start, range.end)
                )
                guard let data = rangeData else {
                    throw S3Error(
                        status: .internalServerError, code: "InternalError",
                        message: "We encountered an internal error. Please try again.",
                        requestId: req.id)
                }
                objectData = data
            } else {
                let (_, fullData) = try ObjectFileHandler.readVersion(
                    bucketName: bucketName,
                    key: keyPath,
                    versionId: versionId,
                    loadData: true
                )
                guard let data = fullData else {
                    throw S3Error(
                        status: .internalServerError, code: "InternalError",
                        message: "We encountered an internal error. Please try again.",
                        requestId: req.id)
                }
                objectData = data
            }

            return S3Service.buildVersionedObjectMetadataResponse(
                meta: meta, includeBody: true, data: objectData, range: byteRange)

        } catch let error as S3Error {
            throw error
        } catch {
            // Fallback to non-versioned path for backwards compatibility
            let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
            guard ObjectFileHandler.keyExists(for: bucketName, key: keyPath, path: path) else {
                throw S3Error(
                    status: .notFound, code: "NoSuchKey",
                    message: "The specified key does not exist.", requestId: req.id)
            }

            if !hasRangeHeader {
                let (m, fullData) = try ObjectFileHandler.read(from: path, loadData: true)
                try S3Service.validateConditionalHeaders(req: req, meta: m)
                guard let data = fullData else {
                    throw S3Error(
                        status: .internalServerError, code: "InternalError",
                        message: "We encountered an internal error. Please try again.",
                        requestId: req.id)
                }
                return S3Service.buildVersionedObjectMetadataResponse(
                    meta: m, includeBody: true, data: data, range: nil)
            }

            let (m, _) = try ObjectFileHandler.read(from: path, loadData: false)
            try S3Service.validateConditionalHeaders(req: req, meta: m)
            let byteRange = S3RangeParser.parseRange(from: req, fileSize: m.size)

            let data: Data
            if let range = byteRange {
                let (_, rangeData) = try ObjectFileHandler.read(
                    from: path, loadData: true, range: (range.start, range.end))
                data = rangeData!
            } else {
                let (_, fullData) = try ObjectFileHandler.read(from: path)
                data = fullData!
            }

            return S3Service.buildVersionedObjectMetadataResponse(
                meta: m, includeBody: true, data: data, range: byteRange)
        }
    }

    // DELETE /:bucketName/*key
    @Sendable
    func handleObjectDelete(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let keyPath = S3Service.extractObjectKey(from: req)

        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        // Check for versionId query parameter
        let versionId = req.query[String.self, at: "versionId"]

        // Get bucket versioning status from cache
        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        var headers = HTTPHeaders()

        if let versionId = versionId {
            // Delete specific version (permanent delete)
            do {
                try ObjectFileHandler.deleteVersion(
                    bucketName: bucketName, key: keyPath, versionId: versionId)
                headers.add(name: "x-amz-version-id", value: versionId)
            } catch {
                // Version might not exist - S3 returns success anyway
            }
        } else if versioningStatus == .enabled {
            // Versioning enabled, no versionId - create delete marker
            let deleteMarker = try ObjectFileHandler.createDeleteMarker(
                bucketName: bucketName, key: keyPath)
            headers.add(name: "x-amz-version-id", value: deleteMarker.versionId ?? "null")
            headers.add(name: "x-amz-delete-marker", value: "true")
        } else {
            // Versioning disabled or suspended - permanent delete
            // Check versioned storage first
            if ObjectFileHandler.isVersioned(bucketName: bucketName, key: keyPath) {
                // Delete all versions
                let versions = try ObjectFileHandler.listVersions(
                    bucketName: bucketName, key: keyPath)
                for version in versions {
                    if let vid = version.versionId {
                        try? ObjectFileHandler.deleteVersion(
                            bucketName: bucketName, key: keyPath, versionId: vid)
                    }
                }
            }

            // Also delete non-versioned path if exists
            let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
            if FileManager.default.fileExists(atPath: path) {
                try FileManager.default.removeItem(atPath: path)
            }
        }

        return Response(status: .noContent, headers: headers)
    }
}
