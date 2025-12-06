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

        let query = req.url.query ?? ""
        if S3Service.shouldHandleSubresource(query: query) {
            if let response = try S3Service.handleSubresourceQuery(query: query, req: req) {
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

        let xmlData = try S3Service.buildListObjectsResponse(
            params: params,
            objects: objects,
            commonPrefixes: commonPrefixes,
            isTruncated: isTruncated,
            nextMarker: nextMarker
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

        let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
        try S3Service.verifyObjectExists(
            bucketName: bucketName, key: keyPath, path: path, requestId: req.id)

        let (meta, _) = try ObjectFileHandler.read(from: path, loadData: false)

        // Validate conditional request headers
        try S3Service.validateConditionalHeaders(req: req, meta: meta)

        return S3Service.buildObjectMetadataResponse(meta: meta)
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

        // Check if this is a copy operation
        if let copySource = try S3Service.parseCopySource(from: req) {
            return try await handleCopyObject(
                req: req,
                destinationBucket: bucketName,
                destinationKey: keyPath,
                copySource: copySource
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
        let meta = ObjectMeta(
            bucketName: bucketName,
            key: keyPath,
            size: dataToWrite.count,
            contentType: req.headers.contentType?.description ?? "application/octet-stream",
            etag: etag,
            updatedAt: Date()
        )

        let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
        try ObjectFileHandler.write(metadata: meta, data: dataToWrite, to: path)

        var headers = HTTPHeaders()
        headers.add(name: "ETag", value: "\"\(etag)\"")
        return Response(status: .ok, headers: headers)
    }

    // Helper method to handle copy operations
    @Sendable
    private func handleCopyObject(
        req: Request,
        destinationBucket: String,
        destinationKey: String,
        copySource: CopySource
    ) async throws -> Response {
        // Verify source bucket exists
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

        // Write to destination
        let destinationPath = ObjectFileHandler.storagePath(
            for: destinationBucket, key: destinationKey)
        try ObjectFileHandler.write(metadata: destinationMeta, data: data, to: destinationPath)

        // Build copy result response (S3 returns XML for copy operations)
        let copyResult = """
            <?xml version="1.0" encoding="UTF-8"?>
            <CopyObjectResult>
                <LastModified>\(ISO8601DateFormatter().string(from: destinationMeta.updatedAt))</LastModified>
                <ETag>"\(etag)"</ETag>
            </CopyObjectResult>
            """

        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/xml")
        return Response(status: .ok, headers: headers, body: .init(string: copyResult))
    }

    // GET /:bucketName/*key
    @Sendable
    func handleObjectGet(req: Request) async throws -> Response {
        let bucketName = try S3Service.extractBucketName(from: req)
        let keyPath = S3Service.extractObjectKey(from: req)

        try await S3Service.verifyBucketExists(bucketName, requestId: req.id)
        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
        try S3Service.verifyObjectExists(
            bucketName: bucketName, key: keyPath, path: path, requestId: req.id)

        // Check if Range header is present before reading
        let hasRangeHeader = req.headers.first(name: .range) != nil

        // If no range header, read everything in a single pass
        if !hasRangeHeader {
            let (meta, fullData) = try ObjectFileHandler.read(from: path, loadData: true)

            // Validate conditional request headers
            try S3Service.validateConditionalHeaders(req: req, meta: meta)

            guard let data = fullData else {
                throw S3Error(
                    status: .internalServerError, code: "InternalError",
                    message: "We encountered an internal error. Please try again.",
                    requestId: req.id)
            }

            return S3Service.buildObjectMetadataResponse(
                meta: meta, includeBody: true, data: data, range: nil)
        }

        // For range requests, we need metadata first to parse the range
        let (meta, _) = try ObjectFileHandler.read(from: path, loadData: false)

        // Validate conditional request headers
        try S3Service.validateConditionalHeaders(req: req, meta: meta)

        // Parse range header now that we have file size
        let byteRange = S3RangeParser.parseRange(from: req, fileSize: meta.size)

        let objectData: Data
        if let range = byteRange {
            // Read only the requested range
            let (_, rangeData) = try ObjectFileHandler.read(
                from: path, loadData: true, range: (range.start, range.end))
            guard let data = rangeData else {
                throw S3Error(
                    status: .internalServerError, code: "InternalError",
                    message: "We encountered an internal error. Please try again.",
                    requestId: req.id)
            }
            objectData = data
        } else {
            // Range header was present but invalid/unsatisfiable, read entire file
            let (_, fullData) = try ObjectFileHandler.read(from: path)
            guard let data = fullData else {
                throw S3Error(
                    status: .internalServerError, code: "InternalError",
                    message: "We encountered an internal error. Please try again.",
                    requestId: req.id)
            }
            objectData = data
        }

        return S3Service.buildObjectMetadataResponse(
            meta: meta, includeBody: true, data: objectData, range: byteRange)
    }

    // DELETE /:bucketName/*key
    @Sendable
    func handleObjectDelete(req: Request) async throws -> HTTPStatus {
        let bucketName = try S3Service.extractBucketName(from: req)
        let keyPath = S3Service.extractObjectKey(from: req)

        _ = try await S3Service.authenticateWithCache(req: req, bucketName: bucketName)

        let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
        if FileManager.default.fileExists(atPath: path) {
            try FileManager.default.removeItem(atPath: path)
        }

        return .noContent
    }
}
