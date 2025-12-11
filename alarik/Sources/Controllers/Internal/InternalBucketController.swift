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
import Foundation
import Vapor
import XMLCoder
import ZIPFoundation

struct InternalBucketController: RouteCollection {
    struct UploadInput: Content {
        let data: File
    }

    struct VersioningStatusDTO: Content {
        let status: String
    }

    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("buckets").get(use: self.listBuckets)
        routes.grouped("buckets").post(use: self.createBucket)
        routes.grouped("buckets").grouped(":bucketName").delete(use: self.deleteBucket)
        routes.grouped("buckets").grouped(":bucketName").grouped("versioning").get(
            use: self.getVersioning)
        routes.grouped("buckets").grouped(":bucketName").grouped("versioning").put(
            use: self.setVersioning)
        routes.grouped("objects").get(use: self.listObjects)
        routes.grouped("objects").post(use: self.uploadObject)
        routes.grouped("objects").delete(use: self.deleteObject)
        routes.grouped("objects", "download").post(use: self.downloadObjects)
        routes.grouped("objects", "versions").get(use: self.listObjectVersions)
        routes.grouped("objects", "version").delete(use: self.deleteObjectVersion)
    }

    @Sendable
    func listBuckets(req: Request) async throws -> Page<Bucket> {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)
        return try await Bucket.query(on: req.db)
            .filter(\.$user.$id == sessionToken.userId)
            .sort(\.$creationDate, .descending)
            .paginate(for: req)
    }

    @Sendable
    func createBucket(req: Request) async throws -> Bucket.ResponseDTO {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        try Bucket.Create.validate(content: req)

        let create: Bucket.Create = try req.content.decode(Bucket.Create.self)

        try await BucketService.create(
            on: req.db, bucketName: create.name, userId: sessionToken.userId,
            versioningEnabled: create.versioningEnabled)

        // Fetch the created bucket from the database to get the ID
        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == create.name)
                .filter(\.$user.$id == sessionToken.userId)
                .first()
        else {
            throw Abort(.internalServerError, reason: "Failed to retrieve created bucket")
        }

        return bucket.toResponseDTO()
    }

    @Sendable
    func deleteBucket(req: Request) async throws -> HTTPStatus {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        // Verify bucket exists and belongs to user
        guard
            try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .filter(\.$user.$id == sessionToken.userId)
                .first() != nil
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        // Check if bucket is empty
        if ObjectFileHandler.hasBucketObjects(bucketName: bucketName) {
            throw Abort(.conflict, reason: "The bucket is not empty")
        }

        try await BucketService.delete(
            on: req.db, bucketName: bucketName, userId: sessionToken.userId)

        return .noContent
    }

    @Sendable
    func listObjects(req: Request) async throws -> Page<ObjectMeta.ResponseDTO> {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        // Verify bucket exists and belongs to user
        guard
            try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .filter(\.$user.$id == sessionToken.userId)
                .first() != nil
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        let prefix = req.query[String.self, at: "prefix"] ?? ""
        let delimiter = req.query[String.self, at: "delimiter"] ?? "/"

        let (objects, commonPrefixes, _, _) = try ObjectFileHandler.listObjects(
            bucketName: bucketName,
            prefix: prefix,
            delimiter: delimiter,
            maxKeys: 10000
        )

        // Convert objects to DTOs
        var items: [ObjectMeta.ResponseDTO] = []

        // Add folders (common prefixes)
        for commonPrefix in commonPrefixes {
            items.append(ObjectMeta.ResponseDTO(folderKey: commonPrefix))
        }

        // Add files
        for object in objects {
            items.append(ObjectMeta.ResponseDTO(from: object))
        }

        // Sort: folders first, then by name
        items.sort { a, b in
            if a.isFolder != b.isFolder {
                return a.isFolder
            }
            return a.key < b.key
        }

        // Simple pagination for now
        let page = req.query[Int.self, at: "page"] ?? 1
        let per = req.query[Int.self, at: "per"] ?? 100
        let startIndex = (page - 1) * per
        let endIndex = min(startIndex + per, items.count)

        let paginatedItems = startIndex < items.count ? Array(items[startIndex..<endIndex]) : []

        return Page(
            items: paginatedItems,
            metadata: PageMetadata(
                page: page,
                per: per,
                total: items.count
            )
        )
    }

    @Sendable
    func uploadObject(req: Request) async throws -> ObjectMeta.ResponseDTO {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        let prefix = req.query[String.self, at: "prefix"] ?? ""

        // Verify bucket exists and belongs to user
        guard
            try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .filter(\.$user.$id == sessionToken.userId)
                .first() != nil
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        // Parse multipart form data
        let input = try req.content.decode(UploadInput.self)

        let filename = input.data.filename
        guard !filename.isEmpty else {
            throw Abort(.badRequest, reason: "File must have a filename")
        }

        // Construct the full key path (prefix + filename)
        let keyPath = prefix.isEmpty ? filename : "\(prefix)\(filename)"

        // Read file data
        let fileData = Data(buffer: input.data.data)

        // Calculate ETag
        let etag = Insecure.MD5.hash(data: fileData).hex

        // Create object metadata
        var meta = ObjectMeta(
            bucketName: bucketName,
            key: keyPath,
            size: fileData.count,
            contentType: input.data.contentType?.description ?? "application/octet-stream",
            etag: etag,
            updatedAt: Date()
        )

        // Get bucket versioning status from cache
        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        // Write object with versioning support
        if versioningStatus != .disabled {
            let versionId = try ObjectFileHandler.writeVersioned(
                metadata: meta,
                data: fileData,
                bucketName: bucketName,
                key: keyPath,
                versioningStatus: versioningStatus
            )
            meta.versionId = versionId
            meta.isLatest = true
        } else {
            let path = ObjectFileHandler.storagePath(for: bucketName, key: keyPath)
            try ObjectFileHandler.write(metadata: meta, data: fileData, to: path)
        }

        return ObjectMeta.ResponseDTO(from: meta)
    }

    @Sendable
    func deleteObject(req: Request) async throws -> HTTPStatus {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        guard let key = req.query[String.self, at: "key"] else {
            throw Abort(.badRequest, reason: "Missing 'key' query parameter")
        }

        // Verify bucket exists and belongs to user
        guard
            try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .filter(\.$user.$id == sessionToken.userId)
                .first() != nil
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        let versioningStatus = await BucketVersioningCache.shared.getStatus(for: bucketName)

        // Check if this is a folder (prefix) deletion
        if key.hasSuffix("/") {
            // Delete all objects with this prefix
            _ = try ObjectFileHandler.deletePrefix(bucketName: bucketName, prefix: key)
        } else if versioningStatus == .enabled {
            // Versioning enabled - create delete marker instead of permanent delete
            _ = try ObjectFileHandler.createDeleteMarker(bucketName: bucketName, key: key)
        } else {
            // Versioning disabled or suspended - permanent delete
            // Check versioned storage first
            if ObjectFileHandler.isVersioned(bucketName: bucketName, key: key) {
                // Delete all versions
                let versions = try ObjectFileHandler.listVersions(bucketName: bucketName, key: key)
                for version in versions {
                    if let vid = version.versionId {
                        try? ObjectFileHandler.deleteVersion(
                            bucketName: bucketName, key: key, versionId: vid)
                    }
                }
            }

            // Also delete non-versioned path if exists
            let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
            if FileManager.default.fileExists(atPath: path) {
                try FileManager.default.removeItem(atPath: path)
            }
        }

        return .noContent
    }

    @Sendable
    func downloadObjects(req: Request) async throws -> Response {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)
        let input = try req.content.decode(DownloadRequestDTO.self)

        guard !input.keys.isEmpty else {
            throw Abort(.badRequest, reason: "No keys provided for download")
        }

        // Verify bucket exists and belongs to user
        guard
            try await Bucket.query(on: req.db)
                .filter(\.$name == input.bucket)
                .filter(\.$user.$id == sessionToken.userId)
                .first() != nil
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        // If single file, download directly
        if input.keys.count == 1 && !input.keys[0].hasSuffix("/") {
            return try await downloadSingleFile(
                req: req, bucketName: input.bucket, key: input.keys[0], versionId: input.versionId)
        }

        // Multiple files or folders - create ZIP
        return try await downloadAsZip(req: req, bucketName: input.bucket, keys: input.keys)
    }

    private func downloadSingleFile(
        req: Request, bucketName: String, key: String, versionId: String? = nil
    ) async throws
        -> Response
    {
        // Try versioned storage first, then fall back to non-versioned
        let meta: ObjectMeta
        let fileData: Data

        do {
            let (m, data) = try ObjectFileHandler.readVersion(
                bucketName: bucketName,
                key: key,
                versionId: versionId,
                loadData: true
            )

            // Check if latest version is a delete marker
            if m.isDeleteMarker {
                throw Abort(.notFound, reason: "Object not found")
            }

            guard let d = data else {
                throw Abort(.internalServerError, reason: "Failed to read object data")
            }

            meta = m
            fileData = d
        } catch let error as Abort {
            throw error
        } catch {
            // Fall back to non-versioned path
            let path = ObjectFileHandler.storagePath(for: bucketName, key: key)

            guard FileManager.default.fileExists(atPath: path) else {
                throw Abort(.notFound, reason: "Object not found")
            }

            let (m, data) = try ObjectFileHandler.read(from: path, loadData: true)

            guard let d = data else {
                throw Abort(.internalServerError, reason: "Failed to read object data")
            }

            meta = m
            fileData = d
        }

        let response = Response(status: .ok, body: .init(data: fileData))
        response.headers.contentType = HTTPMediaType(
            type: meta.contentType.split(separator: "/").first.map(String.init) ?? "application",
            subType: meta.contentType.split(separator: "/").last.map(String.init)
                ?? "octet-stream"
        )
        response.headers.replaceOrAdd(
            name: "Content-Disposition",
            value: "attachment; filename=\"\(key.split(separator: "/").last ?? "download")\""
        )
        response.headers.replaceOrAdd(
            name: .contentLength,
            value: String(fileData.count)
        )

        return response
    }

    private func downloadAsZip(req: Request, bucketName: String, keys: [String]) async throws
        -> Response
    {
        let tempDir = FileManager.default.temporaryDirectory
        let zipFileName = "download-\(UUID().uuidString).zip"
        let zipURL = tempDir.appendingPathComponent(zipFileName)

        // Create ZIP archive
        let archive: Archive
        do {
            archive = try Archive(
                url: zipURL, accessMode: .create)
        } catch {
            throw Abort(.internalServerError, reason: "Failed to create ZIP archive: \(error)")
        }

        var addedFiles = 0

        // Helper function to read object data (versioned or non-versioned)
        func readObjectData(bucketName: String, key: String) -> Data? {
            // Try versioned storage first
            if let (meta, data) = try? ObjectFileHandler.readVersion(
                bucketName: bucketName,
                key: key,
                versionId: nil,
                loadData: true
            ), !meta.isDeleteMarker, let fileData = data {
                return fileData
            }

            // Fall back to non-versioned path
            let path = ObjectFileHandler.storagePath(for: bucketName, key: key)
            if FileManager.default.fileExists(atPath: path),
                let (_, data) = try? ObjectFileHandler.read(from: path, loadData: true),
                let fileData = data
            {
                return fileData
            }

            return nil
        }

        for key in keys {
            if key.hasSuffix("/") {
                // It's a folder - add all files with this prefix
                let (objects, _, _, _) = try ObjectFileHandler.listObjects(
                    bucketName: bucketName,
                    prefix: key,
                    delimiter: nil,  // No delimiter to get all nested files
                    maxKeys: 10000
                )

                for object in objects {
                    // Skip delete markers
                    if object.isDeleteMarker { continue }

                    if let fileData = readObjectData(bucketName: bucketName, key: object.key) {
                        // Use relative path from the folder prefix
                        let relativePath = String(object.key.dropFirst(key.count))
                        let zipEntryPath = relativePath.isEmpty ? object.key : relativePath

                        try archive.addEntry(
                            with: zipEntryPath, type: .file,
                            uncompressedSize: Int64(fileData.count),
                            bufferSize: 4096,
                            provider: { position, size in
                                let start = Int(position)
                                let end = min(start + size, fileData.count)
                                return fileData[start..<end]
                            })
                        addedFiles += 1
                    }
                }
            } else {
                // Single file - use just the filename without path
                if let fileData = readObjectData(bucketName: bucketName, key: key) {
                    // Extract just the filename from the full key path
                    let filename = key.split(separator: "/").last.map(String.init) ?? key

                    try archive.addEntry(
                        with: filename, type: .file, uncompressedSize: Int64(fileData.count),
                        bufferSize: 4096,
                        provider: { position, size in
                            let start = Int(position)
                            let end = min(start + size, fileData.count)
                            return fileData[start..<end]
                        })
                    addedFiles += 1
                }
            }
        }

        guard addedFiles > 0 else {
            try? FileManager.default.removeItem(at: zipURL)
            throw Abort(.notFound, reason: "No files found to download")
        }

        // Read the ZIP file
        let zipData = try Data(contentsOf: zipURL)

        // Clean up
        try? FileManager.default.removeItem(at: zipURL)

        let response = Response(status: .ok, body: .init(data: zipData))
        response.headers.contentType = .zip
        response.headers.replaceOrAdd(
            name: "Content-Disposition",
            value: "attachment; filename=\"\(bucketName)-download.zip\""
        )
        response.headers.replaceOrAdd(
            name: .contentLength,
            value: String(zipData.count)
        )

        return response
    }

    @Sendable
    func getVersioning(req: Request) async throws -> VersioningStatusDTO {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        // Verify bucket exists and belongs to user
        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .filter(\.$user.$id == sessionToken.userId)
                .first()
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        return VersioningStatusDTO(status: bucket.versioningStatus)
    }

    @Sendable
    func setVersioning(req: Request) async throws -> VersioningStatusDTO {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let bucketName = req.parameters.get("bucketName") else {
            throw Abort(.badRequest, reason: "Missing bucket name")
        }

        let input = try req.content.decode(VersioningStatusDTO.self)

        guard let newStatus = VersioningStatus(rawValue: input.status) else {
            throw Abort(
                .badRequest,
                reason: "Invalid versioning status. Use 'Enabled', 'Suspended', or 'Disabled'")
        }

        guard
            let bucket = try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .filter(\.$user.$id == sessionToken.userId)
                .first()
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        bucket.versioningStatus = newStatus.rawValue
        try await bucket.save(on: req.db)

        await BucketVersioningCache.shared.setStatus(for: bucketName, status: newStatus)

        return VersioningStatusDTO(status: newStatus.rawValue)
    }

    @Sendable
    func listObjectVersions(req: Request) async throws -> [ObjectMeta.ResponseDTO] {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        guard let key = req.query[String.self, at: "key"] else {
            throw Abort(.badRequest, reason: "Missing 'key' query parameter")
        }

        guard
            try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .filter(\.$user.$id == sessionToken.userId)
                .first() != nil
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        let versions = try ObjectFileHandler.listVersions(bucketName: bucketName, key: key)

        return versions.map { ObjectMeta.ResponseDTO(from: $0) }
    }

    @Sendable
    func deleteObjectVersion(req: Request) async throws -> HTTPStatus {
        let sessionToken: SessionToken = try req.auth.require(SessionToken.self)

        guard let bucketName = req.query[String.self, at: "bucket"] else {
            throw Abort(.badRequest, reason: "Missing 'bucket' query parameter")
        }

        guard let key = req.query[String.self, at: "key"] else {
            throw Abort(.badRequest, reason: "Missing 'key' query parameter")
        }

        guard let versionId = req.query[String.self, at: "versionId"] else {
            throw Abort(.badRequest, reason: "Missing 'versionId' query parameter")
        }

        guard
            try await Bucket.query(on: req.db)
                .filter(\.$name == bucketName)
                .filter(\.$user.$id == sessionToken.userId)
                .first() != nil
        else {
            throw Abort(.notFound, reason: "Bucket not found")
        }

        try ObjectFileHandler.deleteVersion(bucketName: bucketName, key: key, versionId: versionId)

        return .noContent
    }
}
