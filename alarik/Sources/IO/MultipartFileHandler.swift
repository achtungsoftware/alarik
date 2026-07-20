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
import Foundation

struct MultipartFileHandler {
    private static let jsonEncoder: JSONEncoder = {
        let encoder = JSONEncoder()
        return encoder
    }()

    private static let jsonDecoder: JSONDecoder = {
        let decoder = JSONDecoder()
        return decoder
    }()

    /// Root path for multipart uploads: Storage/multipart/
    static let rootPath = "Storage/multipart/"

    /// Generates a new upload ID (same pattern as ObjectMeta.generateVersionId)
    static func generateUploadId() -> String {
        UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
    }

    /// Every public function below accepts an `uploadId` echoed back from the client - unlike
    /// `key`, whose `..` components get stripped before touching disk. A crafted value like
    /// `../../otherbucket/<id>` could otherwise escape `Storage/multipart/{bucket}/`, so every
    /// path-building function routes through `uploadPath`, validating here once before any path
    /// is constructed. Rejection matches the "no such upload" case; a real upload ID never fails.
    private static func validateUploadId(_ uploadId: String) throws {
        guard uploadId.count == 32, uploadId.allSatisfy({ $0.isHexDigit && !$0.isUppercase }) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }
    }

    /// Returns the base directory for a multipart upload
    /// Structure: Storage/multipart/{bucketName}/{uploadId}/
    static func uploadPath(for bucketName: String, uploadId: String) throws -> String {
        try validateUploadId(uploadId)
        let encodedBucket = BucketHandler.encodedBucketName(bucketName)
        return "\(rootPath)\(encodedBucket)/\(uploadId)/"
    }

    /// Returns the path to the upload metadata file
    static func metadataPath(for bucketName: String, uploadId: String) throws -> String {
        return "\(try uploadPath(for: bucketName, uploadId: uploadId))meta.json"
    }

    /// Returns the path for a specific part
    static func partPath(for bucketName: String, uploadId: String, partNumber: Int) throws -> String {
        return "\(try uploadPath(for: bucketName, uploadId: uploadId))part-\(partNumber)"
    }

    /// Returns the path to a part's metadata
    static func partMetaPath(for bucketName: String, uploadId: String, partNumber: Int) throws
        -> String
    {
        return "\(try uploadPath(for: bucketName, uploadId: uploadId))part-\(partNumber).meta"
    }

    /// Creates a new multipart upload and returns the upload ID
    static func createUpload(
        bucketName: String,
        key: String,
        contentType: String = "application/octet-stream",
        metadata: [String: String] = [:]
    ) throws -> String {
        let uploadId = generateUploadId()
        let uploadDir = try uploadPath(for: bucketName, uploadId: uploadId)

        // Create the upload directory
        try FileManager.default.createDirectory(
            atPath: uploadDir,
            withIntermediateDirectories: true
        )

        // Write upload metadata
        let meta = MultipartUploadMeta(
            uploadId: uploadId,
            bucketName: bucketName,
            key: key,
            contentType: contentType,
            metadata: metadata,
            initiated: Date()
        )

        let metaData = try jsonEncoder.encode(meta)
        let metaPath = try metadataPath(for: bucketName, uploadId: uploadId)
        try metaData.write(to: URL(fileURLWithPath: metaPath))

        return uploadId
    }

    /// Writes a part and returns its ETag
    static func writePart(
        bucketName: String,
        uploadId: String,
        partNumber: Int,
        data: Data
    ) throws -> String {
        // Validate part number (S3 allows 1-10000)
        guard partNumber >= 1 && partNumber <= 10000 else {
            throw NSError(
                domain: "InvalidPartNumber", code: 400,
                userInfo: [NSLocalizedDescriptionKey: "Part number must be between 1 and 10000"])
        }

        // Verify upload exists
        let metaPath = try metadataPath(for: bucketName, uploadId: uploadId)
        guard FileManager.default.fileExists(atPath: metaPath) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }

        let etag = S3Service.computeETag(data)

        // Write part data
        let partPath = try partPath(for: bucketName, uploadId: uploadId, partNumber: partNumber)
        try data.write(to: URL(fileURLWithPath: partPath))

        try writePartMeta(
            bucketName: bucketName, uploadId: uploadId, partNumber: partNumber,
            etag: etag, size: data.count)

        return etag
    }

    /// `writePart` for a body already spooled to disk: the spool file is renamed into place
    /// (same filesystem - Storage/spool and Storage/multipart share a root), so part uploads
    /// of any size never pass through memory. `etag` is the payload MD5 computed while
    /// spooling. Consumes the spool file on success.
    static func writePartStreamed(
        bucketName: String,
        uploadId: String,
        partNumber: Int,
        spoolPath: String,
        etag: String,
        size: Int
    ) throws -> String {
        guard partNumber >= 1 && partNumber <= 10000 else {
            throw NSError(
                domain: "InvalidPartNumber", code: 400,
                userInfo: [NSLocalizedDescriptionKey: "Part number must be between 1 and 10000"])
        }
        let metaPath = try metadataPath(for: bucketName, uploadId: uploadId)
        guard FileManager.default.fileExists(atPath: metaPath) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }

        let partPath = try partPath(for: bucketName, uploadId: uploadId, partNumber: partNumber)

        // Same durability contract as object writes: flush the part before acknowledging it,
        // otherwise a crash between UploadPart's 200 and CompleteMultipartUpload could lose
        // bytes the client believes are stored.
        if Durability.fsyncEnabled {
            let fd = POSIXFile.open(spoolPath, O_RDONLY)
            guard fd >= 0 else {
                throw NSError(
                    domain: "FileError", code: Int(errno),
                    userInfo: [NSLocalizedDescriptionKey: "Could not open spooled part"])
            }
            let flushed = Durability.flush(fd)
            _ = POSIXFile.close(fd)
            guard flushed == 0 else {
                throw NSError(
                    domain: "FileError", code: Int(errno),
                    userInfo: [NSLocalizedDescriptionKey: "Could not flush part to disk"])
            }
        }
        if POSIXFile.rename(spoolPath, partPath) != 0 {
            // EXDEV: Storage/spool and Storage/multipart live on different filesystems
            // (unusual, but possible with per-directory mounts) - fall back to a copy
            guard errno == EXDEV else {
                throw NSError(
                    domain: "FileError", code: Int(errno),
                    userInfo: [NSLocalizedDescriptionKey: "Could not move part into place"])
            }
            try? FileManager.default.removeItem(atPath: partPath)
            try FileManager.default.copyItem(atPath: spoolPath, toPath: partPath)
        }

        try writePartMeta(
            bucketName: bucketName, uploadId: uploadId, partNumber: partNumber,
            etag: etag, size: size)

        return etag
    }

    /// `writePart` where the payload is a region of an existing file (UploadPartCopy):
    /// window-copies the region into the part file, computing the part's MD5 on the way.
    static func writePartFromFile(
        bucketName: String,
        uploadId: String,
        partNumber: Int,
        sourcePath: String,
        sourceOffset: Int,
        sourceSize: Int
    ) throws -> String {
        guard partNumber >= 1 && partNumber <= 10000 else {
            throw NSError(
                domain: "InvalidPartNumber", code: 400,
                userInfo: [NSLocalizedDescriptionKey: "Part number must be between 1 and 10000"])
        }
        let metaPath = try metadataPath(for: bucketName, uploadId: uploadId)
        guard FileManager.default.fileExists(atPath: metaPath) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }

        let sourceFd = POSIXFile.open(sourcePath, O_RDONLY)
        guard sourceFd >= 0 else {
            throw NSError(
                domain: "FileError", code: Int(errno),
                userInfo: [NSLocalizedDescriptionKey: "Could not open copy source"])
        }
        defer { _ = POSIXFile.close(sourceFd) }
        _ = POSIXFile.lseek(sourceFd, off_t(sourceOffset), SEEK_SET)

        let partPath = try partPath(for: bucketName, uploadId: uploadId, partNumber: partNumber)
        var writer = try AtomicObjectWriter(finalPath: partPath)
        var md5 = Insecure.MD5()
        do {
            let windowSize = Constants.fileCopyWindowSize
            var window = [UInt8](repeating: 0, count: windowSize)
            var remaining = sourceSize
            while remaining > 0 {
                let toRead = Swift.min(windowSize, remaining)
                let bytesRead = POSIXFile.read(sourceFd, &window, toRead)
                guard bytesRead > 0 else {
                    throw NSError(
                        domain: "InvalidFile", code: 0,
                        userInfo: [NSLocalizedDescriptionKey: "Copy source ended early"])
                }
                try window.withUnsafeBytes { raw in
                    let slice = UnsafeRawBufferPointer(rebasing: raw.prefix(bytesRead))
                    md5.update(bufferPointer: slice)
                    try writer.writeRaw(slice)
                }
                remaining -= bytesRead
            }
            try writer.finish()
        } catch {
            writer.abort()
            throw error
        }

        let etag = md5.finalize().hexString()
        try writePartMeta(
            bucketName: bucketName, uploadId: uploadId, partNumber: partNumber,
            etag: etag, size: sourceSize)
        return etag
    }

    private static func writePartMeta(
        bucketName: String, uploadId: String, partNumber: Int, etag: String, size: Int
    ) throws {
        let partMeta = MultipartPartMeta(
            partNumber: partNumber,
            etag: etag,
            size: size,
            lastModified: Date()
        )
        let partMetaData = try jsonEncoder.encode(partMeta)
        let partMetaPath = try partMetaPath(for: bucketName, uploadId: uploadId, partNumber: partNumber)
        try partMetaData.write(to: URL(fileURLWithPath: partMetaPath))
    }

    /// Everything `completeUpload` does short of the actual write: validates/orders parts,
    /// verifies each part's ETag, computes the S3-style multipart ETag, and builds the final
    /// `ObjectMeta` plus the `payloadSources` list the write step streams from. Split out so an
    /// erasure-coded completion (`ErasureCodedWriteCoordinator.write`) can reuse this identical
    /// validation/assembly logic - only the final write step differs.
    struct CompletionPlan {
        let key: String
        let objectMeta: ObjectMeta
        let payloadSources: [(path: String, offset: Int, size: Int)]
        let etag: String
        let totalSize: Int
    }

    static func prepareCompletion(
        bucketName: String,
        uploadId: String,
        parts: [(partNumber: Int, etag: String)]
    ) throws -> CompletionPlan {
        // Read upload metadata
        let metaPath = try metadataPath(for: bucketName, uploadId: uploadId)
        guard FileManager.default.fileExists(atPath: metaPath) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }

        let metaData = try Data(contentsOf: URL(fileURLWithPath: metaPath))
        let uploadMeta = try jsonDecoder.decode(MultipartUploadMeta.self, from: metaData)

        // Validate and sort parts by part number
        let sortedParts = parts.sorted { $0.partNumber < $1.partNumber }

        // Verify parts are in ascending order (S3 allows gaps, e.g., parts 1, 3, 7)
        // Check for duplicates
        var seenPartNumbers = Set<Int>()
        for part in sortedParts {
            if seenPartNumbers.contains(part.partNumber) {
                throw NSError(
                    domain: "InvalidPartOrder", code: 400,
                    userInfo: [
                        NSLocalizedDescriptionKey:
                            "Duplicate part number: \(part.partNumber)"
                    ])
            }
            seenPartNumbers.insert(part.partNumber)
        }

        // Collect part file regions - the final object is assembled by streaming these
        // straight into the .obj, never concatenated in memory, so upload size never bounds
        // process RAM.
        var payloadSources: [(path: String, offset: Int, size: Int)] = []
        var partEtags: [String] = []
        var totalSize = 0

        for part in sortedParts {
            let partFilePath = try partPath(
                for: bucketName, uploadId: uploadId, partNumber: part.partNumber)

            guard FileManager.default.fileExists(atPath: partFilePath) else {
                throw NSError(
                    domain: "InvalidPart", code: 400,
                    userInfo: [
                        NSLocalizedDescriptionKey: "Part \(part.partNumber) does not exist"
                    ])
            }

            // Read and verify part metadata
            let partMetaFilePath = try partMetaPath(
                for: bucketName, uploadId: uploadId, partNumber: part.partNumber)
            let partSize: Int
            if FileManager.default.fileExists(atPath: partMetaFilePath) {
                let partMetaData = try Data(contentsOf: URL(fileURLWithPath: partMetaFilePath))
                let partMeta = try jsonDecoder.decode(MultipartPartMeta.self, from: partMetaData)

                // Verify ETag matches
                if partMeta.etag != S3Service.normalizeETag(part.etag) {
                    throw NSError(
                        domain: "InvalidPart", code: 400,
                        userInfo: [
                            NSLocalizedDescriptionKey:
                                "ETag mismatch for part \(part.partNumber)"
                        ])
                }
                partSize = partMeta.size
            } else {
                // No part meta (legacy upload dir): size straight from the file
                let attrs = try FileManager.default.attributesOfItem(atPath: partFilePath)
                partSize = (attrs[.size] as? Int) ?? 0
            }

            payloadSources.append((path: partFilePath, offset: 0, size: partSize))
            totalSize += partSize
            partEtags.append(S3Service.normalizeETag(part.etag))
        }

        // Calculate final ETag (S3 style: MD5 of concatenated binary MD5 hashes + "-" + part count)
        // Convert each hex ETag back to binary and concatenate
        var binaryEtags = Data()
        for etag in partEtags {
            // Convert hex string to bytes
            var bytes = [UInt8]()
            var index = etag.startIndex
            while index < etag.endIndex {
                let nextIndex =
                    etag.index(index, offsetBy: 2, limitedBy: etag.endIndex) ?? etag.endIndex
                if let byte = UInt8(etag[index..<nextIndex], radix: 16) {
                    bytes.append(byte)
                }
                index = nextIndex
            }
            binaryEtags.append(contentsOf: bytes)
        }
        let etagHash = S3Service.computeETag(binaryEtags)
        let finalEtag = "\(etagHash)-\(sortedParts.count)"

        let objectMeta = ObjectMeta(
            bucketName: bucketName,
            key: uploadMeta.key,
            size: totalSize,
            contentType: uploadMeta.contentType,
            etag: finalEtag,
            metadata: uploadMeta.metadata,
            updatedAt: Date()
        )

        return CompletionPlan(
            key: uploadMeta.key, objectMeta: objectMeta, payloadSources: payloadSources,
            etag: finalEtag, totalSize: totalSize)
    }

    /// Plain (non-EC) completion: writes the assembled object as a single `.obj` file, streamed
    /// from the part files in fixed-size windows, then cleans up the upload directory.
    static func completeUpload(
        bucketName: String,
        uploadId: String,
        parts: [(partNumber: Int, etag: String)],
        versioningStatus: VersioningStatus
    ) throws -> (etag: String, size: Int, versionId: String?) {
        let plan = try prepareCompletion(bucketName: bucketName, uploadId: uploadId, parts: parts)

        var versionId: String? = nil
        if versioningStatus != .disabled {
            versionId = try ObjectFileHandler.writeVersionedStreamed(
                metadata: plan.objectMeta,
                payloadSources: plan.payloadSources,
                bucketName: bucketName,
                key: plan.key,
                versioningStatus: versioningStatus
            )
        } else {
            let objectPath = ObjectFileHandler.storagePath(for: bucketName, key: plan.key)
            try ObjectFileHandler.writeStreamed(
                metadata: plan.objectMeta, payloadSources: plan.payloadSources, to: objectPath)
        }

        try abortUpload(bucketName: bucketName, uploadId: uploadId)
        return (plan.etag, plan.totalSize, versionId)
    }

    /// Aborts (deletes) a multipart upload and all its parts
    static func abortUpload(bucketName: String, uploadId: String) throws {
        let uploadDir = try uploadPath(for: bucketName, uploadId: uploadId)

        guard FileManager.default.fileExists(atPath: uploadDir) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }

        try FileManager.default.removeItem(atPath: uploadDir)

        // Clean up empty bucket directory if needed
        let bucketDir = "\(rootPath)\(BucketHandler.encodedBucketName(bucketName))/"
        if let contents = try? FileManager.default.contentsOfDirectory(atPath: bucketDir),
            contents.isEmpty
        {
            try? FileManager.default.removeItem(atPath: bucketDir)
        }
    }

    /// Lists all parts for an upload
    static func listParts(
        bucketName: String,
        uploadId: String,
        maxParts: Int = 1000,
        partNumberMarker: Int = 0
    ) throws -> (parts: [MultipartPartMeta], isTruncated: Bool, nextPartNumberMarker: Int?) {
        let metaPath = try metadataPath(for: bucketName, uploadId: uploadId)
        guard FileManager.default.fileExists(atPath: metaPath) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }

        let uploadDir = try uploadPath(for: bucketName, uploadId: uploadId)
        let contents = try FileManager.default.contentsOfDirectory(atPath: uploadDir)

        var parts: [MultipartPartMeta] = []

        for filename in contents {
            // Look for part metadata files
            if filename.hasSuffix(".meta") && filename.hasPrefix("part-") {
                let partMetaPath = "\(uploadDir)\(filename)"
                let partMetaData = try Data(contentsOf: URL(fileURLWithPath: partMetaPath))
                let partMeta = try jsonDecoder.decode(MultipartPartMeta.self, from: partMetaData)

                // Apply marker filter
                if partMeta.partNumber > partNumberMarker {
                    parts.append(partMeta)
                }
            }
        }

        // Sort by part number
        parts.sort { $0.partNumber < $1.partNumber }

        // Apply max limit
        let isTruncated = parts.count > maxParts
        let limitedParts = Array(parts.prefix(maxParts))
        let nextMarker = isTruncated ? limitedParts.last?.partNumber : nil

        return (limitedParts, isTruncated, nextMarker)
    }

    /// Lists all in-progress multipart uploads for a bucket
    static func listUploads(
        bucketName: String,
        prefix: String = "",
        keyMarker: String? = nil,
        uploadIdMarker: String? = nil,
        maxUploads: Int = 1000
    ) throws -> (
        uploads: [MultipartUploadMeta], isTruncated: Bool, nextKeyMarker: String?,
        nextUploadIdMarker: String?
    ) {
        let encodedBucket = BucketHandler.encodedBucketName(bucketName)
        let bucketDir = "\(rootPath)\(encodedBucket)/"

        guard FileManager.default.fileExists(atPath: bucketDir) else {
            return ([], false, nil, nil)
        }

        let uploadIds = try FileManager.default.contentsOfDirectory(atPath: bucketDir)
        var uploads: [MultipartUploadMeta] = []

        for uploadId in uploadIds {
            let metaPath = "\(bucketDir)\(uploadId)/meta.json"
            guard FileManager.default.fileExists(atPath: metaPath) else {
                continue
            }

            let metaData = try Data(contentsOf: URL(fileURLWithPath: metaPath))
            let meta = try jsonDecoder.decode(MultipartUploadMeta.self, from: metaData)

            // Apply prefix filter
            if !prefix.isEmpty && !meta.key.hasPrefix(prefix) {
                continue
            }

            // Apply marker filter
            if let keyMarker = keyMarker {
                if meta.key < keyMarker {
                    continue
                }
                if meta.key == keyMarker, let uploadIdMarker = uploadIdMarker {
                    if meta.uploadId <= uploadIdMarker {
                        continue
                    }
                }
            }

            uploads.append(meta)
        }

        // Sort by key, then by uploadId
        uploads.sort {
            if $0.key != $1.key {
                return $0.key < $1.key
            }
            return $0.uploadId < $1.uploadId
        }

        // Apply limit
        let isTruncated = uploads.count > maxUploads
        let limitedUploads = Array(uploads.prefix(maxUploads))

        var nextKeyMarker: String? = nil
        var nextUploadIdMarker: String? = nil

        if isTruncated, let last = limitedUploads.last {
            nextKeyMarker = last.key
            nextUploadIdMarker = last.uploadId
        }

        return (limitedUploads, isTruncated, nextKeyMarker, nextUploadIdMarker)
    }

    /// Gets the metadata for an upload
    static func getUploadMeta(bucketName: String, uploadId: String) throws -> MultipartUploadMeta {
        let metaPath = try metadataPath(for: bucketName, uploadId: uploadId)
        guard FileManager.default.fileExists(atPath: metaPath) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }

        let metaData = try Data(contentsOf: URL(fileURLWithPath: metaPath))
        return try jsonDecoder.decode(MultipartUploadMeta.self, from: metaData)
    }

    /// Checks if an upload exists
    static func uploadExists(bucketName: String, uploadId: String) -> Bool {
        guard let metaPath = try? metadataPath(for: bucketName, uploadId: uploadId) else {
            return false
        }
        return FileManager.default.fileExists(atPath: metaPath)
    }
}
