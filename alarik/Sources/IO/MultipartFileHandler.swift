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

    /// Returns the base directory for a multipart upload
    /// Structure: Storage/multipart/{bucketName}/{uploadId}/
    static func uploadPath(for bucketName: String, uploadId: String) -> String {
        let encodedBucket =
            bucketName.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucketName
        return "\(rootPath)\(encodedBucket)/\(uploadId)/"
    }

    /// Returns the path to the upload metadata file
    static func metadataPath(for bucketName: String, uploadId: String) -> String {
        return "\(uploadPath(for: bucketName, uploadId: uploadId))meta.json"
    }

    /// Returns the path for a specific part
    static func partPath(for bucketName: String, uploadId: String, partNumber: Int) -> String {
        return "\(uploadPath(for: bucketName, uploadId: uploadId))part-\(partNumber)"
    }

    /// Returns the path to a part's metadata
    static func partMetaPath(for bucketName: String, uploadId: String, partNumber: Int) -> String {
        return "\(uploadPath(for: bucketName, uploadId: uploadId))part-\(partNumber).meta"
    }

    /// Creates a new multipart upload and returns the upload ID
    static func createUpload(
        bucketName: String,
        key: String,
        contentType: String = "application/octet-stream",
        metadata: [String: String] = [:]
    ) throws -> String {
        let uploadId = generateUploadId()
        let uploadDir = uploadPath(for: bucketName, uploadId: uploadId)

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
        let metaPath = metadataPath(for: bucketName, uploadId: uploadId)
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
        let metaPath = metadataPath(for: bucketName, uploadId: uploadId)
        guard FileManager.default.fileExists(atPath: metaPath) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }

        let etag = S3Service.computeETag(data)

        // Write part data
        let partPath = partPath(for: bucketName, uploadId: uploadId, partNumber: partNumber)
        try data.write(to: URL(fileURLWithPath: partPath))

        // Write part metadata
        let partMeta = MultipartPartMeta(
            partNumber: partNumber,
            etag: etag,
            size: data.count,
            lastModified: Date()
        )
        let partMetaData = try jsonEncoder.encode(partMeta)
        let partMetaPath = partMetaPath(for: bucketName, uploadId: uploadId, partNumber: partNumber)
        try partMetaData.write(to: URL(fileURLWithPath: partMetaPath))

        return etag
    }

    /// Completes the multipart upload by concatenating parts and returns the final ETag
    static func completeUpload(
        bucketName: String,
        uploadId: String,
        parts: [(partNumber: Int, etag: String)],
        versioningStatus: VersioningStatus
    ) throws -> (etag: String, size: Int, versionId: String?) {
        // Read upload metadata
        let metaPath = metadataPath(for: bucketName, uploadId: uploadId)
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

        // Concatenate all parts
        var finalData = Data()
        var partEtags: [String] = []

        for part in sortedParts {
            let partFilePath = partPath(
                for: bucketName, uploadId: uploadId, partNumber: part.partNumber)

            guard FileManager.default.fileExists(atPath: partFilePath) else {
                throw NSError(
                    domain: "InvalidPart", code: 400,
                    userInfo: [
                        NSLocalizedDescriptionKey: "Part \(part.partNumber) does not exist"
                    ])
            }

            // Read and verify part metadata
            let partMetaFilePath = partMetaPath(
                for: bucketName, uploadId: uploadId, partNumber: part.partNumber)
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
            }

            // Read part data
            let partData = try Data(contentsOf: URL(fileURLWithPath: partFilePath))
            finalData.append(partData)
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

        // Create the final object using ObjectFileHandler
        let objectMeta = ObjectMeta(
            bucketName: bucketName,
            key: uploadMeta.key,
            size: finalData.count,
            contentType: uploadMeta.contentType,
            etag: finalEtag,
            metadata: uploadMeta.metadata,
            updatedAt: Date()
        )

        // Write object with versioning support
        var versionId: String? = nil

        if versioningStatus != .disabled {
            versionId = try ObjectFileHandler.writeVersioned(
                metadata: objectMeta,
                data: finalData,
                bucketName: bucketName,
                key: uploadMeta.key,
                versioningStatus: versioningStatus
            )
        } else {
            let objectPath = ObjectFileHandler.storagePath(for: bucketName, key: uploadMeta.key)
            try ObjectFileHandler.write(metadata: objectMeta, data: finalData, to: objectPath)
        }

        // Clean up multipart upload directory
        try abortUpload(bucketName: bucketName, uploadId: uploadId)

        return (finalEtag, finalData.count, versionId)
    }

    /// Aborts (deletes) a multipart upload and all its parts
    static func abortUpload(bucketName: String, uploadId: String) throws {
        let uploadDir = uploadPath(for: bucketName, uploadId: uploadId)

        guard FileManager.default.fileExists(atPath: uploadDir) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }

        try FileManager.default.removeItem(atPath: uploadDir)

        // Clean up empty bucket directory if needed
        let bucketDir = "\(rootPath)\(bucketName)/"
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
        let metaPath = metadataPath(for: bucketName, uploadId: uploadId)
        guard FileManager.default.fileExists(atPath: metaPath) else {
            throw NSError(
                domain: "NoSuchUpload", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "The specified upload does not exist"])
        }

        let uploadDir = uploadPath(for: bucketName, uploadId: uploadId)
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
        let encodedBucket =
            bucketName.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucketName
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
        let metaPath = metadataPath(for: bucketName, uploadId: uploadId)
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
        let metaPath = metadataPath(for: bucketName, uploadId: uploadId)
        return FileManager.default.fileExists(atPath: metaPath)
    }
}
