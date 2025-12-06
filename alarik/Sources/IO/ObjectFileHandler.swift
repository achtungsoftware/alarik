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

struct ObjectFileHandler {
    private static let jsonEncoder: JSONEncoder = {
        let encoder = JSONEncoder()
        return encoder
    }()

    private static let jsonDecoder: JSONDecoder = {
        let decoder = JSONDecoder()
        return decoder
    }()

    /// Writes metadata + data to a single file.
    static func write(metadata: ObjectMeta, data: Data, to path: String) throws {
        let fileURL = URL(fileURLWithPath: path)
        let fileManager = FileManager.default

        // Create directory only if it doesn't exist
        let folderURL = fileURL.deletingLastPathComponent()
        if !fileManager.fileExists(atPath: folderURL.path) {
            try fileManager.createDirectory(at: folderURL, withIntermediateDirectories: true)
        }

        // Serialize metadata to JSON
        let jsonData = try jsonEncoder.encode(metadata)
        let metadataLength = UInt32(jsonData.count)

        // Pre-allocate total size to avoid multiple reallocations
        var fileData = Data(capacity: 4 + jsonData.count + data.count)

        // Append 4-byte length (big-endian)
        withUnsafeBytes(of: metadataLength.bigEndian) {
            fileData.append(contentsOf: $0)
        }
        fileData.append(jsonData)
        fileData.append(data)

        // Write atomically
        try fileData.write(to: fileURL, options: .atomic)
    }

    /// Reads metadata + data from file. Set `loadData` to false for HEAD (metadata only).
    /// Optionally provide a byte range for partial reads.
    static func read(from path: String, loadData: Bool = true, range: (start: Int, end: Int)? = nil)
        throws -> (ObjectMeta, Data?)
    {
        // Use low-level file descriptor to avoid URL parsing overhead
        let fd = POSIXFile.open(path, O_RDONLY)
        guard fd >= 0 else {
            throw NSError(
                domain: "FileError", code: Int(errno),
                userInfo: [NSLocalizedDescriptionKey: "Could not open file"])
        }
        defer { _ = POSIXFile.close(fd) }

        // Read 4-byte length prefix
        var lengthBytes = [UInt8](repeating: 0, count: 4)
        let lengthRead = POSIXFile.read(fd, &lengthBytes, 4)
        guard lengthRead == 4 else {
            throw NSError(
                domain: "InvalidFile", code: 0,
                userInfo: [NSLocalizedDescriptionKey: "Missing length prefix"])
        }

        let metadataLength = UInt32(
            bigEndian: lengthBytes.withUnsafeBytes { $0.load(as: UInt32.self) })

        // Read metadata JSON
        var jsonBytes = [UInt8](repeating: 0, count: Int(metadataLength))
        let jsonRead = POSIXFile.read(fd, &jsonBytes, Int(metadataLength))
        guard jsonRead == Int(metadataLength) else {
            throw NSError(
                domain: "InvalidFile", code: 0,
                userInfo: [NSLocalizedDescriptionKey: "Incomplete metadata"])
        }

        let metadata = try jsonBytes.withUnsafeBufferPointer { buffer in
            try jsonDecoder.decode(ObjectMeta.self, from: Data(buffer))
        }

        // For metadata-only reads, we're done
        if !loadData {
            return (metadata, nil)
        }

        let dataOffset = 4 + Int(metadataLength)

        // For range reads
        if let range = range {
            // Seek to the start of the range (relative to object data)
            _ = POSIXFile.lseek(fd, off_t(dataOffset + range.start), SEEK_SET)

            let bytesToRead = range.end - range.start + 1
            var rangeBytes = [UInt8](repeating: 0, count: bytesToRead)
            let rangeRead = POSIXFile.read(fd, &rangeBytes, bytesToRead)
            guard rangeRead > 0 else {
                throw NSError(
                    domain: "InvalidFile", code: 0,
                    userInfo: [NSLocalizedDescriptionKey: "Could not read range"])
            }

            return (metadata, Data(rangeBytes[0..<rangeRead]))
        }

        // For full reads, get file size and read remaining data
        var statInfo = stat()
        guard POSIXFile.fstat(fd, &statInfo) == 0 else {
            throw NSError(
                domain: "FileError", code: Int(errno),
                userInfo: [NSLocalizedDescriptionKey: "Could not stat file"])
        }

        let fileSize = Int(statInfo.st_size)
        let dataSize = fileSize - dataOffset

        if dataSize <= 0 {
            return (metadata, Data())
        }

        var dataBytes = [UInt8](repeating: 0, count: dataSize)
        let dataRead = POSIXFile.read(fd, &dataBytes, dataSize)
        guard dataRead == dataSize else {
            throw NSError(
                domain: "InvalidFile", code: 0,
                userInfo: [NSLocalizedDescriptionKey: "Incomplete data"])
        }

        return (metadata, Data(dataBytes))
    }

    /// Generates the storage path for an object.
    static func storagePath(for bucketName: String, key: String) -> String {
        // Cache allowed character set
        let encodedBucket =
            bucketName.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucketName

        // Optimize path sanitization
        let sanitizedKey: String
        if key.contains("..") {
            let components = key.components(separatedBy: "/")
            sanitizedKey = components.map { $0.replacingOccurrences(of: "..", with: "") }.joined(
                separator: "/")
        } else {
            sanitizedKey = key
        }

        return "\(BucketHandler.rootPath)\(encodedBucket)/\(sanitizedKey).obj"
    }

    /// Checks if an object exists (as a directory/file).
    static func keyExists(for bucketName: String, key: String, path: String? = nil) -> Bool {
        let _path: String = path ?? storagePath(for: bucketName, key: key)
        guard FileManager.default.fileExists(atPath: _path) else {
            return false
        }

        return true
    }

    /// Checks if a bucket contains any objects.
    static func hasBucketObjects(bucketName: String) -> Bool {
        let bucketURL = BucketHandler.bucketURL(for: bucketName)

        guard FileManager.default.fileExists(atPath: bucketURL.path) else {
            return false
        }

        guard
            let enumerator = FileManager.default.enumerator(
                at: bucketURL,
                includingPropertiesForKeys: [.isDirectoryKey],
                options: [.skipsHiddenFiles]
            )
        else {
            return false
        }

        // Check if there's at least one .obj file
        for case let fileURL as URL in enumerator {
            if fileURL.pathExtension == "obj" {
                return true
            }
        }

        return false
    }

    /// Deletes an object from storage.
    static func delete(bucketName: String, key: String) throws {
        let path = storagePath(for: bucketName, key: key)

        guard FileManager.default.fileExists(atPath: path) else {
            throw NSError(
                domain: "ObjectNotFound", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "Object does not exist"])
        }

        try FileManager.default.removeItem(atPath: path)
    }

    /// Deletes all objects with a given prefix (folder deletion).
    static func deletePrefix(bucketName: String, prefix: String) throws -> Int {
        // Sanitize the prefix - remove path traversal attempts
        var sanitizedPrefix = prefix
        if sanitizedPrefix.contains("..") {
            let components = sanitizedPrefix.components(separatedBy: "/")
            sanitizedPrefix = components.map { $0.replacingOccurrences(of: "..", with: "") }
                .filter { !$0.isEmpty }
                .joined(separator: "/")

            // Add trailing slash back if original had it
            if prefix.hasSuffix("/") && !sanitizedPrefix.hasSuffix("/") {
                sanitizedPrefix += "/"
            }
        }

        // Validate prefix is not empty or just "/"
        guard !sanitizedPrefix.isEmpty && sanitizedPrefix != "/" else {
            throw NSError(
                domain: "InvalidPrefix", code: 400,
                userInfo: [NSLocalizedDescriptionKey: "Invalid prefix for deletion"])
        }

        let bucketURL = BucketHandler.bucketURL(for: bucketName)

        guard FileManager.default.fileExists(atPath: bucketURL.path) else {
            return 0
        }

        guard
            let enumerator = FileManager.default.enumerator(
                at: bucketURL,
                includingPropertiesForKeys: [.isDirectoryKey],
                options: [.skipsHiddenFiles]
            )
        else {
            return 0
        }

        var deletedCount = 0

        // Collect all objects with the prefix
        for case let fileURL as URL in enumerator {
            guard fileURL.pathExtension == "obj" else { continue }

            // Get relative path from bucket root
            let relativePath = fileURL.path.replacingOccurrences(
                of: bucketURL.path + "/",
                with: ""
            )

            // Remove .obj extension to get the key
            let key = String(relativePath.dropLast(4))

            // Check if this key starts with the sanitized prefix
            if key.hasPrefix(sanitizedPrefix) {
                try? FileManager.default.removeItem(at: fileURL)
                deletedCount += 1
            }
        }

        return deletedCount
    }

    /// Lists all objects in a bucket with optional prefix filtering
    static func listObjects(
        bucketName: String,
        prefix: String = "",
        delimiter: String? = nil,
        maxKeys: Int = 1000,
        marker: String? = nil
    ) throws -> (
        objects: [ObjectMeta], commonPrefixes: [String], isTruncated: Bool, nextMarker: String?
    ) {
        let bucketURL = BucketHandler.bucketURL(for: bucketName)

        guard FileManager.default.fileExists(atPath: bucketURL.path) else {
            return ([], [], false, nil)
        }

        guard
            let enumerator = FileManager.default.enumerator(
                at: bucketURL,
                includingPropertiesForKeys: [.isDirectoryKey],
                options: [.skipsHiddenFiles]
            )
        else {
            return ([], [], false, nil)
        }

        var allObjects: [(key: String, meta: ObjectMeta)] = []
        var commonPrefixesSet: Set<String> = []

        // Collect all objects
        for case let fileURL as URL in enumerator {
            guard fileURL.pathExtension == "obj" else { continue }

            // Get relative path from bucket root
            let relativePath = fileURL.path.replacingOccurrences(
                of: bucketURL.path + "/",
                with: ""
            )

            // Remove .obj extension to get the key
            let key = String(relativePath.dropLast(4))

            // Apply prefix filter
            if !prefix.isEmpty && !key.hasPrefix(prefix) {
                continue
            }

            // Skip if before marker
            if let marker = marker, key <= marker {
                continue
            }

            // Handle delimiter (for directory-like listing)
            if let delimiter = delimiter, !delimiter.isEmpty {
                // Only support single-character delimiters (like S3: usually "/")
                guard delimiter.count == 1, let delimChar = delimiter.first else {
                    continue  // or throw – invalid delimiter
                }

                let keyAfterPrefix = String(key.dropFirst(prefix.count))
                if let delimiterIndex = keyAfterPrefix.firstIndex(of: delimChar) {
                    let prefixSlice = String(keyAfterPrefix[..<delimiterIndex])
                    let commonPrefix = prefix + prefixSlice + delimiter
                    commonPrefixesSet.insert(commonPrefix)
                    continue
                }
            }

            // Read metadata
            do {
                let (meta, _) = try read(from: fileURL.path, loadData: false)
                allObjects.append((key: key, meta: meta))
            } catch {
                // Skip objects with read errors
                continue
            }
        }

        // Sort objects by key
        allObjects.sort { $0.key < $1.key }

        // Sort common prefixes
        let sortedCommonPrefixes = commonPrefixesSet.sorted()

        // S3 behavior: maxKeys limits the TOTAL number of keys + common prefixes returned
        // We need to interleave objects and common prefixes in sorted order, then limit
        var finalObjects: [ObjectMeta] = []
        var finalCommonPrefixes: [String] = []
        var isTruncated = false
        var nextMarker: String? = nil

        var objectIndex = 0
        var prefixIndex = 0
        var totalCount = 0

        while totalCount < maxKeys
            && (objectIndex < allObjects.count || prefixIndex < sortedCommonPrefixes.count)
        {
            let nextObject = objectIndex < allObjects.count ? allObjects[objectIndex].key : nil
            let nextPrefix =
                prefixIndex < sortedCommonPrefixes.count ? sortedCommonPrefixes[prefixIndex] : nil

            // Determine which comes first lexicographically
            if let obj = nextObject, let pfx = nextPrefix {
                if obj < pfx {
                    finalObjects.append(allObjects[objectIndex].meta)
                    nextMarker = obj
                    objectIndex += 1
                } else {
                    finalCommonPrefixes.append(pfx)
                    nextMarker = pfx
                    prefixIndex += 1
                }
            } else if let obj = nextObject {
                finalObjects.append(allObjects[objectIndex].meta)
                nextMarker = obj
                objectIndex += 1
            } else if let pfx = nextPrefix {
                finalCommonPrefixes.append(pfx)
                nextMarker = pfx
                prefixIndex += 1
            }

            totalCount += 1
        }

        // Check if there are more items
        if objectIndex < allObjects.count || prefixIndex < sortedCommonPrefixes.count {
            isTruncated = true
        } else {
            nextMarker = nil
        }

        return (finalObjects, finalCommonPrefixes, isTruncated, nextMarker)
    }
}
