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

    /// Checks if a bucket contains any objects (including versioned objects).
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

        // Check if there's at least one .obj file (either versioned or non-versioned)
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
    /// This method returns the LATEST version of each object (or the non-versioned object)
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

        // Track unique keys - we'll need to look up the latest version for each
        var versionedKeys: Set<String> = []
        var nonVersionedKeys: Set<String> = []
        var commonPrefixesSet: Set<String> = []

        // First pass: collect all unique keys and common prefixes
        for case let fileURL as URL in enumerator {
            guard fileURL.pathExtension == "obj" else { continue }

            // Get relative path from bucket root
            let relativePath = fileURL.path.replacingOccurrences(
                of: bucketURL.path + "/",
                with: ""
            )

            // Parse the path to determine the object key
            let key: String
            let isVersioned: Bool

            if relativePath.contains(".versions/") {
                // Versioned path: key.versions/versionId.obj
                // Extract the key by removing .versions/... suffix
                let parts = relativePath.components(separatedBy: ".versions/")
                key = parts[0]
                isVersioned = true
            } else {
                // Non-versioned path: key.obj
                key = String(relativePath.dropLast(4))
                isVersioned = false
            }

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
                guard delimiter.count == 1, let delimChar = delimiter.first else {
                    continue
                }

                let keyAfterPrefix = String(key.dropFirst(prefix.count))
                if let delimiterIndex = keyAfterPrefix.firstIndex(of: delimChar) {
                    let prefixSlice = String(keyAfterPrefix[..<delimiterIndex])
                    let commonPrefix = prefix + prefixSlice + delimiter
                    commonPrefixesSet.insert(commonPrefix)
                    continue
                }
            }

            // Track this key
            if isVersioned {
                versionedKeys.insert(key)
            } else {
                nonVersionedKeys.insert(key)
            }
        }

        // Second pass: read the latest version for each key
        var latestByKey: [String: ObjectMeta] = [:]

        // Process versioned keys - use the .latest pointer to find the latest version
        for key in versionedKeys {
            do {
                // Try to read the latest version using the .latest pointer
                let (meta, _) = try readVersion(
                    bucketName: bucketName,
                    key: key,
                    versionId: nil,  // nil means "get latest"
                    loadData: false
                )

                // Skip delete markers
                if meta.isDeleteMarker {
                    continue
                }

                latestByKey[key] = meta
            } catch {
                // If we can't read the latest, skip this key
                continue
            }
        }

        // Process non-versioned keys (only if not already covered by versioned)
        for key in nonVersionedKeys {
            if latestByKey[key] != nil {
                continue  // Already have a versioned latest
            }

            let path = storagePath(for: bucketName, key: key)
            do {
                let (meta, _) = try read(from: path, loadData: false)

                // Skip delete markers
                if meta.isDeleteMarker {
                    continue
                }

                latestByKey[key] = meta
            } catch {
                continue
            }
        }

        // Convert to array and sort
        var allObjects: [(key: String, meta: ObjectMeta)] = latestByKey.map { ($0.key, $0.value) }
        allObjects.sort { $0.key < $1.key }

        // Sort common prefixes
        let sortedCommonPrefixes = commonPrefixesSet.sorted()

        // S3 behavior: maxKeys limits the TOTAL number of keys + common prefixes returned
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

    /// Returns the base directory path for versioned objects
    /// Structure: Storage/buckets/{bucket}/{key}/.versions/
    static func versionedBasePath(for bucketName: String, key: String) -> String {
        let encodedBucket =
            bucketName.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucketName

        let sanitizedKey: String
        if key.contains("..") {
            let components = key.components(separatedBy: "/")
            sanitizedKey = components.map { $0.replacingOccurrences(of: "..", with: "") }.joined(
                separator: "/")
        } else {
            sanitizedKey = key
        }

        return "\(BucketHandler.rootPath)\(encodedBucket)/\(sanitizedKey).versions/"
    }

    /// Returns the path for a specific version of an object
    static func versionedPath(for bucketName: String, key: String, versionId: String) -> String {
        return "\(versionedBasePath(for: bucketName, key: key))\(versionId).obj"
    }

    /// Returns the path to the .latest file that tracks the current version
    static func latestPointerPath(for bucketName: String, key: String) -> String {
        return "\(versionedBasePath(for: bucketName, key: key)).latest"
    }

    /// Checks if an object has any versions (is versioned)
    static func isVersioned(bucketName: String, key: String) -> Bool {
        let basePath = versionedBasePath(for: bucketName, key: key)
        return FileManager.default.fileExists(atPath: basePath)
    }

    /// Writes a versioned object. Returns the new version ID.
    static func writeVersioned(
        metadata: ObjectMeta,
        data: Data,
        bucketName: String,
        key: String,
        versioningStatus: VersioningStatus
    ) throws -> String {
        let versionId: String
        let path: String

        switch versioningStatus {
        case .enabled:
            // Generate a new version ID and store in versioned directory
            versionId = ObjectMeta.generateVersionId()
            path = versionedPath(for: bucketName, key: key, versionId: versionId)

            // Mark all existing versions as not latest
            try markAllVersionsNotLatest(bucketName: bucketName, key: key)

        case .suspended:
            // Use "null" as version ID, overwrite any existing null version
            versionId = "null"
            path = versionedPath(for: bucketName, key: key, versionId: versionId)

            // Mark all existing versions as not latest
            try markAllVersionsNotLatest(bucketName: bucketName, key: key)

        case .disabled:
            // No versioning - use the old single-file path
            // If there are existing versions, this is an error state
            // For simplicity, we just write to the non-versioned path
            versionId = "null"
            path = storagePath(for: bucketName, key: key)
        }

        // Create metadata with version info
        var versionedMeta = metadata
        versionedMeta.versionId = versionId
        versionedMeta.isLatest = true
        versionedMeta.isDeleteMarker = false

        // Write the object
        try write(metadata: versionedMeta, data: data, to: path)

        // Update the .latest pointer (only for versioned objects)
        if versioningStatus != .disabled {
            try updateLatestPointer(bucketName: bucketName, key: key, versionId: versionId)
        }

        return versionId
    }

    /// Reads a specific version of an object, or the latest if versionId is nil
    static func readVersion(
        bucketName: String,
        key: String,
        versionId: String?,
        loadData: Bool = true,
        range: (start: Int, end: Int)? = nil
    ) throws -> (ObjectMeta, Data?) {
        let path: String

        if let versionId = versionId {
            // Read specific version
            path = versionedPath(for: bucketName, key: key, versionId: versionId)
        } else if isVersioned(bucketName: bucketName, key: key) {
            // Read latest version from versioned storage
            guard let latestVersionId = try getLatestVersionId(bucketName: bucketName, key: key) else {
                throw NSError(
                    domain: "ObjectNotFound", code: 404,
                    userInfo: [NSLocalizedDescriptionKey: "No versions found for object"])
            }
            path = versionedPath(for: bucketName, key: key, versionId: latestVersionId)
        } else {
            // Non-versioned object - use old path
            path = storagePath(for: bucketName, key: key)
        }

        return try read(from: path, loadData: loadData, range: range)
    }

    /// Gets the latest version ID for a key
    static func getLatestVersionId(bucketName: String, key: String) throws -> String? {
        let pointerPath = latestPointerPath(for: bucketName, key: key)

        guard FileManager.default.fileExists(atPath: pointerPath) else {
            return nil
        }

        let versionId = try String(contentsOfFile: pointerPath, encoding: .utf8).trimmingCharacters(
            in: .whitespacesAndNewlines)
        return versionId.isEmpty ? nil : versionId
    }

    /// Updates the .latest pointer file
    static func updateLatestPointer(bucketName: String, key: String, versionId: String) throws {
        let pointerPath = latestPointerPath(for: bucketName, key: key)
        let pointerURL = URL(fileURLWithPath: pointerPath)

        // Ensure directory exists
        let dirURL = pointerURL.deletingLastPathComponent()
        if !FileManager.default.fileExists(atPath: dirURL.path) {
            try FileManager.default.createDirectory(at: dirURL, withIntermediateDirectories: true)
        }

        try versionId.write(toFile: pointerPath, atomically: true, encoding: .utf8)
    }

    /// Marks all versions of an object as not latest
    static func markAllVersionsNotLatest(bucketName: String, key: String) throws {
        let basePath = versionedBasePath(for: bucketName, key: key)
        let baseURL = URL(fileURLWithPath: basePath)

        guard FileManager.default.fileExists(atPath: basePath) else {
            return
        }

        let contents = try FileManager.default.contentsOfDirectory(
            at: baseURL, includingPropertiesForKeys: nil)

        for fileURL in contents where fileURL.pathExtension == "obj" {
            do {
                let (meta, data) = try read(from: fileURL.path, loadData: true)
                if meta.isLatest {
                    var updatedMeta = meta
                    updatedMeta.isLatest = false
                    try write(metadata: updatedMeta, data: data ?? Data(), to: fileURL.path)
                }
            } catch {
                // Skip files we can't read
                continue
            }
        }
    }

    /// Lists all versions of an object
    static func listVersions(
        bucketName: String,
        key: String
    ) throws -> [ObjectMeta] {
        var versions: [ObjectMeta] = []

        // Check versioned storage first
        let basePath = versionedBasePath(for: bucketName, key: key)
        if FileManager.default.fileExists(atPath: basePath) {
            let baseURL = URL(fileURLWithPath: basePath)
            let contents = try FileManager.default.contentsOfDirectory(
                at: baseURL, includingPropertiesForKeys: nil)

            for fileURL in contents where fileURL.pathExtension == "obj" {
                do {
                    let (meta, _) = try read(from: fileURL.path, loadData: false)
                    versions.append(meta)
                } catch {
                    continue
                }
            }
        }

        // Also check non-versioned path (for objects created before versioning was enabled)
        let nonVersionedPath = storagePath(for: bucketName, key: key)
        if FileManager.default.fileExists(atPath: nonVersionedPath) {
            do {
                let (meta, _) = try read(from: nonVersionedPath, loadData: false)
                // Only add if not already in versions (by versionId)
                if !versions.contains(where: { $0.versionId == meta.versionId }) {
                    versions.append(meta)
                }
            } catch {
                // Ignore errors
            }
        }

        // Sort by updatedAt descending (newest first)
        versions.sort { $0.updatedAt > $1.updatedAt }

        return versions
    }

    /// Lists all versions across all keys in a bucket
    static func listAllVersions(
        bucketName: String,
        prefix: String = "",
        delimiter: String? = nil,
        keyMarker: String? = nil,
        versionIdMarker: String? = nil,
        maxKeys: Int = 1000
    ) throws -> (
        versions: [ObjectMeta],
        deleteMarkers: [ObjectMeta],
        commonPrefixes: [String],
        isTruncated: Bool,
        nextKeyMarker: String?,
        nextVersionIdMarker: String?
    ) {
        let bucketURL = BucketHandler.bucketURL(for: bucketName)

        guard FileManager.default.fileExists(atPath: bucketURL.path) else {
            return ([], [], [], false, nil, nil)
        }

        guard
            let enumerator = FileManager.default.enumerator(
                at: bucketURL,
                includingPropertiesForKeys: [.isDirectoryKey],
                options: [.skipsHiddenFiles]
            )
        else {
            return ([], [], [], false, nil, nil)
        }

        var allVersions: [ObjectMeta] = []
        var allDeleteMarkers: [ObjectMeta] = []
        var commonPrefixSet: Set<String> = []

        // Collect all .obj files (both versioned and non-versioned)
        for case let fileURL as URL in enumerator {
            guard fileURL.pathExtension == "obj" else { continue }

            // Parse the path to get key and version info
            let relativePath = fileURL.path.replacingOccurrences(
                of: bucketURL.path + "/",
                with: ""
            )

            // Determine if this is a versioned path
            var objectKey: String
            if relativePath.contains(".versions/") {
                // Versioned path: key.versions/versionId.obj
                let parts = relativePath.components(separatedBy: ".versions/")
                objectKey = parts[0]
            } else {
                // Non-versioned path: key.obj
                objectKey = String(relativePath.dropLast(4))
            }

            // Apply prefix filter
            if !prefix.isEmpty && !objectKey.hasPrefix(prefix) {
                continue
            }

            // Handle delimiter for folder-like navigation
            if let delimiter = delimiter, !delimiter.isEmpty, let delimiterChar = delimiter.first {
                // Get the part after prefix
                let afterPrefix = String(objectKey.dropFirst(prefix.count))

                // Check if there's a delimiter in the remaining path
                if let delimiterIndex = afterPrefix.firstIndex(of: delimiterChar) {
                    // This is a "folder" - extract the common prefix
                    let folderPrefix = prefix + String(afterPrefix[..<afterPrefix.index(after: delimiterIndex)])
                    commonPrefixSet.insert(folderPrefix)
                    continue  // Don't include this as a version entry
                }
            }

            // Read metadata
            do {
                let (meta, _) = try read(from: fileURL.path, loadData: false)

                // Apply marker filtering
                if let keyMarker = keyMarker {
                    if meta.key < keyMarker {
                        continue
                    }
                    if meta.key == keyMarker, let versionIdMarker = versionIdMarker {
                        if let versionId = meta.versionId, versionId <= versionIdMarker {
                            continue
                        }
                    }
                }

                if meta.isDeleteMarker {
                    allDeleteMarkers.append(meta)
                } else {
                    allVersions.append(meta)
                }
            } catch {
                continue
            }
        }

        // Sort by key, then by updatedAt descending
        allVersions.sort {
            if $0.key != $1.key {
                return $0.key < $1.key
            }
            return $0.updatedAt > $1.updatedAt
        }

        allDeleteMarkers.sort {
            if $0.key != $1.key {
                return $0.key < $1.key
            }
            return $0.updatedAt > $1.updatedAt
        }

        // Sort common prefixes
        let sortedCommonPrefixes = commonPrefixSet.sorted()

        // Apply maxKeys limit
        let totalCount = allVersions.count + allDeleteMarkers.count
        let isTruncated = totalCount > maxKeys

        var limitedVersions = allVersions
        var limitedDeleteMarkers = allDeleteMarkers

        if isTruncated {
            // Merge and limit
            var combined: [(meta: ObjectMeta, isDeleteMarker: Bool)] = []
            combined.append(contentsOf: allVersions.map { ($0, false) })
            combined.append(contentsOf: allDeleteMarkers.map { ($0, true) })

            combined.sort {
                if $0.meta.key != $1.meta.key {
                    return $0.meta.key < $1.meta.key
                }
                return $0.meta.updatedAt > $1.meta.updatedAt
            }

            let limited = Array(combined.prefix(maxKeys))
            limitedVersions = limited.filter { !$0.isDeleteMarker }.map { $0.meta }
            limitedDeleteMarkers = limited.filter { $0.isDeleteMarker }.map { $0.meta }
        }

        // Get next markers
        var nextKeyMarker: String? = nil
        var nextVersionIdMarker: String? = nil

        if isTruncated {
            // Find the last item to get next markers
            let lastVersion = limitedVersions.last
            let lastDeleteMarker = limitedDeleteMarkers.last

            if let lv = lastVersion, let ldm = lastDeleteMarker {
                if lv.key > ldm.key || (lv.key == ldm.key && lv.updatedAt < ldm.updatedAt) {
                    nextKeyMarker = lv.key
                    nextVersionIdMarker = lv.versionId
                } else {
                    nextKeyMarker = ldm.key
                    nextVersionIdMarker = ldm.versionId
                }
            } else if let lv = lastVersion {
                nextKeyMarker = lv.key
                nextVersionIdMarker = lv.versionId
            } else if let ldm = lastDeleteMarker {
                nextKeyMarker = ldm.key
                nextVersionIdMarker = ldm.versionId
            }
        }

        return (limitedVersions, limitedDeleteMarkers, sortedCommonPrefixes, isTruncated, nextKeyMarker, nextVersionIdMarker)
    }

    /// Deletes a specific version of an object
    static func deleteVersion(bucketName: String, key: String, versionId: String) throws {
        let path = versionedPath(for: bucketName, key: key, versionId: versionId)

        guard FileManager.default.fileExists(atPath: path) else {
            throw NSError(
                domain: "ObjectNotFound", code: 404,
                userInfo: [NSLocalizedDescriptionKey: "Version does not exist"])
        }

        // Check if this is the latest version
        let isLatest = try getLatestVersionId(bucketName: bucketName, key: key) == versionId

        // Delete the version file
        try FileManager.default.removeItem(atPath: path)

        // If we deleted the latest version, update the pointer to the next most recent
        if isLatest {
            try updateLatestToMostRecent(bucketName: bucketName, key: key)
        }

        // Clean up empty directories
        try cleanupEmptyVersionsDirectory(bucketName: bucketName, key: key)
    }

    /// Creates a delete marker (soft delete for versioned buckets)
    static func createDeleteMarker(bucketName: String, key: String) throws -> ObjectMeta {
        let versionId = ObjectMeta.generateVersionId()
        let path = versionedPath(for: bucketName, key: key, versionId: versionId)

        // Mark all existing versions as not latest
        try markAllVersionsNotLatest(bucketName: bucketName, key: key)

        // Create delete marker metadata
        let meta = ObjectMeta(
            bucketName: bucketName,
            key: key,
            size: 0,
            contentType: "",
            etag: "",
            updatedAt: Date(),
            versionId: versionId,
            isLatest: true,
            isDeleteMarker: true
        )

        // Write empty data with delete marker metadata
        try write(metadata: meta, data: Data(), to: path)

        // Update latest pointer
        try updateLatestPointer(bucketName: bucketName, key: key, versionId: versionId)

        return meta
    }

    /// Updates the .latest pointer to the most recent non-deleted version
    private static func updateLatestToMostRecent(bucketName: String, key: String) throws {
        let versions = try listVersions(bucketName: bucketName, key: key)

        // Find the most recent version (already sorted by updatedAt descending)
        if let mostRecent = versions.first {
            try updateLatestPointer(bucketName: bucketName, key: key, versionId: mostRecent.versionId ?? "null")

            // Update the metadata to mark it as latest
            let path = versionedPath(for: bucketName, key: key, versionId: mostRecent.versionId ?? "null")
            if FileManager.default.fileExists(atPath: path) {
                let (meta, data) = try read(from: path, loadData: true)
                var updatedMeta = meta
                updatedMeta.isLatest = true
                try write(metadata: updatedMeta, data: data ?? Data(), to: path)
            }
        } else {
            // No versions left, remove the .latest pointer
            let pointerPath = latestPointerPath(for: bucketName, key: key)
            try? FileManager.default.removeItem(atPath: pointerPath)
        }
    }

    /// Cleans up empty .versions directories
    private static func cleanupEmptyVersionsDirectory(bucketName: String, key: String) throws {
        let basePath = versionedBasePath(for: bucketName, key: key)
        let baseURL = URL(fileURLWithPath: basePath)

        guard FileManager.default.fileExists(atPath: basePath) else {
            return
        }

        let contents = try FileManager.default.contentsOfDirectory(at: baseURL, includingPropertiesForKeys: nil)

        // Check if only .latest file remains or directory is empty
        let objFiles = contents.filter { $0.pathExtension == "obj" }
        if objFiles.isEmpty {
            // Remove the entire .versions directory
            try? FileManager.default.removeItem(at: baseURL)
        }
    }

    /// Checks if a versioned object exists (has any versions or a non-versioned file)
    static func versionedKeyExists(bucketName: String, key: String, versionId: String? = nil) -> Bool {
        if let versionId = versionId {
            // Check specific version
            let path = versionedPath(for: bucketName, key: key, versionId: versionId)
            return FileManager.default.fileExists(atPath: path)
        }

        // Check for any version
        if isVersioned(bucketName: bucketName, key: key) {
            // Has versioned storage - check if there's a latest version
            if let _ = try? getLatestVersionId(bucketName: bucketName, key: key) {
                return true
            }
        }

        // Check non-versioned path
        let nonVersionedPath = storagePath(for: bucketName, key: key)
        return FileManager.default.fileExists(atPath: nonVersionedPath)
    }
}
