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

struct ObjectFileHandler {
    private static let jsonEncoder: JSONEncoder = {
        let encoder = JSONEncoder()
        return encoder
    }()

    private static let jsonDecoder: JSONDecoder = {
        let decoder = JSONDecoder()
        return decoder
    }()

    /// Writes metadata + data to a single file - atomically (temp file + rename, so readers
    /// never see a partial object) and durably (fsync of both the file and its directory
    /// before acknowledging, unless `ALARIK_FSYNC=false`; see `AtomicObjectWriter`).
    static func write(metadata: ObjectMeta, data: Data, to path: String) throws {
        var writer = try AtomicObjectWriter(finalPath: path)
        do {
            try writeHeader(metadata: metadata, to: writer)
            try writer.write(data)
            try writer.finish()
        } catch {
            writer.abort()
            throw error
        }
    }

    /// Writes the `.obj` header (4-byte big-endian JSON length + metadata JSON) to a writer.
    private static func writeHeader(metadata: ObjectMeta, to writer: AtomicObjectWriter) throws {
        let jsonData = try jsonEncoder.encode(metadata)
        var header = Data(capacity: 4 + jsonData.count)
        withUnsafeBytes(of: UInt32(jsonData.count).bigEndian) {
            header.append(contentsOf: $0)
        }
        header.append(jsonData)
        try writer.write(header)
    }

    /// Like `write(metadata:data:to:)`, but the payload comes from an existing file on disk
    /// instead of memory - copied across in fixed-size windows, so the object is never fully
    /// buffered. `payloadOffset`/`payloadSize` select the source region (0 / file size for a
    /// raw spool file; the header offset for another `.obj`).
    static func writeStreamed(
        metadata: ObjectMeta,
        payloadFile sourcePath: String,
        payloadOffset: Int,
        payloadSize: Int,
        to path: String
    ) throws {
        try writeStreamed(
            metadata: metadata,
            payloadSources: [(path: sourcePath, offset: payloadOffset, size: payloadSize)],
            to: path)
    }

    /// Multi-source variant: concatenates several file regions into one `.obj` payload -
    /// this is how CompleteMultipartUpload assembles parts without ever holding more than
    /// one copy window in memory.
    static func writeStreamed(
        metadata: ObjectMeta,
        payloadSources: [(path: String, offset: Int, size: Int)],
        to path: String
    ) throws {
        var writer = try AtomicObjectWriter(finalPath: path)
        do {
            try writeHeader(metadata: metadata, to: writer)

            let windowSize = Constants.fileCopyWindowSize
            var window = [UInt8](repeating: 0, count: windowSize)

            for source in payloadSources {
                let sourceFd = POSIXFile.open(source.path, O_RDONLY)
                guard sourceFd >= 0 else {
                    throw NSError(
                        domain: "FileError", code: Int(errno),
                        userInfo: [NSLocalizedDescriptionKey: "Could not open payload source"])
                }
                defer { _ = POSIXFile.close(sourceFd) }
                _ = POSIXFile.lseek(sourceFd, off_t(source.offset), SEEK_SET)

                var remaining = source.size
                while remaining > 0 {
                    let toRead = Swift.min(windowSize, remaining)
                    let bytesRead = POSIXFile.read(sourceFd, &window, toRead)
                    guard bytesRead > 0 else {
                        throw NSError(
                            domain: "InvalidFile", code: 0,
                            userInfo: [NSLocalizedDescriptionKey: "Payload source ended early"])
                    }
                    try window.withUnsafeBytes { raw in
                        try writer.writeRaw(
                            UnsafeRawBufferPointer(rebasing: raw.prefix(bytesRead)))
                    }
                    remaining -= bytesRead
                }
            }

            try writer.finish()
        } catch {
            writer.abort()
            throw error
        }
    }

    /// Updates an object's metadata in place without ever buffering its payload - used by
    /// tagging/metadata-only endpoints (PutObjectTagging, DeleteObjectTagging, the console's
    /// metadata editor) so changing a small JSON blob doesn't cost a full read+rewrite of a
    /// payload that could be gigabytes. The payload is window-copied from the file to itself
    /// (safe: `AtomicObjectWriter` writes to a distinct temp file and only renames over the
    /// original once the copy is fully done - same reasoning as `markAllVersionsNotLatest`).
    static func rewriteMetadata(
        at path: String, transform: (inout ObjectMeta) -> Void
    ) throws -> ObjectMeta {
        let location = try payloadLocation(path: path)
        var updatedMeta = location.meta
        transform(&updatedMeta)
        try writeStreamed(
            metadata: updatedMeta,
            payloadFile: path,
            payloadOffset: location.payloadOffset,
            payloadSize: location.payloadSize,
            to: path)
        return updatedMeta
    }

    /// MD5 of a file region, computed in fixed-size windows (bounded memory). Used by
    /// CopyObject when the source's own ETag isn't a plain MD5 (multipart "-N" ETags) and the
    /// destination needs one without buffering the payload.
    static func md5HexOfFileRegion(path: String, offset: Int, size: Int) throws -> String {
        let fd = POSIXFile.open(path, O_RDONLY)
        guard fd >= 0 else {
            throw NSError(
                domain: "FileError", code: Int(errno),
                userInfo: [NSLocalizedDescriptionKey: "Could not open file"])
        }
        defer { _ = POSIXFile.close(fd) }
        _ = POSIXFile.lseek(fd, off_t(offset), SEEK_SET)

        var md5 = Insecure.MD5()
        let windowSize = Constants.fileCopyWindowSize
        var window = [UInt8](repeating: 0, count: windowSize)
        var remaining = size
        while remaining > 0 {
            let toRead = Swift.min(windowSize, remaining)
            let bytesRead = POSIXFile.read(fd, &window, toRead)
            guard bytesRead > 0 else {
                throw NSError(
                    domain: "InvalidFile", code: 0,
                    userInfo: [NSLocalizedDescriptionKey: "File region ended early"])
            }
            window.withUnsafeBytes { raw in
                md5.update(bufferPointer: UnsafeRawBufferPointer(rebasing: raw.prefix(bytesRead)))
            }
            remaining -= bytesRead
        }
        return md5.finalize().hexString()
    }

    /// Reads an object file's metadata plus where its payload starts and how long it is -
    /// everything a streaming GET needs to serve the body straight off disk without ever
    /// buffering it.
    static func payloadLocation(path: String) throws -> (
        meta: ObjectMeta, payloadOffset: Int, payloadSize: Int
    ) {
        let snapshot = try openPayloadSnapshot(path: path)
        _ = POSIXFile.close(snapshot.fd)
        return (snapshot.meta, snapshot.payloadOffset, snapshot.payloadSize)
    }

    /// Like `payloadLocation`, but hands back the open file descriptor the header was parsed
    /// from. Streaming reads that continue on this fd see a consistent snapshot of the file
    /// even if the object is concurrently overwritten (the rename swaps the directory entry;
    /// the open fd keeps the original inode). The caller owns the fd and must close it.
    static func openPayloadSnapshot(path: String) throws -> (
        fd: Int32, meta: ObjectMeta, payloadOffset: Int, payloadSize: Int
    ) {
        let fd = POSIXFile.open(path, O_RDONLY)
        guard fd >= 0 else {
            throw NSError(
                domain: "FileError", code: Int(errno),
                userInfo: [NSLocalizedDescriptionKey: "Could not open file"])
        }

        do {
            var lengthBytes = [UInt8](repeating: 0, count: 4)
            guard POSIXFile.read(fd, &lengthBytes, 4) == 4 else {
                throw NSError(
                    domain: "InvalidFile", code: 0,
                    userInfo: [NSLocalizedDescriptionKey: "Missing length prefix"])
            }
            let metadataLength = UInt32(
                bigEndian: lengthBytes.withUnsafeBytes { $0.load(as: UInt32.self) })

            var jsonBytes = [UInt8](repeating: 0, count: Int(metadataLength))
            guard POSIXFile.read(fd, &jsonBytes, Int(metadataLength)) == Int(metadataLength)
            else {
                throw NSError(
                    domain: "InvalidFile", code: 0,
                    userInfo: [NSLocalizedDescriptionKey: "Incomplete metadata"])
            }
            let meta = try jsonBytes.withUnsafeBufferPointer { buffer in
                try jsonDecoder.decode(ObjectMeta.self, from: Data(buffer))
            }

            var statInfo = stat()
            guard POSIXFile.fstat(fd, &statInfo) == 0 else {
                throw NSError(
                    domain: "FileError", code: Int(errno),
                    userInfo: [NSLocalizedDescriptionKey: "Could not stat file"])
            }

            let payloadOffset = 4 + Int(metadataLength)
            let payloadSize = Int(statInfo.st_size) - payloadOffset
            return (fd, meta, payloadOffset, Swift.max(0, payloadSize))
        } catch {
            _ = POSIXFile.close(fd)
            throw error
        }
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
        let encodedBucket = BucketHandler.encodedBucketName(bucketName)

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

    /// Checks if a bucket contains any objects (including versioned objects and delete
    /// markers). Deliberately does NOT skip hidden entries: versioned objects live under
    /// hidden `.versions` directories, and skipping those would report a bucket that still
    /// holds versions as "empty" - S3 refuses to delete a bucket while any version or
    /// delete marker remains. Empty directory skeletons (left behind after deleting a nested
    /// key) don't count: S3 has no directories, only keys.
    static func hasBucketObjects(bucketName: String) -> Bool {
        let bucketURL = BucketHandler.bucketURL(for: bucketName)

        guard FileManager.default.fileExists(atPath: bucketURL.path) else {
            return false
        }

        guard
            let enumerator = FileManager.default.enumerator(
                at: bucketURL,
                includingPropertiesForKeys: [.isDirectoryKey]
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
        let encodedBucket = BucketHandler.encodedBucketName(bucketName)

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
        let (versionId, path, versionedMeta) = try prepareVersionedWrite(
            metadata: metadata, bucketName: bucketName, key: key,
            versioningStatus: versioningStatus)

        try write(metadata: versionedMeta, data: data, to: path)

        if versioningStatus != .disabled {
            try updateLatestPointer(bucketName: bucketName, key: key, versionId: versionId)
        }

        return versionId
    }

    /// `writeVersioned` with the payload coming from files on disk instead of memory (spool
    /// file for streamed PUTs, part files for CompleteMultipartUpload, a source `.obj` for
    /// CopyObject). Returns the new version ID.
    static func writeVersionedStreamed(
        metadata: ObjectMeta,
        payloadSources: [(path: String, offset: Int, size: Int)],
        bucketName: String,
        key: String,
        versioningStatus: VersioningStatus
    ) throws -> String {
        let (versionId, path, versionedMeta) = try prepareVersionedWrite(
            metadata: metadata, bucketName: bucketName, key: key,
            versioningStatus: versioningStatus)

        try writeStreamed(metadata: versionedMeta, payloadSources: payloadSources, to: path)

        if versioningStatus != .disabled {
            try updateLatestPointer(bucketName: bucketName, key: key, versionId: versionId)
        }

        return versionId
    }

    /// Shared setup for both versioned-write flavors: picks the version ID and target path
    /// for the bucket's versioning state, demotes existing versions, and stamps the metadata.
    private static func prepareVersionedWrite(
        metadata: ObjectMeta,
        bucketName: String,
        key: String,
        versioningStatus: VersioningStatus
    ) throws -> (versionId: String, path: String, meta: ObjectMeta) {
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

        var versionedMeta = metadata
        versionedMeta.versionId = versionId
        versionedMeta.isLatest = true
        versionedMeta.isDeleteMarker = false

        return (versionId, path, versionedMeta)
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

    /// Resolves the *current* version of an object regardless of the bucket's versioning state -
    /// nil if it doesn't exist (or its latest version is a delete marker), instead of throwing.
    /// Tries the versioned path first (covers enabled/suspended buckets), then falls back to the
    /// plain non-versioned path.
    static func readCurrentObject(bucketName: String, key: String, loadData: Bool) throws -> (
        meta: ObjectMeta, data: Data?
    )? {
        if let (meta, data) = try? readVersion(
            bucketName: bucketName, key: key, versionId: nil, loadData: loadData),
            !meta.isDeleteMarker
        {
            return (meta, data)
        }

        let path = storagePath(for: bucketName, key: key)
        guard keyExists(for: bucketName, key: key, path: path) else {
            return nil
        }
        let (meta, data) = try read(from: path, loadData: loadData)
        return (meta, data)
    }

    /// Resolves the on-disk path for a specific version (or the current/latest version if
    /// `versionId` is nil), regardless of the bucket's versioning state - nil if it doesn't
    /// exist. Used by operations that need to modify a version's metadata in place (e.g. object
    /// tagging), where the caller needs the path itself, not just the decoded contents.
    ///
    /// For a "current version" lookup (`versionId == nil`), a delete marker counts as "doesn't
    /// exist" - matching GetObject's own delete-marker handling, and `readCurrentObject`'s. An
    /// explicit `versionId` may still target a delete marker directly (e.g. to inspect it).
    static func resolvePath(bucketName: String, key: String, versionId: String?) throws -> String?
    {
        let path: String
        if let versionId = versionId {
            path = versionedPath(for: bucketName, key: key, versionId: versionId)
        } else if isVersioned(bucketName: bucketName, key: key) {
            guard let latestVersionId = try getLatestVersionId(bucketName: bucketName, key: key)
            else {
                return nil
            }
            path = versionedPath(for: bucketName, key: key, versionId: latestVersionId)
        } else {
            path = storagePath(for: bucketName, key: key)
        }

        guard keyExists(for: bucketName, key: key, path: path) else {
            return nil
        }

        if versionId == nil {
            let (meta, _) = try read(from: path, loadData: false)
            if meta.isDeleteMarker {
                return nil
            }
        }

        return path
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

    /// Updates the .latest pointer file. Runs on every write to a versioned bucket, so the
    /// common case (directory already exists) is optimistic: try the write first, and only
    /// pay for stat-ing + creating the parent directory on the rare miss (same pattern as
    /// `AtomicObjectWriter.init` and `SpoolSink.spillToDisk`).
    static func updateLatestPointer(bucketName: String, key: String, versionId: String) throws {
        let pointerPath = latestPointerPath(for: bucketName, key: key)
        do {
            try versionId.write(toFile: pointerPath, atomically: true, encoding: .utf8)
        } catch {
            let dirURL = URL(fileURLWithPath: pointerPath).deletingLastPathComponent()
            try FileManager.default.createDirectory(at: dirURL, withIntermediateDirectories: true)
            try versionId.write(toFile: pointerPath, atomically: true, encoding: .utf8)
        }
    }

    /// Marks all versions of an object as not latest
    /// Runs on every write to a versioned (or suspended-versioning) bucket, over every prior
    /// version of the key. Only the metadata's `isLatest` flag ever needs changing here, so
    /// this is header-only for every file it merely checks (`payloadLocation`, not `read`),
    /// and window-copies the payload back into place (self-rewrite, safe per
    /// `AtomicObjectWriter`'s temp-file-then-rename design: the source stays open and intact
    /// until the atomic rename at the very end) rather than buffering it as `Data` - a
    /// multi-gigabyte object's every subsequent version write used to also mean a full extra
    /// read+copy of that same multi-gigabyte payload here, just to flip one boolean.
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
                let location = try payloadLocation(path: fileURL.path)
                guard location.meta.isLatest else { continue }

                var updatedMeta = location.meta
                updatedMeta.isLatest = false
                try writeStreamed(
                    metadata: updatedMeta,
                    payloadFile: fileURL.path,
                    payloadOffset: location.payloadOffset,
                    payloadSize: location.payloadSize,
                    to: fileURL.path)
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
            // The "null" version of a key that was never written under versioning lives at
            // the plain non-versioned path, not in the .versions directory - but listings
            // still (correctly, matching S3) report it as VersionId "null", and clients
            // like mc echo that id back in versioned deletes. Deleting versionId "null" must
            // therefore remove the plain file too, or `mc rb --force` style flows leave the
            // object behind while believing the delete succeeded.
            if versionId == "null" {
                let plainPath = storagePath(for: bucketName, key: key)
                if FileManager.default.fileExists(atPath: plainPath) {
                    try FileManager.default.removeItem(atPath: plainPath)
                    return
                }
            }
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

    /// Creates a delete marker for a *suspended*-versioning bucket - distinct from
    /// `createDeleteMarker` (which is for `.enabled` buckets only). Per S3's documented
    /// suspended-versioning delete semantics, this must only ever affect the "null" version:
    /// it's overwritten with a delete marker (same "overwrite any existing null version" rule
    /// `prepareVersionedWrite`'s `.suspended` case already applies to writes), and every other,
    /// genuinely-versioned object created while versioning was enabled is left completely
    /// untouched and still retrievable by its real version ID. Using `createDeleteMarker`
    /// instead here would be wrong in two ways: it'd mint a fresh random version ID instead of
    /// "null" (S3 always reports "null" for a suspended-bucket delete), and demoting-not-deleting
    /// the prior versions is what `createDeleteMarker` already does correctly - the actual bug
    /// this exists to fix lived one level up, in the caller that used to hard-delete every
    /// version instead of calling anything like this at all.
    static func createNullVersionDeleteMarker(bucketName: String, key: String) throws -> ObjectMeta {
        // The existing "null" version may currently live at the plain non-versioned path (an
        // object written before versioning was ever enabled on this bucket) rather than inside
        // .versions/ - deleteVersion(versionId: "null") already knows this, so the marker write
        // below doesn't silently leave that plain file behind as an orphaned duplicate.
        let plainPath = storagePath(for: bucketName, key: key)
        if FileManager.default.fileExists(atPath: plainPath) {
            try FileManager.default.removeItem(atPath: plainPath)
        }

        let versionId = "null"
        let path = versionedPath(for: bucketName, key: key, versionId: versionId)

        // Demotes prior real versions to not-latest - never deletes them, matching
        // prepareVersionedWrite's .suspended case exactly.
        try markAllVersionsNotLatest(bucketName: bucketName, key: key)

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

        try write(metadata: meta, data: Data(), to: path)
        try updateLatestPointer(bucketName: bucketName, key: key, versionId: versionId)

        return meta
    }

    /// Updates the .latest pointer to the most recent non-deleted version
    private static func updateLatestToMostRecent(bucketName: String, key: String) throws {
        let versions = try listVersions(bucketName: bucketName, key: key)

        // Find the most recent version (already sorted by updatedAt descending)
        if let mostRecent = versions.first {
            try updateLatestPointer(bucketName: bucketName, key: key, versionId: mostRecent.versionId ?? "null")

            // Update the metadata to mark it as latest - header-only read + self window-copy
            // (see markAllVersionsNotLatest) rather than buffering the whole payload just to
            // flip one boolean.
            let path = versionedPath(for: bucketName, key: key, versionId: mostRecent.versionId ?? "null")
            if let location = try? payloadLocation(path: path) {
                var updatedMeta = location.meta
                updatedMeta.isLatest = true
                try writeStreamed(
                    metadata: updatedMeta,
                    payloadFile: path,
                    payloadOffset: location.payloadOffset,
                    payloadSize: location.payloadSize,
                    to: path)
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
