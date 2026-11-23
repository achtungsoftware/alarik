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
    /// Upper bound on a `.obj` file's declared JSON-metadata length. The header is one
    /// `ObjectMeta` - even pathological tag/metadata sets stay far below this. The cap exists so
    /// a bit-rotted 4-byte length prefix (e.g. flipped to 0xFFFFFFFF) is rejected as corrupt
    /// instead of triggering a multi-gigabyte allocation on every open of that file.
    static let maxMetadataHeaderLength = 1 << 20  // 1 MiB

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
        // The header's size is stamped from the bytes actually written, never trusted from the
        // caller: reads verify the two agree, so a caller's stale size would make its own
        // object unreadable.
        var metadata = metadata
        metadata.size = data.count
        try ObjectKeyLock.withLock(bucketName: metadata.bucketName, key: metadata.key) {
            defer { StorageStatsService.shared.noteChange(bucketName: metadata.bucketName) }
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
        // Size stamped from the sources, same reasoning as `write(metadata:data:to:)`.
        var metadata = metadata
        metadata.size = payloadSources.reduce(0) { $0 + $1.size }
        try ObjectKeyLock.withLock(bucketName: metadata.bucketName, key: metadata.key) {
            defer { StorageStatsService.shared.noteChange(bucketName: metadata.bucketName) }
            var writer = try AtomicObjectWriter(finalPath: path)
            do {
                try writeHeader(metadata: metadata, to: writer)

                var window = [UInt8](repeating: 0, count: Constants.fileCopyWindowSize)

                for source in payloadSources {
                    let sourceFd = POSIXFile.open(source.path, O_RDONLY)
                    guard sourceFd >= 0 else {
                        throw NSError(
                            domain: "FileError", code: Int(errno),
                            userInfo: [NSLocalizedDescriptionKey: "Could not open payload source"])
                    }
                    defer { _ = POSIXFile.close(sourceFd) }
                    try copyRegion(
                        fd: sourceFd, offset: source.offset, size: source.size, into: writer,
                        window: &window)
                }

                try writer.finish()
            } catch {
                writer.abort()
                throw error
            }
        }
    }

    /// Payload from an already-open descriptor (`openPayloadSnapshot`), so a metadata-only
    /// rewrite copies the exact inode its header came from. Reopening by path raced a
    /// concurrent overwrite: the new file was read at the old header's offset and renamed back
    /// over the fresh PUT with stale metadata and a shifted payload. Takes no lock itself: every
    /// caller already holds `ObjectKeyLock` for the key.
    private static func writeStreamed(
        metadata: ObjectMeta, payloadFd fd: Int32, payloadOffset: Int, payloadSize: Int,
        to path: String
    ) throws {
        defer { StorageStatsService.shared.noteChange(bucketName: metadata.bucketName) }
        var writer = try AtomicObjectWriter(finalPath: path)
        do {
            try writeHeader(metadata: metadata, to: writer)
            var window = [UInt8](repeating: 0, count: Constants.fileCopyWindowSize)
            try copyRegion(fd: fd, offset: payloadOffset, size: payloadSize, into: writer, window: &window)
            try writer.finish()
        } catch {
            writer.abort()
            throw error
        }
    }

    /// Copies `size` bytes from `offset` of `fd` into `writer`, one window at a time.
    private static func copyRegion(
        fd: Int32, offset: Int, size: Int, into writer: AtomicObjectWriter, window: inout [UInt8]
    ) throws {
        _ = POSIXFile.lseek(fd, off_t(offset), SEEK_SET)
        var remaining = size
        while remaining > 0 {
            let toRead = Swift.min(window.count, remaining)
            let bytesRead = POSIXFile.read(fd, &window, toRead)
            guard bytesRead > 0 else {
                throw NSError(
                    domain: "InvalidFile", code: 0,
                    userInfo: [NSLocalizedDescriptionKey: "Payload source ended early"])
            }
            try window.withUnsafeBytes { raw in
                try writer.writeRaw(UnsafeRawBufferPointer(rebasing: raw.prefix(bytesRead)))
            }
            remaining -= bytesRead
        }
    }

    /// Updates an object's metadata in place without ever buffering its payload - used by
    /// tagging/metadata-only endpoints (PutObjectTagging, DeleteObjectTagging, the console's
    /// metadata editor) so changing a small JSON blob doesn't cost a full read+rewrite of a
    /// payload that could be gigabytes. The payload is window-copied from the file to itself
    /// (safe: `AtomicObjectWriter` writes to a distinct temp file and only renames over the
    /// original once the copy is fully done - same reasoning as `markAllVersionsNotLatest`).
    static func rewriteMetadata(
        bucketName: String, key: String, at path: String, transform: (inout ObjectMeta) -> Void
    ) throws -> ObjectMeta {
        try ObjectKeyLock.withLock(bucketName: bucketName, key: key) {
            let snapshot = try openPayloadSnapshot(path: path)
            defer { _ = POSIXFile.close(snapshot.fd) }
            var updatedMeta = snapshot.meta
            transform(&updatedMeta)
            try writeStreamed(
                metadata: updatedMeta, payloadFd: snapshot.fd,
                payloadOffset: snapshot.payloadOffset, payloadSize: snapshot.payloadSize,
                to: path)
            return updatedMeta
        }
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

    /// Streams a file region once and returns its base64 flexible checksum in `algorithm` (the
    /// lowercase S3 suffix). Returns nil for an algorithm we don't implement. Used by CopyObject
    /// to recompute the destination's full-object checksum from the copied bytes.
    static func flexibleChecksumOfFileRegion(
        path: String, offset: Int, size: Int, algorithm: String
    ) throws -> String? {
        guard var checksum = TrailerChecksum(trailerHeaderName: "x-amz-checksum-\(algorithm)") else {
            return nil
        }
        let fd = POSIXFile.open(path, O_RDONLY)
        guard fd >= 0 else {
            throw NSError(
                domain: "FileError", code: Int(errno),
                userInfo: [NSLocalizedDescriptionKey: "Could not open file"])
        }
        defer { _ = POSIXFile.close(fd) }
        _ = POSIXFile.lseek(fd, off_t(offset), SEEK_SET)

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
                checksum.update(rawBufferPointer: UnsafeRawBufferPointer(rebasing: raw.prefix(bytesRead)))
            }
            remaining -= bytesRead
        }
        return checksum.finalizeBase64()
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
            guard metadataLength > 0, Int(metadataLength) <= maxMetadataHeaderLength else {
                throw NSError(
                    domain: "InvalidFile", code: 0,
                    userInfo: [NSLocalizedDescriptionKey: "Corrupt metadata length prefix"])
            }

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
            let payloadSize = Swift.max(0, Int(statInfo.st_size) - payloadOffset)
            // A truncated or over-long payload must never be served under a header that
            // promises another length.
            guard payloadSize == meta.size else {
                throw IntegrityError.sizeMismatch(path: path, expected: meta.size, actual: payloadSize)
            }
            return (fd, meta, payloadOffset, payloadSize)
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
        guard metadataLength > 0, Int(metadataLength) <= maxMetadataHeaderLength else {
            throw NSError(
                domain: "InvalidFile", code: 0,
                userInfo: [NSLocalizedDescriptionKey: "Corrupt metadata length prefix"])
        }

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

        let dataOffset = 4 + Int(metadataLength)

        // The header's size against the file's: a truncated object is damaged, not short.
        var statInfo = stat()
        guard POSIXFile.fstat(fd, &statInfo) == 0 else {
            throw NSError(
                domain: "FileError", code: Int(errno),
                userInfo: [NSLocalizedDescriptionKey: "Could not stat file"])
        }
        let dataSize = Swift.max(0, Int(statInfo.st_size) - dataOffset)
        guard dataSize == metadata.size else {
            throw IntegrityError.sizeMismatch(path: path, expected: metadata.size, actual: dataSize)
        }

        // For metadata-only reads, we're done
        if !loadData {
            return (metadata, nil)
        }

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

        // Full read: the whole payload, verified against the header's checksum.
        var dataBytes = [UInt8](repeating: 0, count: dataSize)
        if dataSize > 0 {
            let dataRead = POSIXFile.read(fd, &dataBytes, dataSize)
            guard dataRead == dataSize else {
                throw NSError(
                    domain: "InvalidFile", code: 0,
                    userInfo: [NSLocalizedDescriptionKey: "Incomplete data"])
            }
        }
        if var verifier = PayloadVerifier(meta: metadata, path: path) {
            dataBytes.withUnsafeBytes { verifier.update($0) }
            try verifier.verify()
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

        // Check if there's at least one .obj or .ecshard file (either versioned or non-versioned) -
        // any single shard is enough here (unlike stats counting, existence doesn't need to
        // dedupe against the other k+m-1 shards of the same object).
        for case let fileURL as URL in enumerator {
            if fileURL.pathExtension == "obj" || fileURL.pathExtension == "ecshard" {
                return true
            }
        }

        return false
    }

    /// Deletes an object from storage.
    static func delete(bucketName: String, key: String) throws {
        try ObjectKeyLock.withLock(bucketName: bucketName, key: key) {
            defer { StorageStatsService.shared.noteChange(bucketName: bucketName) }
            let path = storagePath(for: bucketName, key: key)

            guard FileManager.default.fileExists(atPath: path) else {
                throw NSError(
                    domain: "ObjectNotFound", code: 404,
                    userInfo: [NSLocalizedDescriptionKey: "Object does not exist"])
            }

            try FileManager.default.removeItem(atPath: path)
            pruneEmptyParentDirectories(forKey: key, bucketName: bucketName)
        }
    }

    /// After an object's files are removed, remove any now-empty ancestor directories up to (but
    /// never including) the bucket root. Because a key maps to a real nested path, a deleted folder
    /// would otherwise linger as an empty directory and show up as a phantom folder in the
    /// directory-scoped listing. `rmdir` only ever removes an *empty* directory (it fails with
    /// ENOTEMPTY otherwise), so a concurrent write that repopulated the folder is never clobbered -
    /// the walk simply stops. Best-effort: a dir that can't be removed just stays.
    static func pruneEmptyParentDirectories(forKey key: String, bucketName: String) {
        // Derive the bucket root the same way `storagePath` builds its base, so both are the same
        // (relative) form - deriving it from a file URL instead yields an absolute path that would
        // never prefix-match the relative `storagePath`, silently disabling all pruning.
        let bucketRoot = "\(BucketHandler.rootPath)\(BucketHandler.encodedBucketName(bucketName))"
        var dir = (storagePath(for: bucketName, key: key) as NSString).deletingLastPathComponent
        while dir.hasPrefix(bucketRoot + "/") {
            if dir.withCString({ rmdir($0) }) != 0 {
                // rmdir only fails on a non-empty directory. Retry once after clearing OS-metadata
                // junk (e.g. macOS `.DS_Store`) that would otherwise leave a folder lingering as a
                // phantom - but only when that junk is the folder's *sole* contents, so any real
                // object artifact keeps the folder intact.
                guard removeOSMetadataIfSoleContents(of: dir),
                    dir.withCString({ rmdir($0) }) == 0
                else { break }
            }
            dir = (dir as NSString).deletingLastPathComponent
        }
    }

    /// Removes the directory a `folder/` prefix maps to when nothing under it is object data -
    /// only (nested) empty directories and OS junk - then prunes its parents. Returns false and
    /// touches nothing when any object artifact exists anywhere below. Bottom-up `rmdir` keeps
    /// this race-safe against a concurrent write (issue #25: phantom folders after EC deletes).
    @discardableResult
    static func removeEmptyPrefixDirectory(bucketName: String, prefix: String) -> Bool {
        guard prefix.hasSuffix("/"), !prefix.contains("..") else { return false }
        let bucketRoot = "\(BucketHandler.rootPath)\(BucketHandler.encodedBucketName(bucketName))"
        let dir = bucketRoot + "/" + String(prefix.dropLast())
        guard dir.hasPrefix(bucketRoot + "/"), removeDirectoryIfNoObjectData(dir) else {
            return false
        }
        // Reuse the ancestor walk: a key named like the removed directory has exactly its parents.
        pruneEmptyParentDirectories(forKey: String(prefix.dropLast()), bucketName: bucketName)
        return true
    }

    private static func removeDirectoryIfNoObjectData(_ dir: String) -> Bool {
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: dir, isDirectory: &isDirectory),
            isDirectory.boolValue,
            let entries = try? FileManager.default.contentsOfDirectory(atPath: dir)
        else { return false }
        for name in entries {
            let path = "\(dir)/\(name)"
            var entryIsDirectory: ObjCBool = false
            guard FileManager.default.fileExists(atPath: path, isDirectory: &entryIsDirectory) else {
                continue
            }
            if entryIsDirectory.boolValue {
                // `.versions` / `.ecshards` hold object data even when their key has no plain file.
                guard !name.hasSuffix(".versions"), !name.hasSuffix(".ecshards"),
                    removeDirectoryIfNoObjectData(path)
                else { return false }
            } else {
                guard ignorableDirectoryEntries.contains(name) || name.hasPrefix("._") else {
                    return false
                }
            }
        }
        _ = removeOSMetadataIfSoleContents(of: dir)
        return dir.withCString({ rmdir($0) }) == 0
    }

    /// OS/desktop metadata files that can appear in a directory without being object data.
    private static let ignorableDirectoryEntries: Set<String> = [
        ".DS_Store", ".localized", "Thumbs.db", "desktop.ini",
    ]

    /// Removes the contents of `dir` and returns true **only** when every entry is a known-junk
    /// *regular file* (macOS `.DS_Store`, AppleDouble `._*`, Windows `Thumbs.db`/`desktop.ini`).
    /// A single real file - or any subdirectory - makes it a no-op returning false, so this can
    /// never recursively delete real data.
    private static func removeOSMetadataIfSoleContents(of dir: String) -> Bool {
        guard let entries = try? FileManager.default.contentsOfDirectory(atPath: dir),
            !entries.isEmpty
        else { return false }
        for name in entries {
            let path = "\(dir)/\(name)"
            var isDirectory: ObjCBool = false
            let exists = FileManager.default.fileExists(atPath: path, isDirectory: &isDirectory)
            let isJunk = ignorableDirectoryEntries.contains(name) || name.hasPrefix("._")
            guard exists, !isDirectory.boolValue, isJunk else { return false }
        }
        for name in entries {
            try? FileManager.default.removeItem(atPath: "\(dir)/\(name)")
        }
        return true
    }

    /// Lists all objects in a bucket with optional prefix filtering
    /// This method returns the LATEST version of each object (or the non-versioned object)
    /// `keyFilter` turns the call into a bounded scan: `maxKeys` then counts every key examined
    /// after `marker` (so the walk stays bounded) while only keys passing the filter are
    /// returned with their metadata, and `nextMarker` is the last key *examined*. That is what
    /// a substring search needs - headers are read for matches only, never for the keys skipped.
    /// Only the recursive walk honours it (a search is always recursive, delimiter nil).
    static func listObjects(
        bucketName: String,
        prefix: String = "",
        delimiter: String? = nil,
        maxKeys: Int = 1000,
        marker: String? = nil,
        keyFilter: (@Sendable (String) -> Bool)? = nil
    ) throws -> (
        objects: [ObjectMeta], commonPrefixes: [String], isTruncated: Bool, nextMarker: String?
    ) {
        let bucketURL = BucketHandler.bucketURL(for: bucketName)

        guard FileManager.default.fileExists(atPath: bucketURL.path) else {
            return ([], [], false, nil)
        }

        // Fast path for a plain folder view: the prefix names a real directory (empty, or ends in
        // "/") and the delimiter is "/", so a single directory level answers the request - with
        // or without a marker - without walking the whole bucket. Arbitrary prefixes that don't
        // land on a directory boundary and versions listings fall through to the recursive walk
        // below, which stays exactly as it was.
        if delimiter == "/", keyFilter == nil, prefix.isEmpty || prefix.hasSuffix("/"),
            !prefix.contains(".."), Self.markerIsAtThisLevel(marker, prefix: prefix)
        {
            return listObjectsInDirectory(
                bucketName: bucketName, prefix: prefix, maxKeys: maxKeys, marker: marker)
        }

        // Every key under `prefix` lives below the directory its folder part names, so the walk
        // starts there rather than at the bucket root - a search scoped to `docs/rep` costs the
        // `docs/` subtree, not the bucket.
        let walkRoot: URL
        if !prefix.contains(".."), let lastSlash = prefix.lastIndex(of: "/") {
            walkRoot = bucketURL.appendingPathComponent(String(prefix[..<lastSlash]))
        } else {
            walkRoot = bucketURL
        }
        guard FileManager.default.fileExists(atPath: walkRoot.path),
            let enumerator = FileManager.default.enumerator(
                at: walkRoot,
                includingPropertiesForKeys: [.isDirectoryKey],
                // No .skipsHiddenFiles: dot-prefixed keys (e.g. ".env") are real
                // objects. Internal artifacts are excluded by the .obj filter below.
                options: []
            )
        else {
            return ([], [], false, nil)
        }

        // Track unique keys. Versioned keys need their `.latest` resolved now (the latest may be
        // a delete marker, which must not be listed); plain keys are read lazily in `metaFor`,
        // only once they made it into the returned page - a plain object is never a marker.
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

            // Page on the key, then roll it up to its common prefix if the delimiter says so. A
            // page whose last entry was the folder `docs/` continues with marker `docs/`, and
            // `docs/a` (> `docs/`) must then not re-create that same folder on the next page.
            if let marker, key <= marker { continue }
            if let commonPrefix = Self.commonPrefix(of: key, prefix: prefix, delimiter: delimiter) {
                if commonPrefix != marker { commonPrefixesSet.insert(commonPrefix) }
                continue
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
        // Keys whose latest version is a delete marker. A plain `.obj` written before versioning
        // was enabled is left in place when the key is later deleted, so the marker must
        // suppress it here or the deleted key keeps showing up in listings.
        var tombstonedKeys: Set<String> = []

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
                    tombstonedKeys.insert(key)
                    continue
                }

                latestByKey[key] = meta
            } catch {
                // If we can't read the latest, skip this key
                continue
            }
        }

        // Plain keys not superseded by a versioned latest are listed, their header read lazily.
        let plainKeys = nonVersionedKeys.filter {
            latestByKey[$0] == nil && !tombstonedKeys.contains($0)
        }

        // EC objects never appear in the `.obj` walk above - every shard header carries the
        // current `isLatest`/`isDeleteMarker` state directly (no separate pointer read needed),
        // so this is a straight filter over the local shard headers, applying the same
        // prefix/marker/delimiter rules the `.obj` pass applies.
        for meta in ErasureCodedObjectHandler.listLocalShardVersions(bucketName: bucketName)
        where meta.isLatest {
            let key = meta.key
            if !prefix.isEmpty && !key.hasPrefix(prefix) { continue }
            if let marker, key <= marker { continue }
            if let commonPrefix = Self.commonPrefix(of: key, prefix: prefix, delimiter: delimiter) {
                if commonPrefix != marker { commonPrefixesSet.insert(commonPrefix) }
                continue
            }
            if meta.isDeleteMarker { continue }
            latestByKey[key] = meta
        }

        return finalizeListing(
            objectKeys: Array(latestByKey.keys) + plainKeys, commonPrefixes: Array(commonPrefixesSet),
            maxKeys: maxKeys, keyFilter: keyFilter
        ) { key in
            if let meta = latestByKey[key] { return meta }
            return try? read(from: storagePath(for: bucketName, key: key), loadData: false).0
        }
    }

    /// The common prefix `key` rolls up to under a single-character `delimiter` (S3 semantics:
    /// `prefix` + everything up to and including the first delimiter after it), or nil when the
    /// key sits directly at this level or no delimiter applies.
    static func commonPrefix(of key: String, prefix: String, delimiter: String?) -> String? {
        guard let delimiter, delimiter.count == 1, let delimChar = delimiter.first else { return nil }
        let keyAfterPrefix = key.dropFirst(prefix.count)
        guard let delimiterIndex = keyAfterPrefix.firstIndex(of: delimChar) else { return nil }
        return prefix + String(keyAfterPrefix[..<delimiterIndex]) + delimiter
    }

    /// True when `marker` names an entry directly at `prefix`'s level (a key there, or a folder
    /// `prefix + name + "/"` - which is what every `nextMarker` this layer hands out is), or no
    /// marker at all. A marker *inside* a subfolder (S3 start-after a key deep in `docs/`) still
    /// has to show `docs/` when that folder holds keys after it - only the recursive walk, which
    /// sees those keys, can decide that, so the directory fast path declines it.
    static func markerIsAtThisLevel(_ marker: String?, prefix: String) -> Bool {
        guard let marker else { return true }
        guard marker.hasPrefix(prefix) else { return true }
        return commonPrefix(of: marker, prefix: prefix, delimiter: "/").map { $0 == marker } ?? true
    }

    /// Sorts the collected objects and common prefixes, merges them in lexicographic order, and
    /// truncates to `maxKeys` (S3 counts objects + prefixes together toward the limit). Shared by
    /// the recursive walk and the directory-scoped fast path so both produce byte-identical S3
    /// listing semantics.
    private static func finalizeListing(
        objectKeys: [String], commonPrefixes: [String], maxKeys: Int,
        keyFilter: (@Sendable (String) -> Bool)? = nil,
        metaFor: (String) -> ObjectMeta?
    ) -> (
        objects: [ObjectMeta], commonPrefixes: [String], isTruncated: Bool, nextMarker: String?
    ) {
        let sortedKeys = objectKeys.sorted()
        let sortedPrefixes = commonPrefixes.sorted()

        var finalObjects: [ObjectMeta] = []
        var finalCommonPrefixes: [String] = []
        var nextMarker: String? = nil

        var keyIndex = 0
        var prefixIndex = 0
        var totalCount = 0

        while totalCount < maxKeys
            && (keyIndex < sortedKeys.count || prefixIndex < sortedPrefixes.count)
        {
            let nextKey = keyIndex < sortedKeys.count ? sortedKeys[keyIndex] : nil
            let nextPrefix = prefixIndex < sortedPrefixes.count ? sortedPrefixes[prefixIndex] : nil

            // Object key or common prefix, whichever is lexicographically first.
            if let key = nextKey, nextPrefix == nil || key < nextPrefix! {
                // `metaFor` resolves the key's metadata only now that it's in the page - so a
                // header is read per returned object, not per object present. A nil result
                // (raced concurrent delete) is simply dropped from the page. A filtered-out key
                // still counts toward the window and moves the marker (see `listObjects`).
                if keyFilter?(key) ?? true, let meta = metaFor(key) { finalObjects.append(meta) }
                nextMarker = key
                keyIndex += 1
            } else if let prefix = nextPrefix {
                finalCommonPrefixes.append(prefix)
                nextMarker = prefix
                prefixIndex += 1
            }

            totalCount += 1
        }

        let isTruncated = keyIndex < sortedKeys.count || prefixIndex < sortedPrefixes.count
        if !isTruncated { nextMarker = nil }

        return (finalObjects, finalCommonPrefixes, isTruncated, nextMarker)
    }

    /// Directory-scoped fast path for the folder view (`delimiter == "/"`, prefix empty or ending
    /// in `/`, any marker). Because a key maps to a real nested path
    /// (`folder/file.txt` -> `.../folder/file.txt.obj`), a single level of `folder/` is all that a
    /// folder listing needs - `.obj` files are objects, `<name>.versions/` / `<name>.ecshards/`
    /// are versioned / erasure-coded objects, and any other subdirectory is a common prefix. This
    /// makes browsing a folder O(entries in that folder) instead of walking the whole bucket, and
    /// it never descends into sibling folders. Object metadata (and the versioned/EC latest
    /// resolution) is read only for the entries actually at this level and after `marker`, so
    /// paging through a huge folder costs one header read per returned object, not per object
    /// present (issue #27). Same output contract as the recursive walk, so the S3 listing tests
    /// cover both.
    private static func listObjectsInDirectory(
        bucketName: String, prefix: String, maxKeys: Int, marker: String?
    ) -> (
        objects: [ObjectMeta], commonPrefixes: [String], isTruncated: Bool, nextMarker: String?
    ) {
        let bucketURL = BucketHandler.bucketURL(for: bucketName)
        // prefix is empty or ends in "/", so it names a directory - drop the trailing slash.
        let dirURL =
            prefix.isEmpty ? bucketURL : bucketURL.appendingPathComponent(String(prefix.dropLast()))

        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: dirURL.path, isDirectory: &isDirectory),
            isDirectory.boolValue,
            let entries = try? FileManager.default.contentsOfDirectory(
                at: dirURL, includingPropertiesForKeys: [.isDirectoryKey], options: [])
        else {
            return ([], [], false, nil)
        }

        // Plain `.obj` files: their key is the filename, and a plain object is never a delete
        // marker (those only ever live under `.versions/`), so the header read is deferred - only
        // the objects that survive sort+truncation into the returned page are read (see `metaFor`
        // below). This keeps a folder with far more direct objects than `maxKeys` from reading
        // every header just to discard most of them.
        var objectPathByKey: [String: String] = [:]
        // Versioned / EC objects need a read now (to find the latest and drop delete-marker
        // latests). A key resolved here supersedes a plain `.obj` of the same key (EC > versioned
        // > non-versioned), matching the recursive walk's precedence.
        var resolvedByKey: [String: ObjectMeta] = [:]
        var resolvedRankByKey: [String: Int] = [:]
        // Keys whose latest version is a delete marker - a legacy plain `.obj` of the same key
        // must not be listed in its place (see `listObjects`' `tombstonedKeys`).
        var tombstonedKeys: Set<String> = []
        var commonPrefixesSet: Set<String> = []

        func resolve(_ key: String, _ meta: ObjectMeta, rank: Int) {
            guard !meta.isDeleteMarker, rank >= (resolvedRankByKey[key] ?? -1) else { return }
            resolvedByKey[key] = meta
            resolvedRankByKey[key] = rank
        }

        // Everything an entry can list as, derived from its name alone - cheap enough to decide
        // "is this past the marker" before paying for any header read.
        func listedName(_ name: String, isDirectory: Bool) -> String? {
            if !isDirectory { return name.hasSuffix(".obj") ? prefix + String(name.dropLast(4)) : nil }
            if name.hasSuffix(".versions") { return prefix + String(name.dropLast(".versions".count)) }
            if name.hasSuffix(".ecshards") { return prefix + String(name.dropLast(".ecshards".count)) }
            return prefix + name + "/"
        }

        for entry in entries {
            let name = entry.lastPathComponent
            let entryIsDir =
                (try? entry.resourceValues(forKeys: [.isDirectoryKey]))?.isDirectory ?? false
            guard let listed = listedName(name, isDirectory: entryIsDir) else { continue }
            if let marker, listed <= marker { continue }

            if !entryIsDir {
                objectPathByKey[listed] = entry.path
            } else if name.hasSuffix(".versions") {
                let key = listed
                if let (meta, _) = try? readVersion(
                    bucketName: bucketName, key: key, versionId: nil, loadData: false)
                {
                    if meta.isDeleteMarker { tombstonedKeys.insert(key) }
                    resolve(key, meta, rank: 1)
                } else if let meta = ErasureCodedObjectHandler.listLocalShardVersions(
                    bucketName: bucketName, key: key
                ).first(where: \.isLatest) {
                    // `readVersion` only knows `.obj` versions; an erasure-coded version lives in
                    // `<versionId>.ecshards/` under the same directory and is found by its header.
                    if meta.isDeleteMarker { tombstonedKeys.insert(key) }
                    resolve(key, meta, rank: 2)
                }
            } else if name.hasSuffix(".ecshards") {
                let key = listed
                // Whichever shard index this node holds - see `listLocalShardVersions`. Trust the
                // header over the filename.
                if let header = ErasureCodedObjectHandler.anyLocalShardHeader(
                    inShardDirectory: entry.path),
                    header.objectMeta.isLatest, header.objectMeta.bucketName == bucketName
                {
                    resolve(key, header.objectMeta, rank: 2)
                }
            } else {
                commonPrefixesSet.insert(listed)
            }
        }

        // A key resolved as versioned/EC wins over a plain `.obj` of the same key, and so does a
        // versioned delete marker.
        for key in resolvedByKey.keys { objectPathByKey[key] = nil }
        for key in tombstonedKeys where resolvedByKey[key] == nil { objectPathByKey[key] = nil }
        let objectKeys = Array(objectPathByKey.keys) + Array(resolvedByKey.keys)

        return finalizeListing(
            objectKeys: objectKeys, commonPrefixes: Array(commonPrefixesSet), maxKeys: maxKeys
        ) { key in
            if let meta = resolvedByKey[key] { return meta }
            guard let path = objectPathByKey[key],
                let (meta, _) = try? read(from: path, loadData: false)
            else { return nil }
            return meta
        }
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

    /// Neutralizes path metacharacters in a client-supplied version id before it's interpolated
    /// into a filesystem path. Every legitimate version id is 32 lowercase hex chars or the
    /// literal `"null"` sentinel, so stripping separators/`..` can't break a real lookup; a
    /// stripped versionId simply resolves to a nonexistent file (NoSuchVersion). Without this,
    /// `?versionId=../../other-bucket/key` on GET/DELETE would escape the key's `.versions/`
    /// directory for a cross-bucket read/delete.
    static func sanitizedVersionId(_ versionId: String) -> String {
        guard versionId.contains("/") || versionId.contains("\\") || versionId.contains("..") else {
            return versionId
        }
        return versionId
            .replacingOccurrences(of: "/", with: "")
            .replacingOccurrences(of: "\\", with: "")
            .replacingOccurrences(of: "..", with: "")
    }

    /// Returns the path for a specific version of an object
    static func versionedPath(for bucketName: String, key: String, versionId: String) -> String {
        return "\(versionedBasePath(for: bucketName, key: key))\(sanitizedVersionId(versionId)).obj"
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
        return try ObjectKeyLock.withLock(bucketName: bucketName, key: key) {
            let (versionId, path, versionedMeta) = try prepareVersionedWrite(
                metadata: metadata, bucketName: bucketName, key: key,
                versioningStatus: versioningStatus)

            try write(metadata: versionedMeta, data: data, to: path)

            if versioningStatus != .disabled {
                try updateLatestPointer(bucketName: bucketName, key: key, versionId: versionId)
            }

            return versionId
        }
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
        return try ObjectKeyLock.withLock(bucketName: bucketName, key: key) {
            let (versionId, path, versionedMeta) = try prepareVersionedWrite(
                metadata: metadata, bucketName: bucketName, key: key,
                versioningStatus: versioningStatus)

            try writeStreamed(metadata: versionedMeta, payloadSources: payloadSources, to: path)

            if versioningStatus != .disabled {
                try updateLatestPointer(bucketName: bucketName, key: key, versionId: versionId)
            }

            return versionId
        }
    }

    /// Shared setup for both versioned-write flavors: picks the version ID and target path
    /// for the bucket's versioning state, demotes existing versions, and stamps the metadata.
    /// Not private: `ErasureCodedWriteCoordinator`'s caller reuses the identical version-id-
    /// minting/demote-versions logic - the `path` it returns is just unused there, since an EC
    /// write's shard base path is a different, independently-computed scheme keyed by the same
    /// version id.
    static func prepareVersionedWrite(
        metadata: ObjectMeta,
        bucketName: String,
        key: String,
        versioningStatus: VersioningStatus
    ) throws -> (versionId: String, path: String, meta: ObjectMeta) {
        return try ObjectKeyLock.withLock(bucketName: bucketName, key: key) {
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
        // A versioned latest - including a delete marker - is authoritative: a plain `.obj`
        // from before versioning was enabled must not resurface once the key is deleted.
        if let (meta, data) = try? readVersion(
            bucketName: bucketName, key: key, versionId: nil, loadData: loadData)
        {
            return meta.isDeleteMarker ? nil : (meta, data)
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
    /// exist. Used by operations that need the path itself, not just the decoded contents (e.g.
    /// object tagging). For a "current version" lookup, a delete marker counts as "doesn't
    /// exist" - matching GetObject's own handling; an explicit `versionId` may still target one.
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
        // Through the same writer as the version file: a pointer that survives a crash only in
        // the page cache leaves the just-acknowledged version unreadable ("No versions found")
        // or serves the previous one as current.
        var writer = try AtomicObjectWriter(finalPath: latestPointerPath(for: bucketName, key: key))
        do {
            try writer.write(Data(versionId.utf8))
            try writer.finish()
        } catch {
            writer.abort()
            throw error
        }
    }

    /// Marks all versions of an object as not latest. Runs on every write to a versioned (or
    /// suspended-versioning) bucket, over every prior version of the key. Only the metadata's
    /// `isLatest` flag ever needs changing, so this is header-only for every file it checks
    /// (`payloadLocation`, not `read`), and window-copies the payload back into place rather
    /// than buffering it as `Data`, avoiding a full read+copy of a multi-gigabyte payload just
    /// to flip one boolean.
    static func markAllVersionsNotLatest(bucketName: String, key: String) throws {
        try ObjectKeyLock.withLock(bucketName: bucketName, key: key) {
            let basePath = versionedBasePath(for: bucketName, key: key)
            let baseURL = URL(fileURLWithPath: basePath)

            guard FileManager.default.fileExists(atPath: basePath) else {
                return
            }

            let contents = try FileManager.default.contentsOfDirectory(
                at: baseURL, includingPropertiesForKeys: nil)

            for fileURL in contents where fileURL.pathExtension == "obj" {
                do {
                    let snapshot = try openPayloadSnapshot(path: fileURL.path)
                    defer { _ = POSIXFile.close(snapshot.fd) }
                    guard snapshot.meta.isLatest else { continue }

                    var updatedMeta = snapshot.meta
                    updatedMeta.isLatest = false
                    try writeStreamed(
                        metadata: updatedMeta, payloadFd: snapshot.fd,
                        payloadOffset: snapshot.payloadOffset, payloadSize: snapshot.payloadSize,
                        to: fileURL.path)
                } catch {
                    // Skip files we can't read
                    continue
                }
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

        // EC versions of this key, one entry per version from whichever shard this node holds
        // (see `listAllVersions`'s identical reasoning).
        for meta in ErasureCodedObjectHandler.listLocalShardVersions(bucketName: bucketName, key: key)
        where !versions.contains(where: { $0.versionId == meta.versionId }) {
            versions.append(meta)
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
                // No .skipsHiddenFiles: dot-prefixed keys are real objects.
                // Internal artifacts are excluded by the .obj filter below.
                options: []
            )
        else {
            return ([], [], [], false, nil, nil)
        }

        // Every version (or delete marker) under `prefix`, before any marker is applied.
        // Paging is positional over the fully sorted list below, never a filter on version id
        // strings: version ids are random, so `versionId <= marker` skipped and duplicated
        // versions across pages.
        var entries: [(meta: ObjectMeta, isDeleteMarker: Bool)] = []
        var commonPrefixSet: Set<String> = []

        func admit(_ meta: ObjectMeta) {
            if !prefix.isEmpty && !meta.key.hasPrefix(prefix) { return }
            if let commonPrefix = Self.commonPrefix(of: meta.key, prefix: prefix, delimiter: delimiter) {
                commonPrefixSet.insert(commonPrefix)
                return
            }
            entries.append((meta, meta.isDeleteMarker))
        }

        // Collect all .obj files (both versioned and non-versioned)
        for case let fileURL as URL in enumerator {
            guard fileURL.pathExtension == "obj" else { continue }

            // Cheap name-based prefix check before paying for the header read.
            let relativePath = fileURL.path.replacingOccurrences(of: bucketURL.path + "/", with: "")
            let objectKey: String
            if relativePath.contains(".versions/") {
                objectKey = relativePath.components(separatedBy: ".versions/")[0]
            } else {
                objectKey = String(relativePath.dropLast(4))
            }
            if !prefix.isEmpty && !objectKey.hasPrefix(prefix) { continue }

            guard let (meta, _) = try? read(from: fileURL.path, loadData: false) else { continue }
            admit(meta)
        }

        // EC versions never appear in the `.obj` walk above - `listLocalShardVersions` yields
        // exactly one `ObjectMeta` per version this node holds any shard of, no re-read needed.
        for meta in ErasureCodedObjectHandler.listLocalShardVersions(bucketName: bucketName) {
            admit(meta)
        }

        // S3 order: key ascending, newest version first; version id breaks exact timestamp ties
        // so the order is stable across requests.
        entries.sort {
            if $0.meta.key != $1.meta.key { return $0.meta.key < $1.meta.key }
            if $0.meta.updatedAt != $1.meta.updatedAt { return $0.meta.updatedAt > $1.meta.updatedAt }
            return ($0.meta.versionId ?? "null") < ($1.meta.versionId ?? "null")
        }

        // Resume after the marker's position. A key marker alone (or a version marker that no
        // longer exists) resumes after the whole key.
        var start = 0
        if let keyMarker, !keyMarker.isEmpty {
            if let versionIdMarker, !versionIdMarker.isEmpty,
                let position = entries.firstIndex(where: {
                    $0.meta.key == keyMarker && ($0.meta.versionId ?? "null") == versionIdMarker
                })
            {
                start = position + 1
            } else {
                start = entries.firstIndex(where: { $0.meta.key > keyMarker }) ?? entries.count
            }
        }

        let page = entries[start..<Swift.min(entries.count, start + Swift.max(maxKeys, 0))]
        let isTruncated = start + Swift.max(maxKeys, 0) < entries.count
        let versions = page.filter { !$0.isDeleteMarker }.map(\.meta)
        let deleteMarkers = page.filter(\.isDeleteMarker).map(\.meta)

        // Folders before the key marker were already listed on an earlier page.
        let sortedCommonPrefixes = commonPrefixSet.sorted().filter { prefixEntry in
            guard let keyMarker, !keyMarker.isEmpty else { return true }
            return prefixEntry > keyMarker
        }

        let nextKeyMarker = isTruncated ? page.last?.meta.key : nil
        let nextVersionIdMarker = isTruncated ? (page.last?.meta.versionId ?? "null") : nil
        return (versions, deleteMarkers, sortedCommonPrefixes, isTruncated, nextKeyMarker, nextVersionIdMarker)
    }

    /// Deletes a specific version of an object
    static func deleteVersion(bucketName: String, key: String, versionId: String) throws {
        try ObjectKeyLock.withLock(bucketName: bucketName, key: key) {
            defer { StorageStatsService.shared.noteChange(bucketName: bucketName) }
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
                        pruneEmptyParentDirectories(forKey: key, bucketName: bucketName)
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

            // Clean up empty directories - the `.versions/` dir once its history is gone, then any
            // now-empty parent folders so the object's folder doesn't linger as a phantom.
            try cleanupEmptyVersionsDirectory(bucketName: bucketName, key: key)
            pruneEmptyParentDirectories(forKey: key, bucketName: bucketName)
        }
    }

    /// Creates a delete marker (soft delete for versioned buckets)
    static func createDeleteMarker(bucketName: String, key: String) throws -> ObjectMeta {
        return try ObjectKeyLock.withLock(bucketName: bucketName, key: key) {
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
    }

    /// Creates a delete marker for a *suspended*-versioning bucket - distinct from
    /// `createDeleteMarker` (which is for `.enabled` buckets only). Per S3's documented
    /// suspended-versioning delete semantics, this must only ever affect the "null" version: it's
    /// overwritten with a delete marker, while every other genuinely-versioned object stays
    /// untouched and retrievable by its real version ID. Using `createDeleteMarker` here would be
    /// wrong: it mints a fresh random version ID instead of the "null" S3 expects.
    static func createNullVersionDeleteMarker(bucketName: String, key: String) throws -> ObjectMeta {
        return try ObjectKeyLock.withLock(bucketName: bucketName, key: key) {
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
    }

    /// Updates the .latest pointer to the most recent non-deleted version
    private static func updateLatestToMostRecent(bucketName: String, key: String) throws {
        try ObjectKeyLock.withLock(bucketName: bucketName, key: key) {
            let versions = try listVersions(bucketName: bucketName, key: key)

            // Find the most recent version (already sorted by updatedAt descending)
            if let mostRecent = versions.first {
                try updateLatestPointer(bucketName: bucketName, key: key, versionId: mostRecent.versionId ?? "null")

                // Update the metadata to mark it as latest - header-only read + self window-copy
                // (see markAllVersionsNotLatest) rather than buffering the whole payload just to
                // flip one boolean.
                let path = versionedPath(for: bucketName, key: key, versionId: mostRecent.versionId ?? "null")
                if let snapshot = try? openPayloadSnapshot(path: path) {
                    defer { _ = POSIXFile.close(snapshot.fd) }
                    var updatedMeta = snapshot.meta
                    updatedMeta.isLatest = true
                    try writeStreamed(
                        metadata: updatedMeta, payloadFd: snapshot.fd,
                        payloadOffset: snapshot.payloadOffset, payloadSize: snapshot.payloadSize,
                        to: path)
                }
            } else {
                // No versions left, remove the .latest pointer
                let pointerPath = latestPointerPath(for: bucketName, key: key)
                try? FileManager.default.removeItem(atPath: pointerPath)
            }
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

        // Only safe to remove the whole .versions directory once neither format has anything
        // left in it - a key's version history could be a mix of pre-EC `.obj` versions and
        // later `.ecshards` ones sharing this same parent directory.
        let objFiles = contents.filter { $0.pathExtension == "obj" }
        let ecshardDirs = contents.filter { $0.pathExtension == "ecshards" }
        if objFiles.isEmpty && ecshardDirs.isEmpty {
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
