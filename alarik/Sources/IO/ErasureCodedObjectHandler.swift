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

/// One `.ecshard` file's header: everything needed to independently verify and decode this
/// shard without consulting any other file. `objectMeta` is the full object metadata (not just
/// a pointer) - mirrors `.obj`'s header, and lets rank-0 self-describe a version from shard 0
/// alone with no extra replicated pointer file.
struct ErasureCodedShardHeader: Codable, Sendable {
    let shardIndex: Int
    let dataShards: Int
    let parityShards: Int
    let stripeUnitSize: Int
    let stripeCount: Int
    let objectMeta: ObjectMeta
}

enum ErasureCodedObjectHandlerError: Error, CustomStringConvertible {
    case openFailed(path: String, errno: Int32)
    case invalidHeader(path: String)
    case shortRead(path: String)
    case stripeIndexOutOfRange(shardPath: String, index: Int)
    case checksumMismatch(shardIndex: Int, stripeIndex: Int)

    var description: String {
        switch self {
        case .openFailed(let path, let code):
            "Could not open '\(path)' (errno \(code))"
        case .invalidHeader(let path):
            "Invalid or truncated .ecshard header at '\(path)'"
        case .shortRead(let path):
            "Unexpected end of file reading '\(path)'"
        case .stripeIndexOutOfRange(let path, let index):
            "Stripe index \(index) out of range for '\(path)'"
        case .checksumMismatch(let shardIndex, let stripeIndex):
            "Checksum mismatch: shard \(shardIndex), stripe \(stripeIndex)"
        }
    }
}

/// Path scheme and low-level per-shard file I/O for erasure-coded objects - the `.ecshard`
/// counterpart to `ObjectFileHandler`'s `.obj` handling. Each (bucket, key[, versionId]) that's
/// erasure-coded owns a directory of `k+m` shard files (one node's local slice each), mirroring
/// `ObjectFileHandler`'s `.versions/` directory-of-files convention.
enum ErasureCodedObjectHandler {
    static let jsonEncoder = JSONEncoder()
    static let jsonDecoder = JSONDecoder()

    private static func sanitizedKey(_ key: String) -> String {
        guard key.contains("..") else { return key }
        return key.components(separatedBy: "/")
            .map { $0.replacingOccurrences(of: "..", with: "") }
            .joined(separator: "/")
    }

    /// Directory holding every shard file for the non-versioned (or versioning-disabled)
    /// current object at `key`.
    static func shardBasePath(bucketName: String, key: String) -> String {
        let encodedBucket = BucketHandler.encodedBucketName(bucketName)
        return "\(BucketHandler.rootPath)\(encodedBucket)/\(sanitizedKey(key)).ecshards/"
    }

    static func shardPath(bucketName: String, key: String, shardIndex: Int) -> String {
        "\(shardBasePath(bucketName: bucketName, key: key))\(shardIndex).ecshard"
    }

    /// Directory holding every shard file for one specific version - the EC counterpart of
    /// `ObjectFileHandler.versionedPath`, under the same `.versions/` parent directory.
    static func versionedShardBasePath(bucketName: String, key: String, versionId: String) -> String {
        let encodedBucket = BucketHandler.encodedBucketName(bucketName)
        return "\(BucketHandler.rootPath)\(encodedBucket)/\(sanitizedKey(key)).versions/\(versionId).ecshards/"
    }

    static func versionedShardPath(
        bucketName: String, key: String, versionId: String, shardIndex: Int
    ) -> String {
        "\(versionedShardBasePath(bucketName: bucketName, key: key, versionId: versionId))\(shardIndex).ecshard"
    }

    /// Version-aware convenience overloads: a `nil` `versionId` selects the non-versioned path,
    /// a non-`nil` one the versioned path. Collapses the `versionId != nil ? versioned : plain`
    /// ternary that otherwise recurs at every shard call site into one place.
    static func shardBasePath(bucketName: String, key: String, versionId: String?) -> String {
        if let versionId {
            return versionedShardBasePath(bucketName: bucketName, key: key, versionId: versionId)
        }
        return shardBasePath(bucketName: bucketName, key: key)
    }

    static func shardPath(
        bucketName: String, key: String, versionId: String?, shardIndex: Int
    ) -> String {
        if let versionId {
            return versionedShardPath(
                bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex)
        }
        return shardPath(bucketName: bucketName, key: key, shardIndex: shardIndex)
    }

    /// Rewrites one local shard file's header in place, streaming every stripe record
    /// (checksum + payload) across byte-for-byte unchanged - the EC counterpart of
    /// `ObjectFileHandler.rewriteMetadata`, same "never touch the payload" cost profile. Returns
    /// the updated header so the caller (a cluster fan-out) doesn't need to re-read it.
    static func rewriteShardMetadata(
        at path: String, transform: (inout ObjectMeta) -> Void
    ) throws -> ErasureCodedShardHeader {
        let reader = try ErasureCodedShardReader(path: path)
        defer { reader.close() }

        var updatedMeta = reader.header.objectMeta
        transform(&updatedMeta)
        let updatedHeader = ErasureCodedShardHeader(
            shardIndex: reader.header.shardIndex, dataShards: reader.header.dataShards,
            parityShards: reader.header.parityShards, stripeUnitSize: reader.header.stripeUnitSize,
            stripeCount: reader.header.stripeCount, objectMeta: updatedMeta)

        var writer = try ErasureCodedShardWriter(path: path, header: updatedHeader)
        do {
            for stripeIndex in 0..<reader.header.stripeCount {
                try writer.appendStripe(try reader.readStripe(stripeIndex))
            }
            try writer.finish()
        } catch {
            writer.abort()
            throw error
        }
        return updatedHeader
    }

    /// Demotes every OTHER local version's shard for `key` to `isLatest = false` - the EC
    /// counterpart of `ObjectFileHandler.markAllVersionsNotLatest`. Called on every node that
    /// receives a shard whose header says `isLatest = true` (see
    /// `InternalClusterErasureCodedController.handlePush`), not just the write coordinator -
    /// each of the `k+m` nodes must independently keep its own local `.latest` pointer and
    /// per-shard `isLatest` flags correct, since any of them can serve a read independently.
    static func markAllLocalShardsNotLatest(bucketName: String, key: String, exceptVersionId: String? = nil) {
        // Demote every shard directory holding a locally-latest shard for this key. Two families
        // of directory can exist simultaneously and both must be swept: the versioned ones under
        // `key.versions/<versionId>.ecshards/`, and the non-versioned `key.ecshards/` (an object
        // written while versioning was disabled, still `isLatest` in its own header - missed here,
        // it would keep claiming latest forever once versioning is later enabled). `exceptVersionId`
        // lets a commit skip re-demoting the version it's promoting.
        var shardDirs: [(url: URL, versionId: String?)] = []

        let versionsBase = ObjectFileHandler.versionedBasePath(for: bucketName, key: key)
        if FileManager.default.fileExists(atPath: versionsBase),
            let versionDirs = try? FileManager.default.contentsOfDirectory(
                at: URL(fileURLWithPath: versionsBase), includingPropertiesForKeys: nil)
        {
            for dir in versionDirs where dir.pathExtension == "ecshards" {
                // `<versionId>.ecshards` -> versionId is the filename without the extension.
                let versionId = dir.deletingPathExtension().lastPathComponent
                shardDirs.append((dir, versionId))
            }
        }

        let plainBase = shardBasePath(bucketName: bucketName, key: key)
        if FileManager.default.fileExists(atPath: plainBase) {
            shardDirs.append((URL(fileURLWithPath: plainBase), nil))
        }

        for (dir, versionId) in shardDirs {
            if let exceptVersionId, versionId == exceptVersionId { continue }
            guard
                let shardFiles = try? FileManager.default.contentsOfDirectory(
                    at: dir, includingPropertiesForKeys: nil)
            else { continue }
            for shardFile in shardFiles where shardFile.pathExtension == "ecshard" {
                guard let reader = try? ErasureCodedShardReader(path: shardFile.path) else { continue }
                let isLatest = reader.header.objectMeta.isLatest
                reader.close()
                guard isLatest else { continue }
                _ = try? rewriteShardMetadata(at: shardFile.path) { $0.isLatest = false }
            }
        }
    }

    /// Promotes this node's local shard(s) for one specific version back to `isLatest = true` -
    /// the inverse of a demote, used by the failed-quorum restore path to un-demote the prior
    /// version whose promotion was rolled back. No-op when this node holds no shard for the
    /// version (nothing local to promote).
    static func markLocalShardsLatest(bucketName: String, key: String, versionId: String?) {
        for index in locallyHeldShardIndices(bucketName: bucketName, key: key, versionId: versionId) {
            let path = shardPath(
                bucketName: bucketName, key: key, versionId: versionId, shardIndex: index)
            _ = try? rewriteShardMetadata(at: path) { $0.isLatest = true }
        }
    }

    /// The shard indices this node physically holds for (bucketName, key[, versionId]) - read
    /// straight from the `.ecshards` directory's filenames. Normally exactly one entry (a node
    /// holds a single shard per version), but transiently more during reindexing, when a node has
    /// received its new-rank shard but not yet reclaimed the old-rank one. Crucially: a shard's
    /// index is fixed at encode time and stored in its filename, but which node holds a given
    /// index drifts as HRW ranks shift on membership change - so this is the *only* correct way to
    /// answer "what does this node actually hold", never "does the file matching my current rank
    /// exist". Empty when the object isn't erasure-coded here (or doesn't exist).
    static func locallyHeldShardIndices(bucketName: String, key: String, versionId: String?) -> [Int] {
        let basePath =
            versionId != nil
            ? versionedShardBasePath(bucketName: bucketName, key: key, versionId: versionId!)
            : shardBasePath(bucketName: bucketName, key: key)
        guard let entries = try? FileManager.default.contentsOfDirectory(atPath: basePath) else {
            return []
        }
        var indices: [Int] = []
        for entry in entries where entry.hasSuffix(".ecshard") {
            let name = String(entry.dropLast(".ecshard".count))
            if let index = Int(name) { indices.append(index) }
        }
        return indices
    }

    /// True when this node holds any shard at all for (bucketName, key[, versionId]) - the
    /// location-independent replacement for "does my current-rank shard file exist" that every
    /// EC-detection call site needs (a node holding a drifted-index shard after a membership
    /// change still IS a shard holder, and must never be treated as a non-holder).
    static func holdsAnyLocalShard(bucketName: String, key: String, versionId: String?) -> Bool {
        !locallyHeldShardIndices(bucketName: bucketName, key: key, versionId: versionId).isEmpty
    }

    /// Removes just the one `staleShardIndex` shard file (not the whole `.ecshards` directory),
    /// then removes the now-empty directory only if nothing else remains. Distinct from a full
    /// version delete (`shardBasePath` removal): during reindexing a node legitimately holds both
    /// its freshly delivered new-rank shard and its not-yet-reclaimed old-rank shard in the same
    /// directory, so reclaiming the stale one must never take the live one with it.
    static func removeLocalShard(bucketName: String, key: String, versionId: String?, shardIndex: Int) {
        try? FileManager.default.removeItem(
            atPath: shardPath(
                bucketName: bucketName, key: key, versionId: versionId, shardIndex: shardIndex))

        let basePath = shardBasePath(bucketName: bucketName, key: key, versionId: versionId)
        if let remaining = try? FileManager.default.contentsOfDirectory(atPath: basePath),
            remaining.isEmpty
        {
            try? FileManager.default.removeItem(atPath: basePath)
        }
    }

    /// Undoes a rolled-back latest promotion for one node: repoints `.latest` to `priorVersionId`
    /// (re-promoting that version's local shard), or removes the pointer entirely when there was
    /// no prior version. Called on every responsible node after a coordinator's EC write fails
    /// quorum, so a peer that had already repointed to the now-deleted version doesn't leave
    /// `.latest` dangling at a shardless version (which a subsequent GET would resolve to and 404).
    static func restoreLatest(bucketName: String, key: String, priorVersionId: String?) {
        if let priorVersionId {
            try? ObjectFileHandler.updateLatestPointer(
                bucketName: bucketName, key: key, versionId: priorVersionId)
            markLocalShardsLatest(bucketName: bucketName, key: key, versionId: priorVersionId)
        } else {
            let pointerPath = ObjectFileHandler.latestPointerPath(for: bucketName, key: key)
            try? FileManager.default.removeItem(atPath: pointerPath)
        }
    }

    /// Walks every local `.ecshard` file across every bucket, parsing just each file's header
    /// (never any stripe data) - the EC counterpart of `ObjectFileHandler.listAllVersions`, used
    /// by `ErasureCodedRebalanceService`'s self-scoped membership-change walk. A node only ever
    /// physically holds the shard(s) it's currently or previously responsible for, so this is
    /// inherently local, same as every other cluster-rebalance walk in this codebase.
    static func listAllLocalShards() -> [(path: String, header: ErasureCodedShardHeader)] {
        guard
            let enumerator = FileManager.default.enumerator(
                at: BucketHandler.rootURL, includingPropertiesForKeys: [.isDirectoryKey],
                options: [.skipsHiddenFiles])
        else { return [] }

        var results: [(path: String, header: ErasureCodedShardHeader)] = []
        for case let fileURL as URL in enumerator {
            guard fileURL.pathExtension == "ecshard" else { continue }
            guard let reader = try? ErasureCodedShardReader(path: fileURL.path) else { continue }
            results.append((fileURL.path, reader.header))
            reader.close()
        }
        return results
    }

    /// Every local shard-0 header for `bucketName` (optionally restricted to one `key`) - the
    /// EC analog of walking `.obj` files, used by listing/lifecycle code that must see both
    /// formats (`ObjectFileHandler.listObjects`/`listVersions`/`listAllVersions`). Shard index 0
    /// lives on rank-0 by construction, so this yields exactly one hit per (key, version) this
    /// node happens to be rank-0 for - never a fraction, never duplicated by the other `k+m-1`
    /// shards, and the header's `ObjectMeta` is already complete (no separate re-read needed the
    /// way a bare `.obj` path requires).
    static func listLocalShardZeroEntries(bucketName: String, key: String? = nil) -> [ObjectMeta] {
        listAllLocalShards()
            .filter { $0.header.shardIndex == 0 && $0.header.objectMeta.bucketName == bucketName }
            .filter { key == nil || $0.header.objectMeta.key == key }
            .map(\.header.objectMeta)
    }
}

/// Writes one `.ecshard` file: header, then one `[32-byte SHA256 checksum][stripeUnitSize-byte
/// payload]` record per stripe, appended as they're produced - fully streaming, no need to know
/// the stripe count's checksums ahead of the header (unlike a checksum-table-in-header design).
/// Atomic and durable via `AtomicObjectWriter`, exactly like one `.obj` file.
struct ErasureCodedShardWriter {
    private var writer: AtomicObjectWriter
    private(set) var stripesWritten = 0

    init(path: String, header: ErasureCodedShardHeader) throws {
        writer = try AtomicObjectWriter(finalPath: path)
        do {
            let jsonData = try ErasureCodedObjectHandler.jsonEncoder.encode(header)
            var headerBytes = Data(capacity: 4 + jsonData.count)
            withUnsafeBytes(of: UInt32(jsonData.count).bigEndian) {
                headerBytes.append(contentsOf: $0)
            }
            headerBytes.append(jsonData)
            try writer.write(headerBytes)
        } catch {
            writer.abort()
            throw error
        }
    }

    mutating func appendStripe(_ stripeData: Data) throws {
        do {
            let checksum = Data(SHA256.hash(data: stripeData))
            try writer.write(checksum)
            try writer.write(stripeData)
            stripesWritten += 1
        } catch {
            writer.abort()
            throw error
        }
    }

    mutating func finish() throws {
        try writer.finish()
    }

    mutating func abort() {
        writer.abort()
    }
}

/// Reads one `.ecshard` file: parses the header on open, then serves individual stripes by
/// index with checksum verification. A class (not a struct, unlike the writer) so a decode
/// holding several of these at once can't leak file descriptors on an early-thrown error - the
/// `deinit` closes the fd even if a caller forgets to.
final class ErasureCodedShardReader {
    let path: String
    let header: ErasureCodedShardHeader
    private let fd: Int32
    private let payloadOffset: Int
    private let stripeRecordSize: Int
    private var closed = false

    init(path: String) throws {
        let fd = POSIXFile.open(path, O_RDONLY)
        guard fd >= 0 else {
            throw ErasureCodedObjectHandlerError.openFailed(path: path, errno: errno)
        }

        do {
            var lengthBytes = [UInt8](repeating: 0, count: 4)
            guard POSIXFile.read(fd, &lengthBytes, 4) == 4 else {
                throw ErasureCodedObjectHandlerError.invalidHeader(path: path)
            }
            let headerLength = Int(
                UInt32(bigEndian: lengthBytes.withUnsafeBytes { $0.load(as: UInt32.self) }))

            var jsonBytes = [UInt8](repeating: 0, count: headerLength)
            guard POSIXFile.read(fd, &jsonBytes, headerLength) == headerLength else {
                throw ErasureCodedObjectHandlerError.invalidHeader(path: path)
            }
            let header = try jsonBytes.withUnsafeBufferPointer { buffer in
                try ErasureCodedObjectHandler.jsonDecoder.decode(
                    ErasureCodedShardHeader.self, from: Data(buffer))
            }

            self.path = path
            self.header = header
            self.fd = fd
            self.payloadOffset = 4 + headerLength
            self.stripeRecordSize = 32 + header.stripeUnitSize
        } catch {
            _ = POSIXFile.close(fd)
            throw error
        }
    }

    /// Reads and checksum-verifies stripe `index`. Throws `.checksumMismatch` rather than
    /// returning silently-corrupt bytes - callers (`StripeDecoder`) treat that exactly like a
    /// missing shard and reconstruct from survivors instead.
    func readStripe(_ index: Int) throws -> Data {
        guard index >= 0, index < header.stripeCount else {
            throw ErasureCodedObjectHandlerError.stripeIndexOutOfRange(shardPath: path, index: index)
        }
        let recordOffset = payloadOffset + index * stripeRecordSize

        var checksumBytes = [UInt8](repeating: 0, count: 32)
        guard POSIXFile.pread(fd, &checksumBytes, 32, off_t(recordOffset)) == 32 else {
            throw ErasureCodedObjectHandlerError.shortRead(path: path)
        }
        var stripeBytes = [UInt8](repeating: 0, count: header.stripeUnitSize)
        guard
            POSIXFile.pread(fd, &stripeBytes, header.stripeUnitSize, off_t(recordOffset + 32))
                == header.stripeUnitSize
        else {
            throw ErasureCodedObjectHandlerError.shortRead(path: path)
        }

        let actualChecksum = Array(SHA256.hash(data: Data(stripeBytes)))
        guard actualChecksum == checksumBytes else {
            throw ErasureCodedObjectHandlerError.checksumMismatch(
                shardIndex: header.shardIndex, stripeIndex: index)
        }
        return Data(stripeBytes)
    }

    func close() {
        guard !closed else { return }
        closed = true
        _ = POSIXFile.close(fd)
    }

    deinit { close() }
}
