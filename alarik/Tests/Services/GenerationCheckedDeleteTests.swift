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
import Testing

@testable import Alarik

/// A replicated or outbox-replayed delete names the generation it removes. These cover the two
/// receivers: the plain `.obj` path and the erasure-coded shard directory.
@Suite("Generation-checked deletes", .serialized)
struct GenerationCheckedDeleteTests {
    private func meta(_ bucket: String, _ key: String, etag: String) -> ObjectMeta {
        ObjectMeta(bucketName: bucket, key: key, size: 1, contentType: "text/plain", etag: etag, updatedAt: Date())
    }

    @Test("a plain delete naming an older ETag leaves a re-created object alone")
    func plainDeleteChecksGeneration() throws {
        let bucket = "gen-plain-bucket"
        let root = "\(BucketHandler.rootPath)\(bucket)"
        try? FileManager.default.removeItem(atPath: root)
        try FileManager.default.createDirectory(atPath: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: root) }
        let path = ObjectFileHandler.storagePath(for: bucket, key: "k")

        // The object was deleted and re-created with new content before the delete arrives.
        try ObjectFileHandler.write(metadata: meta(bucket, "k", etag: "new"), data: Data("x".utf8), to: path)
        let skipped = try S3Service.deleteObject(
            bucketName: bucket, key: "k", versionId: nil, versioningStatus: .disabled, expectedETag: "old")
        #expect(skipped.versionId == nil && !skipped.isDeleteMarker)
        #expect(FileManager.default.fileExists(atPath: path))

        // The generation it names, or no expectation at all, removes the object.
        _ = try S3Service.deleteObject(
            bucketName: bucket, key: "k", versionId: nil, versioningStatus: .disabled, expectedETag: "new")
        #expect(!FileManager.default.fileExists(atPath: path))
        try ObjectFileHandler.write(metadata: meta(bucket, "k", etag: "again"), data: Data("x".utf8), to: path)
        _ = try S3Service.deleteObject(bucketName: bucket, key: "k", versionId: nil, versioningStatus: .disabled)
        #expect(!FileManager.default.fileExists(atPath: path))
    }

    @Test("a shard delete naming an older ETag leaves a re-created generation's shards alone")
    func shardDeleteChecksGeneration() throws {
        let bucket = "gen-shard-bucket"
        defer { try? FileManager.default.removeItem(at: BucketHandler.bucketURL(for: bucket)) }
        let shardPath = ErasureCodedObjectHandler.shardPath(bucketName: bucket, key: "k", shardIndex: 1)
        func writeShard(etag: String) throws {
            try FileManager.default.createDirectory(
                atPath: (shardPath as NSString).deletingLastPathComponent, withIntermediateDirectories: true)
            var writer = try ErasureCodedShardWriter(
                path: shardPath,
                header: ErasureCodedShardHeader(
                    shardIndex: 1, dataShards: 2, parityShards: 2, stripeUnitSize: 4096,
                    stripeCount: 0, objectMeta: meta(bucket, "k", etag: etag)))
            try writer.finish()
        }
        try writeShard(etag: "new")

        #expect(
            !ErasureCodedObjectHandler.removeShardDirectory(
                bucketName: bucket, key: "k", versionId: nil, unlessETagDiffersFrom: "old"))
        #expect(FileManager.default.fileExists(atPath: shardPath))
        #expect(
            ErasureCodedObjectHandler.removeShardDirectory(
                bucketName: bucket, key: "k", versionId: nil, unlessETagDiffersFrom: "new"))
        #expect(!FileManager.default.fileExists(atPath: shardPath))
        // Nothing there: nothing removed, no error.
        #expect(
            !ErasureCodedObjectHandler.removeShardDirectory(
                bucketName: bucket, key: "k", versionId: nil, unlessETagDiffersFrom: nil))
    }
}
