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
import Logging
import Testing

@testable import Alarik

@Suite("Key path migration")
struct KeyPathMigrationTests {
    private let fm = FileManager.default

    private func tempRoot() throws -> String {
        let dir = NSTemporaryDirectory() + "kpm-" + UUID().uuidString + "/"
        try fm.createDirectory(atPath: dir, withIntermediateDirectories: true)
        return dir
    }

    /// Writes a real `.obj` file whose header names `key` and `versionId`.
    private func writeObject(_ path: String, key: String, versionId: String? = nil) throws {
        try fm.createDirectory(
            atPath: (path as NSString).deletingLastPathComponent, withIntermediateDirectories: true)
        let meta = ObjectMeta(
            bucketName: "b", key: key, size: 1, contentType: "text/plain", etag: "e", updatedAt: Date(),
            versionId: versionId)
        try ObjectFileHandler.write(metadata: meta, data: Data("x".utf8), to: path)
    }

    private func exists(_ path: String) -> Bool { fm.fileExists(atPath: path) }

    @Test("pre-escaping names are renamed into their escaped form, real sidecars and plain names untouched")
    func renamesLegacyNames() throws {
        let root = try tempRoot()
        defer { try? fm.removeItem(atPath: root) }
        let b = root + "b/"
        let v1 = ObjectMeta.generateVersionId()
        let v2 = ObjectMeta.generateVersionId()

        // Legacy: key `model.obj` with a versions sidecar, folder `data.obj/`, folder `backup.versions/`.
        try writeObject(b + "model.obj.obj", key: "model.obj")
        try writeObject(b + "model.obj.versions/\(v1).obj", key: "model.obj", versionId: v1)
        try writeObject(b + "data.obj/file.txt.obj", key: "data.obj/file.txt")
        try writeObject(b + "backup.versions/notes.txt.obj", key: "backup.versions/notes.txt")
        try writeObject(b + "deep/x.ecshards/inner.obj", key: "deep/x.ecshards/inner")
        // Already correct: a real sidecar, a shard directory, plain keys.
        try writeObject(b + "report.versions/\(v2).obj", key: "report", versionId: v2)
        try writeObject(b + "report.obj", key: "report")
        try fm.createDirectory(atPath: b + "enc.ecshards", withIntermediateDirectories: true)
        fm.createFile(atPath: b + "enc.ecshards/0.ecshard", contents: Data("s".utf8))
        try writeObject(b + "docs/readme.obj", key: "docs/readme")
        fm.createFile(atPath: b + "docs/" + AtomicObjectWriter.tempFilePrefix + UUID().uuidString, contents: Data())
        // Same bytes on disk, told apart only by the recorded key: a legacy `tilde.obj~` (the key
        // itself ends in the marker) versus an already-escaped `other.obj~` (key `other.obj`).
        try writeObject(b + "tilde.obj~.obj", key: "tilde.obj~")
        try writeObject(b + "other.obj~.obj", key: "other.obj")
        // A sidecar known only by its pointer file is left where it is.
        try fm.createDirectory(atPath: b + "ptr.versions", withIntermediateDirectories: true)
        fm.createFile(atPath: b + "ptr.versions/.latest", contents: Data("v".utf8))
        // Only a `.tmp-<UUID>` is a writer's temp file; an object keyed `.tmp-foo.obj` migrates.
        try writeObject(b + ".tmp-foo.obj.obj", key: ".tmp-foo.obj")
        // An unambiguous name is renamed from its name alone; its header is never consulted.
        fm.createFile(atPath: b + "junk.obj.obj", contents: Data("not an object header".utf8))

        let report = KeyPathMigration.migrate(root: root)

        #expect(report.renamed == 8)
        #expect(exists(b + ".tmp-foo.obj~.obj") && !exists(b + ".tmp-foo.obj.obj"))
        #expect(exists(b + "junk.obj~.obj") && !exists(b + "junk.obj.obj"))
        #expect(!KeyPathMigration.needsRecordedKey("model.obj"))
        #expect(KeyPathMigration.needsRecordedKey("model.obj~"))
        #expect(exists(b + "tilde.obj~~.obj") && !exists(b + "tilde.obj~.obj"))
        #expect(exists(b + "other.obj~.obj"))
        #expect(exists(b + "ptr.versions/.latest"))
        #expect(report.conflicts.isEmpty)
        #expect(exists(b + "model.obj~.obj") && !exists(b + "model.obj.obj"))
        #expect(exists(b + "model.obj~.versions/\(v1).obj"))
        #expect(exists(b + "data.obj~/file.txt.obj") && !exists(b + "data.obj"))
        #expect(exists(b + "backup.versions~/notes.txt.obj"))
        #expect(exists(b + "deep/x.ecshards~/inner.obj"))
        #expect(exists(b + "report.versions/\(v2).obj"))
        #expect(exists(b + "report.obj"))
        #expect(exists(b + "enc.ecshards/0.ecshard"))
        #expect(exists(b + "docs/readme.obj"))

        // The renamed object now resolves through the normal path helpers.
        let resolved = ObjectFileHandler.storagePath(for: "b", key: "model.obj")
        #expect(resolved.hasSuffix("/b/model.obj~.obj"))

        // Idempotent.
        #expect(KeyPathMigration.migrate(root: root) == KeyPathMigration.Report())
    }

    @Test("the done-marker is written even when its directory does not exist yet")
    func writesMarkerIntoMissingDirectory() throws {
        let root = try tempRoot()
        defer { try? fm.removeItem(atPath: root) }
        try writeObject(root + "buckets/b/model.obj.obj", key: "model.obj")
        let marker = root + "state/key-escaping-migrated"

        KeyPathMigration.runIfNeeded(logger: Logger(label: "test"), root: root + "buckets/", markerPath: marker)

        #expect(exists(marker))
        #expect(exists(root + "buckets/b/model.obj~.obj"))
        // With the marker in place a fresh legacy name is left alone: the walk no longer runs.
        try writeObject(root + "buckets/b/later.obj.obj", key: "later.obj")
        KeyPathMigration.runIfNeeded(logger: Logger(label: "test"), root: root + "buckets/", markerPath: marker)
        #expect(exists(root + "buckets/b/later.obj.obj"))
    }

    @Test("an object rewritten after the upgrade is never replaced by its legacy copy")
    func keepsNewerObjectOnConflict() throws {
        let root = try tempRoot()
        defer { try? fm.removeItem(atPath: root) }
        try writeObject(root + "b/dup.obj.obj", key: "dup.obj")
        try writeObject(root + "b/dup.obj~.obj", key: "dup.obj")
        let newer = fm.contents(atPath: root + "b/dup.obj~.obj")

        let report = KeyPathMigration.migrate(root: root)

        #expect(report.renamed == 0)
        #expect(report.conflicts == [root + "b/dup.obj.obj"])
        #expect(fm.contents(atPath: root + "b/dup.obj~.obj") == newer)
        #expect(exists(root + "b/dup.obj.obj"))
    }
}
