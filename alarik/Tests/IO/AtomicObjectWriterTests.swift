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

@Suite("AtomicObjectWriter tests", .serialized)
struct AtomicObjectWriterTests {
    private func tempDirectory() throws -> String {
        let dir = NSTemporaryDirectory() + "aow-" + UUID().uuidString + "/"
        try FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)
        return dir
    }

    @Test("finish leaves the final file in place, its directory flushed, and no temp file behind")
    func finishIsCleanAndDurable() throws {
        let dir = try tempDirectory()
        defer { try? FileManager.default.removeItem(atPath: dir) }
        var writer = try AtomicObjectWriter(finalPath: dir + "nested/object.obj")
        try writer.write(Data("payload".utf8))
        try writer.finish()

        #expect(FileManager.default.contents(atPath: dir + "nested/object.obj") == Data("payload".utf8))
        let leftovers = try FileManager.default.contentsOfDirectory(atPath: dir + "nested")
            .filter { $0.hasPrefix(AtomicObjectWriter.tempFilePrefix) }
        #expect(leftovers.isEmpty)
        #expect(POSIXFile.fsyncDirectory(dir + "nested") == 0)
        #expect(POSIXFile.fsyncDirectory(dir + "does-not-exist") != 0)
    }

    @Test("removeOrphanedTempFiles removes only old temp files, never fresh ones or objects")
    func orphanSweep() throws {
        let dir = try tempDirectory()
        defer { try? FileManager.default.removeItem(atPath: dir) }
        let fm = FileManager.default
        try fm.createDirectory(atPath: dir + "bucket/prefix", withIntermediateDirectories: true)
        let old = dir + "bucket/prefix/" + AtomicObjectWriter.tempFilePrefix + UUID().uuidString
        let fresh = dir + "bucket/" + AtomicObjectWriter.tempFilePrefix + UUID().uuidString
        let object = dir + "bucket/prefix/real.obj"
        // Objects and folders whose keys merely start with the temp prefix are not temp files.
        let lookalikeObject = dir + "bucket/" + AtomicObjectWriter.tempFilePrefix + "report.obj"
        let lookalikeFolder = dir + "bucket/" + AtomicObjectWriter.tempFilePrefix + "cache"
        try fm.createDirectory(atPath: lookalikeFolder, withIntermediateDirectories: true)
        for path in [old, fresh, object, lookalikeObject, lookalikeFolder + "/inner.obj"] {
            fm.createFile(atPath: path, contents: Data("x".utf8))
        }
        let ancient = Date().addingTimeInterval(-7200)
        for path in [old, lookalikeObject, lookalikeFolder] {
            try fm.setAttributes([.modificationDate: ancient], ofItemAtPath: path)
        }

        let removed = AtomicObjectWriter.removeOrphanedTempFiles(under: dir, olderThan: 3600)
        #expect(removed == 1)
        #expect(!fm.fileExists(atPath: old))
        #expect(fm.fileExists(atPath: fresh))
        #expect(fm.fileExists(atPath: object))
        #expect(fm.fileExists(atPath: lookalikeObject))
        #expect(fm.fileExists(atPath: lookalikeFolder + "/inner.obj"))
    }

    @Test("only the exact .tmp-<UUID> shape counts as a temp file")
    func tempFileNameShape() {
        #expect(AtomicObjectWriter.isTempFileName(".tmp-" + UUID().uuidString))
        #expect(!AtomicObjectWriter.isTempFileName(".tmp-report.obj"))
        #expect(!AtomicObjectWriter.isTempFileName(".tmp-cache"))
        #expect(!AtomicObjectWriter.isTempFileName(".tmp-"))
        #expect(!AtomicObjectWriter.isTempFileName("tmp-" + UUID().uuidString))
    }
}
