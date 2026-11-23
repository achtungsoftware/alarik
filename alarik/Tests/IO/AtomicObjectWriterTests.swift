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

}
