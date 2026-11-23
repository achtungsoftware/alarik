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

@Suite("Windowed file reader")
struct WindowedFileReaderTests {
    private func tempFile(_ bytes: [UInt8]) throws -> String {
        let path = NSTemporaryDirectory() + "wfr-\(UUID().uuidString)"
        try Data(bytes).write(to: URL(fileURLWithPath: path))
        return path
    }

    @Test("a region and a whole file arrive in order, in windows no larger than the configured size")
    func regionsAndWholeFile() throws {
        let bytes = (0..<(Constants.fileCopyWindowSize * 2 + 7)).map { UInt8(truncatingIfNeeded: $0) }
        let path = try tempFile(bytes)
        defer { try? FileManager.default.removeItem(atPath: path) }

        var reader = WindowedFileReader()
        var whole: [UInt8] = []
        var windows = 0
        try reader.readFile(at: path) { window in
            #expect(window.count <= Constants.fileCopyWindowSize)
            whole += window
            windows += 1
        }
        #expect(whole == bytes)
        #expect(windows == 3)

        var region: [UInt8] = []
        try reader.read(path: path, offset: 5, size: 10) { region += $0 }
        #expect(region == Array(bytes[5..<15]))
    }

    @Test("a region past the end of the file is damage, and a missing file names itself")
    func failures() throws {
        let path = try tempFile([1, 2, 3])
        defer { try? FileManager.default.removeItem(atPath: path) }
        var reader = WindowedFileReader()

        var seen = 0
        #expect {
            try reader.read(path: path, offset: 0, size: 4) { seen += $0.count }
        } throws: { error in
            if case StorageError.truncatedFile(let failedPath) = error { return failedPath == path }
            return false
        }
        #expect(seen == 3)

        #expect {
            try reader.readFile(at: path + ".missing") { _ in }
        } throws: { error in
            if case StorageError.openFailed(let failedPath, let code) = error {
                return failedPath == path + ".missing" && code == ENOENT
            }
            return false
        }
    }
}
