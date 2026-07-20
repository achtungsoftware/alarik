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
import Vapor
import VaporTesting

@testable import Alarik

struct StorageHelper {
    public static func cleanStorage() throws {
        try cleanDirectoryContents("Storage/buckets/")
        try cleanDirectoryContents("Storage/multipart/")
        // Outbox task files live outside Storage/buckets/ (deliberately - they're node-affine,
        // not key-affine, see OutboxMailbox's doc comments) so they need their own cleanup, or
        // one test's leftover rows silently leak into the next test's counts.
        try cleanDirectoryContents(OutboxMailboxFileHandler.rootPath)
        try cleanDirectoryContents(OutboxMailboxFileHandler.backupRootPath)
        try cleanDirectoryContents(OutboxMailboxFileHandler.pendingEnqueueRootPath)
    }

    private static func cleanDirectoryContents(_ path: String) throws {
        let fm = FileManager.default
        let url = URL(filePath: path)

        guard fm.fileExists(atPath: url.path) else { return }

        let contents = try fm.contentsOfDirectory(
            at: url, includingPropertiesForKeys: nil, options: [])

        for item in contents {
            if item.lastPathComponent == ".gitkeep" {
                continue
            }
            try fm.removeItem(at: item)
        }
    }
}
