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

struct StorageHelper {
    public static func cleanStorage() throws {
        try cleanBucketsFolder()
        try cleanDatabase()
    }

    private static func cleanBucketsFolder() throws {
        let fm = FileManager.default
        let bucketsURL = URL(filePath: "Storage/buckets/")

        // Ensure the directory exists
        guard fm.fileExists(atPath: bucketsURL.path) else { return }

        let contents = try fm.contentsOfDirectory(
            at: bucketsURL, includingPropertiesForKeys: nil, options: [])

        for item in contents {
            // Skip .gitkeep
            if item.lastPathComponent == ".gitkeep" {
                continue
            }
            try fm.removeItem(at: item)
        }
    }

    private static func cleanDatabase() throws {
        let fm = FileManager.default
        let dbURL = URL(filePath: "db.sqlite")

        if fm.fileExists(atPath: dbURL.path) {
            try fm.removeItem(at: dbURL)
        }
    }
}
