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

import Vapor

struct BucketHandler {

    static let rootPath = "Storage/buckets/"
    static let rootURL = URL(fileURLWithPath: rootPath)

    public static func bucketURL(for name: String) -> URL {
        let encoded = name.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? ""
        return rootURL.appendingPathComponent(encoded)
    }

    /// Creates a bucket directory if it doesn't exist.
    static func create(name: String) throws {
        let fm = FileManager.default
        try fm.createDirectory(at: bucketURL(for: name), withIntermediateDirectories: true)
    }

    /// Deletes a bucket directory if empty (only meta.json allowed).
    static func delete(name: String) throws {
        let fm = FileManager.default
        let dataURL = bucketURL(for: name)
        // Get contents of the bucket directory
        let contents = try fm.contentsOfDirectory(atPath: dataURL.path)
        // Since meta.json is now separate, the data directory should be completely empty
        if !contents.isEmpty {
            throw S3Error(
                status: .conflict,
                code: "BucketNotEmpty",
                message: "The bucket you tried to delete is not empty"
            )
        }
        // Remove the data directory
        try fm.removeItem(at: dataURL)
    }

    /// Lists all bucket names.
    static func list() throws -> [String] {
        let items = try FileManager.default.contentsOfDirectory(
            at: rootURL,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: []
        )
        var buckets: [String] = []
        for url in items {
            if let values = try? url.resourceValues(forKeys: [.isDirectoryKey]),
                values.isDirectory == true
            {
                buckets.append(url.lastPathComponent)
            }
        }
        return buckets.sorted()
    }

    /// Counts the number of keys recursively in the bucket.
    static func countKeys(name: String) throws -> Int {
        let url = bucketURL(for: name)
        guard
            let enumerator = FileManager.default.enumerator(
                at: url, includingPropertiesForKeys: [.isDirectoryKey], options: [])
        else {
            return 0
        }
        var count = 0
        for case let fileURL as URL in enumerator {
            do {
                let resourceValues = try fileURL.resourceValues(forKeys: [.isDirectoryKey])
                if let isDirectory = resourceValues.isDirectory, !isDirectory {
                    count += 1
                }
            } catch {
                // Skip any errors in resource value fetching
                continue
            }
        }
        return count
    }
}
