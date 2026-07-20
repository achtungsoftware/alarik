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
        let encoded = encodedBucketName(name)
        return rootURL.appendingPathComponent(encoded)
    }

    /// Sanitizes and percent-encodes a bucket name for safe use in filesystem paths. `key`
    /// has always had its `..` path components stripped before touching disk (in 3 separate
    /// spots); `bucketName` was historically only percent-encoded, never `..`-stripped - since
    /// `.` is an allowed path character, percent-encoding alone never blocks a
    /// `../../etc`-style traversal. Every bucket-name-to-path construction site should route
    /// through this rather than re-implementing its own encoding.
    static func encodedBucketName(_ name: String) -> String {
        let sanitized: String
        if name.contains("..") {
            let components = name.components(separatedBy: "/")
            sanitized = components.map { $0.replacingOccurrences(of: "..", with: "") }.joined(
                separator: "/")
        } else {
            sanitized = name
        }
        return sanitized.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? sanitized
    }

    /// Creates a bucket directory if it doesn't exist.
    static func create(name: String) throws {
        let fm = FileManager.default
        try fm.createDirectory(at: bucketURL(for: name), withIntermediateDirectories: true)
    }

    /// Deletes a bucket directory
    static func delete(name: String, force: Bool) throws {
        let fm = FileManager.default
        let dataURL = bucketURL(for: name)
        // "Empty" means no objects (no .obj file anywhere, versioned or not) - NOT "no
        // directory entries". Deleting a nested key leaves its empty parent-directory
        // skeleton behind, and S3 has no concept of directories, so those leftovers must
        // never block deleting a bucket whose objects are all gone.
        if !force && ObjectFileHandler.hasBucketObjects(bucketName: name) {
            throw S3Error(
                status: .conflict,
                code: "BucketNotEmpty",
                message: "The bucket you tried to delete is not empty"
            )
        }
        // Remove the data directory
        try fm.removeItem(at: dataURL)
    }

    /// Force deletes a bucket directory including all its contents.
    static func forceDelete(name: String) throws {
        let fm = FileManager.default
        let dataURL = bucketURL(for: name)
        guard fm.fileExists(atPath: dataURL.path) else {
            return
        }
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
                values.isDirectory == true, !MetadataNamespace.isReserved(url.lastPathComponent)
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

    /// Disk usage and object count under `prefix` within a bucket - an empty prefix (the
    /// default) means the whole bucket, a non-empty one scopes to that folder, so this serves
    /// both the admin bucket list and per-folder stats in the object browser. One enumerator
    /// pass sums every file's on-disk size and counts `.obj` files (the object storage unit -
    /// see `ObjectFileHandler`), so a size+count pair costs exactly one filesystem walk.
    static func calculateStats(bucketName: String, prefix: String = "") -> (
        sizeBytes: Int64, objectCount: Int
    ) {
        var sanitizedPrefix = prefix
        if sanitizedPrefix.contains("..") {
            let components = sanitizedPrefix.components(separatedBy: "/")
            sanitizedPrefix = components.map { $0.replacingOccurrences(of: "..", with: "") }
                .filter { !$0.isEmpty }
                .joined(separator: "/")
        }

        let url =
            sanitizedPrefix.isEmpty
            ? bucketURL(for: bucketName)
            : bucketURL(for: bucketName).appendingPathComponent(sanitizedPrefix)

        let fileManager = FileManager.default
        guard fileManager.fileExists(atPath: url.path) else {
            return (0, 0)
        }

        guard
            let enumerator = fileManager.enumerator(
                at: url,
                includingPropertiesForKeys: [.fileSizeKey],
                options: [.skipsHiddenFiles]
            )
        else {
            return (0, 0)
        }

        var totalSize: Int64 = 0
        var objectCount = 0
        while let fileURL = enumerator.nextObject() as? URL {
            if let resourceValues = try? fileURL.resourceValues(forKeys: [.fileSizeKey]),
                let fileSize = resourceValues.fileSize
            {
                totalSize += Int64(fileSize)
            }
            if fileURL.pathExtension == "obj" {
                objectCount += 1
            } else if fileURL.pathExtension == "ecshard" && fileURL.lastPathComponent == "0.ecshard" {
                // A node only ever holds one shard per version - counting index 0 specifically
                // (rather than every `.ecshard` file) avoids overcounting an EC object k+m times.
                objectCount += 1
            }
        }
        return (totalSize, objectCount)
    }
}
