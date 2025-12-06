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

struct ByteRange {
    let start: Int
    let end: Int

    init(start: Int, end: Int) {
        self.start = start
        self.end = end
    }

    var length: Int {
        end - start + 1
    }

    func validate(fileSize: Int) -> Bool {
        start >= 0 && end < fileSize && start <= end
    }

    func contentRange(fileSize: Int) -> String {
        "bytes \(start)-\(end)/\(fileSize)"
    }
}

struct S3RangeParser {
    /// Parses HTTP Range header (e.g., "bytes=0-1023", "bytes=-500", "bytes=1024-")
    /// Returns nil if no range header or invalid format
    static func parseRange(from req: Request, fileSize: Int) -> ByteRange? {
        guard let rangeHeader = req.headers.first(name: "Range") else {
            return nil
        }

        return parseRangeHeader(rangeHeader, fileSize: fileSize)
    }

    static func parseRangeHeader(_ rangeHeader: String, fileSize: Int) -> ByteRange? {
        // Expected format: "bytes=start-end"
        guard rangeHeader.hasPrefix("bytes=") else {
            return nil
        }

        let rangeSpec = String(rangeHeader.dropFirst(6))
        let parts = rangeSpec.split(separator: "-", maxSplits: 1, omittingEmptySubsequences: false)

        guard parts.count == 2 else {
            return nil
        }

        let startStr = String(parts[0])
        let endStr = String(parts[1])

        let start: Int
        let end: Int

        if startStr.isEmpty {
            // Suffix range: "bytes=-500" means last 500 bytes
            guard let suffixLength = Int(endStr), suffixLength > 0 else {
                return nil
            }
            start = max(0, fileSize - suffixLength)
            end = fileSize - 1
        } else if endStr.isEmpty {
            // Open-ended range: "bytes=1024-" means from byte 1024 to end
            guard let startByte = Int(startStr), startByte >= 0 else {
                return nil
            }
            start = startByte
            end = fileSize - 1
        } else {
            // Complete range: "bytes=0-1023"
            guard let startByte = Int(startStr),
                  let endByte = Int(endStr),
                  startByte >= 0,
                  startByte <= endByte else {
                return nil
            }
            start = startByte
            end = min(endByte, fileSize - 1)
        }

        let range = ByteRange(start: start, end: end)
        return range.validate(fileSize: fileSize) ? range : nil
    }
}
