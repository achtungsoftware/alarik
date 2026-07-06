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

    /// How a Range header parsed relative to a concrete object size. AWS treats the two
    /// failure modes differently (verified against GetObject behavior): a malformed header
    /// is silently ignored (full object, 200), while a well-formed but unsatisfiable one -
    /// e.g. a start offset at or beyond the object size - is a hard `416 InvalidRange`.
    enum ParseResult {
        case noRange
        case satisfiable(ByteRange)
        case unsatisfiable
    }

    /// Parses the request's Range header for a GET, throwing `416 InvalidRange` when the
    /// range is well-formed but unsatisfiable. Returns nil when there is no (usable) range.
    static func parseRange(from req: Request, fileSize: Int) throws -> ByteRange? {
        guard let rangeHeader = req.headers.first(name: "Range") else {
            return nil
        }

        switch parse(rangeHeader, fileSize: fileSize) {
        case .noRange:
            return nil
        case .satisfiable(let range):
            return range
        case .unsatisfiable:
            throw S3Error(
                status: .init(statusCode: 416), code: "InvalidRange",
                message: "The requested range is not satisfiable")
        }
    }

    /// Non-throwing variant used by UploadPartCopy's `x-amz-copy-source-range`, where any
    /// unusable range is mapped to an error by the caller.
    static func parseRangeHeader(_ rangeHeader: String, fileSize: Int) -> ByteRange? {
        if case .satisfiable(let range) = parse(rangeHeader, fileSize: fileSize) {
            return range
        }
        return nil
    }

    static func parse(_ rangeHeader: String, fileSize: Int) -> ParseResult {
        // Expected format: "bytes=start-end"
        guard rangeHeader.hasPrefix("bytes=") else {
            return .noRange
        }

        let rangeSpec = String(rangeHeader.dropFirst(6))
        let parts = rangeSpec.split(separator: "-", maxSplits: 1, omittingEmptySubsequences: false)

        guard parts.count == 2 else {
            return .noRange
        }

        let startStr = String(parts[0])
        let endStr = String(parts[1])

        let start: Int
        let end: Int

        if startStr.isEmpty {
            // Suffix range: "bytes=-500" means last 500 bytes. A suffix larger than the
            // object clamps to the whole object (RFC 7233, matches S3) - it is only
            // unsatisfiable when the object is empty.
            guard let suffixLength = Int(endStr), suffixLength > 0 else {
                return .noRange
            }
            guard fileSize > 0 else {
                return .unsatisfiable
            }
            start = max(0, fileSize - suffixLength)
            end = fileSize - 1
        } else if endStr.isEmpty {
            // Open-ended range: "bytes=1024-" means from byte 1024 to end
            guard let startByte = Int(startStr), startByte >= 0 else {
                return .noRange
            }
            guard startByte < fileSize else {
                return .unsatisfiable
            }
            start = startByte
            end = fileSize - 1
        } else {
            // Complete range: "bytes=0-1023" - the end clamps to the object size, but a
            // start at or beyond the object is unsatisfiable
            guard let startByte = Int(startStr),
                let endByte = Int(endStr),
                startByte >= 0,
                startByte <= endByte
            else {
                return .noRange
            }
            guard startByte < fileSize else {
                return .unsatisfiable
            }
            start = startByte
            end = min(endByte, fileSize - 1)
        }

        let range = ByteRange(start: start, end: end)
        return range.validate(fileSize: fileSize) ? .satisfiable(range) : .unsatisfiable
    }
}
