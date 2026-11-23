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

/// Client faults of the multipart layer, each carrying the S3 code and status it answers with.
/// Anything else `MultipartFileHandler` throws is a server fault and must not be reported as
/// the client's mistake.
enum MultipartError: Error, CustomStringConvertible {
    case noSuchUpload
    case invalidPartNumber(Int)
    case duplicatePart(Int)
    case missingPart(Int)
    case etagMismatch(partNumber: Int)
    case entityTooSmall(partNumber: Int, size: Int, minimum: Int)

    var s3Code: String {
        switch self {
        case .noSuchUpload: "NoSuchUpload"
        case .invalidPartNumber: "InvalidArgument"
        case .duplicatePart: "InvalidPartOrder"
        case .missingPart, .etagMismatch: "InvalidPart"
        case .entityTooSmall: "EntityTooSmall"
        }
    }

    var status: HTTPResponseStatus {
        self.s3Code == "NoSuchUpload" ? .notFound : .badRequest
    }

    var message: String {
        switch self {
        case .noSuchUpload: "The specified upload does not exist."
        case .invalidPartNumber(let number): "Part number must be an integer between 1 and 10000, inclusive: \(number)"
        case .duplicatePart(let number): "Duplicate part number: \(number)"
        case .missingPart(let number): "Part \(number) does not exist"
        case .etagMismatch(let number): "ETag mismatch for part \(number)"
        case .entityTooSmall(let number, let size, let minimum):
            "Your proposed upload is smaller than the minimum allowed size: part \(number) is \(size) bytes, the minimum is \(minimum)"
        }
    }

    var description: String { message }
}
