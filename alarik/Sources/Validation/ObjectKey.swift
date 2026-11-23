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

/// Limits a key must meet before it becomes a path. A NUL ends a C path early (`a\0b.obj`
/// would open `a`), a segment over NAME_MAX (255 bytes) does not fit a directory entry once the
/// `.obj` / `.versions` / `.ecshards` suffix is added, and S3 itself caps keys at 1024 bytes.
enum ObjectKeyValidation {
    /// S3's own limit, in UTF-8 bytes.
    static let maxKeyBytes = 1024
    /// NAME_MAX less the longest on-disk suffix, with headroom for escaping path-unsafe names.
    static let maxComponentBytes = 240

    enum Failure: Equatable {
        case containsNUL
        case tooLong
        case componentTooLong

        /// S3's code for the condition; a NUL is not a length problem.
        var s3Code: String {
            switch self {
            case .containsNUL: "InvalidArgument"
            case .tooLong, .componentTooLong: "KeyTooLongError"
            }
        }

        var message: String {
            switch self {
            case .containsNUL: "Object key must not contain a NUL character."
            case .tooLong: "Your key is too long: at most \(maxKeyBytes) bytes."
            case .componentTooLong:
                "Your key is too long: at most \(maxComponentBytes) bytes between slashes."
            }
        }
    }

    static func failure(of key: String) -> Failure? {
        if key.utf8.contains(0) { return .containsNUL }
        if key.utf8.count > maxKeyBytes { return .tooLong }
        let slash = UInt8(ascii: "/")
        if key.utf8.split(separator: slash, omittingEmptySubsequences: false)
            .contains(where: { $0.count > maxComponentBytes })
        {
            return .componentTooLong
        }
        return nil
    }

    /// For S3 handlers: the matching S3 error.
    static func validate(_ key: String, requestId: String) throws {
        if let failure = failure(of: key) {
            throw S3Error(
                status: .badRequest, code: failure.s3Code, message: failure.message,
                requestId: requestId)
        }
    }

    /// For console handlers: a plain 400.
    static func validate(_ key: String) throws {
        if let failure = failure(of: key) { throw Abort(.badRequest, reason: failure.message) }
    }
}
