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

import Crypto
import Foundation
import NIOCore
import Vapor

/// What a `.obj` read found on disk that contradicts the object's own header. Surfaces as a
/// server error (never a 404 or a silently wrong body): the object is damaged, not missing.
enum IntegrityError: Error, CustomStringConvertible {
    case sizeMismatch(path: String, expected: Int, actual: Int)
    case checksumMismatch(path: String, expected: String, actual: String)

    var description: String {
        switch self {
        case .sizeMismatch(let path, let expected, let actual):
            "Object at '\(path)' is \(actual) bytes but its header says \(expected)"
        case .checksumMismatch(let path, let expected, let actual):
            "Object at '\(path)' failed its integrity check (expected \(expected), got \(actual))"
        }
    }
}

/// Verifies a plain `.obj` payload against its own header as the bytes pass by: the stored
/// full-object checksum when there is one, else the ETag (the MD5 of a single-part object).
/// Erasure-coded shards carry per-stripe checksums; `.obj` files had nothing, so a bit-rotted
/// payload was served as 200. Multipart ETags are not content hashes, so an object with only a
/// `-N` ETag and no full-object checksum cannot be verified and gets no verifier.
///
/// `ALARIK_VERIFY_ON_READ=false` turns verification off for deployments that would rather
/// spend the CPU elsewhere.
struct PayloadVerifier {
    private enum Digest {
        case md5(Insecure.MD5)
        case flexible(TrailerChecksum)
    }

    private var digest: Digest
    private let expected: String
    private let path: String

    static var isEnabled: Bool {
        Environment.sanitizedGet("ALARIK_VERIFY_ON_READ")?.lowercased() != "false"
    }

    init?(meta: ObjectMeta, path: String) {
        guard Self.isEnabled else { return nil }
        if let checksum = meta.checksum, checksum.type == .fullObject,
            let computer = TrailerChecksum(trailerHeaderName: "x-amz-checksum-\(checksum.algorithm)")
        {
            digest = .flexible(computer)
            expected = checksum.value
        } else if !meta.etag.contains("-"), meta.etag.count == 32 {
            digest = .md5(Insecure.MD5())
            expected = meta.etag.lowercased()
        } else {
            return nil
        }
        self.path = path
    }

    mutating func update(_ raw: UnsafeRawBufferPointer) {
        switch digest {
        case .md5(var hasher):
            hasher.update(bufferPointer: raw)
            digest = .md5(hasher)
        case .flexible(var computer):
            computer.update(rawBufferPointer: raw)
            digest = .flexible(computer)
        }
    }

    mutating func update(_ bytes: ByteBufferView) {
        bytes.withUnsafeBytes { update($0) }
    }

    func verify() throws {
        let actual: String
        switch digest {
        case .md5(let hasher): actual = hasher.finalize().hexString()
        case .flexible(let computer): actual = computer.finalizeBase64()
        }
        guard actual == expected else {
            throw IntegrityError.checksumMismatch(path: path, expected: expected, actual: actual)
        }
    }
}
