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

/// The sentinel values a client can put in `x-amz-content-sha256` in place of a literal payload
/// hash, plus helpers to classify one. Centralized here because the SigV4 validator, the streaming
/// spooler, and the buffered body collector all have to agree on which sentinels mean "aws-chunked
/// framing" and which of those carry a trailer - getting them out of sync silently breaks uploads.
///
/// The four SigV4 streaming forms, per AWS's SigV4 streaming docs:
/// - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` - signed chunks, no trailer (the original form).
/// - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` - signed chunks + a signed trailer.
/// - `STREAMING-UNSIGNED-PAYLOAD-TRAILER` - unsigned chunks + an unsigned trailer. This is what
///   modern AWS SDKs (e.g. AWS SDK for .NET v4) send by default now that request checksums are on.
/// - `UNSIGNED-PAYLOAD` - not chunked at all; a single unframed body, integrity left to TLS.
enum S3PayloadHash {
    static let streamingSigned = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    static let streamingSignedTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
    static let streamingUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    static let unsigned = "UNSIGNED-PAYLOAD"

    /// True for any value that means "the body is aws-chunked framed" - the three streaming forms.
    static func isStreaming(_ value: String?) -> Bool {
        guard let value else { return false }
        return value == streamingSigned || value == streamingSignedTrailer
            || value == streamingUnsignedTrailer
    }

    /// True when the chunk size lines carry `;chunk-signature=` and each chunk must be verified
    /// against the SigV4 chain (both AWS4-HMAC-SHA256 streaming forms). False for the unsigned form.
    static func hasChunkSignatures(_ value: String) -> Bool {
        value == streamingSigned || value == streamingSignedTrailer
    }

    /// True when trailer headers (e.g. `x-amz-checksum-crc32`) follow the terminating zero chunk.
    static func hasTrailer(_ value: String) -> Bool {
        value == streamingSignedTrailer || value == streamingUnsignedTrailer
    }
}
