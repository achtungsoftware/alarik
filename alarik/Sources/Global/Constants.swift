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

/// Alarik version string (updated by publish.sh)
public let alarikVersion = "1.0.0-beta-5"

/// The externally-reachable base URL of this instance's S3 API (no trailing slash), used to
/// build absolute URLs such as presigned share links. The incoming request's Host header isn't
/// used for this since Alarik may sit behind a reverse proxy with a different public hostname.
public let apiBaseURL: String = Environment.sanitizedGet("API_BASE_URL") ?? "http://localhost:8080"

/// Global hex lookup table for optimal performance
public let hexLookupTable: InlineArray<16, UInt8> = [
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,  // 0-7
    0x38, 0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66   // 8-9, a-f
]

public enum Constants {
    /// Object payloads larger than this are streamed to/from disk instead of being buffered
    /// whole in memory. Below it, buffering wins: one small read/write beats the extra file
    /// IO round trips, and that's where object-count-heavy workloads live.
    public static let streamingThreshold = 4 * 1024 * 1024

    /// Chunk size for streaming GET response bodies off disk.
    public static let streamingReadChunkSize = 128 * 1024

    /// Window size for file-to-file payload copies (spool -> .obj assembly, multipart
    /// completion, CopyObject). Bounds memory per in-flight copy regardless of object size.
    public static let fileCopyWindowSize = 1 << 20

    /// Directory that holds in-flight upload spool files. Lives under Storage/ so renaming a
    /// finished spool into Storage/multipart is a same-filesystem move in the common case.
    public static let spoolDirectory = "Storage/spool/"
}