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

import Testing
import Vapor

@testable import Alarik

@Suite("Stored object headers")
struct StoredObjectHeadersTests {
    @Test("aws-chunked is dropped from Content-Encoding and the header omitted when nothing remains")
    func chunkedEncodingIsNotStored() {
        var headers = HTTPHeaders()
        headers.add(name: "content-encoding", value: "aws-chunked")
        headers.add(name: "cache-control", value: "max-age=60")
        #expect(S3Service.storedObjectHeaders(from: headers) == ["cache-control": "max-age=60"])

        var onlyChunked = HTTPHeaders()
        onlyChunked.add(name: "Content-Encoding", value: "AWS-Chunked")
        #expect(S3Service.storedObjectHeaders(from: onlyChunked) == nil)

        var combined = HTTPHeaders()
        combined.add(name: "content-encoding", value: "aws-chunked, gzip")
        #expect(S3Service.storedObjectHeaders(from: combined) == ["content-encoding": "gzip"])

        var plain = HTTPHeaders()
        plain.add(name: "content-encoding", value: "gzip")
        #expect(S3Service.storedObjectHeaders(from: plain) == ["content-encoding": "gzip"])
    }

    @Test("other encodings keep their order")
    func remainingEncodings() {
        #expect(S3Service.contentEncodingWithoutChunking("br , aws-chunked,gzip") == "br, gzip")
        #expect(S3Service.contentEncodingWithoutChunking("aws-chunked") == nil)
        #expect(S3Service.contentEncodingWithoutChunking("identity") == "identity")
    }
}
