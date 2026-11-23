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

@testable import Alarik

@Suite("Path safety")
struct PathSafetyTests {
    @Test("stripping .. keeps every component in place, so a key always maps to the same path")
    func strippingDotDot() {
        #expect(PathSafety.strippingDotDot("docs/report.pdf") == "docs/report.pdf")
        #expect(PathSafety.strippingDotDot("../../etc/passwd") == "//etc/passwd")
        #expect(PathSafety.strippingDotDot("a/..b../c") == "a/b/c")
        #expect(PathSafety.strippingDotDot("docs/../") == "docs//")
        #expect(PathSafety.strippingDotDot("") == "")
    }

    @Test("a sanitized prefix drops the components .. emptied and keeps its folder slash")
    func sanitizedPrefix() {
        #expect(PathSafety.sanitizedPrefix("docs/") == "docs/")
        #expect(PathSafety.sanitizedPrefix("../../") == "/")
        #expect(PathSafety.sanitizedPrefix("../docs/../sub/") == "docs/sub/")
        #expect(PathSafety.sanitizedPrefix("..") == "")
    }

    @Test("every on-disk form of a key hangs off one path-safe stem")
    func keyBasePath() {
        let bucket = "b"
        let key = "../docs/../x"
        let stem = ObjectFileHandler.keyBasePath(bucketName: bucket, key: key)
        #expect(stem == "Storage/buckets/b//docs//x")
        #expect(ObjectFileHandler.storagePath(for: bucket, key: key) == stem + ".obj")
        #expect(ObjectFileHandler.versionedBasePath(for: bucket, key: key) == stem + ".versions/")
        #expect(ErasureCodedObjectHandler.shardBasePath(bucketName: bucket, key: key) == stem + ".ecshards/")
        #expect(
            ErasureCodedObjectHandler.versionedShardBasePath(bucketName: bucket, key: key, versionId: "../v")
                == stem + ".versions/v.ecshards/")
    }
}
