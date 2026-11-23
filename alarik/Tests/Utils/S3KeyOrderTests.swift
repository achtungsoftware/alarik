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

@Suite("S3 key order")
struct S3KeyOrderTests {
    @Test("keys order by UTF-8 byte value, not by Unicode scalar or canonical equivalence")
    func byteOrder() {
        // ASCII: uppercase before lowercase, "-" and "." before "/".
        #expect(S3KeyOrder.precedes("Z", "a"))
        #expect(S3KeyOrder.precedes("foo-bar", "foo.txt"))
        #expect(S3KeyOrder.precedes("foo.txt", "foo/a"))
        #expect(!S3KeyOrder.precedes("a", "a"))
        // Any non-ASCII byte (>= 0x80) sorts after every ASCII key.
        #expect(S3KeyOrder.precedes("~", "é"))

        // Precomposed U+00E9 (C3 A9) and decomposed e + U+0301 (65 CC 81) are equal as Swift
        // Strings but distinct keys to S3, ordered by their first differing byte.
        let composed = "\u{E9}"
        let decomposed = "e\u{301}"
        #expect(composed == decomposed)
        #expect(!S3KeyOrder.same(composed, decomposed))
        #expect(S3KeyOrder.precedes(decomposed, composed))
    }

    @Test("prefix matching is bytewise too, so a prefix may end inside a grapheme")
    func bytePrefix() {
        #expect(S3KeyOrder.hasPrefix("docs/a", "docs/"))
        #expect(!S3KeyOrder.hasPrefix("doc", "docs/"))
        let decomposed = "e\u{301}.txt"
        #expect(!decomposed.hasPrefix("e"))
        #expect(S3KeyOrder.hasPrefix(decomposed, "e"))
        // The same split drives common-prefix roll-ups.
        #expect(ObjectFileHandler.commonPrefix(of: "e\u{301}/x", prefix: "e", delimiter: "/") == "e\u{301}/")
    }
}
