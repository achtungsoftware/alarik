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

@Suite("Key path encoding")
struct KeyPathEncodingTests {
    @Test("components that would collide with Alarik's own names are escaped, everything else is verbatim")
    func escaping() {
        #expect(KeyPathEncoding.encodedPath(forKey: "docs/report.pdf") == "docs/report.pdf")
        #expect(KeyPathEncoding.encodedPath(forKey: "report.obj") == "report.obj~")
        #expect(KeyPathEncoding.encodedPath(forKey: "backup.versions/1.txt") == "backup.versions~/1.txt")
        #expect(KeyPathEncoding.encodedPath(forKey: "x.ecshards") == "x.ecshards~")
        #expect(KeyPathEncoding.encodedPath(forKey: "a//b") == "a/~/b")
        #expect(KeyPathEncoding.encodedPath(forKey: "a/./b") == "a/.~/b")
        // A folder marker keeps the `.obj` file with an empty stem it always had.
        #expect(KeyPathEncoding.encodedPath(forKey: "docs/") == "docs/")
        // Names that already end in the marker and would read as escaped get one more.
        #expect(KeyPathEncoding.encodedPath(forKey: "x.obj~") == "x.obj~~")
        #expect(KeyPathEncoding.encodedPath(forKey: "notes.txt~") == "notes.txt~")
        #expect(KeyPathEncoding.encodedPath(forKey: "a/~/b") == "a/~~/b")
        #expect(KeyPathEncoding.encodedPath(forKey: "a/~") == "a/~")

        #expect(KeyPathEncoding.encodedDirectoryPath(forPrefix: "") == "")
        #expect(KeyPathEncoding.encodedDirectoryPath(forPrefix: "docs/") == "docs")
        #expect(KeyPathEncoding.encodedDirectoryPath(forPrefix: "x.obj/sub/") == "x.obj~/sub")
        #expect(KeyPathEncoding.encodedDirectoryPath(forPrefix: "a//") == "a/~")
    }

    @Test("decoding inverts encoding for every component, and leaves legacy names alone")
    func roundTrip() {
        let components = ["plain", "report.obj", "x.versions", "y.ecshards", "x.obj~", "notes.txt~", ".", "~", "~~", ""]
        for component in components {
            for asDirectory in [true, false] {
                let encoded = KeyPathEncoding.encodeComponent(Substring(component), asDirectory: asDirectory)
                #expect(
                    KeyPathEncoding.decodeComponent(encoded, asDirectory: asDirectory) == component,
                    "\(component) as \(asDirectory ? "directory" : "stem")")
            }
        }
        // Stored before escaping existed: a stem ending in `~` whose inner name never needed it.
        #expect(KeyPathEncoding.decodeComponent("notes.txt~", asDirectory: false) == "notes.txt~")
        #expect(KeyPathEncoding.decodeComponent("~", asDirectory: false) == "~")
        // Escaped forms decode back.
        #expect(KeyPathEncoding.decodeComponent("report.obj~", asDirectory: false) == "report.obj")
        #expect(KeyPathEncoding.decodeComponent("~", asDirectory: true) == "")
        #expect(KeyPathEncoding.decodeComponent(".~", asDirectory: true) == ".")
    }
}
