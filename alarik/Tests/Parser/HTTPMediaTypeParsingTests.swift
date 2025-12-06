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

import NIOHTTP1
import Testing
import Vapor

@testable import Alarik

@Suite("HTTPMediaType Parsing Tests", .serialized)
struct HTTPMediaTypeParsingTests {

    @Test("Successful parsing of basic MIME types")
    func testBasicParsing() {
        let json = HTTPMediaType.from(string: "application/json")
        #expect(json != nil)
        #expect(json?.type == "application")
        #expect(json?.subType == "json")
        #expect(json?.parameters.isEmpty == true)

        let html = HTTPMediaType.from(string: "text/html")
        #expect(html != nil)
        #expect(html?.type == "text")
        #expect(html?.subType == "html")
    }

    @Test("Successful parsing of MIME types with parameters")
    func testParsingWithParameters() {
        let utf8 = HTTPMediaType.from(string: "text/plain; charset=utf-8")
        #expect(utf8 != nil)
        #expect(utf8?.type == "text")
        #expect(utf8?.subType == "plain")
        #expect(utf8?.parameters["charset"] == "utf-8", "Should parse charset parameter")

        let boundary = HTTPMediaType.from(
            string: "multipart/form-data; boundary=----WebKitFormBoundary")
        #expect(boundary != nil)
        #expect(boundary?.subType == "form-data")
        #expect(
            boundary?.parameters["boundary"] == "----WebKitFormBoundary",
            "Should parse boundary parameter")

        let quoted = HTTPMediaType.from(string: "image/jpeg; quality=\"0.8\"")
        #expect(quoted != nil)
        #expect(quoted?.parameters["quality"] == "0.8", "Should remove quotes from parameter value")

        let multipleParams = HTTPMediaType.from(string: "application/xml; type=rss; charset=latin1")
        #expect(multipleParams != nil)
        #expect(multipleParams?.parameters["type"] == "rss")
        #expect(multipleParams?.parameters["charset"] == "latin1")
    }

    @Test("Successful parsing with various whitespaces")
    func testWhitespaceHandling() {
        let whitespace = HTTPMediaType.from(string: " application/json ; param = value ")
        #expect(whitespace != nil)
        #expect(whitespace?.type == "application")
        #expect(whitespace?.subType == "json")
        #expect(
            whitespace?.parameters["param"] == "value",
            "Should correctly handle whitespace around separator")
    }

    @Test("Failable parsing of invalid formats")
    func testInvalidFormats() {
        // Missing slash
        #expect(HTTPMediaType.from(string: "applicationjson") == nil)
        // Missing subtype
        #expect(HTTPMediaType.from(string: "application/") == nil)
        // Missing type
        #expect(HTTPMediaType.from(string: "/json") == nil)
        // Empty string
        #expect(HTTPMediaType.from(string: "") == nil)
        // Empty subtype with space
        #expect(HTTPMediaType.from(string: "text/ ") == nil)
        // Parameter with missing value
        #expect(HTTPMediaType.from(string: "text/plain; key=") != nil)  // Note: This is usually accepted
        // Totally malformed
        #expect(HTTPMediaType.from(string: "foo bar baz") == nil)
    }
}
