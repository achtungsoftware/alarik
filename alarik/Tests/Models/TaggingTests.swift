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

import Foundation
import Testing

@testable import Alarik

@Suite("Tagging tests")
struct TaggingTests {

    // MARK: - parse(xml:)

    @Test("parse - single tag")
    func testParseSingleTag() throws {
        let xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <TagSet>
                  <Tag><Key>env</Key><Value>prod</Value></Tag>
               </TagSet>
            </Tagging>
            """
        let tagging = try Tagging.parse(xml: xml, requestId: "test")
        #expect(tagging.tags == ["env": "prod"])
    }

    @Test("parse - multiple tags")
    func testParseMultipleTags() throws {
        let xml = """
            <Tagging><TagSet>
                <Tag><Key>env</Key><Value>prod</Value></Tag>
                <Tag><Key>team</Key><Value>storage</Value></Tag>
            </TagSet></Tagging>
            """
        let tagging = try Tagging.parse(xml: xml, requestId: "test")
        #expect(tagging.tags == ["env": "prod", "team": "storage"])
    }

    @Test("parse - empty TagSet produces no tags")
    func testParseEmptyTagSet() throws {
        let xml = "<Tagging><TagSet></TagSet></Tagging>"
        let tagging = try Tagging.parse(xml: xml, requestId: "test")
        #expect(tagging.tags.isEmpty)
    }

    @Test("parse - self-closing empty TagSet produces no tags")
    func testParseSelfClosingEmptyTagSet() throws {
        let xml = "<Tagging><TagSet/></Tagging>"
        let tagging = try Tagging.parse(xml: xml, requestId: "test")
        #expect(tagging.tags.isEmpty)
    }

    @Test("parse - a Tag with no Value defaults to an empty string")
    func testParseTagWithNoValue() throws {
        let xml = "<Tagging><TagSet><Tag><Key>onlykey</Key></Tag></TagSet></Tagging>"
        let tagging = try Tagging.parse(xml: xml, requestId: "test")
        #expect(tagging.tags == ["onlykey": ""])
    }

    @Test("parse - XML-escaped characters in Key/Value are unescaped")
    func testParseUnescapesXMLEntities() throws {
        let xml =
            "<Tagging><TagSet><Tag><Key>a&amp;b</Key><Value>x&lt;y&gt;z</Value></Tag></TagSet></Tagging>"
        let tagging = try Tagging.parse(xml: xml, requestId: "test")
        #expect(tagging.tags == ["a&b": "x<y>z"])
    }

    @Test("parse - whitespace around Key/Value is trimmed")
    func testParseTrimsWhitespace() throws {
        let xml = "<Tagging><TagSet><Tag><Key>  env  </Key><Value>  prod  </Value></Tag></TagSet></Tagging>"
        let tagging = try Tagging.parse(xml: xml, requestId: "test")
        #expect(tagging.tags == ["env": "prod"])
    }

    @Test("parse - more than 10 tags is rejected with InvalidTag")
    func testParseTooManyTagsRejected() throws {
        let tagElements = (0..<11).map { "<Tag><Key>k\($0)</Key><Value>v\($0)</Value></Tag>" }
            .joined()
        let xml = "<Tagging><TagSet>\(tagElements)</TagSet></Tagging>"

        #expect(throws: S3Error.self) {
            try Tagging.parse(xml: xml, requestId: "test-request-id")
        }

        do {
            _ = try Tagging.parse(xml: xml, requestId: "test-request-id")
            Issue.record("Expected Tagging.parse to throw")
        } catch let error as S3Error {
            #expect(error.code == "InvalidTag")
            #expect(error.status == .badRequest)
            #expect(error.requestId == "test-request-id")
        }
    }

    @Test("parse - exactly 10 tags is accepted")
    func testParseExactlyMaxTagCountAccepted() throws {
        let tagElements = (0..<10).map { "<Tag><Key>k\($0)</Key><Value>v\($0)</Value></Tag>" }
            .joined()
        let xml = "<Tagging><TagSet>\(tagElements)</TagSet></Tagging>"
        let tagging = try Tagging.parse(xml: xml, requestId: "test")
        #expect(tagging.tags.count == 10)
    }

    // MARK: - parseHeaderValue(_:)

    @Test("parseHeaderValue - single key=value pair")
    func testParseHeaderValueSinglePair() {
        let tagging = Tagging.parseHeaderValue("env=prod")
        #expect(tagging.tags == ["env": "prod"])
    }

    @Test("parseHeaderValue - multiple pairs separated by &")
    func testParseHeaderValueMultiplePairs() {
        let tagging = Tagging.parseHeaderValue("env=prod&team=storage")
        #expect(tagging.tags == ["env": "prod", "team": "storage"])
    }

    @Test("parseHeaderValue - percent-encoded keys and values are decoded")
    func testParseHeaderValuePercentEncoded() {
        let tagging = Tagging.parseHeaderValue("my%20key=my%20value")
        #expect(tagging.tags == ["my key": "my value"])
    }

    @Test("parseHeaderValue - empty string produces no tags")
    func testParseHeaderValueEmptyString() {
        let tagging = Tagging.parseHeaderValue("")
        #expect(tagging.tags.isEmpty)
    }

    @Test("parseHeaderValue - a pair with no '=' is skipped")
    func testParseHeaderValueSkipsPairWithoutEquals() {
        let tagging = Tagging.parseHeaderValue("novalue&env=prod")
        #expect(tagging.tags == ["env": "prod"])
    }

    @Test("parseHeaderValue - a key with an empty value is kept")
    func testParseHeaderValueEmptyValue() {
        let tagging = Tagging.parseHeaderValue("env=")
        #expect(tagging.tags == ["env": ""])
    }

    // MARK: - toXML()

    @Test("toXML - produces a Tag element per entry")
    func testToXMLProducesTagPerEntry() {
        let tagging = Tagging(tags: ["env": "prod"])
        let xml = tagging.toXML()
        #expect(xml.contains("<Tag><Key>env</Key><Value>prod</Value></Tag>"))
        #expect(xml.contains("<TagSet>"))
        #expect(xml.contains("<Tagging"))
    }

    @Test("toXML - escapes special XML characters in keys and values")
    func testToXMLEscapesSpecialCharacters() {
        let tagging = Tagging(tags: ["a&b": "x<y>z"])
        let xml = tagging.toXML()
        #expect(xml.contains("<Key>a&amp;b</Key>"))
        #expect(xml.contains("<Value>x&lt;y&gt;z</Value>"))
    }

    @Test("toXML - empty tags produces an empty TagSet")
    func testToXMLEmptyTags() {
        let tagging = Tagging(tags: [:])
        let xml = tagging.toXML()
        #expect(xml.contains("<TagSet></TagSet>"))
    }

    @Test("toXML round-trips through parse")
    func testToXMLRoundTripsThroughParse() throws {
        let original = Tagging(tags: ["env": "prod", "team": "storage"])
        let xml = original.toXML()
        let parsed = try Tagging.parse(xml: xml, requestId: "test")
        #expect(parsed.tags == original.tags)
    }

    // MARK: - Equatable

    @Test("Equatable - same tags are equal regardless of insertion order")
    func testEquatable() {
        let a = Tagging(tags: ["a": "1", "b": "2"])
        let b = Tagging(tags: ["b": "2", "a": "1"])
        #expect(a == b)
    }

    @Test("Equatable - different tags are not equal")
    func testEquatableDifferent() {
        let a = Tagging(tags: ["a": "1"])
        let b = Tagging(tags: ["a": "2"])
        #expect(a != b)
    }

    // MARK: - maxTagCount

    @Test("maxTagCount is 10, matching the verified AWS limit")
    func testMaxTagCountIsTen() {
        #expect(Tagging.maxTagCount == 10)
    }
}
