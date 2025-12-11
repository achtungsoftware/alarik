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

@Suite("S3RangeParser tests")
struct S3RangeParserTests {

    @Test("Parse complete range - bytes=0-9")
    func testParseCompleteRange() {
        let range = S3RangeParser.parseRangeHeader("bytes=0-9", fileSize: 100)

        #expect(range != nil)
        #expect(range?.start == 0)
        #expect(range?.end == 9)
        #expect(range?.length == 10)
    }

    @Test("Parse complete range - bytes=50-99")
    func testParseCompleteRangeMidFile() {
        let range = S3RangeParser.parseRangeHeader("bytes=50-99", fileSize: 100)

        #expect(range != nil)
        #expect(range?.start == 50)
        #expect(range?.end == 99)
        #expect(range?.length == 50)
    }

    @Test("Parse complete range - end beyond file size")
    func testParseCompleteRangeBeyondFileSize() {
        let range = S3RangeParser.parseRangeHeader("bytes=0-200", fileSize: 100)

        #expect(range != nil)
        #expect(range?.start == 0)
        #expect(range?.end == 99)  // Clamped to file size
        #expect(range?.length == 100)
    }

    @Test("Parse open-ended range - bytes=50-")
    func testParseOpenEndedRange() {
        let range = S3RangeParser.parseRangeHeader("bytes=50-", fileSize: 100)

        #expect(range != nil)
        #expect(range?.start == 50)
        #expect(range?.end == 99)
        #expect(range?.length == 50)
    }

    @Test("Parse open-ended range - bytes=0-")
    func testParseOpenEndedRangeFromStart() {
        let range = S3RangeParser.parseRangeHeader("bytes=0-", fileSize: 100)

        #expect(range != nil)
        #expect(range?.start == 0)
        #expect(range?.end == 99)
        #expect(range?.length == 100)
    }

    @Test("Parse suffix range - bytes=-10")
    func testParseSuffixRange() {
        let range = S3RangeParser.parseRangeHeader("bytes=-10", fileSize: 100)

        #expect(range != nil)
        #expect(range?.start == 90)
        #expect(range?.end == 99)
        #expect(range?.length == 10)
    }

    @Test("Parse suffix range - bytes=-5 on small file")
    func testParseSuffixRangeSmallFile() {
        let range = S3RangeParser.parseRangeHeader("bytes=-5", fileSize: 16)

        #expect(range != nil)
        #expect(range?.start == 11)
        #expect(range?.end == 15)
        #expect(range?.length == 5)
    }

    @Test("Parse suffix range - larger than file")
    func testParseSuffixRangeLargerThanFile() {
        let range = S3RangeParser.parseRangeHeader("bytes=-200", fileSize: 100)

        #expect(range != nil)
        #expect(range?.start == 0)  // Clamped to start
        #expect(range?.end == 99)
        #expect(range?.length == 100)
    }

    @Test("Content-Range header format")
    func testContentRangeFormat() {
        let range = ByteRange(start: 0, end: 9)
        let header = range.contentRange(fileSize: 100)

        #expect(header == "bytes 0-9/100")
    }

    @Test("Content-Range header format - suffix range")
    func testContentRangeFormatSuffix() {
        let range = ByteRange(start: 90, end: 99)
        let header = range.contentRange(fileSize: 100)

        #expect(header == "bytes 90-99/100")
    }

    @Test("Invalid range - missing bytes= prefix")
    func testInvalidRangeMissingPrefix() {
        let range = S3RangeParser.parseRangeHeader("0-9", fileSize: 100)
        #expect(range == nil)
    }

    @Test("Invalid range - start > end")
    func testInvalidRangeStartGreaterThanEnd() {
        let range = S3RangeParser.parseRangeHeader("bytes=50-10", fileSize: 100)
        #expect(range == nil)
    }

    @Test("Invalid range - negative start")
    func testInvalidRangeNegativeStart() {
        let range = S3RangeParser.parseRangeHeader("bytes=-10-50", fileSize: 100)
        #expect(range == nil)
    }

    @Test("Invalid range - start beyond file size")
    func testInvalidRangeStartBeyondFileSize() {
        let range = S3RangeParser.parseRangeHeader("bytes=200-300", fileSize: 100)
        #expect(range == nil)
    }

    @Test("Invalid range - empty string")
    func testInvalidRangeEmptyString() {
        let range = S3RangeParser.parseRangeHeader("", fileSize: 100)
        #expect(range == nil)
    }

    @Test("Invalid range - malformed format")
    func testInvalidRangeMalformed() {
        let range = S3RangeParser.parseRangeHeader("bytes=abc-def", fileSize: 100)
        #expect(range == nil)
    }

    @Test("Edge case - single byte range")
    func testEdgeCaseSingleByte() {
        let range = S3RangeParser.parseRangeHeader("bytes=0-0", fileSize: 100)

        #expect(range != nil)
        #expect(range?.start == 0)
        #expect(range?.end == 0)
        #expect(range?.length == 1)
    }

    @Test("Edge case - last byte")
    func testEdgeCaseLastByte() {
        let range = S3RangeParser.parseRangeHeader("bytes=99-99", fileSize: 100)

        #expect(range != nil)
        #expect(range?.start == 99)
        #expect(range?.end == 99)
        #expect(range?.length == 1)
    }

    @Test("Edge case - suffix of 1 byte")
    func testEdgeCaseSuffixOneByte() {
        let range = S3RangeParser.parseRangeHeader("bytes=-1", fileSize: 100)

        #expect(range != nil)
        #expect(range?.start == 99)
        #expect(range?.end == 99)
        #expect(range?.length == 1)
    }

    @Test("Edge case - empty file")
    func testEdgeCaseEmptyFile() {
        let range = S3RangeParser.parseRangeHeader("bytes=0-0", fileSize: 0)
        #expect(range == nil)
    }

    @Test("Validate range within file bounds")
    func testValidateRangeValid() {
        let range = ByteRange(start: 0, end: 99)
        #expect(range.validate(fileSize: 100) == true)
    }

    @Test("Validate range - end beyond file")
    func testValidateRangeEndBeyondFile() {
        let range = ByteRange(start: 0, end: 100)
        #expect(range.validate(fileSize: 100) == false)
    }

    @Test("Validate range - negative start")
    func testValidateRangeNegativeStart() {
        let range = ByteRange(start: -1, end: 10)
        #expect(range.validate(fileSize: 100) == false)
    }

    @Test("Validate range - start > end")
    func testValidateRangeStartGreaterThanEnd() {
        let range = ByteRange(start: 50, end: 10)
        #expect(range.validate(fileSize: 100) == false)
    }
}
