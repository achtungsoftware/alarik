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

@Suite("Date HTTP Formatting Tests", .serialized)
struct DateHTTPFormattingTests {

    @Test("RFC 1123 formatting produces correct format")
    func rfc1123Formatting() {
        var components = DateComponents()
        components.year = 1994
        components.month = 11
        components.day = 6
        components.hour = 8
        components.minute = 49
        components.second = 37
        components.timeZone = TimeZone(secondsFromGMT: 0)

        let calendar = Calendar(identifier: .gregorian)
        let date = calendar.date(from: components)!

        let formatted = date.rfc1123String
        #expect(formatted == "Sun, 06 Nov 1994 08:49:37 GMT")
    }

    @Test("RFC 1123 formatting with single digit day")
    func rfc1123SingleDigitDay() {
        var components = DateComponents()
        components.year = 2025
        components.month = 1
        components.day = 5
        components.hour = 12
        components.minute = 30
        components.second = 45
        components.timeZone = TimeZone(secondsFromGMT: 0)

        let calendar = Calendar(identifier: .gregorian)
        let date = calendar.date(from: components)!

        let formatted = date.rfc1123String
        #expect(formatted == "Sun, 05 Jan 2025 12:30:45 GMT")
    }

    @Test("RFC 1123 formatting with midnight time")
    func rfc1123Midnight() {
        var components = DateComponents()
        components.year = 2025
        components.month = 12
        components.day = 31
        components.hour = 0
        components.minute = 0
        components.second = 0
        components.timeZone = TimeZone(secondsFromGMT: 0)

        let calendar = Calendar(identifier: .gregorian)
        let date = calendar.date(from: components)!

        let formatted = date.rfc1123String
        #expect(formatted == "Wed, 31 Dec 2025 00:00:00 GMT")
    }

    @Test("ISO8601 formatting produces correct format")
    func iso8601Formatting() {
        var components = DateComponents()
        components.year = 2024
        components.month = 12
        components.day = 1
        components.hour = 8
        components.minute = 49
        components.second = 37
        components.nanosecond = 123_000_000
        components.timeZone = TimeZone(secondsFromGMT: 0)

        let calendar = Calendar(identifier: .gregorian)
        let date = calendar.date(from: components)!

        let formatted = date.iso8601String
        #expect(formatted == "2024-12-01T08:49:37.123Z")
    }

    @Test("ISO8601 formatting with no milliseconds")
    func iso8601NoMilliseconds() {
        var components = DateComponents()
        components.year = 2025
        components.month = 1
        components.day = 15
        components.hour = 12
        components.minute = 30
        components.second = 45
        components.nanosecond = 0
        components.timeZone = TimeZone(secondsFromGMT: 0)

        let calendar = Calendar(identifier: .gregorian)
        let date = calendar.date(from: components)!

        let formatted = date.iso8601String
        #expect(formatted == "2025-01-15T12:30:45.000Z")
    }

    @Test("RFC 1123 parsing works correctly")
    func rfc1123Parsing() {
        let date = Date.fromHTTPDateString("Sun, 06 Nov 1994 08:49:37 GMT")
        #expect(date != nil)

        let calendar = Calendar(identifier: .gregorian)
        let components = calendar.dateComponents(in: TimeZone(secondsFromGMT: 0)!, from: date!)
        #expect(components.year == 1994)
        #expect(components.month == 11)
        #expect(components.day == 6)
        #expect(components.hour == 8)
        #expect(components.minute == 49)
        #expect(components.second == 37)
    }

    @Test("RFC 1123 parsing with single digit day")
    func rfc1123ParsingSingleDigitDay() {
        let date = Date.fromHTTPDateString("Mon, 01 Jan 2025 00:00:00 GMT")
        #expect(date != nil)

        let calendar = Calendar(identifier: .gregorian)
        let components = calendar.dateComponents(in: TimeZone(secondsFromGMT: 0)!, from: date!)
        #expect(components.year == 2025)
        #expect(components.month == 1)
        #expect(components.day == 1)
    }

    @Test("RFC 1123 parsing rejects invalid format")
    func rfc1123ParsingInvalid() {
        #expect(Date.fromHTTPDateString("Invalid") == nil)
        #expect(Date.fromHTTPDateString("Sun 06 Nov 1994 08:49:37 GMT") == nil)  // Missing comma
        #expect(Date.fromHTTPDateString("Sun, 06 Nov 1994 08:49:37") == nil)  // Missing GMT
        #expect(Date.fromHTTPDateString("Sun, 32 Nov 1994 08:49:37 GMT") == nil)  // Invalid day
    }

    @Test("RFC 850 parsing works correctly")
    func rfc850Parsing() {
        let date = Date.fromHTTPDateString("Sunday, 06-Nov-94 08:49:37 GMT")
        #expect(date != nil)

        let calendar = Calendar(identifier: .gregorian)
        let components = calendar.dateComponents(in: TimeZone(secondsFromGMT: 0)!, from: date!)
        #expect(components.year == 1994)
        #expect(components.month == 11)
        #expect(components.day == 6)
        #expect(components.hour == 8)
        #expect(components.minute == 49)
        #expect(components.second == 37)
    }

    @Test("RFC 850 parsing with 2-digit year conversion")
    func rfc850YearConversion() {
        // Years < 50 should become 2000+
        let date2025 = Date.fromHTTPDateString("Wednesday, 01-Jan-25 00:00:00 GMT")
        #expect(date2025 != nil)
        let components2025 = Calendar(identifier: .gregorian).dateComponents(
            in: TimeZone(secondsFromGMT: 0)!, from: date2025!)
        #expect(components2025.year == 2025)

        // Years >= 50 should become 1900+
        let date1999 = Date.fromHTTPDateString("Friday, 31-Dec-99 23:59:59 GMT")
        #expect(date1999 != nil)
        let components1999 = Calendar(identifier: .gregorian).dateComponents(
            in: TimeZone(secondsFromGMT: 0)!, from: date1999!)
        #expect(components1999.year == 1999)
    }

    @Test("RFC 850 parsing rejects invalid format")
    func rfc850ParsingInvalid() {
        #expect(Date.fromHTTPDateString("Sunday 06-Nov-94 08:49:37 GMT") == nil)  // Missing comma
        #expect(Date.fromHTTPDateString("Sunday, 06-Nov-94 08:49:37") == nil)  // Missing GMT
        #expect(Date.fromHTTPDateString("Sunday, 32-Nov-94 08:49:37 GMT") == nil)  // Invalid day
    }

    @Test("ANSI C parsing works correctly")
    func ansiCParsing() {
        let date = Date.fromHTTPDateString("Sun Nov  6 08:49:37 1994")
        #expect(date != nil)

        let calendar = Calendar(identifier: .gregorian)
        let components = calendar.dateComponents(in: TimeZone(secondsFromGMT: 0)!, from: date!)
        #expect(components.year == 1994)
        #expect(components.month == 11)
        #expect(components.day == 6)
        #expect(components.hour == 8)
        #expect(components.minute == 49)
        #expect(components.second == 37)
    }

    @Test("ANSI C parsing with double-digit day")
    func ansiCDoubleDigitDay() {
        let date = Date.fromHTTPDateString("Wed Dec 31 23:59:59 2025")
        #expect(date != nil)

        let calendar = Calendar(identifier: .gregorian)
        let components = calendar.dateComponents(in: TimeZone(secondsFromGMT: 0)!, from: date!)
        #expect(components.year == 2025)
        #expect(components.month == 12)
        #expect(components.day == 31)
    }

    @Test("ANSI C parsing rejects invalid format")
    func ansiCParsingInvalid() {
        #expect(Date.fromHTTPDateString("Sun Nov 6 08:49:37") == nil)  // Missing year
        #expect(Date.fromHTTPDateString("Sun Xyz  6 08:49:37 1994") == nil)  // Invalid month
    }

    @Test("RFC 1123 format round-trip")
    func rfc1123RoundTrip() {
        var components = DateComponents()
        components.year = 2025
        components.month = 6
        components.day = 15
        components.hour = 14
        components.minute = 30
        components.second = 45
        components.timeZone = TimeZone(secondsFromGMT: 0)

        let calendar = Calendar(identifier: .gregorian)
        let originalDate = calendar.date(from: components)!

        let formatted = originalDate.rfc1123String
        let parsedDate = Date.fromHTTPDateString(formatted)

        #expect(parsedDate != nil)

        // Compare timestamps (truncate to seconds)
        let originalTimestamp = Int(originalDate.timeIntervalSince1970)
        let parsedTimestamp = Int(parsedDate!.timeIntervalSince1970)
        #expect(originalTimestamp == parsedTimestamp)
    }

    @Test("ISO8601 format produces valid timestamp")
    func iso8601Timestamp() {
        var components = DateComponents()
        components.year = 2025
        components.month = 6
        components.day = 15
        components.hour = 14
        components.minute = 30
        components.second = 45
        components.nanosecond = 500_000_000
        components.timeZone = TimeZone(secondsFromGMT: 0)

        let calendar = Calendar(identifier: .gregorian)
        let date = calendar.date(from: components)!

        let formatted = date.iso8601String
        // Should include milliseconds
        #expect(formatted.contains(".500Z"))
        #expect(formatted.hasPrefix("2025-06-15T14:30:45"))
    }

    @Test("Whitespace trimming in parsing")
    func whitespaceTrimming() {
        let date = Date.fromHTTPDateString("  Sun, 06 Nov 1994 08:49:37 GMT  ")
        #expect(date != nil)
    }

    @Test("All months parse correctly")
    func allMonths() {
        let months = [
            ("Jan", 1), ("Feb", 2), ("Mar", 3), ("Apr", 4),
            ("May", 5), ("Jun", 6), ("Jul", 7), ("Aug", 8),
            ("Sep", 9), ("Oct", 10), ("Nov", 11), ("Dec", 12)
        ]

        for (monthStr, monthNum) in months {
            let dateStr = "Sun, 15 \(monthStr) 2025 12:00:00 GMT"
            let date = Date.fromHTTPDateString(dateStr)
            #expect(date != nil, "Failed to parse \(monthStr)")

            let calendar = Calendar(identifier: .gregorian)
            let components = calendar.dateComponents(in: TimeZone(secondsFromGMT: 0)!, from: date!)
            #expect(components.month == monthNum, "Month \(monthStr) parsed as \(components.month ?? -1)")
        }
    }

    @Test("All weekdays format correctly")
    func allWeekdays() {
        // Test each day of a known week
        let dates = [
            (year: 2025, month: 1, day: 5, weekday: "Sun"),  // Sunday
            (year: 2025, month: 1, day: 6, weekday: "Mon"),  // Monday
            (year: 2025, month: 1, day: 7, weekday: "Tue"),  // Tuesday
            (year: 2025, month: 1, day: 8, weekday: "Wed"),  // Wednesday
            (year: 2025, month: 1, day: 9, weekday: "Thu"),  // Thursday
            (year: 2025, month: 1, day: 10, weekday: "Fri"), // Friday
            (year: 2025, month: 1, day: 11, weekday: "Sat")  // Saturday
        ]

        for testDate in dates {
            var components = DateComponents()
            components.year = testDate.year
            components.month = testDate.month
            components.day = testDate.day
            components.hour = 12
            components.minute = 0
            components.second = 0
            components.timeZone = TimeZone(secondsFromGMT: 0)

            let calendar = Calendar(identifier: .gregorian)
            let date = calendar.date(from: components)!
            let formatted = date.rfc1123String

            #expect(formatted.hasPrefix(testDate.weekday), "Expected \(testDate.weekday) for \(testDate.year)-\(testDate.month)-\(testDate.day), got \(formatted)")
        }
    }
}
