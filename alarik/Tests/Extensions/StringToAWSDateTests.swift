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

@Suite("StringToAWSDate Tests", .serialized)
struct StringToAWSDateTests {

    @Test("Valid AWS date parses correctly")
    func validDate() {
        let date = "20250715T123045Z".toAWSDate()
        #expect(date != nil)

        let calendar = Calendar(identifier: .gregorian)
        let components = calendar.dateComponents(in: .gmt, from: date!)
        #expect(components.year == 2025)
        #expect(components.month == 7)
        #expect(components.day == 15)
        #expect(components.hour == 12)
        #expect(components.minute == 30)
        #expect(components.second == 45)
    }

    @Test("Leap year February 29 works")
    func leapYear() {
        let date = "20240229T121212Z".toAWSDate()  // 2024 is a leap year
        #expect(date != nil)
        let invalid = "20230229T121212Z".toAWSDate()  // 2023 is not
        #expect(invalid == nil)  // Calendar correctly rejects invalid leap day
    }

    @Test("Wrong length returns nil")
    func wrongLength() {
        #expect("20250715T12304Z".toAWSDate() == nil)  // 15 chars
        #expect("20250715T123045ZZ".toAWSDate() == nil)  // 17 chars
        #expect("".toAWSDate() == nil)
    }

    @Test("Missing trailing Z returns nil")
    func missingZ() {
        #expect("20250715T123045".toAWSDate() == nil)
        #expect("20250715T123045x".toAWSDate() == nil)
    }

    @Test("Lowercase z is rejected")
    func lowercaseZ() {
        #expect("20250715T123045z".toAWSDate() == nil)
    }

    @Test("Missing T separator returns nil")
    func missingT() {
        #expect("20250715123045Z".toAWSDate() == nil)
        #expect("20250715X123045Z".toAWSDate() == nil)
    }

    @Test("Invalid month (00, 13+) returns nil")
    func invalidMonth() {
        #expect("20250015T123045Z".toAWSDate() == nil)  // month 00
        #expect("20251315T123045Z".toAWSDate() == nil)  // month 13
    }

    @Test("Invalid day returns nil")
    func invalidDay() {
        #expect("20250700T123045Z".toAWSDate() == nil)  // day 00
        #expect("20250732T123045Z".toAWSDate() == nil)  // day 32
    }

    @Test("Invalid hour/minute/second returns nil")
    func invalidTime() {
        #expect("20250715T256000Z".toAWSDate() == nil)  // hour 25
        #expect("20250715T126500Z".toAWSDate() == nil)  // minute 65
        #expect("20250715T123460Z".toAWSDate() == nil)  // second 60
    }

    @Test("Non-digit characters return nil")
    func nonDigits() {
        #expect("2025AB15T123045Z".toAWSDate() == nil)
        #expect("20250715T12AB45Z".toAWSDate() == nil)
    }

    @Test("Year 0000 is rejected (Gregorian calendar starts later)")
    func year0000() {
        #expect("00000101T000000Z".toAWSDate() == nil)
    }

    @Test("Very large year (e.g., 9999) is accepted by DateComponents")
    func largeYear() {
        let date = "99991231T235959Z".toAWSDate()
        #expect(date != nil)  // DateComponents allows it
    }
}
