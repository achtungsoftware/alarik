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

private let shortWeekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
private let longWeekdays = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
private let shortMonths = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

extension Date {
    /// Formats the date using the strict RFC 1123 format required by HTTP headers.
    /// Format: "Sun, 06 Nov 1994 08:49:37 GMT"
    /// This is much faster than DateFormatter
    var rfc1123String: String {
        let calendar = Calendar(identifier: .gregorian)
        let components = calendar.dateComponents(
            in: TimeZone(secondsFromGMT: 0)!,
            from: self
        )

        guard let year = components.year,
              let month = components.month,
              let day = components.day,
              let hour = components.hour,
              let minute = components.minute,
              let second = components.second,
              let weekday = components.weekday
        else {
            return ""
        }

        let weekdayStr = shortWeekdays[(weekday + 6) % 7]  // Adjust Sunday = 1 to Sunday = 0
        let monthStr = shortMonths[month - 1]

        return String(format: "%@, %02d %@ %04d %02d:%02d:%02d GMT",
                     weekdayStr, day, monthStr, year, hour, minute, second)
    }

    /// Formats the date using ISO8601 format with fractional seconds (for S3 responses)
    /// Format: "2024-12-01T08:49:37.123Z"
    /// This is much faster than ISO8601DateFormatter
    var iso8601String: String {
        let calendar = Calendar(identifier: .gregorian)
        let components = calendar.dateComponents(
            in: TimeZone(secondsFromGMT: 0)!,
            from: self
        )

        guard let year = components.year,
              let month = components.month,
              let day = components.day,
              let hour = components.hour,
              let minute = components.minute,
              let second = components.second,
              let nanosecond = components.nanosecond
        else {
            return ""
        }

        let milliseconds = nanosecond / 1_000_000

        return String(format: "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                     year, month, day, hour, minute, second, milliseconds)
    }

    /// Parses HTTP date formats (RFC 7231)
    /// Supports: IMF-fixdate (RFC 1123), obsolete RFC 850, ANSI C asctime()
    /// This is much faster than DateFormatter
    static func fromHTTPDateString(_ dateString: String) -> Date? {
        let trimmed = dateString.trimmingCharacters(in: .whitespaces)

        // Try RFC 1123: "Sun, 06 Nov 1994 08:49:37 GMT"
        if let date = parseRFC1123(trimmed) {
            return date
        }

        // Try RFC 850: "Sunday, 06-Nov-94 08:49:37 GMT"
        if let date = parseRFC850(trimmed) {
            return date
        }

        // Try ANSI C: "Sun Nov  6 08:49:37 1994"
        if let date = parseANSIC(trimmed) {
            return date
        }

        return nil
    }

    /// Parses RFC 1123 format: "Sun, 06 Nov 1994 08:49:37 GMT"
    private static func parseRFC1123(_ str: String) -> Date? {
        let parts = str.split(separator: " ")
        guard parts.count == 6,
              parts[0].hasSuffix(","),
              let day = Int(parts[1]),
              let year = Int(parts[3]),
              parts[5] == "GMT"
        else {
            return nil
        }

        guard let month = shortMonths.firstIndex(of: String(parts[2])) else {
            return nil
        }

        let timeParts = parts[4].split(separator: ":")
        guard timeParts.count == 3,
              let hour = Int(timeParts[0]),
              let minute = Int(timeParts[1]),
              let second = Int(timeParts[2])
        else {
            return nil
        }

        return createDate(year: year, month: month + 1, day: day, hour: hour, minute: minute, second: second)
    }

    /// Parses RFC 850 format: "Sunday, 06-Nov-94 08:49:37 GMT"
    private static func parseRFC850(_ str: String) -> Date? {
        let parts = str.split(separator: " ")
        guard parts.count == 4,
              parts[0].hasSuffix(","),
              parts[3] == "GMT"
        else {
            return nil
        }

        let dateParts = parts[1].split(separator: "-")
        guard dateParts.count == 3,
              let day = Int(dateParts[0]),
              let yearShort = Int(dateParts[2])
        else {
            return nil
        }

        guard let month = shortMonths.firstIndex(of: String(dateParts[1])) else {
            return nil
        }

        // Convert 2-digit year to 4-digit (RFC 850 uses 2-digit years)
        let year = yearShort < 50 ? 2000 + yearShort : 1900 + yearShort

        let timeParts = parts[2].split(separator: ":")
        guard timeParts.count == 3,
              let hour = Int(timeParts[0]),
              let minute = Int(timeParts[1]),
              let second = Int(timeParts[2])
        else {
            return nil
        }

        return createDate(year: year, month: month + 1, day: day, hour: hour, minute: minute, second: second)
    }

    /// Parses ANSI C format: "Sun Nov  6 08:49:37 1994"
    private static func parseANSIC(_ str: String) -> Date? {
        let parts = str.split(separator: " ", omittingEmptySubsequences: false)
        guard parts.count >= 5 else {
            return nil
        }

        // Handle double space before single-digit day
        let dayIndex = parts[2].isEmpty ? 3 : 2
        let timeIndex = dayIndex + 1
        let yearIndex = timeIndex + 1

        guard yearIndex < parts.count,
              let day = Int(parts[dayIndex]),
              let year = Int(parts[yearIndex])
        else {
            return nil
        }

        guard let month = shortMonths.firstIndex(of: String(parts[1])) else {
            return nil
        }

        let timeParts = parts[timeIndex].split(separator: ":")
        guard timeParts.count == 3,
              let hour = Int(timeParts[0]),
              let minute = Int(timeParts[1]),
              let second = Int(timeParts[2])
        else {
            return nil
        }

        return createDate(year: year, month: month + 1, day: day, hour: hour, minute: minute, second: second)
    }

    /// Helper to create a Date from components in UTC
    /// Validates that the date components are actually valid (e.g., rejects Nov 32)
    private static func createDate(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int) -> Date? {
        var components = DateComponents()
        components.year = year
        components.month = month
        components.day = day
        components.hour = hour
        components.minute = minute
        components.second = second
        components.timeZone = TimeZone(secondsFromGMT: 0)

        let calendar = Calendar(identifier: .gregorian)
        guard let date = calendar.date(from: components) else {
            return nil
        }

        // Verify the date components didn't get adjusted (e.g., Nov 32 -> Dec 2)
        let verifiedComponents = calendar.dateComponents(
            in: TimeZone(secondsFromGMT: 0)!, from: date)
        guard verifiedComponents.year == year,
              verifiedComponents.month == month,
              verifiedComponents.day == day,
              verifiedComponents.hour == hour,
              verifiedComponents.minute == minute,
              verifiedComponents.second == second
        else {
            return nil
        }

        return date
    }
}
