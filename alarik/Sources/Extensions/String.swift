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

extension String {
    /// Fast parsing of YYYYMMDD'T'HHMMSSZ to Date in UTC
    /// This is much faster than swift's `DateFormatter`
    func toAWSDate() -> Date? {
        guard self.count == 16,
            self[self.index(before: self.endIndex)] == "Z",
            self[self.index(self.startIndex, offsetBy: 8)] == "T"
        else {
            return nil
        }

        let yearStr = self.prefix(4)
        let monthStr = self[
            self.index(self.startIndex, offsetBy: 4)..<self.index(self.startIndex, offsetBy: 6)]
        let dayStr = self[
            self.index(self.startIndex, offsetBy: 6)..<self.index(self.startIndex, offsetBy: 8)]
        let hourStr = self[
            self.index(self.startIndex, offsetBy: 9)..<self.index(self.startIndex, offsetBy: 11)]
        let minStr = self[
            self.index(self.startIndex, offsetBy: 11)..<self.index(self.startIndex, offsetBy: 13)]
        let secStr = self[
            self.index(self.startIndex, offsetBy: 13)..<self.index(self.startIndex, offsetBy: 15)]

        guard
            let year = Int(yearStr),
            let month = Int(monthStr),
            let day = Int(dayStr),
            let hour = Int(hourStr),
            let minute = Int(minStr),
            let second = Int(secStr)
        else { return nil }

        if year < 1 || month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour > 23
            || minute < 0 || minute > 59 || second < 0 || second > 59
        {
            return nil
        }

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
