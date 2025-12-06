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
import NIOHTTP1
import Vapor

extension HTTPMediaType {
    /// Attempts to parse a raw Content-Type string into an HTTPMediaType struct.
    ///
    /// The parser handles the main type/subtype and extracts case-insensitive parameters.
    /// Example: "application/json; charset=utf-8; foo=bar"
    ///
    /// - Parameter string: The raw Content-Type string.
    /// - Returns: An initialized `HTTPMediaType` struct, or `nil` if parsing fails.
    public static func from(string: String) -> HTTPMediaType? {
        // Fast path for common simple types without parameters
        if !string.contains(";") {
            // Simple type/subtype without parameters
            guard let slashIndex = string.firstIndex(of: "/") else {
                return nil
            }
            let type = String(string[..<slashIndex]).trimmingWhitespace()
            let subType = String(string[string.index(after: slashIndex)...]).trimmingWhitespace()
            guard !type.isEmpty, !subType.isEmpty else {
                return nil
            }
            return HTTPMediaType(type: type, subType: subType, parameters: [:])
        }

        // Parse with parameters using UTF8 view for efficiency
        let utf8 = string.utf8

        // Find the first semicolon to split primary type from parameters
        guard let semicolonIndex = utf8.firstIndex(of: UInt8(ascii: ";")) else {
            return nil
        }

        let primaryPart = String(string[..<String.Index(semicolonIndex, within: string)!])

        // Parse type/subtype
        guard let slashIndex = primaryPart.firstIndex(of: "/") else {
            return nil
        }

        let calculatedType = String(primaryPart[..<slashIndex]).trimmingWhitespace()
        let calculatedSubType = String(primaryPart[primaryPart.index(after: slashIndex)...]).trimmingWhitespace()

        guard !calculatedType.isEmpty, !calculatedSubType.isEmpty else {
            return nil
        }

        // Parse parameters - only allocate dictionary if we have parameters
        var calculatedParameters: [String: String] = [:]
        var remaining = string[String.Index(semicolonIndex, within: string)!...]

        while let semicolonPos = remaining.firstIndex(of: ";") {
            remaining = remaining[remaining.index(after: semicolonPos)...]

            // Find the next semicolon or end of string
            let paramEnd = remaining.firstIndex(of: ";") ?? remaining.endIndex
            let paramString = String(remaining[..<paramEnd])

            // Parse key=value
            if let equalsIndex = paramString.firstIndex(of: "=") {
                let rawKey = String(paramString[..<equalsIndex]).trimmingWhitespace().lowercased()
                var rawValue = String(paramString[paramString.index(after: equalsIndex)...]).trimmingWhitespace()

                // Remove quotes from value if present
                if rawValue.hasPrefix("\"") && rawValue.hasSuffix("\"") && rawValue.count >= 2 {
                    rawValue = String(rawValue.dropFirst().dropLast())
                }

                if !rawKey.isEmpty {
                    calculatedParameters[rawKey] = rawValue
                }
            }

            if paramEnd == remaining.endIndex {
                break
            }
        }

        return HTTPMediaType(
            type: calculatedType, subType: calculatedSubType, parameters: calculatedParameters)
    }
}

private extension String {
    /// Fast whitespace trimming for ASCII strings
    func trimmingWhitespace() -> String {
        var start = startIndex
        var end = endIndex

        while start < end && (self[start] == " " || self[start] == "\t" || self[start] == "\n" || self[start] == "\r") {
            start = index(after: start)
        }

        while end > start {
            let prev = index(before: end)
            if self[prev] == " " || self[prev] == "\t" || self[prev] == "\n" || self[prev] == "\r" {
                end = prev
            } else {
                break
            }
        }

        return String(self[start..<end])
    }
}
