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

import Vapor

extension Environment {
    /// Reads an environment variable with one matching pair of surrounding quotes stripped -
    /// every Alarik configuration read goes through this instead of the raw `get`.
    ///
    /// Why: docker-compose's list-style `environment:` entries pass quotes through literally -
    /// `- ADMIN_PASSWORD="alarik"` sets the value to `"alarik"`, quotes included - while the
    /// same line in a `.env` file has its quotes consumed by the parser. Users write one syntax
    /// expecting the other's behavior and end up locked out with a password that secretly has
    /// quotes in it (GitHub issue #8). Stripping exactly one matching pair on read makes both
    /// spellings mean the same thing.
    static func sanitizedGet(_ key: String) -> String? {
        get(key).map(stripSurroundingQuotes)
    }

    /// Removes exactly one pair of matching surrounding quotes (`"…"` or `'…'`), and nothing
    /// else - interior quotes, mismatched quotes, and whitespace are all left untouched, so any
    /// value that wasn't wrapped this way passes through byte-identical.
    static func stripSurroundingQuotes(_ value: String) -> String {
        guard value.count >= 2,
            let first = value.first, let last = value.last,
            first == last, first == "\"" || first == "'"
        else {
            return value
        }
        return String(value.dropFirst().dropLast())
    }
}
