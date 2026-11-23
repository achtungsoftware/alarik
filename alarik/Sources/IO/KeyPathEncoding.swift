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

/// How a key's slash-separated components become directory entries. Alarik's own names share
/// those directories - `<stem>.obj`, `<stem>.versions/`, `<stem>.ecshards/` - so a component
/// ending in one of those suffixes (the key `report.obj`, the folder `backup.versions/`) would
/// collide with another key's files, and `.` or an empty component (`a//b`) collapses in the
/// filesystem. Such a component is stored with a trailing `~`; a component that would then read
/// as escaped (`x.obj~`) gets one more. Everything else is stored verbatim, so existing data
/// keeps its paths: a name ending in `~` decodes as escaped only when what precedes it needed
/// escaping, so a plain `notes.txt~` stays what it is.
enum KeyPathEncoding {
    static let marker: Character = "~"
    private static let reservedSuffixes = [".obj", ".versions", ".ecshards"]

    /// `asDirectory` is true for every component but a key's last: an empty last component is
    /// the folder marker `docs/`, stored as the `.obj` file with an empty stem it always was.
    static func needsEscape(_ component: Substring, asDirectory: Bool) -> Bool {
        if component.isEmpty { return asDirectory }
        if component == "." { return true }
        if reservedSuffixes.contains(where: { component.hasSuffix($0) }) { return true }
        if component.last == marker { return needsEscape(component.dropLast(), asDirectory: asDirectory) }
        return false
    }

    static func encodeComponent(_ component: Substring, asDirectory: Bool) -> String {
        needsEscape(component, asDirectory: asDirectory) ? component + String(marker) : String(component)
    }

    static func decodeComponent(_ name: String, asDirectory: Bool) -> String {
        guard name.last == marker else { return name }
        let inner = name.dropLast()
        return needsEscape(inner, asDirectory: asDirectory) ? String(inner) : name
    }

    /// The path of `key` below its bucket, without the form suffix.
    static func encodedPath(forKey key: String) -> String {
        let components = key.split(separator: "/", omittingEmptySubsequences: false)
        return components.enumerated().map { index, component in
            encodeComponent(component, asDirectory: index < components.count - 1)
        }.joined(separator: "/")
    }

    /// The directory a folder prefix (`docs/`, or `docs` - both name the folder) maps to below
    /// its bucket; empty for the bucket root.
    static func encodedDirectoryPath(forPrefix prefix: String) -> String {
        var components = prefix.split(separator: "/", omittingEmptySubsequences: false)
        if components.last?.isEmpty == true { components.removeLast() }
        return components.map { encodeComponent($0, asDirectory: true) }.joined(separator: "/")
    }
}
