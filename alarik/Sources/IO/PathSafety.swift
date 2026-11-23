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

/// The one place a client-supplied name loses its `..` before it becomes a path segment.
/// Percent-encoding alone never blocks `../../etc`-style traversal (`.` is a legal path
/// character), so every bucket, key and prefix is passed through here on its way to disk.
enum PathSafety {
    /// `..` removed from every slash-separated component; the components themselves - empty
    /// ones and a trailing slash included - stay where they are, so a key maps to the same
    /// path every time.
    static func strippingDotDot(_ name: String) -> String {
        guard name.contains("..") else { return name }
        return name.split(separator: "/", omittingEmptySubsequences: false)
            .map { $0.replacingOccurrences(of: "..", with: "") }
            .joined(separator: "/")
    }

    /// A listing prefix: `..` stripped and the components it leaves empty dropped, keeping the
    /// trailing slash that marks a folder. `../../` collapses to `/`, which callers reject.
    static func sanitizedPrefix(_ prefix: String) -> String {
        guard prefix.contains("..") else { return prefix }
        var sanitized = prefix.split(separator: "/", omittingEmptySubsequences: false)
            .map { $0.replacingOccurrences(of: "..", with: "") }
            .filter { !$0.isEmpty }
            .joined(separator: "/")
        if prefix.hasSuffix("/") && !sanitized.hasSuffix("/") { sanitized += "/" }
        return sanitized
    }
}
