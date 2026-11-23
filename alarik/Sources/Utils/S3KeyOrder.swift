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

/// S3 orders keys, markers and common prefixes by UTF-8 byte value. Swift's `String` operators
/// instead treat canonically equivalent text as equal and order by scalar, so a precomposed and
/// a decomposed `é` would collide, and a marker could sort differently from the keys around it.
/// Every listing sort, marker comparison and prefix test goes through here.
enum S3KeyOrder {
    @inline(__always)
    static func precedes(_ a: String, _ b: String) -> Bool {
        a.utf8.lexicographicallyPrecedes(b.utf8)
    }

    @inline(__always)
    static func same(_ a: String, _ b: String) -> Bool {
        a.utf8.elementsEqual(b.utf8)
    }

    @inline(__always)
    static func hasPrefix(_ key: String, _ prefix: String) -> Bool {
        key.utf8.starts(with: prefix.utf8)
    }
}
