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

/// The parsing of the `DEFAULT_BUCKETS` value - the seeding itself boots an application, but the
/// name splitting is pure and worth pinning down (whitespace, empties, duplicates, order).
@Suite("CreateDefaultBuckets parsing tests")
struct DefaultBucketSeedTests {
    @Test("splits comma-separated names and trims surrounding whitespace")
    func splitsAndTrims() {
        #expect(
            CreateDefaultBuckets.parseBucketNames("alpha, beta ,  gamma")
                == ["alpha", "beta", "gamma"])
    }

    @Test("drops empty entries from stray or trailing commas")
    func dropsEmpties() {
        #expect(CreateDefaultBuckets.parseBucketNames("alpha,,beta,") == ["alpha", "beta"])
        #expect(CreateDefaultBuckets.parseBucketNames("   ") == [])
        #expect(CreateDefaultBuckets.parseBucketNames("") == [])
    }

    @Test("de-duplicates while preserving first-seen order")
    func deduplicates() {
        #expect(
            CreateDefaultBuckets.parseBucketNames("one,two,one,three,two") == ["one", "two", "three"])
    }
}
