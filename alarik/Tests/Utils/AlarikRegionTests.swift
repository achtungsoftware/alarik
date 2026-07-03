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

/// `ALARIK_REGION` is a process-wide environment variable, so these tests mutate and restore
/// it around each assertion - `.serialized` (mirroring every other suite that touches shared
/// process state in this test target) keeps that safe under `swift test --no-parallel`.
@Suite("AlarikRegion tests", .serialized)
struct AlarikRegionTests {

    @Test("resolves to the default region when ALARIK_REGION is unset")
    func resolvesToDefaultWhenUnset() {
        let original = ProcessInfo.processInfo.environment["ALARIK_REGION"]
        unsetenv("ALARIK_REGION")
        defer {
            if let original {
                setenv("ALARIK_REGION", original, 1)
            }
        }

        #expect(AlarikRegion.resolve() == AlarikRegion.default)
        #expect(AlarikRegion.default == "us-east-1")
    }

    @Test("resolves to ALARIK_REGION when set")
    func resolvesToConfiguredRegion() {
        let original = ProcessInfo.processInfo.environment["ALARIK_REGION"]
        setenv("ALARIK_REGION", "eu-central-1", 1)
        defer {
            if let original {
                setenv("ALARIK_REGION", original, 1)
            } else {
                unsetenv("ALARIK_REGION")
            }
        }

        #expect(AlarikRegion.resolve() == "eu-central-1")
    }
}
