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

@Suite("OIDCDiscoveryCache.rejectMetadataHost tests")
struct OIDCDiscoveryCacheTests {
    @Test("blocks the cloud metadata address written as plain dotted-decimal")
    func blocksDottedDecimal() {
        #expect(throws: (any Error).self) {
            try OIDCDiscoveryCache.rejectMetadataHost(of: "http://169.254.169.254/latest/meta-data")
        }
    }

    @Test("blocks alternate numeric encodings of the metadata address")
    func blocksAlternateEncodings() {
        for url in [
            "http://2852039166/latest/meta-data",  // decimal
            "http://0xA9FEA9FE/latest/meta-data",  // hex
            "http://169.254.1/latest/meta-data",  // short form
        ] {
            #expect(throws: (any Error).self, "expected blocked: \(url)") {
                try OIDCDiscoveryCache.rejectMetadataHost(of: url)
            }
        }
    }

    @Test("blocks IPv6 link-local")
    func blocksIPv6LinkLocal() {
        #expect(throws: (any Error).self) {
            try OIDCDiscoveryCache.rejectMetadataHost(of: "http://[fe80::1]/")
        }
    }

    @Test("does not block general private ranges or loopback - intentionally, for self-hosted IdPs")
    func allowsGeneralPrivateRanges() throws {
        try OIDCDiscoveryCache.rejectMetadataHost(of: "http://10.0.0.5/oidc")
        try OIDCDiscoveryCache.rejectMetadataHost(of: "http://192.168.1.5/oidc")
        try OIDCDiscoveryCache.rejectMetadataHost(of: "http://127.0.0.1/oidc")
    }

    @Test("allows ordinary public hosts")
    func allowsPublicHosts() throws {
        try OIDCDiscoveryCache.rejectMetadataHost(of: "https://accounts.google.com/o/oauth2")
    }
}
