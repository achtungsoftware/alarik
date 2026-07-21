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

/// Regression coverage for issue #14: an Authentik issuer (which ends in a trailing slash) could
/// never match, because the trailing slash was trimmed from the configured value only and then
/// compared against the document's untrimmed one.
@Suite("OIDCDiscoveryCache issuer canonicalization tests")
struct OIDCDiscoveryCacheIssuerTests {
    /// The exact comparison `resolve` performs before trusting a discovery document.
    private func matches(configured: String, documentReports: String) -> Bool {
        OIDCDiscoveryCache.canonicalIssuer(documentReports)
            == OIDCDiscoveryCache.canonicalIssuer(configured)
    }

    @Test("an Authentik-style issuer with a trailing slash matches however the admin typed it")
    func authentikTrailingSlash() {
        let reported = "https://authentik.example.com/application/o/alarik/"
        #expect(matches(configured: reported, documentReports: reported))
        #expect(
            matches(
                configured: "https://authentik.example.com/application/o/alarik",
                documentReports: reported))
    }

    @Test("the doubled-slash workaround from the issue thread keeps working")
    func doubledSlashWorkaroundStillWorks() {
        #expect(
            matches(
                configured: "https://authentik.example.com/application/o/alarik//",
                documentReports: "https://authentik.example.com/application/o/alarik/"))
    }

    @Test("an issuer without a trailing slash still matches (Google-style)")
    func noTrailingSlash() {
        #expect(
            matches(
                configured: "https://accounts.google.com",
                documentReports: "https://accounts.google.com"))
    }

    @Test("a genuinely different issuer is still rejected - normalization must not over-match")
    func differentIssuersStillRejected() {
        #expect(
            !matches(
                configured: "https://authentik.example.com/application/o/alarik/",
                documentReports: "https://evil.example.com/application/o/alarik/"))
        // Same host, different application - the path still has to agree.
        #expect(
            !matches(
                configured: "https://authentik.example.com/application/o/alarik/",
                documentReports: "https://authentik.example.com/application/o/other/"))
        // A trailing slash is the ONLY difference that gets normalized away.
        #expect(
            !matches(
                configured: "https://authentik.example.com/application/o/alarik/",
                documentReports: "https://authentik.example.com/application/o/alarik/x"))
    }

    @Test("the well-known URL built from a canonical issuer never doubles the slash")
    func wellKnownURLIsClean() {
        for configured in [
            "https://authentik.example.com/application/o/alarik",
            "https://authentik.example.com/application/o/alarik/",
            "https://authentik.example.com/application/o/alarik//",
        ] {
            let url =
                "\(OIDCDiscoveryCache.canonicalIssuer(configured))/.well-known/openid-configuration"
            #expect(
                url == "https://authentik.example.com/application/o/alarik/.well-known/openid-configuration"
            )
        }
    }
}
