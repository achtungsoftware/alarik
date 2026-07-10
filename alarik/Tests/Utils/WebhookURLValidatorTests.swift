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

@Suite("WebhookURLValidator tests")
struct WebhookURLValidatorTests {

    @Test("valid http(s) URLs pass structure validation")
    func validStructure() throws {
        try WebhookURLValidator.validateStructure("https://example.com/hook")
        try WebhookURLValidator.validateStructure("http://example.com:8080/hook")
    }

    @Test("non-http schemes and hostless URLs are rejected")
    func invalidStructure() {
        #expect(throws: (any Error).self) {
            try WebhookURLValidator.validateStructure("ftp://example.com/x")
        }
        #expect(throws: (any Error).self) {
            try WebhookURLValidator.validateStructure("not a url")
        }
        #expect(throws: (any Error).self) {
            try WebhookURLValidator.validateStructure("https:///nohost")
        }
    }

    @Test("private, loopback, link-local, and metadata IPv4 hosts are internal")
    func internalIPv4() {
        for url in [
            "http://127.0.0.1/h", "http://127.5.5.5/h",
            "http://10.0.0.1/h", "http://10.255.1.1/h",
            "http://172.16.0.1/h", "http://172.31.255.1/h",
            "http://192.168.1.1/h",
            "http://169.254.169.254/latest/meta-data",
            "http://0.0.0.0/h",
        ] {
            #expect(WebhookURLValidator.isInternalHost(url), "expected internal: \(url)")
        }
    }

    @Test("public IPv4 hosts are not internal")
    func publicIPv4() {
        for url in [
            "http://8.8.8.8/h", "http://1.1.1.1/h",
            "http://172.15.0.1/h",  // just below the 172.16/12 private range
            "http://172.32.0.1/h",  // just above it
            "http://11.0.0.1/h",
        ] {
            #expect(!WebhookURLValidator.isInternalHost(url), "expected public: \(url)")
        }
    }

    @Test("localhost by name is internal")
    func localhostByName() {
        #expect(WebhookURLValidator.isInternalHost("http://localhost/h"))
        #expect(WebhookURLValidator.isInternalHost("http://foo.localhost/h"))
    }

    @Test("IPv6 loopback / link-local / ULA are internal")
    func internalIPv6() {
        #expect(WebhookURLValidator.isInternalHost("http://[::1]/h"))
        #expect(WebhookURLValidator.isInternalHost("http://[fe80::1]/h"))
        #expect(WebhookURLValidator.isInternalHost("http://[fc00::1]/h"))
        #expect(WebhookURLValidator.isInternalHost("http://[fd12:3456::1]/h"))
    }

    @Test("public hostnames starting with fc/fd are NOT misclassified as IPv6 ULA")
    func fcFdHostnamesArePublic() {
        // Regression: an fc00::/7 check that didn't require a colon wrongly flagged these
        #expect(!WebhookURLValidator.isInternalHost("https://fcdn.example.com/h"))
        #expect(!WebhookURLValidator.isInternalHost("https://fd-images.net/h"))
        #expect(!WebhookURLValidator.isInternalHost("https://fastly.example.com/h"))
    }

    @Test("ordinary public hostnames are not internal")
    func publicHostnames() {
        #expect(!WebhookURLValidator.isInternalHost("https://example.com/h"))
        #expect(!WebhookURLValidator.isInternalHost("https://hooks.slack.com/services/x"))
    }

    @Test("alternate IPv4 encodings of internal addresses are still recognized as internal")
    func alternateEncodingsOfInternalAddresses() {
        for url in [
            "http://2130706433/h",  // decimal for 127.0.0.1
            "http://0x7f000001/h",  // hex for 127.0.0.1
            "http://0177.0.0.1/h",  // octal first octet for 127.0.0.1
            "http://127.1/h",  // short form for 127.0.0.1
            "http://127.0.1/h",  // short form for 127.0.0.1
            "http://0x7f.0.0.1/h",  // mixed hex/decimal octets for 127.0.0.1
            "http://2852039166/h",  // decimal for 169.254.169.254 (cloud metadata)
            "http://0xA9FEA9FE/h",  // hex for 169.254.169.254
            "http://0x0a000001/h",  // hex for 10.0.0.1
        ] {
            #expect(WebhookURLValidator.isInternalHost(url), "expected internal: \(url)")
        }
    }

    @Test("alternate IPv4 encodings of public addresses stay public")
    func alternateEncodingsOfPublicAddresses() {
        for url in [
            "http://134744072/h",  // decimal for 8.8.8.8
            "http://0x08080808/h",  // hex for 8.8.8.8
        ] {
            #expect(!WebhookURLValidator.isInternalHost(url), "expected public: \(url)")
        }
    }

    @Test("hostnames that merely look numeric-ish are never misclassified as an IP")
    func numericLookingHostnamesStayPublic() {
        #expect(!WebhookURLValidator.isInternalHost("https://2130706433.example.com/h"))
        #expect(!WebhookURLValidator.isInternalHost("https://0x7f000001.example.com/h"))
        #expect(!WebhookURLValidator.isInternalHost("https://999999999999999999999/h"))
    }
}
