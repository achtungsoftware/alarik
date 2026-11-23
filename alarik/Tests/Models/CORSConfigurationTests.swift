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

@Suite("CORSConfiguration tests")
struct CORSConfigurationTests {
    private func rule(
        origins: [String] = ["https://app.example.com"], methods: [String] = ["GET", "PUT"],
        headers: [String] = ["Content-Type", "x-amz-*"], expose: [String] = ["ETag"],
        maxAge: Int? = 300
    ) -> CORSRule {
        CORSRule(
            id: "r1", allowedOrigins: origins, allowedMethods: methods, allowedHeaders: headers,
            exposeHeaders: expose, maxAgeSeconds: maxAge)
    }

    @Test("parse - a full rule round-trips through toXML")
    func testParseRoundTrip() throws {
        let xml = """
            <CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <CORSRule>
                <ID>r1</ID>
                <AllowedOrigin>https://app.example.com</AllowedOrigin>
                <AllowedOrigin>http://*.example.org</AllowedOrigin>
                <AllowedMethod>GET</AllowedMethod>
                <AllowedMethod>PUT</AllowedMethod>
                <AllowedHeader>Content-Type</AllowedHeader>
                <ExposeHeader>ETag</ExposeHeader>
                <ExposeHeader>x-amz-version-id</ExposeHeader>
                <MaxAgeSeconds>3000</MaxAgeSeconds>
              </CORSRule>
              <CORSRule>
                <AllowedOrigin>*</AllowedOrigin>
                <AllowedMethod>HEAD</AllowedMethod>
              </CORSRule>
            </CORSConfiguration>
            """
        let config = try CORSConfiguration.parse(xml: xml, requestId: "test")
        #expect(config.rules.count == 2)
        let first = config.rules[0]
        #expect(first.id == "r1")
        #expect(first.allowedOrigins == ["https://app.example.com", "http://*.example.org"])
        #expect(first.allowedMethods == ["GET", "PUT"])
        #expect(first.allowedHeaders == ["Content-Type"])
        #expect(first.exposeHeaders == ["ETag", "x-amz-version-id"])
        #expect(first.maxAgeSeconds == 3000)
        let second = config.rules[1]
        #expect(second.id == nil)
        #expect(second.allowedOrigins == ["*"])
        #expect(second.allowedHeaders.isEmpty)
        #expect(second.maxAgeSeconds == nil)

        let reparsed = try CORSConfiguration.parse(xml: config.toXML(), requestId: "test")
        #expect(reparsed == config)
    }

    @Test("parse - MalformedXML for no rules, bad methods and double wildcards")
    func testParseRejects() {
        let empty = "<CORSConfiguration></CORSConfiguration>"
        #expect(throws: S3Error.self) { try CORSConfiguration.parse(xml: empty, requestId: "t") }

        let badMethod = """
            <CORSConfiguration><CORSRule>
              <AllowedOrigin>*</AllowedOrigin><AllowedMethod>PATCH</AllowedMethod>
            </CORSRule></CORSConfiguration>
            """
        #expect(throws: S3Error.self) { try CORSConfiguration.parse(xml: badMethod, requestId: "t") }

        let twoStars = """
            <CORSConfiguration><CORSRule>
              <AllowedOrigin>http://*.*.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod>
            </CORSRule></CORSConfiguration>
            """
        #expect(throws: S3Error.self) { try CORSConfiguration.parse(xml: twoStars, requestId: "t") }

        let noOrigin = """
            <CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>
            """
        #expect(throws: S3Error.self) { try CORSConfiguration.parse(xml: noOrigin, requestId: "t") }
    }

    @Test("validate - the rule count ceiling and negative MaxAgeSeconds")
    func testValidateLimits() {
        let tooMany = Array(repeating: rule(), count: CORSConfiguration.maxRuleCount + 1)
        #expect(throws: CORSConfiguration.ValidationError.self) {
            try CORSConfiguration.validate(tooMany)
        }
        #expect(throws: Never.self) {
            try CORSConfiguration.validate(Array(repeating: rule(), count: CORSConfiguration.maxRuleCount))
        }
        #expect(throws: CORSConfiguration.ValidationError.self) {
            try CORSConfiguration.validate([rule(maxAge: -1)])
        }
    }

    @Test("matches - one wildcard, anywhere in the pattern")
    func testWildcard() {
        #expect(CORSConfiguration.matches(pattern: "*", "https://anything"))
        #expect(CORSConfiguration.matches(pattern: "http://*.example.com", "http://a.example.com"))
        #expect(CORSConfiguration.matches(pattern: "http://*.example.com", "http://a.b.example.com"))
        #expect(!CORSConfiguration.matches(pattern: "http://*.example.com", "http://example.com"))
        #expect(!CORSConfiguration.matches(pattern: "http://*.example.com", "https://a.example.com"))
        #expect(CORSConfiguration.matches(pattern: "x-amz-*", "x-amz-"))
        #expect(CORSConfiguration.matches(pattern: "https://app.example.com", "https://app.example.com"))
        #expect(!CORSConfiguration.matches(pattern: "https://app.example.com", "https://app.example.com:8080"))
    }

    @Test("rule(admitting:) - origin, method and every requested header must match")
    func testAdmits() {
        let config = CORSConfiguration(rules: [rule(), rule(origins: ["*"], methods: ["HEAD"], headers: [])])
        #expect(config.rule(admitting: "https://app.example.com", method: "GET")?.id == "r1")
        #expect(config.rule(admitting: "https://app.example.com", method: "get")?.id == "r1")
        #expect(config.rule(admitting: "https://other.example.com", method: "GET") == nil)
        #expect(config.rule(admitting: "https://other.example.com", method: "HEAD")?.allowedOrigins == ["*"])
        #expect(config.rule(admitting: "https://app.example.com", method: "DELETE") == nil)
        // Headers compare case-insensitively and honour the wildcard.
        #expect(
            config.rule(
                admitting: "https://app.example.com", method: "PUT",
                requestHeaders: ["content-type", "X-Amz-Date"]) != nil)
        #expect(
            config.rule(
                admitting: "https://app.example.com", method: "PUT", requestHeaders: ["Authorization"])
                == nil)
    }
}
