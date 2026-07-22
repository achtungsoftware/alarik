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

@Suite("LifecycleConfiguration tests")
struct LifecycleConfigurationTests {

    // MARK: - parse(xml:)

    @Test("parse - a rule with Expiration.Days")
    func testParseExpirationDays() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><Prefix>logs/</Prefix></Filter>
                <Status>Enabled</Status>
                <Expiration><Days>30</Days></Expiration>
              </Rule>
            </LifecycleConfiguration>
            """
        let config = try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        #expect(config.rules.count == 1)
        let rule = config.rules[0]
        #expect(rule.id == "rule1")
        #expect(rule.enabled == true)
        #expect(rule.prefix == "logs/")
        #expect(rule.expirationDays == 30)
        #expect(rule.noncurrentVersionExpirationDays == nil)
        #expect(rule.abortIncompleteMultipartUploadDays == nil)
    }

    @Test("parse - a rule with NoncurrentVersionExpiration")
    func testParseNoncurrentVersionExpiration() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><Prefix></Prefix></Filter>
                <Status>Enabled</Status>
                <NoncurrentVersionExpiration><NoncurrentDays>100</NoncurrentDays></NoncurrentVersionExpiration>
              </Rule>
            </LifecycleConfiguration>
            """
        let config = try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        #expect(config.rules[0].noncurrentVersionExpirationDays == 100)
    }

    @Test("parse - a rule with AbortIncompleteMultipartUpload")
    func testParseAbortIncompleteMultipartUpload() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><Prefix></Prefix></Filter>
                <Status>Enabled</Status>
                <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>
              </Rule>
            </LifecycleConfiguration>
            """
        let config = try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        #expect(config.rules[0].abortIncompleteMultipartUploadDays == 7)
    }

    @Test("parse - multiple rules")
    func testParseMultipleRules() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><Prefix>logs/</Prefix></Filter>
                <Status>Enabled</Status>
                <Expiration><Days>30</Days></Expiration>
              </Rule>
              <Rule>
                <ID>rule2</ID>
                <Filter><Prefix>tmp/</Prefix></Filter>
                <Status>Disabled</Status>
                <Expiration><Days>1</Days></Expiration>
              </Rule>
            </LifecycleConfiguration>
            """
        let config = try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        #expect(config.rules.count == 2)
        #expect(config.rules[0].id == "rule1")
        #expect(config.rules[0].enabled == true)
        #expect(config.rules[1].id == "rule2")
        #expect(config.rules[1].enabled == false)
    }

    @Test("parse - ID is auto-generated when absent")
    func testParseGeneratesIdWhenAbsent() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <Filter><Prefix></Prefix></Filter>
                <Status>Enabled</Status>
                <Expiration><Days>1</Days></Expiration>
              </Rule>
            </LifecycleConfiguration>
            """
        let config = try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        #expect(!config.rules[0].id.isEmpty)
    }

    @Test("parse - legacy bare Prefix (not nested in Filter) is supported for backward compatibility")
    func testParseLegacyBarePrefix() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Prefix>old-style/</Prefix>
                <Status>Enabled</Status>
                <Expiration><Days>1</Days></Expiration>
              </Rule>
            </LifecycleConfiguration>
            """
        let config = try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        #expect(config.rules[0].prefix == "old-style/")
    }

    @Test("parse - missing Filter/Prefix defaults to the whole bucket (empty prefix)")
    func testParseMissingFilterDefaultsToWholeBucket() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Status>Enabled</Status>
                <Expiration><Days>1</Days></Expiration>
              </Rule>
            </LifecycleConfiguration>
            """
        let config = try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        #expect(config.rules[0].prefix == "")
    }

    @Test("parse - rejects a Rule missing Status")
    func testParseRejectsMissingStatus() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><Prefix></Prefix></Filter>
                <Expiration><Days>1</Days></Expiration>
              </Rule>
            </LifecycleConfiguration>
            """
        #expect(throws: S3Error.self) {
            try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        }
    }

    @Test("parse - rejects a Rule with no supported actions")
    func testParseRejectsRuleWithNoActions() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><Prefix></Prefix></Filter>
                <Status>Enabled</Status>
              </Rule>
            </LifecycleConfiguration>
            """
        #expect(throws: S3Error.self) {
            try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        }
    }

    @Test("parse - rejects a configuration with no rules at all")
    func testParseRejectsNoRules() throws {
        let xml = "<LifecycleConfiguration></LifecycleConfiguration>"
        #expect(throws: S3Error.self) {
            try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        }
    }

    @Test("parse - rejects Transition")
    func testParseRejectsTransition() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><Prefix>documents/</Prefix></Filter>
                <Status>Enabled</Status>
                <Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition>
              </Rule>
            </LifecycleConfiguration>
            """
        do {
            _ = try LifecycleConfiguration.parse(xml: xml, requestId: "test-id")
            Issue.record("Expected parse to throw")
        } catch let error as S3Error {
            #expect(error.code == "MalformedXML")
            #expect(error.status == .badRequest)
        }
    }

    @Test("parse - rejects NoncurrentVersionTransition")
    func testParseRejectsNoncurrentVersionTransition() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><Prefix></Prefix></Filter>
                <Status>Enabled</Status>
                <NoncurrentVersionTransition><NoncurrentDays>30</NoncurrentDays><StorageClass>GLACIER</StorageClass></NoncurrentVersionTransition>
              </Rule>
            </LifecycleConfiguration>
            """
        #expect(throws: S3Error.self) {
            try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        }
    }

    @Test("parse - rejects tag-based filters")
    func testParseRejectsTagFilter() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><Tag><Key>env</Key><Value>prod</Value></Tag></Filter>
                <Status>Enabled</Status>
                <Expiration><Days>1</Days></Expiration>
              </Rule>
            </LifecycleConfiguration>
            """
        #expect(throws: S3Error.self) {
            try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        }
    }

    @Test("parse - rejects And (combined) filters")
    func testParseRejectsAndFilter() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><And><Prefix>x/</Prefix><ObjectSizeGreaterThan>500</ObjectSizeGreaterThan></And></Filter>
                <Status>Enabled</Status>
                <Expiration><Days>1</Days></Expiration>
              </Rule>
            </LifecycleConfiguration>
            """
        #expect(throws: S3Error.self) {
            try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        }
    }

    @Test("parse - rejects ObjectSizeGreaterThan/ObjectSizeLessThan filters")
    func testParseRejectsObjectSizeFilters() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><ObjectSizeGreaterThan>500</ObjectSizeGreaterThan></Filter>
                <Status>Enabled</Status>
                <Expiration><Days>1</Days></Expiration>
              </Rule>
            </LifecycleConfiguration>
            """
        #expect(throws: S3Error.self) {
            try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        }
    }

    @Test("parse - rejects ExpiredObjectDeleteMarker")
    func testParseRejectsExpiredObjectDeleteMarker() throws {
        let xml = """
            <LifecycleConfiguration>
              <Rule>
                <ID>rule1</ID>
                <Filter><Prefix></Prefix></Filter>
                <Status>Enabled</Status>
                <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>
              </Rule>
            </LifecycleConfiguration>
            """
        #expect(throws: S3Error.self) {
            try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        }
    }

    // MARK: - toXML()

    @Test("toXML - produces the expected structure")
    func testToXMLStructure() {
        let config = LifecycleConfiguration(rules: [
            LifecycleRule(
                id: "rule1", enabled: true, prefix: "logs/", expirationDays: 30,
                noncurrentVersionExpirationDays: nil, abortIncompleteMultipartUploadDays: nil)
        ])
        let xml = config.toXML()
        #expect(xml.contains("<ID>rule1</ID>"))
        #expect(xml.contains("<Filter><Prefix>logs/</Prefix></Filter>"))
        #expect(xml.contains("<Status>Enabled</Status>"))
        #expect(xml.contains("<Expiration><Days>30</Days></Expiration>"))
        #expect(xml.contains("<LifecycleConfiguration"))
    }

    @Test("toXML - Disabled status is reflected")
    func testToXMLDisabledStatus() {
        let config = LifecycleConfiguration(rules: [
            LifecycleRule(
                id: "rule1", enabled: false, prefix: "", expirationDays: 1,
                noncurrentVersionExpirationDays: nil, abortIncompleteMultipartUploadDays: nil)
        ])
        #expect(config.toXML().contains("<Status>Disabled</Status>"))
    }

    @Test("toXML round-trips through parse")
    func testToXMLRoundTripsThroughParse() throws {
        let original = LifecycleConfiguration(rules: [
            LifecycleRule(
                id: "rule1", enabled: true, prefix: "logs/", expirationDays: 30,
                noncurrentVersionExpirationDays: 7, abortIncompleteMultipartUploadDays: 3)
        ])
        let xml = original.toXML()
        let parsed = try LifecycleConfiguration.parse(xml: xml, requestId: "test")
        #expect(parsed == original)
    }

    // MARK: - Codable

    @Test("Codable round-trip preserves every rule field")
    func testCodableRoundTrip() throws {
        let original = [
            LifecycleRule(
                id: "rule1", enabled: true, prefix: "logs/", expirationDays: 30,
                noncurrentVersionExpirationDays: 7, abortIncompleteMultipartUploadDays: 3)
        ]
        let restored = try JSONDecoder().decode(
            [LifecycleRule].self, from: try JSONEncoder().encode(original))
        #expect(restored == original)
    }
}
