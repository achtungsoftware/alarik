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

@Suite("ReplicationConfiguration tests")
struct ReplicationConfigurationTests {

    private func target(
        id: UUID = UUID(), endpoint: String = "https://remote.example/", bucket: String = "dest",
        enabled: Bool = true
    ) -> ReplicationTarget {
        ReplicationTarget(
            id: id, endpoint: endpoint, targetBucket: bucket, accessKeyId: "AKID",
            secretAccessKey: "SECRET", region: "us-east-1", enabled: enabled)
    }

    private func rule(
        targetId: UUID, prefix: String? = nil, replicateDeletes: Bool = false,
        replicateExisting: Bool = false, synchronous: Bool = false, enabled: Bool = true
    ) -> ReplicationRule {
        ReplicationRule(
            id: UUID(), targetId: targetId, prefix: prefix, replicateDeletes: replicateDeletes,
            replicateExisting: replicateExisting, synchronous: synchronous, enabled: enabled)
    }

    // MARK: - JSON round-trip

    @Test("JSON round-trip preserves all target and rule fields")
    func jsonRoundTrip() throws {
        let t = target()
        let r = rule(targetId: t.id, prefix: "images/", replicateDeletes: true, replicateExisting: true)
        let config = ReplicationConfiguration(targets: [t], rules: [r])

        let restored = ReplicationConfiguration.fromJSON(config.toJSON())
        #expect(restored == config)
    }

    @Test("fromJSON on garbage returns empty config")
    func fromJSONGarbage() {
        #expect(ReplicationConfiguration.fromJSON("not json").targets.isEmpty)
        #expect(ReplicationConfiguration.fromJSON("not json").rules.isEmpty)
        #expect(ReplicationConfiguration.fromJSON("").rules.isEmpty)
    }

    @Test("JSON round-trip preserves synchronous: true")
    func jsonRoundTripPreservesSynchronous() throws {
        let t = target()
        let r = rule(targetId: t.id, synchronous: true)
        let config = ReplicationConfiguration(targets: [t], rules: [r])

        let restored = ReplicationConfiguration.fromJSON(config.toJSON())
        #expect(restored.rules.first?.synchronous == true)
    }

    @Test("a rule saved before `synchronous` existed decodes as async (false), not a decode failure")
    func missingSynchronousKeyDecodesAsFalse() throws {
        let targetId = UUID()
        let ruleId = UUID()
        // Deliberately omits "synchronous" - the exact shape a config saved by an older
        // version of this struct would have on disk.
        let json = """
            {"targets":[],"rules":[{"id":"\(ruleId.uuidString)","targetId":"\(targetId.uuidString)","prefix":null,"replicateDeletes":false,"replicateExisting":false,"enabled":true}]}
            """
        let config = ReplicationConfiguration.fromJSON(json)
        // A real decode failure falls back to `.empty` - asserting a non-empty rule set here
        // proves the whole config decoded successfully despite the missing key.
        #expect(config.rules.count == 1)
        #expect(config.rules.first?.synchronous == false)
    }

    // MARK: - Target resolution

    @Test("target(for:) resolves a known id and returns nil for an unknown one")
    func targetResolution() {
        let t = target()
        let config = ReplicationConfiguration(targets: [t], rules: [])
        #expect(config.target(for: t.id) == t)
        #expect(config.target(for: UUID()) == nil)
    }

    // MARK: - Rule matching

    @Test("no prefix matches every key")
    func noPrefixMatchesAll() {
        let r = rule(targetId: UUID())
        #expect(r.matches(key: "a.txt"))
        #expect(r.matches(key: "deep/nested/b.txt"))
    }

    @Test("prefix filter")
    func prefixFilter() {
        let r = rule(targetId: UUID(), prefix: "images/")
        #expect(r.matches(key: "images/cat.jpg"))
        #expect(!r.matches(key: "docs/cat.jpg"))
    }

    @Test("empty-string prefix behaves like no filter")
    func emptyPrefixMatchesAll() {
        let r = rule(targetId: UUID(), prefix: "")
        #expect(r.matches(key: "anything.txt"))
    }

    @Test("disabled rule never matches")
    func disabledRule() {
        let r = rule(targetId: UUID(), enabled: false)
        #expect(!r.matches(key: "a.txt"))
    }

    // MARK: - XML

    @Test("empty config renders an empty ReplicationConfiguration element")
    func emptyXML() {
        let xml = ReplicationConfiguration.empty.toXML()
        #expect(xml.contains("<ReplicationConfiguration"))
        #expect(!xml.contains("<Rule>"))
    }

    @Test("XML surfaces each enabled rule with an alarik replication ARN destination")
    func ruleXML() {
        let t = target(bucket: "dest-bucket")
        let r = rule(targetId: t.id, prefix: "p/", replicateDeletes: true)
        let config = ReplicationConfiguration(targets: [t], rules: [r])

        let xml = config.toXML()
        #expect(xml.contains("<ID>\(r.id.uuidString)</ID>"))
        #expect(xml.contains("<Prefix>p/</Prefix>"))
        #expect(xml.contains("<Destination><Bucket>arn:alarik:replication:::\(t.id.uuidString)</Bucket></Destination>"))
        #expect(xml.contains("<DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>"))
        // The SQLKit-interpolation regression must never come back
        #expect(!xml.contains("SQLQueryString"))
    }

    @Test("a disabled rule is omitted from the XML")
    func disabledRuleOmittedFromXML() {
        let t = target()
        let r = rule(targetId: t.id, enabled: false)
        let config = ReplicationConfiguration(targets: [t], rules: [r])
        #expect(!config.toXML().contains("<Rule>"))
    }

    @Test("a rule referencing an unknown target is omitted from the XML")
    func danglingRuleOmittedFromXML() {
        let r = rule(targetId: UUID(), enabled: true)
        let config = ReplicationConfiguration(targets: [], rules: [r])
        #expect(!config.toXML().contains("<Rule>"))
    }
}
