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

@Suite("NotificationConfiguration tests")
struct NotificationConfigurationTests {

    private func rule(
        events: [String], prefix: String? = nil, suffix: String? = nil, enabled: Bool = true
    ) -> NotificationRule {
        NotificationRule(
            id: UUID(), url: "https://example.com/hook", secret: nil, events: events,
            prefix: prefix, suffix: suffix, enabled: enabled)
    }

    // MARK: - JSON round-trip

    @Test("JSON round-trip preserves all rule fields")
    func jsonRoundTrip() throws {
        let config = NotificationConfiguration(rules: [
            NotificationRule(
                id: UUID(), url: "https://a.example/hook", secret: "s3cr3t",
                events: ["s3:ObjectCreated:*"], prefix: "images/", suffix: ".jpg", enabled: true),
            NotificationRule(
                id: UUID(), url: "https://b.example/hook", secret: nil,
                events: ["s3:ObjectRemoved:Delete"], prefix: nil, suffix: nil, enabled: false),
        ])

        let restored = try JSONDecoder().decode(
            NotificationConfiguration.self, from: try JSONEncoder().encode(config))
        #expect(restored == config)
    }

    // MARK: - Rule matching

    @Test("exact event match")
    func exactEventMatch() {
        let r = rule(events: ["s3:ObjectCreated:Put"])
        #expect(r.matches(eventName: "s3:ObjectCreated:Put", key: "a.txt"))
        #expect(!r.matches(eventName: "s3:ObjectCreated:Copy", key: "a.txt"))
    }

    @Test("wildcard family match")
    func wildcardMatch() {
        let r = rule(events: ["s3:ObjectCreated:*"])
        #expect(r.matches(eventName: "s3:ObjectCreated:Put", key: "a.txt"))
        #expect(r.matches(eventName: "s3:ObjectCreated:CompleteMultipartUpload", key: "a.txt"))
        #expect(!r.matches(eventName: "s3:ObjectRemoved:Delete", key: "a.txt"))
    }

    @Test("prefix filter")
    func prefixFilter() {
        let r = rule(events: ["s3:ObjectCreated:*"], prefix: "images/")
        #expect(r.matches(eventName: "s3:ObjectCreated:Put", key: "images/cat.jpg"))
        #expect(!r.matches(eventName: "s3:ObjectCreated:Put", key: "docs/cat.jpg"))
    }

    @Test("suffix filter")
    func suffixFilter() {
        let r = rule(events: ["s3:ObjectCreated:*"], suffix: ".jpg")
        #expect(r.matches(eventName: "s3:ObjectCreated:Put", key: "a/b/cat.jpg"))
        #expect(!r.matches(eventName: "s3:ObjectCreated:Put", key: "a/b/cat.png"))
    }

    @Test("prefix and suffix both required")
    func prefixAndSuffix() {
        let r = rule(events: ["s3:ObjectCreated:*"], prefix: "images/", suffix: ".jpg")
        #expect(r.matches(eventName: "s3:ObjectCreated:Put", key: "images/cat.jpg"))
        #expect(!r.matches(eventName: "s3:ObjectCreated:Put", key: "images/cat.png"))
        #expect(!r.matches(eventName: "s3:ObjectCreated:Put", key: "other/cat.jpg"))
    }

    @Test("disabled rule never matches")
    func disabledRule() {
        let r = rule(events: ["s3:ObjectCreated:*"], enabled: false)
        #expect(!r.matches(eventName: "s3:ObjectCreated:Put", key: "a.txt"))
    }

    // MARK: - XML

    @Test("empty config renders an empty NotificationConfiguration element")
    func emptyXML() {
        let xml = NotificationConfiguration.empty.toXML()
        #expect(xml.contains("<NotificationConfiguration"))
        #expect(!xml.contains("<QueueConfiguration>"))
    }

    @Test("XML surfaces each rule as a QueueConfiguration with an alarik webhook ARN")
    func ruleXML() {
        let id = UUID()
        let config = NotificationConfiguration(rules: [
            NotificationRule(
                id: id, url: "https://x/y", secret: nil, events: ["s3:ObjectCreated:*"],
                prefix: "p/", suffix: ".txt", enabled: true)
        ])
        let xml = config.toXML()
        #expect(xml.contains("<Id>\(id.uuidString)</Id>"))
        #expect(xml.contains("<Queue>arn:alarik:webhook:::\(id.uuidString)</Queue>"))
        #expect(xml.contains("<Event>s3:ObjectCreated:*</Event>"))
        #expect(xml.contains("<Name>prefix</Name><Value>p/</Value>"))
        #expect(xml.contains("<Name>suffix</Name><Value>.txt</Value>"))
        // The SQLKit-interpolation regression must never come back
        #expect(!xml.contains("SQLQueryString"))
    }

    // MARK: - Payload

    @Test("payload matches AWS v2.4 structure with URL-encoded key and sequencer")
    func payloadStructure() throws {
        let json = NotificationService.buildPayload(
            event: .objectCreatedPut, bucketName: "my-bucket", key: "red flower.jpg",
            size: 1024, etag: "abc123", versionId: "v1", requestId: "REQ1", sourceIP: "1.2.3.4",
            configurationId: "cfg1")

        let obj = try JSONSerialization.jsonObject(with: Data(json.utf8)) as! [String: Any]
        let records = obj["Records"] as! [[String: Any]]
        #expect(records.count == 1)
        let record = records[0]
        #expect(record["eventVersion"] as? String == "2.4")
        #expect(record["eventSource"] as? String == "alarik:s3")
        // eventName drops the s3: prefix
        #expect(record["eventName"] as? String == "ObjectCreated:Put")

        let s3 = record["s3"] as! [String: Any]
        #expect((s3["configurationId"] as? String) == "cfg1")
        let bucket = s3["bucket"] as! [String: Any]
        #expect(bucket["name"] as? String == "my-bucket")
        #expect(bucket["arn"] as? String == "arn:aws:s3:::my-bucket")

        let object = s3["object"] as! [String: Any]
        // Space becomes + (AWS URL-encoding)
        #expect(object["key"] as? String == "red+flower.jpg")
        #expect(object["size"] as? Int == 1024)
        #expect(object["eTag"] as? String == "abc123")
        #expect(object["versionId"] as? String == "v1")
        #expect((object["sequencer"] as? String)?.isEmpty == false)

        let response = record["responseElements"] as! [String: Any]
        #expect(response["x-amz-request-id"] as? String == "REQ1")
    }

    @Test("HMAC signature is stable and matches an independent computation")
    func hmacSignature() {
        let payload = #"{"Records":[]}"#
        let sig = NotificationService.signature(payload: payload, secret: "topsecret")
        // 64 lowercase hex chars
        #expect(sig.count == 64)
        #expect(sig == sig.lowercased())
        // Deterministic
        #expect(NotificationService.signature(payload: payload, secret: "topsecret") == sig)
        // Different secret -> different signature
        #expect(NotificationService.signature(payload: payload, secret: "other") != sig)
    }
}
