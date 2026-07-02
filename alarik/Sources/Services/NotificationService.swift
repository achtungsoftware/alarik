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

import Crypto
import Fluent
import Foundation
import Vapor

/// Every event type Alarik emits, in full S3 form (the `s3:` prefix is stripped in the
/// delivered payload's `eventName`, matching AWS's message structure).
enum S3EventType: String, Sendable {
    case objectCreatedPut = "s3:ObjectCreated:Put"
    case objectCreatedCopy = "s3:ObjectCreated:Copy"
    case objectCreatedCompleteMultipartUpload = "s3:ObjectCreated:CompleteMultipartUpload"
    case objectRemovedDelete = "s3:ObjectRemoved:Delete"
    case objectRemovedDeleteMarkerCreated = "s3:ObjectRemoved:DeleteMarkerCreated"
    case lifecycleExpirationDelete = "s3:LifecycleExpiration:Delete"
    case lifecycleExpirationDeleteMarkerCreated = "s3:LifecycleExpiration:DeleteMarkerCreated"
}

/// Builds AWS-compatible event payloads and enqueues them into the persistent outbox
/// (`notification_deliveries`). Delivery itself happens asynchronously in
/// `NotificationDispatcher` - the request path only ever pays for a cache lookup, and,
/// when rules match, one SQLite insert per matching rule.
struct NotificationService {

    // MARK: - AWS event message structure (v2.4)

    private struct EventPayload: Encodable {
        let Records: [Record]
    }

    private struct Record: Encodable {
        let eventVersion = "2.4"
        let eventSource = "alarik:s3"
        let awsRegion = "us-east-1"
        let eventTime: String
        let eventName: String
        let userIdentity: PrincipalIdentity
        let requestParameters: RequestParameters
        let responseElements: ResponseElements
        let s3: S3Entity
    }

    private struct PrincipalIdentity: Encodable {
        let principalId: String
    }

    private struct RequestParameters: Encodable {
        let sourceIPAddress: String
    }

    private struct ResponseElements: Encodable {
        let xAmzRequestId: String
        let xAmzId2: String

        enum CodingKeys: String, CodingKey {
            case xAmzRequestId = "x-amz-request-id"
            case xAmzId2 = "x-amz-id-2"
        }
    }

    private struct S3Entity: Encodable {
        let s3SchemaVersion = "1.0"
        let configurationId: String
        let bucket: BucketEntity
        let object: ObjectEntity
    }

    private struct BucketEntity: Encodable {
        let name: String
        let ownerIdentity: PrincipalIdentity
        let arn: String
    }

    private struct ObjectEntity: Encodable {
        let key: String
        let size: Int?
        let eTag: String?
        let versionId: String?
        let sequencer: String
    }

    /// The `s3:TestEvent` message AWS sends when a notification configuration is tested -
    /// deliberately a different, flat shape (documented AWS behavior).
    private struct TestEventPayload: Encodable {
        let Service = "Alarik S3"
        let Event = "s3:TestEvent"
        let Time: String
        let Bucket: String
        let RequestId: String
        let HostId: String
    }

    // MARK: - Emit

    /// Enqueues an event for every matching rule of the bucket's notification configuration.
    /// Returns immediately when the bucket has none (the common case) - one actor lookup,
    /// no database access. Never throws: a notification enqueue failure must not fail the
    /// object operation that triggered it (the write/delete already happened).
    static func emit(
        event: S3EventType,
        bucketName: String,
        key: String,
        size: Int?,
        etag: String?,
        versionId: String?,
        requestId: String,
        sourceIP: String?,
        on db: any Database
    ) async {
        guard let config = await NotificationConfigCache.shared.config(for: bucketName) else {
            return
        }

        let matching = config.rules.filter { $0.matches(eventName: event.rawValue, key: key) }
        guard !matching.isEmpty else { return }

        for rule in matching {
            let payload = buildPayload(
                event: event, bucketName: bucketName, key: key, size: size, etag: etag,
                versionId: versionId, requestId: requestId, sourceIP: sourceIP,
                configurationId: rule.id.uuidString)

            let delivery = NotificationDelivery(
                bucketName: bucketName,
                ruleId: rule.id,
                url: rule.url,
                secret: rule.secret,
                payload: payload
            )
            do {
                try await delivery.save(on: db)
            } catch {
                db.logger.error(
                    "Failed to enqueue webhook delivery for bucket '\(bucketName)': \(error)")
            }
        }

        NotificationDispatcher.shared.wake()
    }

    /// Enqueues the `s3:TestEvent` message for one specific rule (console "Send test").
    static func emitTestEvent(
        rule: NotificationRule,
        bucketName: String,
        requestId: String,
        on db: any Database
    ) async throws {
        let payload = TestEventPayload(
            Time: Date().iso8601String,
            Bucket: bucketName,
            RequestId: requestId,
            HostId: requestId + "-" + String(Int.random(in: 1000...9999))
        )
        let body = String(decoding: try Self.encoder.encode(payload), as: UTF8.self)

        let delivery = NotificationDelivery(
            bucketName: bucketName,
            ruleId: rule.id,
            url: rule.url,
            secret: rule.secret,
            payload: body
        )
        try await delivery.save(on: db)
        NotificationDispatcher.shared.wake()
    }

    // MARK: - Payload building

    private static var encoder: JSONEncoder {
        let encoder = JSONEncoder()
        // Deterministic output: the HMAC signature covers these exact bytes
        encoder.outputFormatting = [.sortedKeys]
        return encoder
    }

    static func buildPayload(
        event: S3EventType,
        bucketName: String,
        key: String,
        size: Int?,
        etag: String?,
        versionId: String?,
        requestId: String,
        sourceIP: String?,
        configurationId: String
    ) -> String {
        // AWS URL-encodes the object key in the payload (space becomes "+")
        let encodedKey =
            key.addingPercentEncoding(withAllowedCharacters: .urlQueryKeyAllowed)?
                .replacingOccurrences(of: "%20", with: "+") ?? key

        let record = Record(
            eventTime: Date().iso8601String,
            // The payload's eventName drops the "s3:" prefix, per AWS's message structure
            eventName: String(event.rawValue.dropFirst(3)),
            userIdentity: PrincipalIdentity(principalId: "alarik"),
            requestParameters: RequestParameters(sourceIPAddress: sourceIP ?? ""),
            responseElements: ResponseElements(
                xAmzRequestId: requestId,
                xAmzId2: requestId + "-" + String(Int.random(in: 1000...9999))
            ),
            s3: S3Entity(
                configurationId: configurationId,
                bucket: BucketEntity(
                    name: bucketName,
                    ownerIdentity: PrincipalIdentity(principalId: "alarik"),
                    arn: "arn:aws:s3:::\(bucketName)"
                ),
                object: ObjectEntity(
                    key: encodedKey,
                    size: size,
                    eTag: etag,
                    versionId: versionId,
                    // Hex wall-clock nanoseconds: bigger value == later event, which is all
                    // AWS promises for sequencer comparison on the same key
                    sequencer: String(
                        UInt64(Date().timeIntervalSince1970 * 1_000_000_000), radix: 16,
                        uppercase: true)
                )
            )
        )

        guard let data = try? encoder.encode(EventPayload(Records: [record])),
            let json = String(data: data, encoding: .utf8)
        else {
            return #"{"Records":[]}"#
        }
        return json
    }

    /// Hex HMAC-SHA256 of the payload with the rule's secret - GitHub-webhook style, sent as
    /// `X-Alarik-Signature-256` so receivers can verify authenticity and integrity.
    static func signature(payload: String, secret: String) -> String {
        let mac = HMAC<SHA256>.authenticationCode(
            for: Data(payload.utf8), using: SymmetricKey(data: Data(secret.utf8)))
        return Data(mac).hexString()
    }
}

extension CharacterSet {
    /// Characters AWS leaves unencoded in event payload object keys
    fileprivate static let urlQueryKeyAllowed: CharacterSet = {
        var set = CharacterSet.alphanumerics
        set.insert(charactersIn: "-_.!*'()/")
        return set
    }()
}
