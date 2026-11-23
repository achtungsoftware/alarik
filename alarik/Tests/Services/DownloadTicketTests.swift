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
import Foundation
import Testing
import Vapor

@testable import Alarik

@Suite("Download tickets")
struct DownloadTicketTests {
    private let key = DownloadTicketSigner.key(fromSecret: "test-secret-at-least-32-bytes-long!")

    private func ticket(expiresIn seconds: TimeInterval = 60) -> DownloadTicket {
        DownloadTicket(
            bucket: "b", keys: ["docs/a.txt", "docs/b.txt"], versionId: nil, userId: UUID(), inline: true,
            downloadName: "renamed.txt", expiresAt: Date().addingTimeInterval(seconds))
    }

    @Test("a ticket round-trips and keeps every field")
    func roundTrip() throws {
        let issued = ticket()
        let token = try DownloadTicketSigner.issue(issued, key: key)
        let verified = try DownloadTicketSigner.verify(token, key: key)
        #expect(verified.bucket == issued.bucket && verified.keys == issued.keys)
        #expect(verified.userId == issued.userId && verified.inline && verified.downloadName == "renamed.txt")
        #expect(abs(verified.expiresAt.timeIntervalSince(issued.expiresAt)) < 0.001)
        // URL-safe: no characters a query string would mangle.
        #expect(token.allSatisfy { $0.isLetter || $0.isNumber || "-_.".contains($0) })
    }

    @Test("a tampered, foreign-keyed or expired ticket is refused as forbidden")
    func refusals() throws {
        let token = try DownloadTicketSigner.issue(ticket(), key: key)
        let parts = token.split(separator: ".")
        let tampered = String(parts[0]) + "x." + String(parts[1])
        for bad in [tampered, "garbage", "", String(parts[0])] {
            #expect(throws: Abort.self) { try DownloadTicketSigner.verify(bad, key: key) }
        }
        let otherKey = DownloadTicketSigner.key(fromSecret: "another-secret-that-is-32-bytes-long!")
        #expect(throws: Abort.self) { try DownloadTicketSigner.verify(token, key: otherKey) }
        let expired = try DownloadTicketSigner.issue(ticket(expiresIn: -1), key: key)
        #expect(throws: Abort.self) { try DownloadTicketSigner.verify(expired, key: key) }
    }
}
