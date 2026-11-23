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
import Vapor

/// A short-lived, signed permission to stream one console download without the bearer token.
/// The console cannot put its token on an `<a href>` or an `<img src>`, so it used to fetch the
/// bytes itself and hand the browser a blob - the whole object in memory, then copied again.
/// With a ticket the browser streams straight from the API: native download progress, previews
/// that seek, nothing buffered in JavaScript. Stateless (an HMAC over the ticket), so any node
/// honours a ticket another node issued; ownership is checked again when it is redeemed.
struct DownloadTicket: Codable, Equatable {
    let bucket: String
    let keys: [String]
    let versionId: String?
    let userId: UUID
    /// Serve for display (`Content-Disposition: inline`) where the type is safe to render.
    let inline: Bool
    /// The file name the browser saves as, when not the key's own (a version download).
    let downloadName: String?
    let expiresAt: Date
}

enum DownloadTicketSigner {
    /// Long enough for a browser to start the download after the console asked for the ticket;
    /// the bytes themselves may take as long as they take once the request is in.
    static let lifetime: TimeInterval = 300

    struct Key: StorageKey {
        typealias Value = SymmetricKey
    }

    /// Derived from the JWT secret rather than being it, so neither key stands in for the other.
    static func key(fromSecret secret: String) -> SymmetricKey {
        SymmetricKey(data: SHA256.hash(data: Data(("alarik-download-ticket\u{0}" + secret).utf8)))
    }

    private static var encoder: JSONEncoder {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .millisecondsSince1970
        encoder.outputFormatting = [.sortedKeys]
        return encoder
    }

    private static var decoder: JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .millisecondsSince1970
        return decoder
    }

    static func issue(_ ticket: DownloadTicket, key: SymmetricKey) throws -> String {
        let payload = try encoder.encode(ticket)
        let signature = HMAC<SHA256>.authenticationCode(for: payload, using: key)
        return base64url(payload) + "." + base64url(Data(signature))
    }

    /// The ticket, or 403: a bad signature, an unreadable payload and an expired ticket all read
    /// the same to the client, which only needs to ask the console for a fresh one.
    static func verify(_ token: String, key: SymmetricKey, now: Date = Date()) throws -> DownloadTicket {
        guard let dot = token.lastIndex(of: "."),
            let payload = base64urlDecode(token[..<dot]),
            let signature = base64urlDecode(token[token.index(after: dot)...]),
            HMAC<SHA256>.isValidAuthenticationCode(signature, authenticating: payload, using: key),
            let ticket = try? decoder.decode(DownloadTicket.self, from: payload)
        else {
            throw Abort(.forbidden, reason: "Invalid download ticket")
        }
        guard ticket.expiresAt > now else {
            throw Abort(.forbidden, reason: "Download ticket expired")
        }
        return ticket
    }

    private static func base64url(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private static func base64urlDecode(_ text: Substring) -> Data? {
        var base64 = text.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
        while base64.count % 4 != 0 { base64 += "=" }
        return Data(base64Encoded: base64)
    }
}
