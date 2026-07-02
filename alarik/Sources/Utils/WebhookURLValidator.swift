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
import Vapor

/// Validates webhook destination URLs and classifies whether they target an internal/private
/// address. Since the server itself makes the outbound POST, an unrestricted webhook URL is an
/// SSRF vector - so private/loopback/link-local/metadata targets are gated to admin owners
/// (a regular user on a shared instance can only point webhooks at public hosts, while a
/// single-admin homelab keeps full LAN access). A literal-host string check, consistent with
/// the codebase's existing OIDC SSRF guard; not full DNS-rebinding protection.
enum WebhookURLValidator {

    /// Rejects a structurally invalid URL (bad scheme, empty host). Applies to all callers
    /// regardless of privilege.
    static func validateStructure(_ urlString: String) throws {
        guard let url = URL(string: urlString),
            let scheme = url.scheme?.lowercased(),
            scheme == "http" || scheme == "https",
            let host = url.host, !host.isEmpty
        else {
            throw Abort(
                .badRequest,
                reason: "Webhook URL must be a valid http(s) URL with a host.")
        }
    }

    /// Whether the URL's host is a private, loopback, link-local, or cloud-metadata address -
    /// the set that only admins may target.
    static func isInternalHost(_ urlString: String) -> Bool {
        guard let host = URL(string: urlString)?.host?.lowercased() else {
            // Unparseable hosts are treated as internal (fail closed): structure validation
            // rejects them first anyway, this is just belt-and-suspenders.
            return true
        }

        // Strip IPv6 brackets if present
        let bare = host.hasPrefix("[") && host.hasSuffix("]") ? String(host.dropFirst().dropLast()) : host

        // Loopback / localhost by name
        if bare == "localhost" || bare.hasSuffix(".localhost") { return true }

        // IPv6 checks only apply to actual IPv6 literals (which always contain a colon) -
        // guarding on that avoids misclassifying a public hostname like "fcdn.example.com"
        // or "fd-images.net" as an fc00::/7 ULA address.
        if bare.contains(":") {
            // loopback ::1, link-local fe80::/10, unique-local fc00::/7 (fc.. / fd..)
            if bare == "::1" || bare.hasPrefix("fe80:") || bare.hasPrefix("fc") || bare.hasPrefix("fd") {
                return true
            }
        }

        // IPv4: only classify when it actually parses as four octets
        let octets = bare.split(separator: ".", omittingEmptySubsequences: false)
        if octets.count == 4, let parts = try? octets.map({ part -> Int in
            guard let n = Int(part), (0...255).contains(n) else { throw ParseError.notAnOctet }
            return n
        }) {
            let (a, b) = (parts[0], parts[1])
            if a == 127 { return true }  // loopback 127.0.0.0/8
            if a == 10 { return true }  // private 10.0.0.0/8
            if a == 172, (16...31).contains(b) { return true }  // private 172.16.0.0/12
            if a == 192, b == 168 { return true }  // private 192.168.0.0/16
            if a == 169, b == 254 { return true }  // link-local / metadata 169.254.0.0/16
            if a == 0 { return true }  // 0.0.0.0/8 "this host"
        }

        return false
    }

    private enum ParseError: Error { case notAnOctet }
}
