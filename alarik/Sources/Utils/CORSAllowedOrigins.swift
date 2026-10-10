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

/// Browser origins allowed to call Alarik on top of the console's own (`CONSOLE_BASE_URL`), read
/// from `CORS_ALLOWED_ORIGINS` as a comma-separated list.
///
/// Why: an app that uploads straight from the browser (e.g. with presigned URLs) is a second
/// origin next to the console, and with `CONSOLE_BASE_URL` as the only allowed origin one of the
/// two always gets its requests blocked. Setting `CONSOLE_BASE_URL` to the app instead breaks the
/// console (and points the OIDC redirect at the app).
///
/// The CORS middleware compares origins as exact strings, so an entry no browser would ever send
/// as an `Origin` header (a trailing slash, a path, no scheme, a default port) fails boot instead
/// of being accepted and then silently never matching. Entries are lowercased because browsers send the scheme and
/// host in lowercase.
enum CORSAllowedOrigins {
    static func resolve() throws -> [String] {
        try parse(Environment.sanitizedGet("CORS_ALLOWED_ORIGINS"))
    }

    static func parse(_ raw: String?) throws -> [String] {
        guard let raw else { return [] }
        return try raw.split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
            .map { entry in
                guard isOrigin(entry) else {
                    throw CORSAllowedOriginsError(
                        description:
                            "CORS_ALLOWED_ORIGINS entry \"\(entry)\" is not an origin - write it as scheme://host[:port], with no path, trailing slash or default port (browsers leave :80 and :443 out of the Origin header), e.g. https://app.example.com."
                    )
                }
                return entry.lowercased()
            }
    }

    private static func isOrigin(_ value: String) -> Bool {
        guard let components = URLComponents(string: value),
            let scheme = components.scheme?.lowercased(), scheme == "http" || scheme == "https",
            let host = components.host, !host.isEmpty, !host.contains("*")
        else {
            return false
        }
        // Browsers leave the default port out of `Origin`, so `https://host:443` never matches.
        if let port = components.port, port == (scheme == "https" ? 443 : 80) {
            return false
        }
        return components.path.isEmpty && components.query == nil && components.fragment == nil
            && components.user == nil && components.password == nil
    }
}

/// Thrown from `configure(app:)` on a `CORS_ALLOWED_ORIGINS` entry that can never match - see
/// `CORSAllowedOrigins`.
struct CORSAllowedOriginsError: Error, CustomStringConvertible {
    let description: String
}
