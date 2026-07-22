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
import NIOCore
import Vapor

/// Collapse consecutive whitespace to single space (replaces expensive regex)
@inline(__always)
private func collapseWhitespace(_ s: String) -> String {
    var result = ""
    result.reserveCapacity(s.count)
    var lastWasWhitespace = false
    for c in s {
        if c.isWhitespace {
            if !lastWasWhitespace {
                result.append(" ")
                lastWasWhitespace = true
            }
        } else {
            result.append(c)
            lastWasWhitespace = false
        }
    }
    return result
}

private func getRequestValue(for header: String, request: Request) throws -> String {
    if let h = request.headers.first(name: header) {
        return h
    } else if let q = request.query[String.self, at: header] {
        return q
    }
    throw S3Error(
        status: .badRequest, code: "InvalidArgument",
        message: "Missing value for signed header: \(header)")
}

private func getRequestValues(for header: String, request: Request) -> [String] {
    let hs = request.headers[header]
    if !hs.isEmpty {
        return hs
    } else if let q = request.query[String.self, at: header] {
        return [q]
    }
    return []
}

struct S3AuthInfo {
    let accessKey: String
    let signature: String
    let signedHeaders: [String]  // lowercased
    let date: String  // YYYYMMDD
    let region: String
    let service: String
    let algorithm: String
    let fullDate: String  // YYYYMMDDTHHMMSSZ
    let token: String?
    let expires: Int?  // nil for header auth
}

struct S3AuthParser {

    // Constants
    private static let supportedAlgorithm = "AWS4-HMAC-SHA256"
    private static let maxAuthHeaderLength = 4096
    private static let maxCredentialLength = 512
    private static let maxAccessKeyLength = 128
    private static let signatureLength = 64
    private static let dateLengthYYYYMMDD = 8
    private static let aws4RequestSuffix = "aws4_request"
    private static let maxPresignedURLExpiry = 604800  // 7 days in seconds

    static func parse(request: Request) throws -> S3AuthInfo {
        if let authHeader = request.headers.first(name: "authorization") {
            return try parseFromHeader(authHeader: authHeader, request: request)
        } else if request.query[String.self, at: "X-Amz-Algorithm"] != nil {
            return try parseFromQuery(request: request)
        } else {
            throw S3Error(
                status: .forbidden, code: "AccessDenied",
                message: "Access Denied")
        }
    }

    private static func parseFromHeader(authHeader: String, request: Request) throws -> S3AuthInfo {
        // Early validation: check header length to prevent excessive parsing
        guard authHeader.count < maxAuthHeaderLength else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Authorization header too long")
        }

        let components = authHeader.components(separatedBy: " ")
        guard components.count >= 2 else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Invalid Authorization header format")
        }
        let algorithm = components[0]
        guard algorithm == supportedAlgorithm else {
            // S3's code for a bad algorithm in the Authorization header
            throw S3Error(
                status: .badRequest, code: "AuthorizationHeaderMalformed",
                message: "Only \(supportedAlgorithm) is supported")
        }
        let parts = components[1...].joined(separator: " ").components(separatedBy: ",").map {
            $0.trimmingCharacters(in: .whitespaces)
        }
        var credential = ""
        var signedHeadersStr = ""
        var signature = ""
        for part in parts {
            if part.hasPrefix("Credential=") {
                credential = String(part.dropFirst("Credential=".count))
            } else if part.hasPrefix("SignedHeaders=") {
                signedHeadersStr = String(part.dropFirst("SignedHeaders=".count))
            } else if part.hasPrefix("Signature=") {
                signature = String(part.dropFirst("Signature=".count))
            }
        }
        guard !credential.isEmpty, !signedHeadersStr.isEmpty, !signature.isEmpty else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Missing required authorization components")
        }

        let credentialParts = credential.components(separatedBy: "/")
        guard credentialParts.count == 5, credentialParts[4] == aws4RequestSuffix else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid credential format")
        }

        // Validate access key format (alphanumeric, typical length 20)
        guard !credentialParts[0].isEmpty, credentialParts[0].count <= maxAccessKeyLength else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid access key format")
        }

        // Validate date format YYYYMMDD
        guard credentialParts[1].count == dateLengthYYYYMMDD,
            credentialParts[1].allSatisfy({ $0.isNumber })
        else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid date in credential")
        }

        let signedHeaders = signedHeadersStr.components(separatedBy: ";").map { $0.lowercased() }
        let fullDate = try getRequestValue(for: "x-amz-date", request: request)
        let token = try? getRequestValue(for: "x-amz-security-token", request: request)
        return S3AuthInfo(
            accessKey: credentialParts[0],
            signature: signature,
            signedHeaders: signedHeaders,
            date: credentialParts[1],
            region: credentialParts[2],
            service: credentialParts[3],
            algorithm: algorithm,
            fullDate: fullDate,
            token: token?.isEmpty == false ? token : nil,
            expires: nil
        )
    }

    private static func parseFromQuery(request: Request) throws -> S3AuthInfo {
        guard let algorithm = request.query[String.self, at: "X-Amz-Algorithm"],
            algorithm == supportedAlgorithm
        else {
            // S3's code for bad presigned-URL query parameters
            throw S3Error(
                status: .badRequest, code: "AuthorizationQueryParametersError",
                message: "Only \(supportedAlgorithm) is supported")
        }
        guard let credential = request.query[String.self, at: "X-Amz-Credential"] else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Missing X-Amz-Credential")
        }

        // Early validation: check credential length
        guard credential.count < maxCredentialLength else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Credential too long")
        }

        let credentialParts = credential.components(separatedBy: "/")
        guard credentialParts.count == 5, credentialParts[4] == aws4RequestSuffix else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid credential format")
        }

        // Validate access key format
        guard !credentialParts[0].isEmpty, credentialParts[0].count <= maxAccessKeyLength else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid access key format")
        }

        // Validate date format YYYYMMDD
        guard credentialParts[1].count == dateLengthYYYYMMDD,
            credentialParts[1].allSatisfy({ $0.isNumber })
        else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid date in credential")
        }

        guard let signedHeadersStr = request.query[String.self, at: "X-Amz-SignedHeaders"] else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Missing X-Amz-SignedHeaders"
            )
        }
        let signedHeaders = signedHeadersStr.components(separatedBy: ";").map { $0.lowercased() }
        guard let signature = request.query[String.self, at: "X-Amz-Signature"] else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Missing X-Amz-Signature")
        }

        guard let fullDate = request.query[String.self, at: "X-Amz-Date"] else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Missing X-Amz-Date")
        }
        guard let expiresStr = request.query[String.self, at: "X-Amz-Expires"],
            let expires = Int(expiresStr), expires > 0, expires <= maxPresignedURLExpiry
        else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Invalid or missing X-Amz-Expires")
        }
        let token = request.query[String.self, at: "X-Amz-Security-Token"]
        return S3AuthInfo(
            accessKey: credentialParts[0],
            signature: signature,
            signedHeaders: signedHeaders,
            date: credentialParts[1],
            region: credentialParts[2],
            service: credentialParts[3],
            algorithm: algorithm,
            fullDate: fullDate,
            token: token?.isEmpty == false ? token : nil,
            expires: expires
        )
    }
}

struct SigV4Validator {
    let secretKey: String

    private static let emptyPayloadHash =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    /// True when the request carries a body that Vapor hasn't buffered - i.e. the route was
    /// registered with `body: .stream` and the bytes will only arrive in the handler's own
    /// consumer. Detected via the declared length headers (both signed under SigV4, so a
    /// client can't forge "no body" for a request that has one without breaking its own
    /// signature).
    static func hasUnbufferedBody(_ request: Request) -> Bool {
        guard request.body.data == nil else { return false }
        if let lengthStr = request.headers.first(name: .contentLength),
            let length = Int(lengthStr), length > 0
        {
            return true
        }
        return request.headers.first(name: "x-amz-decoded-content-length") != nil
            || request.headers.first(name: .transferEncoding)?.lowercased()
                .contains("chunked") == true
    }

    static func authenticateRequest(for req: Request) async throws -> S3AuthInfo {
        let authInfo: S3AuthInfo = try S3AuthParser.parse(request: req)

        guard
            let credential = await AccessKeySecretKeyMapCache.shared.resolve(
                app: req.application, key: authInfo.accessKey)
        else {
            // S3's dedicated code for an unknown access key (403). Reached only after the
            // authoritative store has also been consulted - see `StoreBackedCache`. An EXPIRED
            // key lands here too: the cache refuses it on expiry rather than waiting for the
            // sweep that deletes the record, so a time-limited credential stops working on time.
            throw S3Error(
                status: .forbidden, code: "InvalidAccessKeyId",
                message: "The access key ID you provided does not exist in our records.")
        }

        let validator: SigV4Validator = SigV4Validator(secretKey: credential.secretKey)
        let isValid: Bool = try validator.validate(request: req, authInfo: authInfo)
        if !isValid {
            // S3's dedicated code for a failed signature check (403)
            throw S3Error(
                status: .forbidden, code: "SignatureDoesNotMatch",
                message:
                    "The request signature we calculated does not match the signature you provided. Check your key and signing method."
            )
        }

        return authInfo
    }

    func validate(request: Request, authInfo: S3AuthInfo) throws -> Bool {
        // Validate required signed headers
        guard authInfo.signedHeaders.contains("host") else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Host must be signed")
        }
        // Header auth only: for query auth (presigned URLs) the date travels as the
        // X-Amz-Date query parameter and SignedHeaders typically lists just "host" -
        // requiring a signed x-amz-date header would reject every standard presigned URL.
        if authInfo.expires == nil {
            guard authInfo.signedHeaders.contains("x-amz-date") else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument",
                    message: "x-amz-date must be signed")
            }
        }
        if authInfo.token != nil {
            guard authInfo.signedHeaders.contains("x-amz-security-token") else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument",
                    message: "x-amz-security-token must be signed if present")
            }
        }
        // Validate date format and match
        guard authInfo.fullDate.count == 16, authInfo.fullDate.hasPrefix(authInfo.date),
            authInfo.fullDate.hasSuffix("Z")
        else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid date format")
        }

        // Region check: the credential scope's region must match this deployment's configured
        // region (`ALARIK_REGION`, default "us-east-1"). Verified against S3 behavior: a
        // region mismatch is a 400 (a malformed request), not a 403 - distinct codes for header
        // vs. query auth, matching AWS's own `AuthorizationHeaderMalformed` /
        // `AuthorizationQueryParametersError` responses.
        let expectedRegion = AlarikRegion.resolve()
        guard authInfo.region == expectedRegion else {
            if authInfo.expires == nil {
                throw S3Error(
                    status: .badRequest, code: "AuthorizationHeaderMalformed",
                    message:
                        "The authorization header is malformed; the region '\(authInfo.region)' is wrong; expecting '\(expectedRegion)'."
                )
            } else {
                throw S3Error(
                    status: .badRequest, code: "AuthorizationQueryParametersError",
                    message:
                        "Error parsing the X-Amz-Credential parameter; the region '\(authInfo.region)' is wrong; expecting '\(expectedRegion)'."
                )
            }
        }

        // Time skew check
        guard let requestDate = authInfo.fullDate.toAWSDate() else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid x-amz-date format")
        }

        // Signed age: positive once the signed instant has passed, negative while it is still
        // in the future. Kept directional - `abs` would make a URL valid for `expires` seconds on
        // BOTH sides of its signed time, so one dated a week ahead would already work today.
        let age = Date().timeIntervalSince(requestDate)
        let maxClockSkew: TimeInterval = 15 * 60

        // Query auth is bounded by its own X-Amz-Expires window (like S3, where a
        // presigned URL can be valid for up to 7 days); the 15-minute skew rule applies
        // only to header auth - imposing it on presigned URLs would silently cap every
        // URL's usable lifetime at 15 minutes regardless of the requested expiry.
        if authInfo.expires == nil {
            guard abs(age) < maxClockSkew else {
                throw S3Error(
                    status: .forbidden, code: "RequestTimeTooSkewed",
                    message: "Request time skew exceeds 15 minutes")
            }
        }

        // Expires check for query auth: valid from the signed instant until it elapses. The
        // negative allowance is clock skew between signer and server, not a usable head start.
        if let expires = authInfo.expires {
            guard age > -maxClockSkew, age < Double(expires) else {
                throw S3Error(
                    status: .forbidden, code: "AccessDenied", message: "Request has expired")
            }
        }

        // Derive signing key once (4 HMAC operations) - reused for all validation attempts
        let kSigning = try deriveSigningKey(
            date: authInfo.date, region: authInfo.region, service: authInfo.service)

        // Resolved and verified once, outside the combination loop below. It depends on neither
        // `sorted` nor `emptyValueEquals`, and verifying it inside meant a buffered request whose
        // signature matched on the last combination hashed its entire body four times.
        let payloadHash = try resolveAndVerifyPayloadHash(
            request: request, isQueryAuth: authInfo.expires != nil)

        // Try multiple combinations to handle different client implementations:
        // - sorted=true: AWS spec compliant query param sorting
        // - sorted=false: Soto-AWS compatibility (preserves original order)
        // - emptyValueEquals=true: AWS CLI style (?versioning=)
        // - emptyValueEquals=false: Soto style (?versioning)
        for sorted in [true, false] {
            for emptyValueEquals in [true, false] {
                if try validateWithQuerySort(
                    request: request, authInfo: authInfo, sorted: sorted,
                    emptyValueEquals: emptyValueEquals, signingKey: kSigning,
                    payloadHash: payloadHash
                ) {
                    return true
                }
            }
        }

        return false
    }

    private func deriveSigningKey(date: String, region: String, service: String) throws -> Data {
        let kSecret = Data(secretKey.utf8)
        var kSecretWithPrefix = Data(capacity: kSecret.count + 4)
        kSecretWithPrefix.append(contentsOf: "AWS4".utf8)
        kSecretWithPrefix.append(kSecret)

        let kDate = try hmacSHA256(key: kSecretWithPrefix, data: Data(date.utf8))
        let kRegion = try hmacSHA256(key: kDate, data: Data(region.utf8))
        let kService = try hmacSHA256(key: kRegion, data: Data(service.utf8))
        return try hmacSHA256(key: kService, data: Data("aws4_request".utf8))
    }

    private func validateWithQuerySort(
        request: Request,
        authInfo: S3AuthInfo,
        sorted: Bool,
        emptyValueEquals: Bool,
        signingKey: Data,
        payloadHash: String
    ) throws -> Bool {
        // Create canonical request
        let canonicalRequest = try createCanonicalRequest(
            request: request,
            signedHeaders: authInfo.signedHeaders,
            isQueryAuth: authInfo.expires != nil,
            sortQueryParams: sorted,
            emptyValueEquals: emptyValueEquals,
            payloadHash: payloadHash
        )

        let stringToSign = createStringToSign(
            algorithm: authInfo.algorithm,
            fullDate: authInfo.fullDate,
            date: authInfo.date,
            region: authInfo.region,
            service: authInfo.service,
            canonicalRequest: canonicalRequest
        )

        // Calculate signature using pre-derived signing key
        let calculatedSignatureData = try hmacSHA256(key: signingKey, data: Data(stringToSign.utf8))

        // For streaming/chunked, this is the seed signature; validate chunks separately
        let isStreaming = canonicalRequest.hasSuffix("\nSTREAMING-AWS4-HMAC-SHA256-PAYLOAD")
        if isStreaming {
            // Use constant-time comparison to prevent timing attacks
            if calculatedSignatureData.constantTimeCompare(to: authInfo.signature) {
                try validateChunked(request: request, authInfo: authInfo, signingKey: signingKey)
                return true
            }
        } else {
            // Use constant-time comparison to prevent timing attacks
            if calculatedSignatureData.constantTimeCompare(to: authInfo.signature) {
                return true
            }
        }

        return false
    }

    /// The payload hash that goes into the canonical request, verified against the actual body
    /// where one is present and already buffered. A streaming route defers the body check to its
    /// consumer (`StreamingBodySpooler`), which hashes the real bytes - deferral, not a skip.
    private func resolveAndVerifyPayloadHash(request: Request, isQueryAuth: Bool) throws -> String {
        guard !isQueryAuth else { return "UNSIGNED-PAYLOAD" }

        let payloadHash = try getRequestValue(for: "x-amz-content-sha256", request: request)
        guard !payloadHash.isEmpty else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Missing required x-amz-content-sha256 header")
        }
        guard payloadHash != "UNSIGNED-PAYLOAD",
            payloadHash != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
        else { return payloadHash }

        if let bodyBuffer = request.body.data {
            let computedHash = Crypto.SHA256.hash(data: bodyBuffer.readableBytesView).hexString()
            guard computedHash == payloadHash else {
                throw S3Error(
                    status: .badRequest, code: "InvalidDigest", message: "Payload hash mismatch")
            }
        } else if !Self.hasUnbufferedBody(request) {
            // No body at all: must match the empty payload hash.
            guard payloadHash == Self.emptyPayloadHash else {
                throw S3Error(
                    status: .badRequest, code: "InvalidDigest",
                    message: "Payload hash mismatch for empty body")
            }
        }
        return payloadHash
    }

    private func createCanonicalRequest(
        request: Request,
        signedHeaders: [String],
        isQueryAuth: Bool,
        sortQueryParams: Bool,
        emptyValueEquals: Bool,
        payloadHash: String
    ) throws -> String {
        let method = request.method.rawValue
        // Canonical URI
        let path = request.url.path.isEmpty ? "/" : request.url.path

        // Canonical query string
        var queryString = ""

        // Get raw query string
        let rawQueryString: String? = {
            let urlStr = request.url.string
            if let qIndex = urlStr.firstIndex(of: "?") {
                let afterQ = urlStr.index(after: qIndex)
                return String(urlStr[afterQ...])
            }
            return nil
        }()

        if let rawQuery = rawQueryString, !rawQuery.isEmpty {

            let params = rawQuery.split(separator: "&")
            var queryItems: [(key: String, value: String, hadEquals: Bool)] = []

            for param in params where !param.isEmpty {
                if let eqIndex = param.firstIndex(of: "=") {
                    let key = String(param[..<eqIndex])
                    let value = String(param[param.index(after: eqIndex)...])

                    // Skip X-Amz-Signature for query auth (presigned URLs)
                    if isQueryAuth && key == "X-Amz-Signature" {
                        continue
                    }

                    // Keep track that this parameter had an = sign
                    // AWS SigV4 requires keeping the = even for empty values
                    queryItems.append((key, value, true))
                } else {
                    // No = sign means parameter without value (e.g., ?versioning)
                    queryItems.append((String(param), "", false))
                }
            }

            if sortQueryParams {
                // AWS SigV4 spec: sort by parameter name, then by value
                queryItems.sort { lhs, rhs in
                    if lhs.key == rhs.key {
                        return lhs.value < rhs.value
                    } else {
                        return lhs.key < rhs.key
                    }
                }
            }

            // Build canonical query string
            // Handle different client implementations:
            // - AWS CLI sends ?versioning= (with equals)
            // - Soto sends ?versioning (without equals)
            queryString = queryItems.map { item in
                if item.value.isEmpty {
                    // Empty value - use emptyValueEquals flag to determine format
                    return emptyValueEquals ? "\(item.key)=" : item.key
                } else {
                    return "\(item.key)=\(item.value)"
                }
            }.joined(separator: "&")
        }

        // Canonical headers - build a lookup dict to avoid repeated O(n) header scans
        let sortedHeaders = signedHeaders.sorted()
        var canonicalHeaders = ""
        canonicalHeaders.reserveCapacity(sortedHeaders.count * 64)  // Estimate

        // Build header lookup once (O(n)) instead of O(n) per lookup
        var headerLookup: [String: [String]] = [:]
        headerLookup.reserveCapacity(sortedHeaders.count)
        for (name, value) in request.headers {
            headerLookup[name.lowercased(), default: []].append(value)
        }

        // Build a case-insensitive query param lookup too, for headers that are only signed
        // via the query string (e.g. presigned URLs signing "x-amz-date", which exists only as
        // ?X-Amz-Date=... - AWS query param names keep their spec casing, unlike header names).
        var queryLookup: [String: String] = [:]
        if let rawQuery = rawQueryString {
            for param in rawQuery.split(separator: "&") where !param.isEmpty {
                guard let eqIndex = param.firstIndex(of: "=") else { continue }
                let key = String(param[..<eqIndex]).lowercased()
                let value = String(param[param.index(after: eqIndex)...])
                queryLookup[key] = value.removingPercentEncoding ?? value
            }
        }

        for header in sortedHeaders {
            // O(1) lookups instead of O(n) case-insensitive scans
            let values =
                headerLookup[header]
                ?? queryLookup[header].map { [$0] }
                ?? []
            let processed = values.map { value -> String in
                let trimmed = value.trimmingCharacters(in: .whitespaces)
                return collapseWhitespace(trimmed)
            }
            let joined = processed.joined(separator: ",")
            canonicalHeaders += "\(header):\(joined)\n"
        }

        // Signed headers - cache the result
        let signedHeadersString = sortedHeaders.joined(separator: ";")

        return method + "\n" + path + "\n" + queryString + "\n" + canonicalHeaders + "\n"
            + signedHeadersString + "\n" + payloadHash
    }

    private func createStringToSign(
        algorithm: String,
        fullDate: String,
        date: String,
        region: String,
        service: String,
        canonicalRequest: String
    ) -> String {
        let credentialScope = "\(date)/\(region)/\(service)/aws4_request"
        let hashedCanonicalRequest = Crypto.SHA256.hash(data: Data(canonicalRequest.utf8))
            .hexString()
        return """
            \(algorithm)
            \(fullDate)
            \(credentialScope)
            \(hashedCanonicalRequest)
            """
    }

    private func validateChunked(request: Request, authInfo: S3AuthInfo, signingKey: Data) throws {
        let decodedLengthStr = try getRequestValue(
            for: "x-amz-decoded-content-length", request: request)
        guard let decodedLength = Int(decodedLengthStr) else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Missing or invalid x-amz-decoded-content-length")
        }
        // Deliberately NOT requiring `Content-Encoding: aws-chunked` here: AWS's docs show it,
        // but S3 accepts streaming payloads without it
        guard let buffer = request.body.data else {
            if Self.hasUnbufferedBody(request) {
                // Streaming route: the seed signature is already verified at this point; the
                // per-chunk signature chain is verified incrementally by the body consumer
                // (StreamingChunkDecoder seeded via `SigV4Validator.chunkSignatureValidator`).
                return
            }
            throw S3Error(
                status: .internalServerError, code: "InternalError", message: "Body not buffered")
        }
        let view = buffer.readableBytesView
        var pos = view.startIndex
        var previousSig = authInfo.signature
        let emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        var totalLength = 0
        while pos < view.endIndex {
            guard let crlfPos = view[pos...].firstIndex(where: { $0 == 13 }),
                view.index(after: crlfPos) < view.endIndex,
                view[view.index(after: crlfPos)] == 10
            else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument", message: "Invalid chunk format")
            }
            let sizeLineView = view[pos..<crlfPos]
            guard let sizeLine = String(bytes: sizeLineView, encoding: .utf8) else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument", message: "Invalid chunk size line"
                )
            }
            let sizeParts = sizeLine.components(separatedBy: ";")
            guard sizeParts.count == 2, sizeParts[1].hasPrefix("chunk-signature=") else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument", message: "Missing chunk-signature"
                )
            }
            let chunkSig = String(sizeParts[1].dropFirst(16))
            guard let chunkSize = Int(sizeParts[0], radix: 16) else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument", message: "Invalid chunk size hex")
            }
            totalLength += chunkSize
            let dataStart = view.index(crlfPos, offsetBy: 2)
            guard
                let dataEnd = view.index(dataStart, offsetBy: chunkSize, limitedBy: view.endIndex),
                dataEnd == view.index(dataStart, offsetBy: chunkSize)
            else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument", message: "Chunk data overflow")
            }
            let chunkData = view[dataStart..<dataEnd]
            let trailingCrlfStart = dataEnd
            guard trailingCrlfStart < view.endIndex, view[trailingCrlfStart] == 13,
                let trailingCrlfEnd = view.index(
                    trailingCrlfStart, offsetBy: 1, limitedBy: view.endIndex),
                view[trailingCrlfEnd] == 10
            else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument", message: "Missing trailing CRLF")
            }
            pos = view.index(after: trailingCrlfEnd)
            let chunkSts = """
                AWS4-HMAC-SHA256-PAYLOAD
                \(authInfo.fullDate)
                \(authInfo.date)/\(authInfo.region)/\(authInfo.service)/aws4_request
                \(previousSig)
                \(emptyHash)
                \(Crypto.SHA256.hash(data: chunkData).hexString())
                """
            let computedChunkSigData = try hmacSHA256(key: signingKey, data: Data(chunkSts.utf8))
            // Use constant-time comparison to prevent timing attacks
            guard computedChunkSigData.constantTimeCompare(to: chunkSig) else {
                throw S3Error(
                    status: .forbidden, code: "SignatureDoesNotMatch",
                    message: "Chunk signature mismatch")
            }
            previousSig = computedChunkSigData.hexString()
            if chunkSize == 0 {
                break
            }
        }
        guard totalLength == decodedLength else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Decoded content length mismatch")
        }
    }

    private func hmacSHA256(key: Data, data: Data) throws -> Data {
        let symmetricKey = SymmetricKey(data: key)
        return Data(HMAC<SHA256>.authenticationCode(for: data, using: symmetricKey))
    }

    /// Builds the per-chunk signature verifier a streaming body consumer needs to validate a
    /// STREAMING-AWS4-HMAC-SHA256-PAYLOAD upload chunk-by-chunk (the deferred counterpart of
    /// `validateChunked`, which handles the buffered case). The seed signature in `authInfo`
    /// has already been verified against the request headers by `authenticateRequest`.
    static func chunkSignatureValidator(for authInfo: S3AuthInfo) async throws
        -> ChunkSignatureValidator
    {
        // Cache-only is correct here, unlike `authenticateRequest`: this only runs once that has
        // already authenticated the same key, and doing so warms the cache via
        // `StoreBackedCache.resolve`. A miss here means the key was revoked mid-upload, which
        // should fail.
        guard
            let secretKey = await AccessKeySecretKeyMapCache.shared.secretKey(
                for: authInfo.accessKey)
        else {
            throw S3Error(
                status: .forbidden, code: "InvalidAccessKeyId",
                message: "The access key ID you provided does not exist in our records.")
        }
        let validator = SigV4Validator(secretKey: secretKey)
        let signingKey = try validator.deriveSigningKey(
            date: authInfo.date, region: authInfo.region, service: authInfo.service)
        return ChunkSignatureValidator(
            signingKey: signingKey,
            fullDate: authInfo.fullDate,
            credentialScope:
                "\(authInfo.date)/\(authInfo.region)/\(authInfo.service)/aws4_request",
            seedSignature: authInfo.signature
        )
    }
}

/// Verifies the SigV4 chunk-signature chain of a streaming upload one chunk at a time.
/// Each chunk's signature covers the previous signature (seeded from the verified request
/// signature) and the SHA256 of that chunk's payload, so chunks can't be reordered, dropped,
/// or tampered with without breaking the chain.
struct ChunkSignatureValidator {
    private let signingKey: SymmetricKey
    private let fullDate: String
    private let credentialScope: String
    private var previousSignature: String

    private static let emptyHash =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    init(signingKey: Data, fullDate: String, credentialScope: String, seedSignature: String) {
        self.signingKey = SymmetricKey(data: signingKey)
        self.fullDate = fullDate
        self.credentialScope = credentialScope
        self.previousSignature = seedSignature
    }

    /// Verifies one chunk given the hex SHA256 of its (decoded) payload and the signature
    /// declared in its size line. Throws SignatureDoesNotMatch on any break in the chain.
    mutating func verify(chunkPayloadHashHex: String, declaredSignature: String) throws {
        let stringToSign = """
            AWS4-HMAC-SHA256-PAYLOAD
            \(fullDate)
            \(credentialScope)
            \(previousSignature)
            \(Self.emptyHash)
            \(chunkPayloadHashHex)
            """
        let computed = Data(
            HMAC<SHA256>.authenticationCode(for: Data(stringToSign.utf8), using: signingKey))
        guard computed.constantTimeCompare(to: declaredSignature) else {
            throw S3Error(
                status: .forbidden, code: "SignatureDoesNotMatch",
                message: "Chunk signature mismatch")
        }
        previousSignature = computed.hexString()
    }
}

