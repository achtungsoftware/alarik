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
fileprivate func collapseWhitespace(_ s: String) -> String {
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

fileprivate func getRequestValue(for header: String, request: Request) throws -> String {
    if let h = request.headers.first(name: header) {
        return h
    } else if let q = request.query[String.self, at: header] {
        return q
    }
    throw S3Error(
        status: .badRequest, code: "InvalidArgument",
        message: "Missing value for signed header: \(header)")
}

fileprivate func getRequestValues(for header: String, request: Request) -> [String] {
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
                status: .badRequest, code: "MissingAuthentication",
                message: "No authentication provided")
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
            throw S3Error(
                status: .badRequest, code: "UnsupportedAlgorithm",
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
            throw S3Error(
                status: .badRequest, code: "UnsupportedAlgorithm",
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

    static func authenticateRequest(for req: Request) async throws -> S3AuthInfo {
        let authInfo: S3AuthInfo = try S3AuthParser.parse(request: req)

        guard
            let secretKey = await AccessKeySecretKeyMapCache.shared.secretKey(
                for: authInfo.accessKey)
        else {
            throw S3Error(
                status: .forbidden, code: "AccessDenied", message: "Error finding secret key")
        }

        let validator: SigV4Validator = SigV4Validator(secretKey: secretKey)
        let isValid: Bool = try validator.validate(request: req, authInfo: authInfo)
        if !isValid {
            throw S3Error(status: .forbidden, code: "AccessDenied", message: "Error validating")
        }

        return authInfo
    }

    func validate(request: Request, authInfo: S3AuthInfo) throws -> Bool {
        // Validate required signed headers
        guard authInfo.signedHeaders.contains("host") else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Host must be signed")
        }
        guard authInfo.signedHeaders.contains("x-amz-date") else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "x-amz-date must be signed")
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

        // Time skew check
        guard let requestDate = authInfo.fullDate.toAWSDate() else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument", message: "Invalid x-amz-date format")
        }

        let skew = abs(Date().timeIntervalSince(requestDate))
        guard skew < 15 * 60 else {
            throw S3Error(
                status: .badRequest, code: "RequestTimeTooSkewed",
                message: "Request time skew exceeds 15 minutes")
        }

        // Expires check for query auth
        if let expires = authInfo.expires {
            guard skew < Double(expires) else {
                throw S3Error(
                    status: .badRequest, code: "AccessDenied", message: "Presigned URL has expired")
            }
        }

        // Derive signing key once (4 HMAC operations) - reused for both validation attempts
        let kSigning = try deriveSigningKey(authInfo: authInfo)

        // Try validation with sorted query params (AWS spec compliant clients like Cyberduck)
        if try validateWithQuerySort(
            request: request, authInfo: authInfo, sorted: true, signingKey: kSigning
        ) {
            return true
        }

        // Try validation with unsorted query params (Soto-AWS bug compatibility)
        if try validateWithQuerySort(
            request: request, authInfo: authInfo, sorted: false, signingKey: kSigning
        ) {
            return true
        }

        return false
    }

    private func deriveSigningKey(authInfo: S3AuthInfo) throws -> Data {
        let kSecret = Data(secretKey.utf8)
        var kSecretWithPrefix = Data(capacity: kSecret.count + 4)
        kSecretWithPrefix.append(contentsOf: "AWS4".utf8)
        kSecretWithPrefix.append(kSecret)

        let kDate = try hmacSHA256(key: kSecretWithPrefix, data: Data(authInfo.date.utf8))
        let kRegion = try hmacSHA256(key: kDate, data: Data(authInfo.region.utf8))
        let kService = try hmacSHA256(key: kRegion, data: Data(authInfo.service.utf8))
        return try hmacSHA256(key: kService, data: Data("aws4_request".utf8))
    }

    private func validateWithQuerySort(
        request: Request,
        authInfo: S3AuthInfo,
        sorted: Bool,
        signingKey: Data
    ) throws -> Bool {
        // Create canonical request
        let canonicalRequest = try createCanonicalRequest(
            request: request,
            signedHeaders: authInfo.signedHeaders,
            isQueryAuth: authInfo.expires != nil,
            sortQueryParams: sorted
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
        let isStreaming = canonicalRequest.contains("\nSTREAMING-AWS4-HMAC-SHA256-PAYLOAD\n")
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

    private func createCanonicalRequest(
        request: Request,
        signedHeaders: [String],
        isQueryAuth: Bool,
        sortQueryParams: Bool
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
            // CRITICAL: Parameters with = but empty value need to keep the =
            // e.g., "prefix=" not "prefix"
            queryString = queryItems.map { item in
                if item.hadEquals {
                    // Had an = sign, so keep it even if value is empty
                    return item.value.isEmpty ? "\(item.key)=" : "\(item.key)=\(item.value)"
                } else {
                    // No = sign in original (e.g., ?versioning)
                    return item.key
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
            let lowercased = name.lowercased()
            if headerLookup[lowercased] != nil {
                headerLookup[lowercased]!.append(value)
            } else {
                headerLookup[lowercased] = [value]
            }
        }

        for header in sortedHeaders {
            // O(1) lookup instead of O(n) case-insensitive scan
            let values = headerLookup[header] ?? {
                if let q = request.query[String.self, at: header] {
                    return [q]
                }
                return []
            }()
            let processed = values.map { value -> String in
                let trimmed = value.trimmingCharacters(in: .whitespaces)
                return collapseWhitespace(trimmed)
            }
            let joined = processed.joined(separator: ",")
            canonicalHeaders += "\(header):\(joined)\n"
        }

        // Signed headers - cache the result
        let signedHeadersString = sortedHeaders.joined(separator: ";")

        // Payload hash
        let payloadHash: String
        if isQueryAuth {
            payloadHash = "UNSIGNED-PAYLOAD"
        } else {
            payloadHash = try getRequestValue(for: "x-amz-content-sha256", request: request)
            guard !payloadHash.isEmpty else {
                throw S3Error(
                    status: .badRequest, code: "InvalidArgument",
                    message: "Missing required x-amz-content-sha256 header")
            }
        }

        // Optional: Verify payload hash if not unsigned or streaming
        if !isQueryAuth && payloadHash != "UNSIGNED-PAYLOAD"
            && payloadHash != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
        {
            if let bodyBuffer = request.body.data {
                let bodyView = bodyBuffer.readableBytesView
                let computedHash = Crypto.SHA256.hash(data: bodyView).hexString()
                guard computedHash == payloadHash else {
                    throw S3Error(
                        status: .badRequest, code: "InvalidDigest", message: "Payload hash mismatch"
                    )
                }
            } else {
                // No body: must match empty payload hash
                guard payloadHash == Self.emptyPayloadHash else {
                    throw S3Error(
                        status: .badRequest, code: "InvalidDigest",
                        message: "Payload hash mismatch for empty body")
                }
            }
        }
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
        let encodings = getRequestValues(for: "content-encoding", request: request).joined(
            separator: ",")
        guard encodings.contains("aws-chunked") else {
            throw S3Error(
                status: .badRequest, code: "InvalidArgument",
                message: "Missing aws-chunked in content-encoding for streaming payload")
        }
        guard let buffer = request.body.data else {
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
                    status: .badRequest, code: "SignatureDoesNotMatch",
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
}
