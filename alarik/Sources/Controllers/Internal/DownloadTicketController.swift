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

import Vapor

/// Redeems a `DownloadTicket`: the one console route outside `InternalAuthenticator`, because
/// the browser fetches it on its own (an anchor, an image, a video) with no way to add the
/// bearer token. The ticket is the credential; what it names is checked again against the
/// issuing user's buckets when it is served.
struct DownloadTicketController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("objects", "download").get(use: self.download)
    }

    @Sendable
    func download(req: Request) async throws -> Response {
        guard let token = req.query[String.self, at: "ticket"] else {
            throw Abort(.unauthorized, reason: "Missing download ticket")
        }
        guard let key = req.application.storage[DownloadTicketSigner.Key.self] else {
            throw Abort(.internalServerError, reason: "Download tickets are not configured")
        }
        let ticket = try DownloadTicketSigner.verify(token, key: key)
        return try await InternalObjectController().download(
            req: req, userId: ticket.userId, bucket: ticket.bucket, keys: ticket.keys,
            versionId: ticket.versionId, inline: ticket.inline, downloadName: ticket.downloadName)
    }
}
