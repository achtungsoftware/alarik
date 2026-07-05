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

import Fluent
import Vapor

extension SharedLink {
    struct ResponseDTO: Content {
        var id: UUID?
        var bucketName: String
        var key: String
        var url: String
        /// Nil for a link that never expires.
        var expiresAt: Date?
        var createdAt: Date
    }

    func toResponseDTO() -> SharedLink.ResponseDTO {
        .init(
            id: self.id,
            bucketName: self.bucketName,
            key: self.key,
            url: "\(apiBaseURL)/api/v1/shared/\(self.id?.uuidString ?? "")",
            expiresAt: self.expiresAt,
            createdAt: self.createdAt
        )
    }
}
