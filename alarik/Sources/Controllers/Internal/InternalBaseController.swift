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
import Foundation
import Vapor
import XMLCoder
import ZIPFoundation

struct InternalBaseController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        routes.grouped("health").get(use: self.health)
        routes.grouped("accessKeyGenerator").get(use: self.accessKeyGenerator)
    }

    @Sendable
    func health(req: Request) async throws -> Response {
        Response(status: .ok)
    }

    @Sendable
    func accessKeyGenerator(req: Request) async throws -> S3Credentials {
        return S3KeyGenerator.generateCredentials()
    }
}
