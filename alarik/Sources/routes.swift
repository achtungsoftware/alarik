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

func routes(_ app: Application) throws {

    // API - INTERNAL
    let apiV1: any RoutesBuilder = app.grouped("api", "v1")
    try apiV1.register(collection: InternalUserController())
    try apiV1.register(collection: InternalAuthOIDCController())
    try apiV1.grouped(InternalAuthenticator()).register(collection: InternalBaseController())
    try apiV1.grouped(InternalAuthenticator()).register(collection: InternalAdminController())
    try apiV1.grouped(InternalAuthenticator()).register(
        collection: InternalAdminOIDCProviderController())
    try apiV1.grouped(InternalAuthenticator()).register(collection: InternalBucketController())
    try apiV1.grouped(InternalAuthenticator()).register(collection: InternalClusterController())

    // Public, unauthenticated shared-file links - kept under /api/v1 (ungrouped, so no
    // InternalAuthenticator applies) rather than top-level, so it can never collide with
    // path-style S3 bucket routing (a bucket literally named "shared" would otherwise shadow
    // or be shadowed by this route).
    try apiV1.register(collection: SharedLinkController())

    // Cluster: inter-node object push/delete and bucket-wide-scan fan-out, guarded by
    // ClusterSecretMiddleware (not InternalAuthenticator - this is node-to-node, not
    // client-facing) - kept top-level, not under /api/v1, since it's never part of the public
    // API surface.
    try app.register(collection: InternalClusterObjectController())
    try app.register(collection: InternalClusterErasureCodedController())
    try app.register(collection: InternalClusterListingController())
    try app.register(collection: InternalClusterMetadataController())
    try app.register(collection: InternalClusterOutboxController())

    // S3
    try app.register(collection: S3Controller())
}
