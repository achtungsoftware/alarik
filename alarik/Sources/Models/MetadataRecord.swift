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

/// A control-plane record stored in `MetadataStore` rather than a database. Conforming types get
/// `find`/`all`/`allVerified`/`save`/`delete`/`create` for free - a model only declares WHERE it
/// lives (`metadataCollection`) and WHAT key it lives under (`metadataId`).
///
/// Everything else is a plain CRUD call parameterised by exactly those two values, so writing it
/// per model was pure copy-paste. The payoff is not just less code: a model that overrides one of
/// these defaults is now doing something genuinely special (see `User`/`AccessKey`, which maintain
/// a secondary index), and that stands out instead of hiding among identical copies.
protocol MetadataRecord: Codable, Sendable {
    /// The `MetadataCollections` constant this type's primary records live in.
    static var metadataCollection: String { get }

    /// The key this record is stored under within its collection. Natural keys are preferred
    /// (`Bucket.name`, `AccessKey.accessKey`); types without one use `id.uuidString`.
    var metadataId: String { get }
}

extension MetadataRecord {
    static func find(app: Application, key: String) async throws -> Self? {
        try await MetadataStore.get(Self.self, app: app, collection: metadataCollection, id: key)
    }

    /// Every record of this type cluster-wide - a full-collection fan-out (see
    /// `MetadataListingService`'s doc comment). These are shallow, low-churn collections, so this
    /// is only ever called from admin/console/background-sweep paths, never per-S3-request - the
    /// hot per-request lookup is always `find`, a single point read.
    ///
    /// Non-throwing on purpose: a listing degrades to a partial answer rather than failing, so
    /// there is no error for a caller to handle. Use `allVerified` when partial isn't acceptable.
    static func all(app: Application) async -> [Self] {
        await MetadataListingService.list(Self.self, app: app, collection: metadataCollection)
    }

    /// `all` plus the listing's completeness verdict - see `LoadCacheLifecycle.reloadAll`, which
    /// may only reconcile REMOVALS against a listing that is verifiably complete.
    static func allVerified(
        app: Application
    ) async -> (records: [Self], presentIds: Set<String>, complete: Bool) {
        await MetadataListingService.listVerified(
            Self.self, app: app, collection: metadataCollection)
    }

    func save(app: Application) async throws {
        try await MetadataStore.put(
            app: app, collection: Self.metadataCollection, id: metadataId, value: self)
    }

    func delete(app: Application) async throws {
        try await MetadataStore.delete(
            app: app, collection: Self.metadataCollection, id: metadataId)
    }

    /// Creates the record, returning false if `metadataId` is already taken.
    func create(app: Application) async throws -> Bool {
        try await MetadataStore.putIfAbsent(
            app: app, collection: Self.metadataCollection, id: metadataId, value: self)
    }
}
