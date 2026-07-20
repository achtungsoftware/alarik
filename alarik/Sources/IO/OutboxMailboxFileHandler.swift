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

/// Raw file I/O for outbox mailbox task records - plain, unencoded, atomically-written small
/// JSON files, deliberately not `.ecshard`: an outbox task is read/written whole (never streamed
/// in chunks) and lives seconds-to-minutes, so there's no reason to pay Reed-Solomon striping or
/// per-stripe checksums for it - `OutboxMailbox`'s own replica-count mirroring is this format's
/// redundancy story instead. See `OutboxMailbox` for the higher-level owner/backup/promotion
/// logic built on top of these primitives.
enum OutboxMailboxFileHandler {
    /// The owner's authoritative copy of every collection's tasks.
    static let rootPath = "Storage/outbox/"
    /// Best-effort mirrors of other nodes' owner copies, held so a briefly-down owner's tasks
    /// aren't lost - see `OutboxMailbox.mirrorBackups`/`promoteOrphanedBackups`.
    static let backupRootPath = "Storage/outbox-backup/"
    /// A creator's own spool for tasks whose owner couldn't be reached at enqueue time - retried
    /// on the creator's own drain tick until the owner comes back, or (for ground-truth-backstopped
    /// collections) abandoned after a bound since the next rebalance/scrub regenerates them.
    static let pendingEnqueueRootPath = "Storage/outbox-pending-enqueue/"

    static func taskPath(root: String, collection: String, ownerNodeId: UUID, taskId: UUID) -> String {
        "\(root)\(collection)/\(ownerNodeId.uuidString)/\(taskId.uuidString).task"
    }

    private static func taskDirectory(root: String, collection: String, ownerNodeId: UUID) -> String {
        "\(root)\(collection)/\(ownerNodeId.uuidString)/"
    }

    /// Atomically writes `data` to `path`, creating any missing parent directories first.
    static func write(path: String, data: Data) throws {
        let directory = (path as NSString).deletingLastPathComponent
        if !FileManager.default.fileExists(atPath: directory) {
            try FileManager.default.createDirectory(
                atPath: directory, withIntermediateDirectories: true)
        }
        var writer = try AtomicObjectWriter(finalPath: path)
        do {
            try writer.write(data)
            try writer.finish()
        } catch {
            writer.abort()
            throw error
        }
    }

    static func read(path: String) -> Data? {
        FileManager.default.contents(atPath: path)
    }

    static func remove(path: String) {
        try? FileManager.default.removeItem(atPath: path)
    }

    static func exists(path: String) -> Bool {
        FileManager.default.fileExists(atPath: path)
    }

    /// Every task id directly under `root/collection/ownerNodeId/` - a single shallow directory
    /// listing, cost proportional to this one owner's own backlog in this one collection, never
    /// cluster-wide.
    static func listTaskIds(root: String, collection: String, ownerNodeId: UUID) -> [UUID] {
        let directory = taskDirectory(root: root, collection: collection, ownerNodeId: ownerNodeId)
        guard let entries = try? FileManager.default.contentsOfDirectory(atPath: directory) else {
            return []
        }
        return entries.compactMap { entry in
            guard entry.hasSuffix(".task") else { return nil }
            return UUID(uuidString: String(entry.dropLast(".task".count)))
        }
    }

    /// Every `(ownerNodeId, taskId)` pair under `root/collection/` - used by the promotion sweep,
    /// which must walk every owner subdirectory it holds backups for for one collection, not just
    /// one specific owner.
    static func listAllOwnerTaskIds(root: String, collection: String) -> [(ownerNodeId: UUID, taskId: UUID)] {
        let collectionDirectory = "\(root)\(collection)/"
        guard
            let ownerEntries = try? FileManager.default.contentsOfDirectory(atPath: collectionDirectory)
        else { return [] }

        var results: [(ownerNodeId: UUID, taskId: UUID)] = []
        for ownerEntry in ownerEntries {
            guard let ownerNodeId = UUID(uuidString: ownerEntry) else { continue }
            for taskId in listTaskIds(root: root, collection: collection, ownerNodeId: ownerNodeId) {
                results.append((ownerNodeId, taskId))
            }
        }
        return results
    }
}
