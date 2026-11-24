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
import Logging

/// One-time rename of names written before `KeyPathEncoding` existed. Up to 1.0.0-beta-16 the
/// key `model.obj` was stored as `model.obj.obj` and the folder `data.obj/` as `data.obj/`;
/// today those read `model.obj~.obj` and `data.obj~/`. The old names still list (the listing
/// decodes leniently) but every direct read resolves the escaped path and misses them, so each
/// is renamed into its escaped form once, at boot, before anything is served.
enum KeyPathMigration {
    /// Written after a complete pass so later boots skip the whole-tree walk.
    static let markerPath = "Storage/key-escaping-migrated"

    struct Report: Equatable {
        var renamed = 0
        /// Old paths left in place because their escaped name already exists - an object that
        /// was rewritten after the upgrade. The newer copy wins; the old one is for the operator.
        var conflicts: [String] = []
    }

    static func runIfNeeded(logger: Logger) {
        guard !FileManager.default.fileExists(atPath: markerPath) else { return }
        let report = migrate(root: BucketHandler.rootPath)
        if report.renamed > 0 {
            logger.notice("Renamed \(report.renamed) pre-beta-17 object path(s) into their escaped form.")
        }
        for path in report.conflicts {
            logger.warning(
                "Left pre-beta-17 path '\(path)' in place: its escaped name already exists and holds the newer object. Remove the old path by hand once the newer object is confirmed."
            )
        }
        FileManager.default.createFile(atPath: markerPath, contents: Data())
    }

    /// Walks every bucket directory under `root`. Idempotent: a second pass renames nothing.
    static func migrate(root: String) -> Report {
        var report = Report()
        guard let buckets = try? FileManager.default.contentsOfDirectory(atPath: root) else { return report }
        for bucket in buckets {
            let path = root + bucket
            if isDirectory(path) { migrateDirectory(path, report: &report) }
        }
        return report
    }

    private static let forms = [".obj", ".versions", ".ecshards"]

    private static func migrateDirectory(_ directory: String, report: inout Report) {
        guard let names = try? FileManager.default.contentsOfDirectory(atPath: directory) else { return }
        for name in names where !name.hasPrefix(AtomicObjectWriter.tempFilePrefix) {
            let path = directory + "/" + name
            let directoryEntry = isDirectory(path)
            let (form, stem) = split(name)

            if !directoryEntry {
                guard form == ".obj" else { continue }
                let recorded = (try? ObjectFileHandler.read(from: path, loadData: false))?.0.key
                let escaped = escapedStem(stem, recordedKey: recorded)
                if escaped != stem { rename(path, to: directory + "/" + escaped + ".obj", report: &report) }
                continue
            }

            // A directory named `<stem>.versions` / `<stem>.ecshards` is a sidecar of key `stem`
            // unless its contents say otherwise; `<stem>.obj` can only be a folder.
            if let form, form != ".obj", let recorded = sidecarKey(path, form: form) {
                let escaped = escapedStem(stem, recordedKey: recorded)
                if escaped != stem { rename(path, to: directory + "/" + escaped + form, report: &report) }
                continue
            }
            let escaped = escapedStem(Substring(name), recordedKey: nil, asDirectory: true)
            var current = path
            if escaped != name {
                current = directory + "/" + escaped
                guard rename(path, to: current, report: &report) else { continue }
            }
            migrateDirectory(current, report: &report)
        }
    }

    /// The name `stem` should have on disk. A legacy name IS the key component; a current name
    /// decodes to it. The object's own header settles which one this is - a legacy `x.obj~`
    /// (key `x.obj~`) and a current `x.obj~` (key `x.obj`) are the same bytes on disk otherwise.
    /// Without a header the name is taken as already decoded-correct, the harmless reading.
    static func escapedStem(_ stem: Substring, recordedKey: String?, asDirectory: Bool = false) -> String {
        let decoded = KeyPathEncoding.decodeComponent(String(stem), asDirectory: asDirectory)
        var logical = decoded
        if let recordedKey {
            let last = recordedKey.split(separator: "/", omittingEmptySubsequences: false).last.map(String.init) ?? ""
            if last == stem || last == decoded { logical = last }
        }
        return KeyPathEncoding.encodeComponent(Substring(logical), asDirectory: asDirectory)
    }

    private static func split(_ name: String) -> (form: String?, stem: Substring) {
        for form in forms where name.hasSuffix(form) {
            return (form, name.dropLast(form.count))
        }
        return (nil, Substring(name))
    }

    /// The key a `<stem>.versions` / `<stem>.ecshards` directory holds versions or shards of, or
    /// nil when it is a user's folder that merely ends that way. A folder's files carry their own
    /// keys and version ids; a sidecar's entries are named by the version id their header records,
    /// and a shard directory holds nothing but `<index>.ecshard`. Undecidable (empty, or a header
    /// that cannot be read) counts as a sidecar with no recorded key, which leaves the name alone.
    private static func sidecarKey(_ directory: String, form: String) -> String?? {
        guard let names = try? FileManager.default.contentsOfDirectory(atPath: directory) else { return .some(nil) }
        if form == ".ecshards" {
            guard names.allSatisfy({ $0.hasSuffix(".ecshard") && Int($0.dropLast(".ecshard".count)) != nil })
            else { return nil }
            return .some(ErasureCodedObjectHandler.anyLocalShardHeader(inShardDirectory: directory)?.objectMeta.key)
        }
        for name in names where name != ".latest" {
            let path = directory + "/" + name
            if name.hasSuffix(".obj"), !isDirectory(path) {
                guard let (meta, _) = try? ObjectFileHandler.read(from: path, loadData: false) else { return .some(nil) }
                return meta.versionId == String(name.dropLast(".obj".count)) ? .some(meta.key) : nil
            }
            if name.hasSuffix(".ecshards"), isDirectory(path) {
                guard let header = ErasureCodedObjectHandler.anyLocalShardHeader(inShardDirectory: path) else {
                    return .some(nil)
                }
                return header.objectMeta.versionId == String(name.dropLast(".ecshards".count))
                    ? .some(header.objectMeta.key) : nil
            }
            return nil
        }
        return .some(nil)
    }

    @discardableResult
    private static func rename(_ from: String, to: String, report: inout Report) -> Bool {
        guard from != to else { return true }
        // `moveItem` refuses to replace: a newer object already at the escaped name survives.
        do {
            try FileManager.default.moveItem(atPath: from, toPath: to)
            report.renamed += 1
            return true
        } catch {
            report.conflicts.append(from)
            return false
        }
    }

    private static func isDirectory(_ path: String) -> Bool {
        var isDir: ObjCBool = false
        return FileManager.default.fileExists(atPath: path, isDirectory: &isDir) && isDir.boolValue
    }
}
