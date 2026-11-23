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

/// Platform-branched filesystem capacity lookup, shared by the admin storage-stats endpoint and
/// the cluster heartbeat's self-reported capacity.
enum DiskSpace {
    /// Hard floor of free space below which uploads are refused, as a percentage of the volume
    /// holding `Storage/`. Independent of the cluster's soft `CLUSTER_MIN_FREE_PERCENT`
    /// redirect: this one applies on every node, clustered or not, because a disk that fills
    /// completely takes metadata writes down with it. `ALARIK_MIN_FREE_PERCENT`, default 1.
    static func minFreePercent() -> Double {
        Environment.sanitizedGet("ALARIK_MIN_FREE_PERCENT").flatMap(Double.init) ?? 1.0
    }

    /// Whether an upload of `declaredLength` bytes (nil when unknown) fits without taking the
    /// volume below the floor. Unknown capacity (0 total) fails open.
    static func hasRoom(
        total: Int64, available: Int64, declaredLength: Int?, minFreePercent: Double
    ) -> Bool {
        guard total > 0 else { return true }
        let remaining = available - Int64(declaredLength ?? 0)
        return Double(remaining) / Double(total) * 100 >= minFreePercent
    }

    /// Throws `InsufficientStorage` when the volume cannot take `declaredLength` more bytes
    /// without crossing the floor. Checked before a body is spooled, so a doomed upload is
    /// refused at once instead of failing with ENOSPC after transferring gigabytes.
    static func ensureRoom(forUpload declaredLength: Int?, requestId: String) throws {
        let (total, available) = availableAndTotal(for: URL(fileURLWithPath: BucketHandler.rootPath))
        guard
            hasRoom(
                total: total, available: available, declaredLength: declaredLength,
                minFreePercent: minFreePercent())
        else {
            throw S3Error(
                status: .insufficientStorage, code: "InsufficientStorage",
                message: "Not enough storage space is available to store this object.",
                requestId: requestId)
        }
    }

    static func availableAndTotal(for url: URL) -> (total: Int64, available: Int64) {
        // Test-only override: e2e cluster tests run multiple node processes on one machine
        // sharing a single physical disk, so every node always reports virtually identical real
        // free space - there's no way to simulate "this node is genuinely lower on space than its
        // peers" without actually writing gigabytes of data. Setting both env vars lets a test
        // inject a specific capacity reading instead, bypassing the real lookup below entirely.
        // Checked first, before touching the filesystem at all.
        if let debugTotal = Environment.sanitizedGet("CLUSTER_DEBUG_TOTAL_BYTES").flatMap(Int64.init),
            let debugAvailable = Environment.sanitizedGet("CLUSTER_DEBUG_AVAILABLE_BYTES").flatMap(
                Int64.init)
        {
            return (debugTotal, debugAvailable)
        }

        let path =
            FileManager.default.fileExists(atPath: url.path)
            ? url.path
            : url.deletingLastPathComponent().path

        #if os(Linux)
            var stat = statvfs()
            guard statvfs(path, &stat) == 0 else {
                return (0, 0)
            }
            let blockSize = UInt64(stat.f_frsize)
            let totalBytes = Int64(UInt64(stat.f_blocks) * blockSize)
            let availableBytes = Int64(UInt64(stat.f_bavail) * blockSize)
            return (totalBytes, availableBytes)
        #else
            do {
                let values = try URL(fileURLWithPath: path).resourceValues(forKeys: [
                    .volumeAvailableCapacityForImportantUsageKey,
                    .volumeTotalCapacityKey,
                ])
                let total = Int64(values.volumeTotalCapacity ?? 0)
                let available = Int64(values.volumeAvailableCapacityForImportantUsage ?? 0)
                return (total, available)
            } catch {
                return (0, 0)
            }
        #endif
    }
}
