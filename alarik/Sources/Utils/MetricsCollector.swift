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

/// In-memory runtime metrics: HTTP traffic counters fed by `MetricsMiddleware`, CPU/memory
/// gauges sampled from `SystemMetrics`, and a per-minute ring buffer covering the last hour
/// so the admin dashboard can draw live charts.
///
/// Everything lives in this actor and resets on restart by design - persisting metrics is a
/// job for a real time-series store, not the object server. Deriving CPU percentages needs two
/// readings, so the actor also keeps the previous reading between samples.
actor MetricsCollector {
    static let shared = MetricsCollector()

    /// One rolled-up minute of activity, in wall-clock minute alignment.
    struct MinuteBucket: Content {
        var timestamp: Date
        var bytesIn: Int64
        var bytesOut: Int64
        var requests: Int64
        var errors: Int64
        /// Average of the CPU/memory samples taken during this minute (nil until sampled).
        var cpuPercent: Double?
        var memoryBytes: Int64?
    }

    struct Snapshot: Content {
        var uptimeSeconds: Double
        var totalBytesIn: Int64
        var totalBytesOut: Int64
        var totalRequests: Int64
        var totalErrors: Int64
        var processCPUPercent: Double?
        var systemCPUPercent: Double?
        var processMemoryBytes: Int64?
        var systemMemoryTotalBytes: Int64?
        var systemMemoryAvailableBytes: Int64?
        var loadAverage1: Double?
        var loadAverage5: Double?
        var loadAverage15: Double?
        var coreCount: Int
        var history: [MinuteBucket]
    }

    private let startedAt = Date()

    // Totals since process start
    private var totalBytesIn: Int64 = 0
    private var totalBytesOut: Int64 = 0
    private var totalRequests: Int64 = 0
    private var totalErrors: Int64 = 0

    // Ring buffer of completed minutes plus the minute currently accumulating
    private static let historyLimit = 60
    private var history: [MinuteBucket] = []
    private var currentBucket: MinuteBucket
    private var cpuSampleSum: Double = 0
    private var cpuSampleCount: Int = 0
    private var memorySampleSum: Int64 = 0
    private var memorySampleCount: Int = 0

    // Previous readings for rate derivation
    private var lastSampleTime: Date?
    private var lastProcessCPUTime: Double?
    private var lastSystemCPUTicks: (busy: UInt64, total: UInt64)?
    private var lastProcessCPUPercent: Double?
    private var lastSystemCPUPercent: Double?

    init() {
        self.currentBucket = Self.emptyBucket(for: Date())
    }

    private static func emptyBucket(for date: Date) -> MinuteBucket {
        MinuteBucket(
            timestamp: Self.minuteStart(of: date),
            bytesIn: 0, bytesOut: 0, requests: 0, errors: 0,
            cpuPercent: nil, memoryBytes: nil
        )
    }

    private static func minuteStart(of date: Date) -> Date {
        Date(timeIntervalSince1970: (date.timeIntervalSince1970 / 60).rounded(.down) * 60)
    }

    /// Called by `MetricsMiddleware` for every completed HTTP request.
    func record(bytesIn: Int, bytesOut: Int, isError: Bool) {
        rollBucketIfNeeded(now: Date())
        totalBytesIn += Int64(bytesIn)
        totalBytesOut += Int64(bytesOut)
        totalRequests += 1
        currentBucket.bytesIn += Int64(bytesIn)
        currentBucket.bytesOut += Int64(bytesOut)
        currentBucket.requests += 1
        if isError {
            totalErrors += 1
            currentBucket.errors += 1
        }
    }

    /// Called periodically (every few seconds) by the background task in `configure`, and once
    /// per `snapshot()` so the gauges are fresh even if the background task isn't running
    /// (e.g. under tests). Derives CPU percentages from the delta since the previous call.
    func sample() {
        let now = Date()
        rollBucketIfNeeded(now: now)

        let processCPUTime = SystemMetrics.processCPUTime()
        let systemTicks = SystemMetrics.systemCPUTicks()

        if let last = lastSampleTime {
            let wallDelta = now.timeIntervalSince(last)
            // Below ~0.5s the delta is mostly noise; keep the previous derived values.
            if wallDelta >= 0.5 {
                if let cpuNow = processCPUTime, let cpuLast = lastProcessCPUTime {
                    lastProcessCPUPercent = max(0, (cpuNow - cpuLast) / wallDelta * 100)
                }
                if let ticksNow = systemTicks, let ticksLast = lastSystemCPUTicks,
                    ticksNow.total > ticksLast.total
                {
                    let busyDelta = Double(ticksNow.busy &- ticksLast.busy)
                    let totalDelta = Double(ticksNow.total - ticksLast.total)
                    lastSystemCPUPercent = max(0, min(100, busyDelta / totalDelta * 100))
                }
                lastSampleTime = now
                lastProcessCPUTime = processCPUTime
                lastSystemCPUTicks = systemTicks
            }
        } else {
            lastSampleTime = now
            lastProcessCPUTime = processCPUTime
            lastSystemCPUTicks = systemTicks
        }

        if let cpu = lastProcessCPUPercent {
            cpuSampleSum += cpu
            cpuSampleCount += 1
            currentBucket.cpuPercent = cpuSampleSum / Double(cpuSampleCount)
        }
        if let memory = SystemMetrics.processMemoryBytes() {
            memorySampleSum += memory
            memorySampleCount += 1
            currentBucket.memoryBytes = memorySampleSum / Int64(memorySampleCount)
        }
    }

    /// Full snapshot for the admin endpoint: totals, live gauges, and the last hour of
    /// per-minute buckets (oldest first, current partial minute last).
    func snapshot() -> Snapshot {
        sample()

        let memory = SystemMetrics.systemMemory()
        let load = SystemMetrics.loadAverages()

        return Snapshot(
            uptimeSeconds: Date().timeIntervalSince(startedAt),
            totalBytesIn: totalBytesIn,
            totalBytesOut: totalBytesOut,
            totalRequests: totalRequests,
            totalErrors: totalErrors,
            processCPUPercent: lastProcessCPUPercent,
            systemCPUPercent: lastSystemCPUPercent,
            processMemoryBytes: SystemMetrics.processMemoryBytes(),
            systemMemoryTotalBytes: memory?.total,
            systemMemoryAvailableBytes: memory?.available,
            loadAverage1: load?.one,
            loadAverage5: load?.five,
            loadAverage15: load?.fifteen,
            coreCount: SystemMetrics.coreCount,
            history: history + [currentBucket]
        )
    }

    /// Moves the accumulating bucket into history once its wall-clock minute has passed,
    /// inserting empty buckets for any fully idle minutes in between so chart gaps show as
    /// zero traffic rather than as missing time.
    private func rollBucketIfNeeded(now: Date) {
        let currentMinute = Self.minuteStart(of: now)
        guard currentMinute > currentBucket.timestamp else { return }

        history.append(currentBucket)

        var nextMinute = currentBucket.timestamp.addingTimeInterval(60)
        while nextMinute < currentMinute && history.count < Self.historyLimit {
            history.append(Self.emptyBucket(for: nextMinute))
            nextMinute = nextMinute.addingTimeInterval(60)
        }

        if history.count > Self.historyLimit {
            history.removeFirst(history.count - Self.historyLimit)
        }

        currentBucket = Self.emptyBucket(for: currentMinute)
        cpuSampleSum = 0
        cpuSampleCount = 0
        memorySampleSum = 0
        memorySampleCount = 0
    }
}
