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
import Testing

@testable import Alarik

@Suite("MetricsCollector tests")
struct MetricsCollectorTests {

    @Test("record accumulates totals and the current bucket")
    func recordAccumulates() async throws {
        let collector = MetricsCollector()

        await collector.record(bytesIn: 100, bytesOut: 2000, isError: false)
        await collector.record(bytesIn: 50, bytesOut: 500, isError: true)

        let snapshot = await collector.snapshot()
        #expect(snapshot.totalBytesIn == 150)
        #expect(snapshot.totalBytesOut == 2500)
        #expect(snapshot.totalRequests == 2)
        #expect(snapshot.totalErrors == 1)

        // Both requests landed in the current (single) minute bucket
        let current = try #require(snapshot.history.last)
        #expect(current.bytesIn == 150)
        #expect(current.bytesOut == 2500)
        #expect(current.requests == 2)
        #expect(current.errors == 1)
    }

    @Test("snapshot reports positive uptime and core count")
    func snapshotBasics() async throws {
        let collector = MetricsCollector()
        let snapshot = await collector.snapshot()

        #expect(snapshot.uptimeSeconds >= 0)
        #expect(snapshot.coreCount >= 1)
        #expect(snapshot.history.count >= 1)
    }

    @Test("history never exceeds 60 buckets and stays chronological")
    func historyBounded() async throws {
        let collector = MetricsCollector()

        // Sampling repeatedly within the same minute must not grow history - only minute
        // rollovers append buckets, and those can't be forced from a test without waiting.
        for _ in 0..<5 {
            await collector.sample()
        }

        let snapshot = await collector.snapshot()
        #expect(snapshot.history.count <= 60)

        let timestamps = snapshot.history.map(\.timestamp)
        #expect(timestamps == timestamps.sorted())
    }

    @Test("system metrics readers return sane values on this platform")
    func systemMetricsSanity() throws {
        // Process CPU time and RSS must be readable on both Linux and macOS
        let cpuTime = try #require(SystemMetrics.processCPUTime())
        #expect(cpuTime > 0)

        let rss = try #require(SystemMetrics.processMemoryBytes())
        #expect(rss > 1024 * 1024)  // a Swift server process is never under 1 MiB

        let memory = try #require(SystemMetrics.systemMemory())
        #expect(memory.total > 0)
        #expect(memory.available >= 0)
        #expect(memory.available <= memory.total)

        let load = try #require(SystemMetrics.loadAverages())
        #expect(load.one >= 0)

        #expect(SystemMetrics.coreCount >= 1)

        #if os(Linux)
            // System-wide CPU ticks are Linux-only and must be present there
            let ticks = try #require(SystemMetrics.systemCPUTicks())
            #expect(ticks.total > 0)
            #expect(ticks.busy <= ticks.total)
        #endif
    }
}
