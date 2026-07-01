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

#if os(Linux)
    import Glibc
#else
    import Darwin
#endif

/// Low-level, dependency-free readers for process/system resource usage. Each reader returns
/// `nil` where a platform simply doesn't expose the value cheaply (e.g. system-wide CPU on
/// macOS) - the dashboard renders what it gets and hides the rest, so partial data on a dev
/// machine is fine as long as the Linux/Docker production path is complete.
enum SystemMetrics {

    /// Cumulative CPU time (user + system) consumed by this process, in seconds.
    /// CPU *percentage* is a rate, so callers derive it from two readings over a wall-clock
    /// interval - see `MetricsCollector`.
    static func processCPUTime() -> Double? {
        var usage = rusage()
        // Glibc imports RUSAGE_SELF as a __rusage_who enum, but getrusage takes a plain Int32
        #if os(Linux)
            let who = Int32(RUSAGE_SELF.rawValue)
        #else
            let who = RUSAGE_SELF
        #endif
        guard getrusage(who, &usage) == 0 else { return nil }
        let user = Double(usage.ru_utime.tv_sec) + Double(usage.ru_utime.tv_usec) / 1_000_000
        let system = Double(usage.ru_stime.tv_sec) + Double(usage.ru_stime.tv_usec) / 1_000_000
        return user + system
    }

    /// System-wide cumulative (busy, total) CPU jiffies since boot. Linux only - derives
    /// system CPU percentage from two readings, same as the process variant.
    static func systemCPUTicks() -> (busy: UInt64, total: UInt64)? {
        #if os(Linux)
            guard let stat = try? String(contentsOfFile: "/proc/stat", encoding: .utf8),
                let cpuLine = stat.split(separator: "\n").first(where: { $0.hasPrefix("cpu ") })
            else { return nil }

            // "cpu  user nice system idle iowait irq softirq steal ..."
            let fields = cpuLine.split(separator: " ").dropFirst().compactMap { UInt64($0) }
            guard fields.count >= 4 else { return nil }

            let total = fields.reduce(0, +)
            let idle = fields[3] + (fields.count > 4 ? fields[4] : 0)  // idle + iowait
            return (busy: total - idle, total: total)
        #else
            return nil
        #endif
    }

    /// Resident set size (physical memory) of this process, in bytes.
    static func processMemoryBytes() -> Int64? {
        #if os(Linux)
            // /proc/self/statm field 2 is resident pages
            guard let statm = try? String(contentsOfFile: "/proc/self/statm", encoding: .utf8)
            else { return nil }
            let fields = statm.split(separator: " ")
            guard fields.count >= 2, let residentPages = Int64(fields[1]) else { return nil }
            return residentPages * Int64(getpagesize())
        #else
            var info = mach_task_basic_info()
            var count = mach_msg_type_number_t(
                MemoryLayout<mach_task_basic_info>.size / MemoryLayout<natural_t>.size)
            let result = withUnsafeMutablePointer(to: &info) {
                $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
                    task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), $0, &count)
                }
            }
            guard result == KERN_SUCCESS else { return nil }
            return Int64(info.resident_size)
        #endif
    }

    /// Total and available system memory in bytes. Container-aware on Linux: when a cgroup v2
    /// memory limit is set (i.e. running in Docker with `--memory`), the limit and current
    /// usage are reported instead of the host's, since that's the ceiling that actually
    /// matters to the deployment.
    static func systemMemory() -> (total: Int64, available: Int64)? {
        #if os(Linux)
            // cgroup v2 (memory.max holds "max" when unlimited)
            if let maxRaw = try? String(
                contentsOfFile: "/sys/fs/cgroup/memory.max", encoding: .utf8),
                let limit = Int64(maxRaw.trimmingCharacters(in: .whitespacesAndNewlines)),
                let currentRaw = try? String(
                    contentsOfFile: "/sys/fs/cgroup/memory.current", encoding: .utf8),
                let current = Int64(
                    currentRaw.trimmingCharacters(in: .whitespacesAndNewlines))
            {
                return (total: limit, available: max(0, limit - current))
            }

            guard let meminfo = try? String(contentsOfFile: "/proc/meminfo", encoding: .utf8)
            else { return nil }

            func kilobytes(for key: String) -> Int64? {
                guard let line = meminfo.split(separator: "\n").first(where: { $0.hasPrefix(key) })
                else { return nil }
                let fields = line.split(separator: " ").compactMap { Int64($0) }
                return fields.first
            }

            guard let totalKB = kilobytes(for: "MemTotal:"),
                let availableKB = kilobytes(for: "MemAvailable:")
            else { return nil }
            return (total: totalKB * 1024, available: availableKB * 1024)
        #else
            let total = Int64(ProcessInfo.processInfo.physicalMemory)

            var stats = vm_statistics64()
            var count = mach_msg_type_number_t(
                MemoryLayout<vm_statistics64>.size / MemoryLayout<integer_t>.size)
            let result = withUnsafeMutablePointer(to: &stats) {
                $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
                    host_statistics64(mach_host_self(), HOST_VM_INFO64, $0, &count)
                }
            }
            guard result == KERN_SUCCESS else { return (total: total, available: 0) }
            let pageSize = Int64(getpagesize())
            // free + inactive is the closest analogue to Linux's MemAvailable
            let available = (Int64(stats.free_count) + Int64(stats.inactive_count)) * pageSize
            return (total: total, available: available)
        #endif
    }

    /// 1/5/15-minute load averages.
    static func loadAverages() -> (one: Double, five: Double, fifteen: Double)? {
        var loads = [Double](repeating: 0, count: 3)
        guard getloadavg(&loads, 3) == 3 else { return nil }
        return (one: loads[0], five: loads[1], fifteen: loads[2])
    }

    static var coreCount: Int {
        ProcessInfo.processInfo.activeProcessorCount
    }
}
