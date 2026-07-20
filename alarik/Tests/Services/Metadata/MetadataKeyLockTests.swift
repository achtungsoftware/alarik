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

@Suite("MetadataKeyLock tests")
struct MetadataKeyLockTests {
    @Test("concurrent withLock calls on the same key never overlap")
    func sameKeyMutualExclusion() async {
        let lock = MetadataKeyLock()
        let tracker = OverlapTracker()

        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<20 {
                group.addTask {
                    await lock.withLock(collection: "users", key: "alice") {
                        await tracker.enter()
                        // Yield to give a racing task a chance to slip in if the lock were broken.
                        await Task.yield()
                        await tracker.exit()
                    }
                }
            }
        }

        #expect(await tracker.maxConcurrent == 1)
        #expect(await tracker.overlapDetected == false)
    }

    @Test("different keys run fully concurrently, never serialized against each other")
    func differentKeysRunConcurrently() async {
        let lock = MetadataKeyLock()
        let tracker = OverlapTracker()
        let barrier = Barrier(expected: 5)

        await withTaskGroup(of: Void.self) { group in
            for index in 0..<5 {
                group.addTask {
                    await lock.withLock(collection: "users", key: "user-\(index)") {
                        await tracker.enter()
                        // Every task waits here until all 5 have entered - only possible if
                        // distinct keys never contend with each other.
                        await barrier.arrive()
                        await tracker.exit()
                    }
                }
            }
        }

        #expect(await tracker.maxConcurrent == 5)
    }

    @Test("a putIfAbsent-shaped race on the same key: exactly one call observes 'absent'")
    func putIfAbsentRaceHasExactlyOneWinner() async {
        let lock = MetadataKeyLock()
        let present = Flag()
        let winners = Counter()

        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<10 {
                group.addTask {
                    await lock.withLock(collection: "buckets", key: "my-bucket") {
                        let wasAlreadyPresent = await present.value
                        if !wasAlreadyPresent {
                            await present.set(true)
                            await winners.increment()
                        }
                    }
                }
            }
        }

        #expect(await winners.value == 1)
    }

    @Test("the lock is released after body throws, so a later call can still proceed")
    func releasesLockAfterThrow() async throws {
        let lock = MetadataKeyLock()
        struct Boom: Error {}

        await #expect(throws: Boom.self) {
            try await lock.withLock(collection: "oidc-states", key: "state-1") {
                throw Boom()
            }
        }

        // If the lock were leaked on throw, this would hang forever - the test timing out is
        // the failure signal.
        let completed = Flag()
        await lock.withLock(collection: "oidc-states", key: "state-1") {
            await completed.set(true)
        }
        #expect(await completed.value)
    }
}

private actor OverlapTracker {
    private(set) var current = 0
    private(set) var maxConcurrent = 0
    private(set) var overlapDetected = false

    func enter() {
        current += 1
        maxConcurrent = max(maxConcurrent, current)
        if current > 1 { overlapDetected = true }
    }

    func exit() {
        current -= 1
    }
}

private actor Flag {
    private(set) var value = false
    func set(_ newValue: Bool) { value = newValue }
}

private actor Counter {
    private(set) var value = 0
    func increment() { value += 1 }
}

/// Waits until `expected` participants have all called `arrive()` before releasing any of them.
private actor Barrier {
    private let expected: Int
    private var arrived = 0
    private var continuations: [CheckedContinuation<Void, Never>] = []

    init(expected: Int) {
        self.expected = expected
    }

    func arrive() async {
        arrived += 1
        if arrived >= expected {
            for continuation in continuations { continuation.resume() }
            continuations.removeAll()
            return
        }
        await withCheckedContinuation { continuation in
            continuations.append(continuation)
        }
    }
}
