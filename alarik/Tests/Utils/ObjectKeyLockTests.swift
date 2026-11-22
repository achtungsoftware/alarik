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

@Suite("ObjectKeyLock tests")
struct ObjectKeyLockTests {
    private final class Box: @unchecked Sendable {
        var counter = 0
        var inside = 0
        var overlapped = false
    }

    @Test("holders of one key run strictly one at a time")
    func mutualExclusion() {
        let box = Box()
        DispatchQueue.concurrentPerform(iterations: 32) { _ in
            for _ in 0..<200 {
                ObjectKeyLock.withLock(bucketName: "b", key: "k") {
                    box.inside += 1
                    if box.inside > 1 { box.overlapped = true }
                    box.counter += 1
                    box.inside -= 1
                }
            }
        }
        #expect(box.counter == 32 * 200)
        #expect(!box.overlapped)
    }

    @Test("re-entering the lock for the same key does not deadlock")
    func reentrant() {
        let value = ObjectKeyLock.withLock(bucketName: "b", key: "k") {
            ObjectKeyLock.withLock(bucketName: "b", key: "k") { 42 }
        }
        #expect(value == 42)
    }

    @Test("a thrown error releases the lock")
    func errorReleases() {
        struct Boom: Error {}
        #expect(throws: Boom.self) {
            try ObjectKeyLock.withLock(bucketName: "b", key: "k") { throw Boom() }
        }
        #expect(ObjectKeyLock.withLock(bucketName: "b", key: "k") { true })
    }
}
