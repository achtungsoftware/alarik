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
import Synchronization

/// Process-wide mutual exclusion per (bucket, key) for the synchronous `.obj` storage layer.
/// Its write paths are multi-step (demote prior versions, write, repoint `.latest`), and two
/// requests interleaving on one key left two `isLatest` versions, or a metadata-only rewrite
/// renamed over a concurrent PUT. Recursive, so a locked entry point may call another for the
/// same key. Only ever held across synchronous code, never an `await`.
enum ObjectKeyLock {
    private struct Entry: @unchecked Sendable {
        let lock = NSRecursiveLock()
        var holders = 0
    }

    private static let table = Mutex<[String: Entry]>([:])

    static func withLock<T>(bucketName: String, key: String, _ body: () throws -> T) rethrows -> T {
        let id = bucketName + "\0" + key
        let lock = table.withLock { entries -> NSRecursiveLock in
            var entry = entries[id] ?? Entry()
            entry.holders += 1
            entries[id] = entry
            return entry.lock
        }
        lock.lock()
        defer {
            lock.unlock()
            table.withLock { entries in
                guard var entry = entries[id] else { return }
                entry.holders -= 1
                entries[id] = entry.holders == 0 ? nil : entry
            }
        }
        return try body()
    }
}
