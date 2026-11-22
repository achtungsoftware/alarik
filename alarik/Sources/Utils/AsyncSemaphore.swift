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

/// A counting semaphore for Swift concurrency: bounds how many tasks run a section at once,
/// queueing the rest in FIFO order. Use it to keep an unbounded burst of requests (a page of
/// console rows each asking for a directory walk) from turning into an unbounded burst of
/// expensive work.
actor AsyncSemaphore {
    private var permits: Int
    private var waiters: [(id: UInt64, continuation: CheckedContinuation<Void, any Error>)] = []

    init(permits: Int) {
        precondition(permits > 0, "AsyncSemaphore needs at least one permit")
        self.permits = permits
    }

    private var nextWaiterId: UInt64 = 0

    /// Runs `body` once a permit is free, releasing it afterwards - also on throw. A task
    /// cancelled while waiting throws `CancellationError` without ever taking a permit, so
    /// abandoned requests (a console page navigated away from) don't hold up live ones.
    func withPermit<T: Sendable>(_ body: @Sendable () async throws -> T) async throws -> T {
        try await acquire()
        defer { release() }
        return try await body()
    }

    private func acquire() async throws {
        try Task.checkCancellation()
        if permits > 0 {
            permits -= 1
            return
        }
        let id = nextWaiterId
        nextWaiterId += 1
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                waiters.append((id, continuation))
            }
        } onCancel: {
            Task { await self.cancelWaiter(id) }
        }
    }

    private func cancelWaiter(_ id: UInt64) {
        guard let index = waiters.firstIndex(where: { $0.id == id }) else { return }
        waiters.remove(at: index).continuation.resume(throwing: CancellationError())
    }

    private func release() {
        if waiters.isEmpty {
            permits += 1
        } else {
            // Hand the permit straight to the next waiter; the count never goes up in between.
            waiters.removeFirst().continuation.resume()
        }
    }
}
