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

/// Folds a burst of rebalance requests into at most one running walk plus one trailing walk.
/// A walk is reactive - every membership broadcast and heartbeat refresh asks for one - and
/// each is a full scan of this node's disk plus a probe per key, so a cluster where several
/// nodes restart together would otherwise stack dozens of concurrent walks. Shared by the plain
/// and the erasure-coded rebalance services, each with its own instance.
actor RebalanceDebouncer {
    private var running = false
    private var pending = false

    /// True when the caller should start a walk now; false when one is already in flight (the
    /// request is folded into a single trailing walk instead).
    func requestStart() -> Bool {
        if running {
            pending = true
            return false
        }
        running = true
        return true
    }

    /// After a walk finishes, whether a coalesced request arrived during it and a trailing walk
    /// should run. Leaves the debouncer running when so, idle otherwise.
    func finish() -> Bool {
        if pending {
            pending = false
            return true
        }
        running = false
        return false
    }
}
