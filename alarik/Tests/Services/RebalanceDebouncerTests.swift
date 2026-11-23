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

import Testing

@testable import Alarik

@Suite("Rebalance debouncer")
struct RebalanceDebouncerTests {
    @Test("a burst of requests runs one walk plus one trailing walk, then goes idle")
    func burstCoalesces() async {
        let debouncer = RebalanceDebouncer()
        #expect(await debouncer.requestStart())
        // Three more requests while running: all folded into a single trailing walk.
        #expect(await debouncer.requestStart() == false)
        #expect(await debouncer.requestStart() == false)
        #expect(await debouncer.requestStart() == false)
        #expect(await debouncer.finish() == true)
        // The trailing walk finishes with nothing new pending: idle again.
        #expect(await debouncer.finish() == false)
        #expect(await debouncer.requestStart())
        #expect(await debouncer.finish() == false)
    }
}
