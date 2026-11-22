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

export type LazyStat<T> = T | "loading" | "error";

/**
 * Per-row figures that are expensive for the server to produce (bucket and folder sizes are a
 * recursive walk each). Rows ask for their figure with `request`; at most `concurrency` loads
 * run at once and the rest wait in FIFO order, so a page of a hundred rows is a trickle of
 * requests instead of a burst (issue #28). `reset` forgets everything and discards the results
 * of loads still in flight, for navigation to a different listing.
 */
export function useLazyStats<T>(concurrency = 2) {
    const states = ref<Record<string, LazyStat<T>>>({});
    const queue: { key: string; load: () => Promise<T> }[] = [];
    let active = 0;
    // Bumped by reset(): a load started under an older generation must not write its result.
    let generation = 0;

    function pump() {
        while (active < concurrency && queue.length > 0) {
            const { key, load } = queue.shift()!;
            const startedIn = generation;
            active++;
            load()
                .then((value) => {
                    if (startedIn === generation) states.value[key] = value;
                })
                .catch(() => {
                    if (startedIn === generation) states.value[key] = "error";
                })
                .finally(() => {
                    active--;
                    pump();
                });
        }
    }

    function request(key: string, load: () => Promise<T>) {
        if (states.value[key] !== undefined) return;
        states.value[key] = "loading";
        queue.push({ key, load });
        pump();
    }

    function reset() {
        generation++;
        queue.length = 0;
        states.value = {};
    }

    return { states, request, reset };
}
