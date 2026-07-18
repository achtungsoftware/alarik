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

/// Pure math over `(k, m)` and byte buffers - no DB, no actor, no cluster required.
@Suite("ReedSolomonEngine tests")
struct ReedSolomonEngineTests {
    private func randomShards(count: Int, length: Int, seed: UInt8) -> [Data] {
        (0..<count).map { i in
            Data((0..<length).map { UInt8(truncatingIfNeeded: Int($0) + Int(seed) + i * 7) })
        }
    }

    // MARK: - Round trips

    @Test(
        "encode + reconstruct-from-data-only round-trips across a range of k/m and sizes",
        arguments: [
            (k: 2, m: 2, len: 0), (k: 2, m: 2, len: 1), (k: 4, m: 2, len: 256),
            (k: 6, m: 3, len: 4096), (k: 1, m: 1, len: 17), (k: 10, m: 4, len: 8191),
        ]
    )
    func roundTripsAcrossSizes(k: Int, m: Int, len: Int) throws {
        let dataShards = randomShards(count: k, length: len, seed: 42)
        let parity = try ReedSolomonEngine.encode(dataShards: dataShards, parityCount: m)
        #expect(parity.count == m)
        #expect(parity.allSatisfy { $0.count == len })

        // Systematic property: with no shard missing, the first k shards ARE the input.
        var available: [Int: Data] = [:]
        for (i, shard) in dataShards.enumerated() { available[i] = shard }
        for (i, shard) in parity.enumerated() { available[k + i] = shard }
        #expect((0..<k).allSatisfy { available[$0] == dataShards[$0] })
    }

    @Test("reconstructing a single missing data shard from survivors recovers it exactly")
    func reconstructsSingleMissingDataShard() throws {
        let k = 4
        let m = 2
        let dataShards = randomShards(count: k, length: 512, seed: 7)
        let parity = try ReedSolomonEngine.encode(dataShards: dataShards, parityCount: m)

        var available: [Int: Data] = [:]
        for (i, shard) in dataShards.enumerated() where i != 1 { available[i] = shard }
        for (i, shard) in parity.enumerated() { available[k + i] = shard }

        let recovered = try ReedSolomonEngine.reconstruct(
            availableShards: available, missingIndices: [1], dataCount: k, parityCount: m)
        #expect(recovered[1] == dataShards[1])
    }

    @Test("reconstructing a missing parity shard from data recovers exactly what encode produced")
    func reconstructsMissingParityShard() throws {
        let k = 3
        let m = 3
        let dataShards = randomShards(count: k, length: 300, seed: 99)
        let parity = try ReedSolomonEngine.encode(dataShards: dataShards, parityCount: m)

        var available: [Int: Data] = [:]
        for (i, shard) in dataShards.enumerated() { available[i] = shard }
        for (i, shard) in parity.enumerated() where i != 0 { available[k + i] = shard }

        let recovered = try ReedSolomonEngine.reconstruct(
            availableShards: available, missingIndices: [k], dataCount: k, parityCount: m)
        #expect(recovered[k] == parity[0])
    }

    @Test("reconstructing from an all-parity survivor set (all k data shards missing) still works")
    func reconstructsFromAllParitySurvivors() throws {
        let k = 4
        let m = 4
        let dataShards = randomShards(count: k, length: 777, seed: 3)
        let parity = try ReedSolomonEngine.encode(dataShards: dataShards, parityCount: m)

        // Only the m=4 parity shards survive; k=4 <= m=4, so this is exactly enough.
        var available: [Int: Data] = [:]
        for (i, shard) in parity.enumerated() { available[k + i] = shard }

        let missing = Array(0..<k)
        let recovered = try ReedSolomonEngine.reconstruct(
            availableShards: available, missingIndices: missing, dataCount: k, parityCount: m)
        for i in 0..<k {
            #expect(recovered[i] == dataShards[i])
        }
    }

    @Test("reconstructing simultaneously-missing data and parity shards recovers both correctly")
    func reconstructsMixedMissingSet() throws {
        let k = 5
        let m = 3
        let dataShards = randomShards(count: k, length: 1000, seed: 55)
        let parity = try ReedSolomonEngine.encode(dataShards: dataShards, parityCount: m)

        var available: [Int: Data] = [:]
        for (i, shard) in dataShards.enumerated() where i != 2 && i != 4 { available[i] = shard }
        for (i, shard) in parity.enumerated() where i != 1 { available[k + i] = shard }

        let recovered = try ReedSolomonEngine.reconstruct(
            availableShards: available, missingIndices: [2, 4, k + 1], dataCount: k, parityCount: m)
        #expect(recovered[2] == dataShards[2])
        #expect(recovered[4] == dataShards[4])
        #expect(recovered[k + 1] == parity[1])
    }

    @Test("zero-byte object round-trips through encode and reconstruct with no special-casing")
    func zeroByteObjectRoundTrips() throws {
        let k = 4
        let m = 2
        let dataShards = Array(repeating: Data(), count: k)
        let parity = try ReedSolomonEngine.encode(dataShards: dataShards, parityCount: m)
        #expect(parity.allSatisfy { $0.isEmpty })

        var available: [Int: Data] = [:]
        for (i, shard) in parity.enumerated() { available[k + i] = shard }
        for i in 1..<k { available[i] = dataShards[i] }

        let recovered = try ReedSolomonEngine.reconstruct(
            availableShards: available, missingIndices: [0], dataCount: k, parityCount: m)
        #expect(recovered[0] == Data())
    }

    // MARK: - Corruption / detection is StripeDecoder's job, this only proves math correctness

    @Test("reconstructing with corrupted (silently wrong) survivor data yields the wrong output")
    func corruptedSurvivorPropagatesIncorrectResult() throws {
        // Not a checksum test (that's Phase 3's job) - just documents that this engine trusts
        // its inputs completely; corruption detection must happen one layer up.
        let k = 3
        let m = 2
        let dataShards = randomShards(count: k, length: 64, seed: 1)
        let parity = try ReedSolomonEngine.encode(dataShards: dataShards, parityCount: m)

        var available: [Int: Data] = [:]
        available[0] = Data(repeating: 0xFF, count: 64)  // corrupted, but same length
        available[1] = dataShards[1]
        for (i, shard) in parity.enumerated() { available[k + i] = shard }

        let recovered = try ReedSolomonEngine.reconstruct(
            availableShards: available, missingIndices: [2], dataCount: k, parityCount: m)
        #expect(recovered[2] != dataShards[2])
    }

    // MARK: - Validation

    @Test("encode rejects mismatched-length shards")
    func encodeRejectsSizeMismatch() {
        let shards = [Data(count: 10), Data(count: 20)]
        #expect(throws: ReedSolomonEngine.EngineError.self) {
            try ReedSolomonEngine.encode(dataShards: shards, parityCount: 2)
        }
    }

    @Test("encode rejects zero parity shards")
    func encodeRejectsZeroParity() {
        #expect(throws: ReedSolomonEngine.EngineError.self) {
            try ReedSolomonEngine.encode(dataShards: [Data(count: 10)], parityCount: 0)
        }
    }

    @Test("reconstruct rejects too few surviving shards")
    func reconstructRejectsTooFewSurvivors() {
        #expect(throws: ReedSolomonEngine.EngineError.self) {
            try ReedSolomonEngine.reconstruct(
                availableShards: [0: Data(count: 10)], missingIndices: [1],
                dataCount: 4, parityCount: 2)
        }
    }

    @Test("reconstruct rejects an out-of-range missing index")
    func reconstructRejectsOutOfRangeIndex() {
        let available: [Int: Data] = [0: Data(count: 8), 1: Data(count: 8), 2: Data(count: 8)]
        #expect(throws: ReedSolomonEngine.EngineError.self) {
            try ReedSolomonEngine.reconstruct(
                availableShards: available, missingIndices: [99], dataCount: 2, parityCount: 2)
        }
    }

    @Test("reconstruct rejects a missing index that's already available")
    func reconstructRejectsAlreadyAvailableIndex() {
        let available: [Int: Data] = [0: Data(count: 8), 1: Data(count: 8), 2: Data(count: 8)]
        #expect(throws: ReedSolomonEngine.EngineError.self) {
            try ReedSolomonEngine.reconstruct(
                availableShards: available, missingIndices: [0], dataCount: 2, parityCount: 2)
        }
    }
}
