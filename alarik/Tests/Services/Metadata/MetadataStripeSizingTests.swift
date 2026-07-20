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

@Suite("MetadataStripeSizing tests")
struct MetadataStripeSizingTests {
    @Test("a tiny record floors at the minimum stripe unit size")
    func tinyRecordFloorsAtMinimum() {
        let size = MetadataStripeSizing.chooseStripeUnitSize(payloadSize: 200, dataShards: 2)
        #expect(size == Constants.metadataMinStripeUnitSize)
    }

    @Test("a payload just above the floor rounds up to a per-shard size still at the floor")
    func smallRecordStillFloors() {
        // 200 bytes / 2 shards = 100 bytes/shard, far below the 4096-byte floor.
        let size = MetadataStripeSizing.chooseStripeUnitSize(payloadSize: 8000, dataShards: 2)
        // 8000 / 2 = 4000, still below the 4096 floor.
        #expect(size == Constants.metadataMinStripeUnitSize)
    }

    @Test("a payload large enough per-shard uses ceil(payloadSize / dataShards)")
    func largerRecordUsesPerShardCeiling() {
        // 20000 bytes / 2 shards = 10000 bytes/shard, above the floor, below the cap.
        let size = MetadataStripeSizing.chooseStripeUnitSize(payloadSize: 20_000, dataShards: 2)
        #expect(size == 10_000)
    }

    @Test("rounds up rather than truncating when payloadSize doesn't divide evenly")
    func roundsUpOnUnevenDivision() {
        // 20001 / 2 = 10000.5 -> ceil = 10001, still exercised above the floor.
        let size = MetadataStripeSizing.chooseStripeUnitSize(payloadSize: 20_001, dataShards: 2)
        #expect(size == 10_001)
    }

    @Test("an unusually large metadata blob is capped at the object-data default")
    func largeBlobCapsAtDefault() {
        let size = MetadataStripeSizing.chooseStripeUnitSize(
            payloadSize: 100 * 1024 * 1024, dataShards: 2)
        #expect(size == Constants.erasureCodingStripeUnitSize)
    }

    @Test("zero-byte payload falls back to the floor rather than dividing by a meaningless size")
    func zeroPayloadFallsBackToFloor() {
        let size = MetadataStripeSizing.chooseStripeUnitSize(payloadSize: 0, dataShards: 2)
        #expect(size == Constants.metadataMinStripeUnitSize)
    }

    @Test("zero dataShards falls back to the floor rather than dividing by zero")
    func zeroDataShardsFallsBackToFloor() {
        let size = MetadataStripeSizing.chooseStripeUnitSize(payloadSize: 1000, dataShards: 0)
        #expect(size == Constants.metadataMinStripeUnitSize)
    }
}
