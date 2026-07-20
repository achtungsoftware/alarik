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

/// Chooses a right-sized Reed-Solomon stripe unit for a metadata record, instead of blindly
/// using the default `Constants.erasureCodingStripeUnitSize` (256 KiB) sized for bulk object
/// data - a small metadata record padded to a full object-sized stripe would waste disk space,
/// a bad fit for collections (like the outbox tables) that can hold thousands of small records.
/// `ErasureCodedShardHeader.stripeUnitSize`/`StripeDecoder` record and read back whatever size
/// encode chose per shard, so only encode needs to pick a smaller one.
enum MetadataStripeSizing {
    /// `ceil(payloadSize / dataShards)`, floored at `Constants.metadataMinStripeUnitSize`
    /// (filesystem block-size alignment - avoids degenerate sub-block stripe units) and capped at
    /// the existing object-data default (an unusually large metadata blob - e.g. a big
    /// bucket-policy JSON - never exceeds today's normal stripe size).
    static func chooseStripeUnitSize(payloadSize: Int, dataShards: Int) -> Int {
        guard dataShards > 0, payloadSize > 0 else { return Constants.metadataMinStripeUnitSize }
        let perShard = (payloadSize + dataShards - 1) / dataShards
        return min(
            max(perShard, Constants.metadataMinStripeUnitSize),
            Constants.erasureCodingStripeUnitSize)
    }
}
