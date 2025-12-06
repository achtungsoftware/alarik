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

import Crypto
import Foundation

extension Digest {
    func hexString() -> String {
        // Optimized hex conversion using global lookup table
        var bytes = [UInt8]()
        bytes.reserveCapacity(64)  // SHA256 produces 32 bytes = 64 hex chars

        for byte in self {
            bytes.append(hexLookupTable[Int(byte >> 4)])
            bytes.append(hexLookupTable[Int(byte & 0x0F)])
        }

        return String(bytes: bytes, encoding: .ascii)!
    }
}
