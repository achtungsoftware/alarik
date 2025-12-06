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

extension Data {
    func hexString() -> String {
        // Optimized hex conversion using global lookup table
        var bytes = [UInt8]()
        bytes.reserveCapacity(self.count * 2)

        for byte in self {
            bytes.append(hexLookupTable[Int(byte >> 4)])
            bytes.append(hexLookupTable[Int(byte & 0x0F)])
        }

        return String(bytes: bytes, encoding: .ascii)!
    }

    /// Constant-time comparison to prevent timing attacks
    /// Compares the hex representation of this Data to a hex string
    func constantTimeCompare(to other: String) -> Bool {
        // Each byte becomes 2 hex chars
        guard self.count * 2 == other.count else { return false }

        var result: UInt8 = 0

        // Use withContiguousStorageIfAvailable to avoid iterator overhead
        let otherResult = other.utf8.withContiguousStorageIfAvailable { otherBuffer -> UInt8 in
            guard otherBuffer.count == self.count * 2 else { return 1 }
            var r: UInt8 = 0
            var idx = 0
            for byte in self {
                let hi = hexLookupTable[Int(byte >> 4)]
                let lo = hexLookupTable[Int(byte & 0x0F)]
                r |= hi ^ otherBuffer[idx]
                r |= lo ^ otherBuffer[idx + 1]
                idx += 2
            }
            return r
        }

        if let r = otherResult {
            result = r
        } else {
            // Fallback for non-contiguous strings (rare)
            let otherUTF8 = Array(other.utf8)
            guard otherUTF8.count == self.count * 2 else { return false }
            var idx = 0
            for byte in self {
                let hi = hexLookupTable[Int(byte >> 4)]
                let lo = hexLookupTable[Int(byte & 0x0F)]
                result |= hi ^ otherUTF8[idx]
                result |= lo ^ otherUTF8[idx + 1]
                idx += 2
            }
        }

        return result == 0
    }
}
