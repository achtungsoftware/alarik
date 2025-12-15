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

/// Alarik version string (updated by publish.sh)
public let alarikVersion = "1.0.0-alpha-11"

/// Global hex lookup table for optimal performance
public let hexLookupTable: InlineArray<16, UInt8> = [
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,  // 0-7
    0x38, 0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66   // 8-9, a-f
]