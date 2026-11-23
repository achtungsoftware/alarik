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

@Suite("DiskSpace tests")
struct DiskSpaceTests {
    @Test("hasRoom keeps the floor after the upload, and fails open on unknown capacity")
    func floor() {
        let total: Int64 = 1_000
        #expect(DiskSpace.hasRoom(total: total, available: 100, declaredLength: 50, minFreePercent: 1))
        #expect(!DiskSpace.hasRoom(total: total, available: 100, declaredLength: 95, minFreePercent: 1))
        #expect(!DiskSpace.hasRoom(total: total, available: 5, declaredLength: nil, minFreePercent: 1))
        #expect(DiskSpace.hasRoom(total: 0, available: 0, declaredLength: 10_000, minFreePercent: 1))
    }

    @Test("disk-full errors from every storage layer are classified as InsufficientStorage")
    func outOfSpaceClassification() {
        #expect(S3ErrorMiddleware.isOutOfSpace(AtomicObjectWriter.WriteError.writeFailed(errno: ENOSPC)))
        #expect(S3ErrorMiddleware.isOutOfSpace(AtomicObjectWriter.WriteError.fsyncFailed(errno: EDQUOT)))
        #expect(!S3ErrorMiddleware.isOutOfSpace(AtomicObjectWriter.WriteError.writeFailed(errno: EIO)))
        #expect(S3ErrorMiddleware.isOutOfSpace(NSError(domain: NSPOSIXErrorDomain, code: Int(ENOSPC))))
        #expect(S3ErrorMiddleware.isOutOfSpace(CocoaError(.fileWriteOutOfSpace)))
        #expect(!S3ErrorMiddleware.isOutOfSpace(NSError(domain: "Other", code: 1)))
    }
}
