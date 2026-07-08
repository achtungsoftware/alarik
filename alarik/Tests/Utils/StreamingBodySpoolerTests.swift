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

/// Locks in exactly when `StreamingBodySpooler` computes the whole-body SHA256 - it must be
/// computed whenever the deferred payload-hash check will run, and skipped in every case
/// where that check is a no-op. A production CPU profile of a PUT-heavy workload found this
/// computed unconditionally, wasting ~50% of total sampled CPU time hashing bytes on the
/// aws-chunked path that were never compared to anything (chunk signatures already verify
/// that path's integrity per-chunk). Getting the condition wrong in the other direction -
/// skipping a check that should run - would silently accept a corrupted or tampered payload,
/// so both directions are worth pinning down explicitly.
@Suite("StreamingBodySpooler whole-body SHA256 gating")
struct StreamingBodySpoolerTests {

    @Test("aws-chunked bodies never need the whole-body hash - chunk signatures already cover it")
    func chunkedNeverNeedsWholeBodyHash() {
        #expect(
            !StreamingBodySpooler.needsWholeBodyHashVerification(
                isChunked: true, isQueryAuth: false, declaredSha: "deadbeef"))
        #expect(
            !StreamingBodySpooler.needsWholeBodyHashVerification(
                isChunked: true, isQueryAuth: false, declaredSha: nil))
    }

    @Test("query-auth (presigned URL) requests never need it - the payload isn't signed")
    func queryAuthNeverNeedsWholeBodyHash() {
        #expect(
            !StreamingBodySpooler.needsWholeBodyHashVerification(
                isChunked: false, isQueryAuth: true, declaredSha: "deadbeef"))
    }

    @Test("no declared hash means nothing to check against")
    func missingDeclaredShaNeedsNoHash() {
        #expect(
            !StreamingBodySpooler.needsWholeBodyHashVerification(
                isChunked: false, isQueryAuth: false, declaredSha: nil))
    }

    @Test("UNSIGNED-PAYLOAD means nothing to check against")
    func unsignedPayloadNeedsNoHash() {
        #expect(
            !StreamingBodySpooler.needsWholeBodyHashVerification(
                isChunked: false, isQueryAuth: false, declaredSha: "UNSIGNED-PAYLOAD"))
    }

    @Test("a real declared hash on a plain buffered header-auth request DOES need verification")
    func realDeclaredShaOnPlainRequestNeedsVerification() {
        #expect(
            StreamingBodySpooler.needsWholeBodyHashVerification(
                isChunked: false, isQueryAuth: false,
                declaredSha: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"))
    }
}
