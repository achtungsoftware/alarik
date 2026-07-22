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

/// Access-key expiry is enforced by the cached credential itself, not by the background sweep that
/// eventually deletes the record. Caching a bare secret meant a key stayed fully usable for up to
/// a sweep interval past its own expiry - and longer on any node that missed the removal
/// broadcast - which defeats the entire point of a time-limited credential.
@Suite("Access key expiry tests")
struct AccessKeyExpiryTests {
    @Test("a credential with no expiry is always valid")
    func neverExpiringIsAlwaysValid() {
        let credential = AccessKeyCredential(secretKey: "s", expiresAt: nil)
        #expect(credential.isValid())
        #expect(credential.isValid(now: Date().addingTimeInterval(10 * 365 * 24 * 3600)))
    }

    @Test("a credential is valid strictly before its expiry and not after")
    func expiryBoundary() {
        let expiry = Date()
        let credential = AccessKeyCredential(secretKey: "s", expiresAt: expiry)
        #expect(credential.isValid(now: expiry.addingTimeInterval(-1)))
        // The expiry instant itself is already too late - a key valid "until T" must not sign at T.
        #expect(credential.isValid(now: expiry) == false)
        #expect(credential.isValid(now: expiry.addingTimeInterval(1)) == false)
    }

    @Test("an expired cached key stops being served without waiting for the sweep")
    func expiredCachedKeyIsRefused() async {
        let cache = AccessKeySecretKeyMapCache()
        await cache.add(
            accessKey: "AKIAEXPIRED", secretKey: "expired-secret",
            expiresAt: Date().addingTimeInterval(-1))
        await cache.add(accessKey: "AKIALIVE", secretKey: "live-secret", expiresAt: nil)

        // This is the property that matters: the record is still cached (nothing has swept it),
        // and it is still refused.
        #expect(await cache.secretKey(for: "AKIAEXPIRED") == nil)
        #expect(await cache.exists(accessKey: "AKIAEXPIRED") == false)
        #expect(await cache.cachedValue(for: "AKIAEXPIRED") == nil)

        #expect(await cache.secretKey(for: "AKIALIVE") == "live-secret")
        #expect(await cache.exists(accessKey: "AKIALIVE"))
    }

    @Test("a key cached before its expiry is refused once that expiry passes")
    func keyExpiringWhileCachedIsRefused() async {
        let cache = AccessKeySecretKeyMapCache()
        // Valid when cached, expired a moment later - the exact window the sweep used to own.
        await cache.add(
            accessKey: "AKIASOON", secretKey: "soon-secret",
            expiresAt: Date().addingTimeInterval(0.2))
        #expect(await cache.secretKey(for: "AKIASOON") == "soon-secret")

        try? await Task.sleep(for: .milliseconds(400))
        #expect(await cache.secretKey(for: "AKIASOON") == nil)
    }

    @Test("expired entries still appear in getMap, which reconciles against a record listing")
    func getMapIncludesExpiredEntries() async {
        let cache = AccessKeySecretKeyMapCache()
        await cache.add(
            accessKey: "AKIAEXPIRED", secretKey: "expired-secret",
            expiresAt: Date().addingTimeInterval(-1))
        // The reconcile pass compares cache membership against what the cluster still STORES; an
        // expired-but-undeleted key is a record that exists, so hiding it here would make the
        // reconcile think it had been deleted.
        #expect(await cache.getMap()["AKIAEXPIRED"] == "expired-secret")
    }
}
