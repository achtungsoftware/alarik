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

/// One node's half of the distributed claim used to make "create only if the name is free"
/// (`MetadataStore.putIfAbsent`) safe across nodes.
///
/// A node grants at most one live reservation per `(collection, id)`. A claimant needs a majority
/// of the record's owners to grant, so two claimants racing the same name can never both proceed:
/// the second cannot reach a majority. Reservations expire so a claimant that dies mid-claim
/// can't block the name forever.
actor MetadataClaimRegistry {
    static let shared = MetadataClaimRegistry()

    /// Long enough to cover a claim's check-then-write, short enough that a crashed claimant only
    /// blocks the name briefly.
    static let defaultTTL: TimeInterval = 10

    private struct Reservation {
        let token: UUID
        let expiresAt: Date
    }

    private var reservations: [String: Reservation] = [:]

    private func slot(_ collection: String, _ id: String) -> String {
        "\(collection)\u{0}\(id)"
    }

    /// Grants the reservation unless another claimant holds a live one. Re-granting the same
    /// token is allowed so a retry doesn't lock the claimant out of its own reservation.
    func reserve(
        collection: String, id: String, token: UUID,
        ttl: TimeInterval = MetadataClaimRegistry.defaultTTL, now: Date = Date()
    ) -> Bool {
        let key = slot(collection, id)
        if let existing = reservations[key], existing.expiresAt > now, existing.token != token {
            return false
        }
        reservations[key] = Reservation(token: token, expiresAt: now.addingTimeInterval(ttl))
        return true
    }

    /// Releases only if `token` still owns the slot, so a late release can't free somebody else's
    /// reservation.
    func release(collection: String, id: String, token: UUID) {
        let key = slot(collection, id)
        guard reservations[key]?.token == token else { return }
        reservations.removeValue(forKey: key)
    }

    /// Drops expired entries - called on reserve paths so the map tracks live claims only.
    func purgeExpired(now: Date = Date()) {
        reservations = reservations.filter { $0.value.expiresAt > now }
    }

    func liveReservationCount(now: Date = Date()) -> Int {
        reservations.values.filter { $0.expiresAt > now }.count
    }
}
