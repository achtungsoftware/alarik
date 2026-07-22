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
/// Who gets to vote on a claim, and how many votes carry it.
///
/// A pure value rather than arithmetic inline in `MetadataStore.withClaimQuorum`, because the
/// rule is subtle and getting it wrong reintroduces duplicate names silently: **only the record's
/// owners vote**. A node can end up coordinating a key it does not own - every owner unreachable,
/// or a forwarded write whose sender resolved a membership view this node disagrees with - and
/// counting its own reservation there would let two such coordinators each pair their local grant
/// with a *different* single owner and both claim the same name.
struct ClaimElectorate: Equatable {
    /// Grants needed to carry the claim.
    let required: Int
    /// Whether this node's own reservation is one of those grants.
    let localVotes: Bool

    init(ownerIds: [UUID], selfNodeId: UUID?) {
        // No owners at all is the standalone (non-clustered) case: this node is the whole
        // cluster, so its own grant is trivially a majority of one.
        guard !ownerIds.isEmpty else {
            self.required = 1
            self.localVotes = true
            return
        }
        self.required = ownerIds.count / 2 + 1
        self.localVotes = selfNodeId.map(ownerIds.contains) ?? false
    }
}

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
