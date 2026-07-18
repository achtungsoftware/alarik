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

import Vapor

/// Per-request local-vs-forward decision - the "any node can front any request" mechanism.
/// Called from every `S3Controller` object handler immediately after the existing auth call
/// (never before - a forward must only ever happen for an already-authenticated,
/// already-authorized request) and immediately after bucket/key extraction (before the body is
/// touched - `.forward` needs `req.body` completely unconsumed to stream it onward).
enum ObjectRoutingService {
    enum RoutingDecision {
        /// Serve locally: single-node/non-clustered mode, this node is one of the object's
        /// responsible nodes, or this request already arrived pre-routed from a trusted peer
        /// (`X-Alarik-Cluster-Forwarded`) and must not be routed again. `peers` carries the
        /// *other* responsible nodes (excluding self) - empty whenever there's nothing to
        /// replicate to (not clustered, no peers registered yet) - so a write handler can
        /// unconditionally pass it to `ClusterReplicationService.replicateWrite` after its own
        /// local write succeeds without re-deriving placement a second time.
        case local(peers: [ClusterNodeInfo])
        /// Not this node's responsibility - forward to one of `candidates`, in preference order.
        case forward(candidates: [ClusterNodeInfo])
    }

    static func routingDecision(req: Request, bucketName: String, key: String) async -> RoutingDecision {
        // Already authenticated and routed by a trusted peer - serve locally unconditionally. A
        // forwarded request must never itself be forwarded again (that would either loop or,
        // worse, mask a real membership-view inconsistency instead of surfacing it). Peers are
        // still recomputed below rather than short-circuited to empty here - the coordinator a
        // peer forwarded to is exactly the node responsible for running quorum fan-out.
        let isTrustedForward = ClusterForwardAuthenticator.isTrustedForward(req)

        let (responsible, config) = await placement(req: req, bucketName: bucketName, key: key)
        guard let config else {
            // Not clustered - always local, nothing to fan out to.
            return .local(peers: [])
        }
        guard !responsible.isEmpty else {
            // Cluster mode is on but no node (including this one) has registered/heartbeated
            // yet - a narrow startup race, not a steady state. Serving locally here rather than
            // failing the request is the safe choice: once membership populates, subsequent
            // requests route normally.
            return .local(peers: [])
        }

        let peers = responsible.filter { $0.id != config.nodeId }

        if isTrustedForward || responsible.contains(where: { $0.id == config.nodeId }) {
            return .local(peers: peers)
        }
        return .forward(candidates: responsible)
    }

    /// Like `routingDecision`, but pins to the *primary* (rank-0) responsible node specifically,
    /// never "any of the responsible nodes." Required for every multipart upload operation
    /// (CreateMultipartUpload/UploadPart/UploadPartCopy/CompleteMultipartUpload/
    /// AbortMultipartUpload/ListParts) - unlike a single-shot PUT/GET/DELETE, where any
    /// responsible replica can independently do the full job, an upload's in-progress part
    /// state exists only on whichever *one* node happened to coordinate its `Create` - it's
    /// never synced to the other responsible nodes while the upload is in flight. Letting each
    /// request in the sequence independently ask "am I one of the responsible nodes, then serve
    /// locally" (as `routingDecision` does) can land different requests for the *same* upload on
    /// different, individually-valid-but-inconsistent nodes, since more than one node can
    /// legitimately answer "yes" to that question. Pinning to the primary specifically guarantees
    /// every request in one upload's lifecycle converges on the identical node, as long as
    /// membership doesn't change mid-upload.
    static func multipartRoutingDecision(req: Request, bucketName: String, key: String) async
        -> RoutingDecision
    {
        let isTrustedForward = ClusterForwardAuthenticator.isTrustedForward(req)

        let (responsible, config) = await placement(req: req, bucketName: bucketName, key: key)
        guard let config else {
            return .local(peers: [])
        }
        guard let primary = responsible.first else {
            return .local(peers: [])
        }

        let peers = responsible.filter { $0.id != config.nodeId }

        if isTrustedForward || primary.id == config.nodeId {
            return .local(peers: peers)
        }
        // Only the primary ever holds real upload state - falling back to a secondary on
        // failure would just find nothing there either, so (unlike routingDecision's
        // full-candidate-list forward) there's no fallback candidate worth offering.
        return .forward(candidates: [primary])
    }

    /// Combines a routing decision with the actual forward call, for write handlers that need
    /// `peers` to replicate a local write to - unlike `routingDecision`/`multipartRoutingDecision`
    /// alone, which just return the decision and leave calling `ClusterForwardingClient.forward`
    /// to every caller. This is the write-path counterpart to the read-only `forwardIfNeeded`
    /// helpers already used for GET/HEAD-shaped handlers, and exists specifically so a routing
    /// fix (like pinning multipart operations to the primary) only has to be made in one place
    /// rather than at every "compute peers or forward" call site by hand.
    enum WriteRouting {
        case local(peers: [ClusterNodeInfo])
        case forwarded(Response)
    }

    static func routeForWrite(
        req: Request, bucketName: String, key: String, requirePrimary: Bool = false
    ) async throws -> WriteRouting {
        let decision =
            requirePrimary
            ? await multipartRoutingDecision(req: req, bucketName: bucketName, key: key)
            : await routingDecision(req: req, bucketName: bucketName, key: key)
        switch decision {
        case .local(let peers):
            if !requirePrimary, let redirect = await capacityRedirectTarget(req: req, peers: peers) {
                let candidates = [redirect] + peers.filter { $0.id != redirect.id }
                return .forwarded(
                    try await ClusterForwardingClient.forward(req: req, candidates: candidates))
            }
            return .local(peers: peers)
        case .forward(let candidates):
            return .forwarded(try await ClusterForwardingClient.forward(req: req, candidates: candidates))
        }
    }

    /// Soft capacity-aware coordination redirect - only ever considered for a write that already
    /// resolved to `.local` via the non-primary-pinned path. Multipart operations
    /// (`requirePrimary: true`) are excluded entirely: only the primary ever holds in-progress
    /// upload state, so redirecting `CreateMultipartUpload`'s coordination would strand
    /// subsequent `UploadPart` calls, which independently re-resolve placement and would still
    /// land back on the *original* HRW primary, not wherever this redirected to. A request that
    /// already arrived pre-routed from a trusted peer is also excluded - it must be served
    /// locally unconditionally per `routingDecision`'s own invariant, or a redirect could bounce
    /// a request between two nodes each convinced the other is less full. `peers` is exactly the
    /// true-responsible set minus self that the caller already computed - never re-derived, never
    /// widened, so this can only ever pick one of the same HRW-chosen nodes.
    private static func capacityRedirectTarget(req: Request, peers: [ClusterNodeInfo]) async
        -> ClusterNodeInfo?
    {
        guard !peers.isEmpty, !ClusterForwardAuthenticator.isTrustedForward(req) else { return nil }
        guard let config = req.application.storage[ClusterConfigurationKey.self],
            let selfNode = await ClusterNodeCache.shared.get(id: config.nodeId)
        else { return nil }
        let redirect = ClusterCapacityPolicy.preferredCoordinator(selfNode: selfNode, peers: peers)
        if let redirect {
            req.logger.info(
                "Cluster capacity redirect: \(config.nodeId) is near-full, handing write coordination for \(req.url.path) to \(redirect.address)"
            )
        }
        return redirect
    }

    /// Pure placement check, independent of how the *current* request arrived - for when a
    /// handler needs to know whether this node holds some OTHER key than the one that already
    /// drove the request's own top-level `routingDecision` (e.g. CopyObject's source key, which
    /// may live in a different bucket/key than the destination `routingDecision` already
    /// resolved). Deliberately never consults `X-Alarik-Cluster-Forwarded` - that header
    /// describes the current request's own destination-routing history, which says nothing
    /// about whether this node happens to hold some unrelated source key.
    static func isResponsible(req: Request, bucketName: String, key: String) async -> (
        isLocal: Bool, candidates: [ClusterNodeInfo]
    ) {
        let (responsible, config) = await placement(req: req, bucketName: bucketName, key: key)
        guard let config, !responsible.isEmpty else {
            return (true, [])
        }
        if responsible.contains(where: { $0.id == config.nodeId }) {
            return (true, [])
        }
        return (false, responsible)
    }

    /// Per-key placement decision for a request that already authenticated the client once for
    /// an entire batch (Multi-Object-Delete) and needs to decide, independently for each key,
    /// whether to act as coordinator locally or delegate to a responsible peer. Deliberately
    /// never consults `X-Alarik-Cluster-Forwarded`, for the same reason `isResponsible` doesn't -
    /// the trust marker describes the request's own top-level routing history, not per-key
    /// sub-decisions made while working through a batch.
    static func coordinationTarget(req: Request, bucketName: String, key: String) async -> (
        isLocal: Bool, peers: [ClusterNodeInfo], responsible: [ClusterNodeInfo]
    ) {
        let (responsible, config) = await placement(req: req, bucketName: bucketName, key: key)
        guard let config, !responsible.isEmpty else {
            return (true, [], [])
        }
        if responsible.contains(where: { $0.id == config.nodeId }) {
            let peers = responsible.filter { $0.id != config.nodeId }
            return (true, peers, responsible)
        }
        return (false, [], responsible)
    }

    /// Shared placement lookup: `nil` config means not clustered, empty `responsible` means
    /// cluster mode is on but membership hasn't populated yet - both are the "treat as local"
    /// case for callers, they just react to it slightly differently (see above).
    private static func placement(req: Request, bucketName: String, key: String) async -> (
        responsible: [ClusterNodeInfo], config: ClusterConfiguration?
    ) {
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            return ([], nil)
        }
        let active = await ClusterNodeCache.shared.activeNodes()
        guard !active.isEmpty else {
            return ([], config)
        }
        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: active)
        return (responsible, config)
    }

    /// Erasure-coded write routing, over the full `k+m` responsible set (not `replicationFactor`'s
    /// top-3), pinned to rank-0 - the write-path counterpart to `multipartRoutingDecision`, and
    /// for the identical reason: only one node can safely assign shard indices to nodes for a
    /// given write, or two nodes racing to coordinate the same key could produce conflicting
    /// shard-index-to-node mappings. Reads don't go through this - gather-and-decode is naturally
    /// idempotent from any of the `k+m` nodes, so `routingDecision` (unpinned) is enough for GET.
    enum ErasureCodedRoutingDecision {
        /// Not clustered, or clustering is on but no erasure coding config resolved - erasure
        /// coding never applies here; caller falls back to a plain single-node local write.
        case notClustered
        case local(peers: [ClusterNodeInfo])
        case forward(candidates: [ClusterNodeInfo])
    }

    /// Unlike `placement`'s "membership hasn't populated yet, just serve locally" fallback, an
    /// undersized cluster is a hard failure here: silently writing to fewer than `k+m` nodes
    /// would mean writing a durability guarantee the deployment doesn't actually have. Never
    /// falls back to a plain local write in cluster mode - that would silently produce a single
    /// non-redundant copy where every reader expects `k+m`-way erasure-coded redundancy.
    static func erasureCodedRoutingDecision(
        req: Request, bucketName: String, key: String
    ) async throws -> ErasureCodedRoutingDecision {
        guard let config = req.application.storage[ClusterConfigurationKey.self],
            let ecConfig = req.application.storage[ClusterErasureCodingConfigKey.self]
        else {
            return .notClustered
        }

        let isTrustedForward = ClusterForwardAuthenticator.isTrustedForward(req)
        let active = await ClusterNodeCache.shared.activeNodes()

        do {
            try PlacementService.ensureErasureCodingAdmission(
                activeNodeCount: active.count, dataShards: ecConfig.dataShards,
                parityShards: ecConfig.parityShards)
        } catch let error as PlacementServiceError {
            throw S3Error(
                status: .serviceUnavailable, code: "ServiceUnavailable",
                message: "\(error)", requestId: req.id)
        }

        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: active, count: ecConfig.totalShards)
        guard let primary = responsible.first else {
            // Unreachable in practice - admission above already guarantees
            // activeNodeCount >= totalShards >= 1 - but never silently treat this as success.
            throw S3Error(
                status: .serviceUnavailable, code: "ServiceUnavailable",
                message: "No cluster node is currently available to coordinate this write.",
                requestId: req.id)
        }

        let peers = responsible.filter { $0.id != config.nodeId }

        if isTrustedForward || primary.id == config.nodeId {
            return .local(peers: peers)
        }
        return .forward(candidates: [primary])
    }

    /// `erasureCodedRoutingDecision` combined with the actual forward call - the EC counterpart
    /// of `routeForWrite`. No capacity-redirect: like multipart's primary pinning, EC coordination
    /// is a hard requirement (only rank-0 may assign shard placement for this write), not the
    /// soft any-of-top-3 preference plain replication's redirect exists for.
    enum ErasureCodedWriteRouting {
        case notClustered
        case local(peers: [ClusterNodeInfo])
        case forwarded(Response)
    }

    /// Read-side EC awareness: is self within the *wider* top-(k+m) responsible set, a strict
    /// superset of the plain top-3 set (top-3 is always the first 3 elements of top-(k+m), same
    /// ranked list, different truncation)? Falls back to plain top-3 `isResponsible`-shaped
    /// behavior when EC isn't configured. Unlike write routing, no pinning here - any of the
    /// `k+m` nodes can independently coordinate a read (gather-and-decode is naturally
    /// idempotent), so this is a locality check, not a routing decision with a single target.
    static func erasureCodedReadPlacement(req: Request, bucketName: String, key: String) async -> (
        isLocal: Bool, candidates: [ClusterNodeInfo], responsible: [ClusterNodeInfo]
    ) {
        guard let config = req.application.storage[ClusterConfigurationKey.self] else {
            return (true, [], [])
        }
        let active = await ClusterNodeCache.shared.activeNodes()
        guard !active.isEmpty else { return (true, [], []) }

        let count: Int
        if let ecConfig = req.application.storage[ClusterErasureCodingConfigKey.self] {
            count = Swift.max(PlacementService.replicationFactor, ecConfig.totalShards)
        } else {
            count = PlacementService.replicationFactor
        }
        let responsible = PlacementService.responsibleNodes(
            bucketName: bucketName, key: key, activeNodes: active, count: count)
        if responsible.contains(where: { $0.id == config.nodeId }) {
            return (true, [], responsible)
        }
        return (false, responsible, responsible)
    }

    /// True when self is among the *legacy* top-3 plain-replication placement for the key -
    /// always exactly `responsible`'s first 3 entries (the top-3-is-a-prefix-of-top-(k+m)
    /// invariant every placement decision in this codebase relies on). `erasureCodedReadPlacement`
    /// only proves membership in the wider top-(k+m) set; once k+m > 3, a node can be in that
    /// wider set without holding a legacy plain replica at all, so read-side handlers that fall
    /// through to the plain `.obj` path (nothing erasure-coded found locally) need this check
    /// before trusting a local read.
    static func isLegacyReplica(responsible: [ClusterNodeInfo], selfNodeId: UUID) -> Bool {
        responsible.prefix(PlacementService.replicationFactor).contains { $0.id == selfNodeId }
    }

    static func routeForErasureCodedWrite(
        req: Request, bucketName: String, key: String
    ) async throws -> ErasureCodedWriteRouting {
        switch try await erasureCodedRoutingDecision(req: req, bucketName: bucketName, key: key) {
        case .notClustered:
            return .notClustered
        case .local(let peers):
            return .local(peers: peers)
        case .forward(let candidates):
            return .forwarded(
                try await ClusterForwardingClient.forward(req: req, candidates: candidates))
        }
    }
}
