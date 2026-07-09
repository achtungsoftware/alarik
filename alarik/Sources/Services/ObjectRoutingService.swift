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
}
