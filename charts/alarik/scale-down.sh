#!/usr/bin/env bash
#
# Safely shrink an Alarik StatefulSet.
#
# WHY THIS EXISTS
#
# `kubectl scale --replicas=N` just deletes the highest-ordinal pods. Their PersistentVolumeClaims
# are retained, but nothing tells the cluster the data should move first - so the shards those pods
# held are simply gone from the live set, leaving every object they contributed to under-replicated
# until a manual resync notices. With enough shards lost at once, objects become unreadable.
#
# Draining first is what makes the difference: it excludes the node from new placement and migrates
# its existing data onto the remaining nodes. Only once that finishes is removing the pod free.
#
# WHY IT ISN'T AUTOMATIC ON SIGTERM
#
# Kubernetes sends an identical SIGTERM for "rolling update, back in 10 seconds with the same
# volume" and "scaled down, gone for good". Draining on every SIGTERM would make each rolling
# update needlessly re-replicate the entire cluster; never draining leaves scale-down unsafe. The
# pod cannot tell the two apart, so removal-for-good is an explicit operator action - this script.
#
# Usage:
#   ./scale-down.sh <release-name> <target-replicas> [namespace]
#
# Example:
#   ./scale-down.sh alarik 3 storage      # 4 -> 3 replicas, draining alarik-3 first

set -euo pipefail

RELEASE="${1:?usage: scale-down.sh <release-name> <target-replicas> [namespace]}"
TARGET="${2:?usage: scale-down.sh <release-name> <target-replicas> [namespace]}"
NAMESPACE="${3:-default}"

KUBECTL=(kubectl --namespace "$NAMESPACE")

current=$("${KUBECTL[@]}" get statefulset "$RELEASE" -o jsonpath='{.spec.replicas}')
if [ "$TARGET" -ge "$current" ]; then
    echo "Target ($TARGET) is not below current ($current). Scaling up needs no drain - just:"
    echo "  kubectl --namespace $NAMESPACE scale statefulset $RELEASE --replicas=$TARGET"
    exit 1
fi

# Erasure coding needs one node per shard, so shrinking below k+m makes every subsequent write
# fail. Refuse rather than let the cluster discover that on its next upload.
data_shards=$("${KUBECTL[@]}" get configmap "${RELEASE}-config" -o jsonpath='{.data.CLUSTER_EC_DATA_SHARDS}' 2>/dev/null || echo "")
parity_shards=$("${KUBECTL[@]}" get configmap "${RELEASE}-config" -o jsonpath='{.data.CLUSTER_EC_PARITY_SHARDS}' 2>/dev/null || echo "")
if [ -n "$data_shards" ] && [ -n "$parity_shards" ]; then
    required=$((data_shards + parity_shards))
    if [ "$TARGET" -lt "$required" ]; then
        echo "ERROR: $TARGET replicas is below k+m ($data_shards+$parity_shards=$required)."
        echo "       Every write would be refused. Lower the shard counts first, or keep >= $required replicas."
        exit 1
    fi
fi

echo "Scaling $RELEASE: $current -> $TARGET replicas in namespace $NAMESPACE"
echo "Pods to drain (highest ordinal first): $(seq $((current - 1)) -1 "$TARGET" | tr '\n' ' ')"
echo ""

# Admin credentials come from the release's own Secret, so the operator doesn't have to pass them.
admin_user=$("${KUBECTL[@]}" get configmap "${RELEASE}-config" -o jsonpath='{.data.ADMIN_USERNAME}')
admin_pass=$("${KUBECTL[@]}" get secret "${RELEASE}-auth" -o jsonpath='{.data.ADMIN_PASSWORD}' | base64 -d)

# Every call runs from inside pod 0 - it already has network access to the cluster API and to its
# peers, so no port-forward or external ingress is needed.
exec_in_pod0() {
    "${KUBECTL[@]}" exec "${RELEASE}-0" -- "$@"
}

echo "Authenticating as $admin_user..."
token=$(exec_in_pod0 curl -s -X POST "http://localhost:8080/api/v1/users/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$admin_user\",\"password\":\"$admin_pass\"}" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')

if [ -z "$token" ]; then
    echo "ERROR: could not authenticate against ${RELEASE}-0."
    exit 1
fi

for ordinal in $(seq $((current - 1)) -1 "$TARGET"); do
    pod="${RELEASE}-${ordinal}"
    echo ""
    echo "── Draining $pod ──"

    # Resolve the pod's node id by its advertised address, since the drain endpoint addresses
    # nodes by id rather than by hostname. Split the array into one line per node object first,
    # then match the object whose address is THIS pod's - so the id is read from the same object,
    # never a neighbouring one. The trailing '\.' anchors the match so alarik-4 can't also match
    # alarik-40. Draining the wrong node here would silently decommission a healthy one.
    node_id=$(exec_in_pod0 curl -s "http://localhost:8080/api/v1/admin/cluster/nodes" \
        -H "Authorization: Bearer $token" \
        | sed 's/},{/}\n{/g' \
        | grep "\"address\":\"http[^\"]*${pod}\." \
        | sed -n 's/.*"id":"\([^"]*\)".*/\1/p' | head -1)

    if [ -z "$node_id" ]; then
        echo "  WARNING: could not resolve a node id for $pod - it may already be gone. Skipping."
        continue
    fi

    echo "  node id: $node_id"
    exec_in_pod0 curl -s -X POST "http://localhost:8080/api/v1/admin/cluster/nodes/$node_id/drain" \
        -H "Authorization: Bearer $token" >/dev/null
    echo "  drain requested; waiting for data to migrate off..."

    # Drain completes when the pod holds no object shards left to migrate. That is the
    # authoritative signal, and it must be counted on the draining pod ITSELF, not inferred from
    # the outbox queues: a drain sheds erasure-coded shards by handing each one directly to its
    # rightful owner (an inline push, not a queued task), so those hand-offs never appear in
    # rebalance/erasure-coding status at all. Waiting on the queues alone would declare the drain
    # finished while shards were still on disk here.
    #
    # `.alarik.sys` is excluded on purpose: control-plane metadata is replicated (whole copies),
    # never reclaimed off a draining node, and stays until the pod is actually removed - so it is
    # not part of "has the object data migrated off".
    #
    # The queues are still checked, to catch reconstruct work other nodes scheduled in response to
    # the drain. Bounded by how much data the pod holds, not a fixed timeout - 30 minutes.
    exec_in_drained_pod() {
        "${KUBECTL[@]}" exec "$pod" -- "$@"
    }
    drained=0
    for _ in $(seq 1 360); do
        shards=$(exec_in_drained_pod sh -c \
            'find /app/Storage/buckets -name "*.ecshard" -not -path "*/.alarik.sys/*" 2>/dev/null | wc -l' \
            2>/dev/null | tr -d '[:space:]')

        rebalance=$(exec_in_pod0 curl -s "http://localhost:8080/api/v1/admin/cluster/rebalance/status" \
            -H "Authorization: Bearer $token")
        erasure=$(exec_in_pod0 curl -s "http://localhost:8080/api/v1/admin/cluster/erasure-coding/status" \
            -H "Authorization: Bearer $token")

        r_pending=$(echo "$rebalance" | sed -n 's/.*"pendingCount":\([0-9]*\).*/\1/p')
        r_failed=$(echo "$rebalance" | sed -n 's/.*"failedCount":\([0-9]*\).*/\1/p')
        e_pending=$(echo "$erasure" | sed -n 's/.*"pendingCount":\([0-9]*\).*/\1/p')
        e_failed=$(echo "$erasure" | sed -n 's/.*"failedCount":\([0-9]*\).*/\1/p')

        # An unparseable value means the node was momentarily unreachable, NOT that there is no
        # work left - treat it as outstanding so a blip can never be read as "drain complete".
        shards=${shards:-1}
        r_pending=${r_pending:-1}; r_failed=${r_failed:-1}
        e_pending=${e_pending:-1}; e_failed=${e_failed:-1}

        printf "\r  shards on %s: %s | replication q: %s/%s | erasure coding q: %s/%s      " \
            "$pod" "$shards" "$r_pending" "$r_failed" "$e_pending" "$e_failed"

        if [ "$shards" = "0" ] \
            && [ "$r_pending" = "0" ] && [ "$r_failed" = "0" ] \
            && [ "$e_pending" = "0" ] && [ "$e_failed" = "0" ]; then
            echo ""
            echo "  $pod drained - no object shards remain on it."
            drained=1
            break
        fi
        sleep 5
    done

    if [ "$drained" != "1" ]; then
        echo ""
        echo "ERROR: $pod did not finish draining. Its data has NOT fully migrated, so removing it"
        echo "       now would leave objects under-replicated. Investigate before scaling:"
        echo "         kubectl --namespace $NAMESPACE exec $pod -- sh -c 'find /app/Storage/buckets -name \"*.ecshard\" -not -path \"*/.alarik.sys/*\" | wc -l'"
        exit 1
    fi

    # Flip the node from `draining` to `removed` now that its data is off it. This stops the
    # surviving cluster from probing its soon-dead address on every metadata listing (which would
    # otherwise keep those listings perpetually "incomplete"). A node that later restarts under
    # the same identity re-registers itself as active, so this isn't a permanent tombstone.
    exec_in_pod0 curl -s -X POST \
        "http://localhost:8080/api/v1/admin/cluster/nodes/$node_id/decommission" \
        -H "Authorization: Bearer $token" >/dev/null 2>&1
    echo "  $pod decommissioned (status -> removed)."
done

echo ""
echo "── All target pods drained. Scaling the StatefulSet ──"
"${KUBECTL[@]}" scale statefulset "$RELEASE" --replicas="$TARGET"

echo ""
echo "Done. The removed pods' PersistentVolumeClaims are RETAINED by Kubernetes - StatefulSets"
echo "never delete them automatically, so scaling back up reuses the same volumes."
echo "Their data has already been migrated, so they are safe to delete once you are satisfied:"
for ordinal in $(seq $((current - 1)) -1 "$TARGET"); do
    echo "  kubectl --namespace $NAMESPACE delete pvc storage-${RELEASE}-${ordinal}"
done
