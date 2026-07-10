<script lang="ts" setup>
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

import { h, resolveComponent } from "vue";
import type { TableColumn } from "@nuxt/ui";

definePageMeta({
    layout: "dashboard",
});

useHead({
    title: "Cluster",
});

const jwtCookie = useJWTCookie();
const toast = useToast();
const { confirm } = useConfirmDialog();
const authHeaders = computed(() => ({ Authorization: `Bearer ${jwtCookie.value}` }));
const apiBase = computed(() => useRuntimeConfig().public.apiBaseUrl);

// A fixed palette cycled by each node's position in `nodes` (stable - sorted by joinedAt server-
// side) - gives every node a consistent color across the storage chart, its legend, and anywhere
// else a node needs a swatch, without needing to persist a color assignment anywhere.
const nodeColorPalette = ["#3b82f6", "#10b981", "#8b5cf6", "#f59e0b", "#ef4444", "#06b6d4", "#ec4899", "#84cc16"];

function shortAddress(address: string): string {
    return address.replace(/^https?:\/\//, "");
}

function formatAge(iso: string): string {
    const seconds = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    return `${Math.floor(seconds / 3600)}h ago`;
}

// The future-facing counterpart of formatAge - for a task's nextAttemptAt, which under the
// dispatcher's exponential backoff can be up to an hour out, the exact scenario that reads as
// "stuck forever" without knowing it's actually still retrying, just slowly.
function formatEta(iso: string): string {
    const seconds = Math.floor((new Date(iso).getTime() - Date.now()) / 1000);
    if (seconds <= 0) return "retrying now";
    if (seconds < 60) return `in ${seconds}s`;
    if (seconds < 3600) return `in ${Math.floor(seconds / 60)}m`;
    return `in ${Math.floor(seconds / 3600)}h`;
}

// ── Nodes ────────────────────────────────────────────────────────────────────

const {
    data: nodes,
    status: nodesStatus,
    refresh: refreshNodes,
} = await useFetch<ClusterNode[]>(`${apiBase.value}/api/v1/admin/cluster/nodes`, {
    headers: authHeaders.value,
    default: () => [],
});

const {
    data: rebalanceStatus,
    refresh: refreshRebalanceStatus,
} = await useFetch<ClusterRebalanceStatus>(`${apiBase.value}/api/v1/admin/cluster/rebalance/status`, {
    headers: authHeaders.value,
    default: () => ({ pendingCount: 0, failedCount: 0, pendingByReason: {}, replicationFactor: 0 }),
});

// Storage breakdown is a full multi-bucket disk walk on every node - expensive, so unlike the
// two fetches above it is fetched once on load and otherwise only on manual refresh, never
// added to the poll timer below.
const {
    data: nodeStorage,
    status: storageStatus,
    refresh: refreshStorage,
} = await useFetch<ClusterNodeStorage[]>(`${apiBase.value}/api/v1/admin/cluster/storage`, {
    headers: authHeaders.value,
    default: () => [],
});

// Drill-down behind "Pending Replication by Reason" - only fetched on demand (not polled): a
// count going up or down is enough for the at-a-glance summary, this is for when an operator
// actually needs to know which node a stuck task targets and why it keeps failing.
const taskDetails = ref<ClusterReplicationTaskDetail[]>([]);
const isLoadingTaskDetails = ref(false);
const hasLoadedTaskDetails = ref(false);

async function loadTaskDetails() {
    isLoadingTaskDetails.value = true;
    try {
        taskDetails.value = await $fetch<ClusterReplicationTaskDetail[]>(`${apiBase.value}/api/v1/admin/cluster/rebalance/tasks`, {
            headers: authHeaders.value,
        });
        hasLoadedTaskDetails.value = true;
    } catch (err: any) {
        toast.add({ title: "Failed to Load", description: err?.data?.reason ?? "Unknown error", icon: "i-lucide-circle-x", color: "error" });
    } finally {
        isLoadingTaskDetails.value = false;
    }
}

// Cheap endpoints (a small table each) - poll like the dashboard's systemStats, so node health
// and rebalance progress stay live without a manual refresh.
let pollTimer: ReturnType<typeof setInterval> | undefined;
onMounted(() => {
    pollTimer = setInterval(() => {
        refreshNodes();
        refreshRebalanceStatus();
    }, 10000);
});
onUnmounted(() => {
    if (pollTimer) clearInterval(pollTimer);
});

function refreshAll() {
    refreshNodes();
    refreshRebalanceStatus();
}

const healthyCount = computed(() => nodes.value.filter((n) => n.isHealthy).length);

const isDegraded = computed(() => nodes.value.some((n) => !n.isHealthy) || rebalanceStatus.value.failedCount > 0);
const degradedDescription = computed(() => {
    const parts: string[] = [];
    const unhealthy = nodes.value.filter((n) => !n.isHealthy).length;
    if (unhealthy > 0) parts.push(`${unhealthy} node${unhealthy === 1 ? "" : "s"} unhealthy`);
    if (rebalanceStatus.value.failedCount > 0) parts.push(`${rebalanceStatus.value.failedCount} replication task${rebalanceStatus.value.failedCount === 1 ? "" : "s"} failed`);
    return parts.join(" · ");
});

function nodeColor(nodeId: string): string {
    const idx = nodes.value.findIndex((n) => n.id === nodeId);
    return nodeColorPalette[(idx >= 0 ? idx : 0) % nodeColorPalette.length]!;
}

function nodeAddress(id: string): string {
    return nodes.value.find((n) => n.id === id)?.address ?? id;
}

const storageTotal = computed(() => nodeStorage.value.reduce((sum, s) => sum + s.sizeBytes, 0));

const storageChartData = computed(() => nodeStorage.value.map((s) => s.sizeBytes));
const storageChartCategories = computed(() => {
    const categories: Record<string, { name: string; color: string }> = {};
    for (const s of nodeStorage.value) {
        categories[s.nodeId] = { name: shortAddress(nodeAddress(s.nodeId)), color: nodeColor(s.nodeId) };
    }
    return categories;
});

const nodeActionLoading = ref<Record<string, boolean>>({});
const isResyncing = ref(false);

async function drainNode(node: ClusterNode) {
    const confirmed = await confirm({
        title: "Drain Node",
        message: `Drain ${node.address}? It will be excluded from new placement and its data migrated to the remaining nodes.`,
        confirmLabel: "Drain",
    });
    if (!confirmed) return;

    nodeActionLoading.value[node.id] = true;
    try {
        await $fetch(`${apiBase.value}/api/v1/admin/cluster/nodes/${node.id}/drain`, {
            method: "POST",
            headers: authHeaders.value,
        });
        toast.add({ title: "Node Draining", description: `${node.address} is being drained.`, icon: "i-lucide-circle-check", color: "success" });
        await refreshAll();
    } catch (err: any) {
        toast.add({ title: "Drain Failed", description: err?.data?.reason ?? "Unknown error", icon: "i-lucide-circle-x", color: "error" });
    } finally {
        nodeActionLoading.value[node.id] = false;
    }
}

// Deliberately cluster-wide, not per-node - ClusterRebalanceService always walks every bucket
// under current membership regardless of which node is asked, so this lives as a single
// page-level action rather than a per-row button implying a narrower scope than it has.
async function resyncCluster() {
    const confirmed = await confirm({
        title: "Resync Cluster",
        message: "Trigger a full cluster-wide rebalance walk? Every object's placement is re-checked against current membership and any needed copies are queued - this isn't scoped to a single node.",
        confirmLabel: "Resync",
    });
    if (!confirmed) return;

    isResyncing.value = true;
    try {
        await $fetch(`${apiBase.value}/api/v1/admin/cluster/resync`, {
            method: "POST",
            headers: authHeaders.value,
        });
        toast.add({ title: "Resync Started", description: "A full cluster-wide rebalance walk has been triggered.", icon: "i-lucide-circle-check", color: "success" });
        await refreshRebalanceStatus();
    } catch (err: any) {
        toast.add({ title: "Resync Failed", description: err?.data?.reason ?? "Unknown error", icon: "i-lucide-circle-x", color: "error" });
    } finally {
        isResyncing.value = false;
    }
}

const nodeColumns: TableColumn<ClusterNode>[] = [
    {
        id: "health",
        header: "Health",
        cell: ({ row }) =>
            h("span", {
                class: `inline-block w-2 h-2 rounded-full ${row.original.isHealthy ? "bg-success" : "bg-error"}`,
                title: row.original.isHealthy ? "Healthy" : "Unreachable / stale heartbeat",
            }),
    },
    {
        accessorKey: "address",
        header: "Address",
    },
    {
        accessorKey: "status",
        header: "Status",
        cell: ({ row }) =>
            h(
                resolveComponent("UBadge"),
                {
                    color: row.original.status === "active" ? "success" : row.original.status === "draining" ? "warning" : "neutral",
                    variant: "subtle",
                    size: "sm",
                },
                () => row.original.status
            ),
    },
    {
        id: "storage",
        header: "Storage Share",
        cell: ({ row }) => {
            if (storageStatus.value === "pending" && nodeStorage.value.length === 0) {
                return h(resolveComponent("LoadingIndicator"), { size: 14 });
            }
            const stat = nodeStorage.value.find((s) => s.nodeId === row.original.id);
            if (!stat) return h("span", { class: "text-xs text-muted" }, "—");
            const pct = storageTotal.value > 0 ? (stat.sizeBytes / storageTotal.value) * 100 : 0;
            return h("div", { class: "flex items-center gap-2 min-w-36" }, [
                h(resolveComponent("UProgress"), { modelValue: pct, max: 100, size: "sm", class: "flex-1" }),
                h("span", { class: "text-xs text-muted whitespace-nowrap" }, `${formatBytes(stat.sizeBytes)} · ${stat.objectCount}`),
            ]);
        },
    },
    {
        id: "heartbeat",
        header: "Last Heartbeat",
        cell: ({ row }) => formatAge(row.original.lastHeartbeatAt),
    },
    {
        id: "joined",
        header: "Joined",
        cell: ({ row }) => new Date(row.original.joinedAt).toLocaleString(),
    },
    {
        id: "actions",
        header: "",
        cell: ({ row }) => {
            const node = row.original;
            if (node.status !== "active") return null;
            return h("div", { class: "flex justify-end" }, [
                h(resolveComponent("UButton"), {
                    label: "Drain",
                    size: "xs",
                    color: "warning",
                    variant: "subtle",
                    loading: nodeActionLoading.value[node.id] ?? false,
                    onClick: () => drainNode(node),
                }),
            ]);
        },
    },
];

// Drill-down table behind "Pending Replication by Reason" - see loadTaskDetails above.
const taskDetailColumns: TableColumn<ClusterReplicationTaskDetail>[] = [
    {
        id: "object",
        header: "Object",
        cell: ({ row }) =>
            h("div", { class: "flex flex-col" }, [
                h("span", { class: "font-medium truncate max-w-64" }, row.original.key),
                h("span", { class: "text-xs text-muted truncate max-w-64" }, row.original.bucketName),
            ]),
    },
    {
        id: "target",
        header: "Target Node",
        cell: ({ row }) =>
            h("div", { class: "flex items-center gap-1.5" }, [
                h("span", {
                    class: "inline-block w-1.5 h-1.5 rounded-full shrink-0",
                    style: { backgroundColor: nodeColor(row.original.targetNodeId) },
                }),
                h("span", { class: "text-sm whitespace-nowrap" }, shortAddress(nodeAddress(row.original.targetNodeId))),
            ]),
    },
    {
        accessorKey: "reason",
        header: "Reason",
        cell: ({ row }) => h(resolveComponent("UBadge"), { color: "neutral", variant: "subtle", size: "sm" }, () => row.original.reason),
    },
    {
        accessorKey: "state",
        header: "State",
        cell: ({ row }) =>
            h(
                resolveComponent("UBadge"),
                { color: row.original.state === "failed" ? "error" : "warning", variant: "subtle", size: "sm" },
                () => row.original.state
            ),
    },
    { accessorKey: "attempts", header: "Attempts" },
    {
        id: "nextAttempt",
        header: "Next Retry",
        cell: ({ row }) =>
            row.original.state === "failed"
                ? h("span", { class: "text-xs text-muted" }, "—")
                : h("span", { class: "text-xs whitespace-nowrap" }, formatEta(row.original.nextAttemptAt)),
    },
    {
        id: "lastError",
        header: "Last Error",
        cell: ({ row }) =>
            row.original.lastError
                ? h("span", { class: "text-xs text-muted truncate max-w-72 block", title: row.original.lastError }, row.original.lastError)
                : h("span", { class: "text-xs text-muted" }, "—"),
    },
];

// ── Placement browser ───────────────────────────────────────────────────────

const placementBucket = ref("");
const placementPrefix = ref("");
// null = "All nodes." A concrete node id turns this from "browse a bucket's placement" into
// "show me exactly which keys this node holds" - the direct answer to the question a wide,
// one-column-per-node grid could only ever give indirectly, and the part that stopped scaling
// once a cluster had more than a handful of nodes.
const placementNodeFilter = ref<string | null>(null);
const placementEntries = ref<ClusterPlacementEntry[]>([]);
const placementTotal = ref(0);
const placementPage = ref(1);
const placementPer = 25;
const isLoadingPlacement = ref(false);
const placementError = ref("");
const hasSearchedPlacement = ref(false);

const placementNodeFilterOptions = computed(() => [
    { label: "All nodes", value: null },
    ...nodes.value.map((node) => ({ label: shortAddress(node.address), value: node.id })),
]);

async function loadPlacement() {
    if (!placementBucket.value) {
        placementEntries.value = [];
        placementTotal.value = 0;
        hasSearchedPlacement.value = false;
        return;
    }
    isLoadingPlacement.value = true;
    placementError.value = "";
    hasSearchedPlacement.value = true;
    try {
        const response = await $fetch<Page<ClusterPlacementEntry>>(`${apiBase.value}/api/v1/admin/cluster/placement`, {
            params: {
                bucket: placementBucket.value,
                prefix: placementPrefix.value || undefined,
                nodeId: placementNodeFilter.value || undefined,
                page: placementPage.value,
                per: placementPer,
            },
            headers: authHeaders.value,
        });
        placementEntries.value = response.items;
        placementTotal.value = response.metadata.total;
    } catch (err: any) {
        placementError.value = err?.data?.reason ?? "Failed to load placement";
        placementEntries.value = [];
    } finally {
        isLoadingPlacement.value = false;
    }
}

watch(placementPage, loadPlacement);
watch(placementNodeFilter, () => {
    placementPage.value = 1;
    loadPlacement();
});

// A single "Placement" column of small chips (primary filled, replicas faint) rather than one
// column per node in the whole cluster - a key always has exactly `replicationFactor` replicas
// regardless of cluster size, so this stays a handful of chips whether the cluster has 4 nodes
// or 40, unlike the old one-column-per-node grid it replaces.
const placementColumns = computed<TableColumn<ClusterPlacementEntry>[]>(() => [
    { accessorKey: "key", header: "Key" },
    {
        id: "size",
        header: "Size",
        cell: ({ row }) => h("span", { class: "text-xs text-muted whitespace-nowrap" }, formatBytes(row.original.size)),
    },
    {
        id: "placement",
        header: "Placement",
        cell: ({ row }) =>
            h(
                "div",
                { class: "flex flex-wrap items-center gap-1.5" },
                row.original.nodeIds.map((id: string, rank: number) =>
                    h(
                        "span",
                        {
                            key: id,
                            class: `inline-flex items-center gap-1 rounded-full px-1.5 py-0.5 text-xs whitespace-nowrap ${rank === 0 ? "bg-primary/10 font-medium" : "bg-muted/50 text-muted"}`,
                            title: `${rank === 0 ? "Primary replica" : `Replica (rank ${rank + 1})`} on ${nodeAddress(id)}`,
                        },
                        [
                            h("span", {
                                class: "inline-block w-1.5 h-1.5 rounded-full shrink-0",
                                style: { backgroundColor: nodeColor(id) },
                            }),
                            h("span", {}, shortAddress(nodeAddress(id))),
                        ]
                    )
                )
            ),
    },
]);
</script>

<template>
    <UDashboardPanel>
        <template #header>
            <UDashboardNavbar title="Cluster">
                <template #right>
                    <UButton
                        @click="resyncCluster"
                        icon="i-lucide-rotate-cw"
                        color="neutral"
                        variant="ghost"
                        :loading="isResyncing"
                    >
                        Resync Cluster
                    </UButton>
                    <UButton @click="refreshAll" icon="i-lucide-refresh-cw" color="neutral" variant="ghost" :loading="nodesStatus === 'pending'">Refresh</UButton>
                </template>
            </UDashboardNavbar>
        </template>

        <template #body>
            <div class="mx-auto max-w-5xl xl:pt-8 w-full space-y-6">
                <UAlert
                    v-if="isDegraded"
                    color="warning"
                    variant="subtle"
                    icon="i-lucide-triangle-alert"
                    title="Cluster degraded"
                    :description="degradedDescription"
                />

                <div class="grid grid-cols-2 lg:grid-cols-4 gap-6">
                    <DetailKeyValueCard title="Healthy Nodes" icon="i-lucide-server" :value="`${healthyCount}/${nodes.length}`" />
                    <DetailKeyValueCard title="Replication Factor" icon="i-lucide-copy" :value="rebalanceStatus.replicationFactor" />
                    <DetailKeyValueCard title="Pending Replication" icon="i-lucide-loader" :value="rebalanceStatus.pendingCount" />
                    <DetailKeyValueCard title="Failed Replication" icon="i-lucide-triangle-alert" :value="rebalanceStatus.failedCount" />
                </div>

                <UCard v-if="Object.keys(rebalanceStatus.pendingByReason).length > 0 || rebalanceStatus.failedCount > 0" variant="subtle" :ui="{ body: taskDetails.length > 0 ? '!p-0' : undefined }">
                    <template #header>
                        <CardHeader title="Pending Replication by Reason" size="sm">
                            <template #rightContent>
                                <UButton
                                    @click="loadTaskDetails"
                                    icon="i-lucide-list"
                                    color="neutral"
                                    variant="ghost"
                                    size="sm"
                                    :loading="isLoadingTaskDetails"
                                >
                                    {{ hasLoadedTaskDetails ? "Refresh Details" : "View Details" }}
                                </UButton>
                            </template>
                        </CardHeader>
                    </template>
                    <div v-if="!hasLoadedTaskDetails" class="flex flex-wrap gap-2">
                        <UBadge v-for="(count, reason) in rebalanceStatus.pendingByReason" :key="reason" color="neutral" variant="subtle">{{ reason }}: {{ count }}</UBadge>
                    </div>
                    <UEmpty
                        v-else-if="taskDetails.length === 0"
                        title="Nothing Outstanding"
                        description="No pending or failed replication tasks right now."
                        icon="i-lucide-circle-check"
                        size="sm"
                        variant="naked"
                        class="py-6"
                    />
                    <template v-else>
                        <p class="px-4 pt-4 pb-2 text-xs text-muted">
                            Sorted by attempt count - the most-stuck tasks first. Capped at 200 rows.
                        </p>
                        <UTable :data="taskDetails" :columns="taskDetailColumns" :ui="{ th: 'cursor-default' }" />
                    </template>
                </UCard>

                <UCard variant="subtle">
                    <template #header>
                        <CardHeader title="Storage Distribution" size="sm">
                            <template #rightContent>
                                <UButton
                                    @click="() => refreshStorage()"
                                    icon="i-lucide-refresh-cw"
                                    color="neutral"
                                    variant="ghost"
                                    size="sm"
                                    :loading="storageStatus === 'pending'"
                                >
                                    Refresh
                                </UButton>
                            </template>
                        </CardHeader>
                    </template>
                    <div v-if="storageStatus === 'pending' && nodeStorage.length === 0" class="flex items-center justify-center p-6">
                        <LoadingIndicator />
                    </div>
                    <UEmpty
                        v-else-if="nodeStorage.length === 0"
                        title="No Storage Data"
                        description="This node is not part of a cluster, or storage data couldn't be loaded."
                        icon="i-lucide-hard-drive"
                        size="sm"
                        variant="naked"
                        class="py-6"
                    />
                    <div v-else class="flex flex-col sm:flex-row items-center gap-6">
                        <DonutChart :data="storageChartData" :categories="storageChartCategories" :radius="80" :height="220" hide-legend />
                        <div class="flex-1 w-full space-y-2">
                            <div v-for="s in nodeStorage" :key="s.nodeId" class="flex items-center gap-2 text-sm">
                                <span class="inline-block w-2.5 h-2.5 rounded-full shrink-0" :style="{ backgroundColor: nodeColor(s.nodeId) }" />
                                <span class="truncate flex-1">{{ shortAddress(nodeAddress(s.nodeId)) }}</span>
                                <span class="text-muted whitespace-nowrap">{{ formatBytes(s.sizeBytes) }}</span>
                                <span class="text-muted text-xs whitespace-nowrap w-12 text-right">{{ storageTotal > 0 ? Math.round((s.sizeBytes / storageTotal) * 100) : 0 }}%</span>
                            </div>
                        </div>
                    </div>
                </UCard>

                <UCard variant="subtle" :ui="{ body: '!p-0' }">
                    <template #header>
                        <CardHeader title="Nodes" size="sm" :badge="nodes.length > 0 ? nodes.length + '' : undefined" />
                    </template>
                    <template #default>
                        <div v-if="nodesStatus === 'pending' && nodes.length === 0" class="flex items-center justify-center p-6">
                            <LoadingIndicator />
                        </div>
                        <UEmpty v-else-if="nodes.length === 0" title="No Nodes" description="This node is not part of a cluster, or no peers have registered yet." icon="i-lucide-server" size="sm" variant="naked" class="py-6" />
                        <UTable v-else :data="nodes" :columns="nodeColumns" :ui="{ th: 'cursor-default' }" />
                    </template>
                </UCard>

                <UCard variant="subtle" :ui="{ body: '!p-0' }">
                    <template #header>
                        <CardHeader title="Object Placement" size="sm" />
                    </template>
                    <template #default>
                        <div class="p-4 flex flex-col sm:flex-row gap-3">
                            <UInput v-model="placementBucket" variant="subtle" placeholder="Bucket name" icon="i-lucide-cylinder" class="w-full sm:w-56" @keyup.enter="placementPage = 1; loadPlacement()" />
                            <UInput v-model="placementPrefix" variant="subtle" placeholder="Prefix (optional)" icon="i-lucide-filter" class="w-full sm:w-56" @keyup.enter="placementPage = 1; loadPlacement()" />
                            <USelectMenu
                                v-model="placementNodeFilter"
                                :items="placementNodeFilterOptions"
                                value-key="value"
                                icon="i-lucide-server"
                                placeholder="All nodes"
                                class="w-full sm:w-48"
                                variant="subtle"
                                size="lg"
                            />
                            <UButton size="lg" label="Search" icon="i-lucide-search" color="neutral" variant="subtle" :loading="isLoadingPlacement" @click="placementPage = 1; loadPlacement()" />
                        </div>
                        <p v-if="hasSearchedPlacement" class="px-4 pb-4 -mt-2 text-xs text-muted">
                            A filled chip marks the primary replica, a faint chip a secondary replica.
                            <template v-if="placementNodeFilter">Showing only keys placed on <strong>{{ shortAddress(nodeAddress(placementNodeFilter)) }}</strong>.</template>
                            Limited to the first 1000 matching objects.
                        </p>

                        <UAlert v-if="placementError" title="Error" :description="placementError" color="error" variant="subtle" class="mx-4 mb-4" />

                        <div v-if="isLoadingPlacement" class="flex items-center justify-center p-6">
                            <LoadingIndicator />
                        </div>
                        <UEmpty
                            v-else-if="placementEntries.length === 0"
                            title="No Objects"
                            :description="hasSearchedPlacement ? 'No matching objects found for this bucket/prefix/node combination.' : 'Search a bucket above to see where its objects are placed.'"
                            icon="i-lucide-map"
                            size="sm"
                            variant="naked"
                            class="py-6"
                        />
                        <template v-else>
                            <UTable :data="placementEntries" :columns="placementColumns" :ui="{ th: 'cursor-default' }" />
                            <div v-if="placementTotal > placementPer" class="flex justify-center p-4">
                                <UPagination v-model:page="placementPage" :items-per-page="placementPer" :total="placementTotal" />
                            </div>
                        </template>
                    </template>
                </UCard>
            </div>
        </template>
    </UDashboardPanel>
</template>
