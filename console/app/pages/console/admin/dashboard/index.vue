<script setup lang="ts">
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

definePageMeta({
    layout: "dashboard",
});

useHead({
    title: `Dashboard`,
});

const jwtCookie = useJWTCookie();

const {
    data: stats,
    status,
    refresh,
} = await useFetch<StorageStats>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/admin/storageStats`, {
    headers: {
        Authorization: `Bearer ${jwtCookie.value}`,
    },
    default: () => ({
        totalBytes: 0,
        availableBytes: 0,
        usedBytes: 0,
        alarikUsedBytes: 0,
        bucketCount: 0,
        userCount: 0,
    }),
});

const { data: sys, refresh: refreshSys } = await useFetch<SystemStats>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/admin/systemStats`, {
    headers: {
        Authorization: `Bearer ${jwtCookie.value}`,
    },
});

// The system stats endpoint is cheap (no storage walk), so poll it for live charts.
// storageStats walks the whole bucket tree and stays manual-refresh only.
let pollTimer: ReturnType<typeof setInterval> | undefined;
onMounted(() => {
    pollTimer = setInterval(() => refreshSys(), 10000);
});
onUnmounted(() => {
    if (pollTimer) clearInterval(pollTimer);
});

const diskUsagePercent = computed(() => {
    if (!stats.value?.totalBytes) return 0;
    return Math.round((stats.value.usedBytes / stats.value.totalBytes) * 100);
});

const alarikUsagePercent = computed(() => {
    if (!stats.value?.totalBytes) return 0;
    return Math.round((stats.value.alarikUsedBytes / stats.value.totalBytes) * 100);
});

const alarikOfUsedPercent = computed(() => {
    if (!stats.value?.usedBytes) return 0;
    return Math.round((stats.value.alarikUsedBytes / stats.value.usedBytes) * 100);
});

// --- System gauges ---

const metrics = computed(() => sys.value?.metrics);

function formatUptime(seconds?: number): string {
    if (seconds === undefined) return "–";
    const d = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    if (d > 0) return `${d}d ${h}h ${m}m`;
    if (h > 0) return `${h}h ${m}m`;
    return `${m}m`;
}

function formatPercent(value?: number): string {
    return value === undefined ? "–" : `${value.toFixed(1)}%`;
}

const systemMemorySubtitle = computed(() => {
    const m = metrics.value;
    if (!m?.systemMemoryTotalBytes) return undefined;
    const used = m.systemMemoryTotalBytes - (m.systemMemoryAvailableBytes ?? 0);
    return `System: ${formatBytes(used)} / ${formatBytes(m.systemMemoryTotalBytes)}`;
});

const loadSubtitle = computed(() => {
    const m = metrics.value;
    if (m?.loadAverage5 === undefined) return undefined;
    return `5m: ${m.loadAverage5.toFixed(2)} · 15m: ${m.loadAverage15?.toFixed(2)} · ${m.coreCount} cores`;
});

// --- Charts (last hour, per-minute buckets) ---

function bucketTime(t: string | number): string {
    const date = typeof t === "number" ? new Date(t * 1000) : new Date(t);
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

const history = computed(() => metrics.value?.history ?? []);

const trafficData = computed(() =>
    history.value.map((b) => ({
        time: bucketTime(b.timestamp),
        in: b.bytesIn,
        out: b.bytesOut,
    }))
);

const requestData = computed(() =>
    history.value.map((b) => ({
        time: bucketTime(b.timestamp),
        requests: b.requests,
        errors: b.errors,
    }))
);

const cpuData = computed(() =>
    history.value.map((b) => ({
        time: bucketTime(b.timestamp),
        cpu: b.cpuPercent ?? 0,
    }))
);

const memoryData = computed(() =>
    history.value.map((b) => ({
        time: bucketTime(b.timestamp),
        memory: b.memoryBytes ?? 0,
    }))
);

const trafficCategories = {
    in: { name: "In", color: "#3b82f6" },
    out: { name: "Out", color: "#10b981" },
};
const requestCategories = {
    requests: { name: "Requests", color: "#8b5cf6" },
    errors: { name: "Errors", color: "#ef4444" },
};
const cpuCategories = {
    cpu: { name: "Process CPU %", color: "#f59e0b" },
};
const memoryCategories = {
    memory: { name: "Process Memory", color: "#06b6d4" },
};

const trafficXFormatter = (i: number) => trafficData.value[i]?.time ?? "";
const requestXFormatter = (i: number) => requestData.value[i]?.time ?? "";
const cpuXFormatter = (i: number) => cpuData.value[i]?.time ?? "";
const memoryXFormatter = (i: number) => memoryData.value[i]?.time ?? "";

function refreshAll() {
    refresh();
    refreshSys();
}
</script>

<template>
    <UDashboardPanel>
        <template #header>
            <UDashboardNavbar title="Dashboard">
                <template #right>
                    <UButton @click="refreshAll" icon="i-lucide-refresh-cw" color="neutral" variant="ghost" :loading="status === 'pending'"> Refresh </UButton>
                </template>
            </UDashboardNavbar>
        </template>

        <template #body>
            <div class="mx-auto max-w-5xl xl:pt-8 w-full space-y-6">
                <!-- System Overview Cards -->
                <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
                    <DetailKeyValueCard
                        title="CPU"
                        icon="i-lucide-cpu"
                        :value="formatPercent(metrics?.processCPUPercent)"
                        :subTitle="metrics?.systemCPUPercent !== undefined ? `System: ${formatPercent(metrics?.systemCPUPercent)}` : undefined"
                    />
                    <DetailKeyValueCard title="Memory" icon="i-lucide-memory-stick" :value="formatBytes(metrics?.processMemoryBytes || 0)" :subTitle="systemMemorySubtitle" />
                    <DetailKeyValueCard title="Load Average" icon="i-lucide-activity" :value="metrics?.loadAverage1?.toFixed(2) ?? '–'" :subTitle="loadSubtitle" />
                    <DetailKeyValueCard title="Uptime" icon="i-lucide-timer" :value="formatUptime(metrics?.uptimeSeconds)" subTitle="Since last restart" />
                </div>

                <!-- Traffic Totals -->
                <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
                    <DetailKeyValueCard title="Traffic In" icon="i-lucide-arrow-down-to-line" :value="formatBytes(metrics?.totalBytesIn || 0)" subTitle="Since last restart" />
                    <DetailKeyValueCard title="Traffic Out" icon="i-lucide-arrow-up-from-line" :value="formatBytes(metrics?.totalBytesOut || 0)" subTitle="Since last restart" />
                    <DetailKeyValueCard title="Requests" icon="i-lucide-globe" :value="(metrics?.totalRequests || 0).toLocaleString()" subTitle="Since last restart" />
                    <DetailKeyValueCard title="Errors" icon="i-lucide-triangle-alert" :value="(metrics?.totalErrors || 0).toLocaleString()" subTitle="4xx / 5xx responses" />
                </div>

                <!-- Live Charts -->
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <UCard variant="subtle">
                        <template #header>
                            <CardHeader title="Traffic (last hour)" size="sm" />
                        </template>
                        <AreaChart :data="trafficData" :height="180" :categories="trafficCategories" :xFormatter="trafficXFormatter" :yFormatter="(v: number) => formatBytes(v)" :xNumTicks="4" :yNumTicks="3" curveType="monotoneX" />
                    </UCard>

                    <UCard variant="subtle">
                        <template #header>
                            <CardHeader title="Requests (last hour)" size="sm" />
                        </template>
                        <AreaChart :data="requestData" :height="180" :categories="requestCategories" :xFormatter="requestXFormatter" :xNumTicks="4" :yNumTicks="3" curveType="monotoneX" />
                    </UCard>

                    <UCard variant="subtle">
                        <template #header>
                            <CardHeader title="CPU (last hour)" size="sm" />
                        </template>
                        <AreaChart :data="cpuData" :height="180" :categories="cpuCategories" :xFormatter="cpuXFormatter" :yFormatter="(v: number) => `${v.toFixed(0)}%`" :xNumTicks="4" :yNumTicks="3" curveType="monotoneX" />
                    </UCard>

                    <UCard variant="subtle">
                        <template #header>
                            <CardHeader title="Memory (last hour)" size="sm" />
                        </template>
                        <AreaChart :data="memoryData" :height="180" :categories="memoryCategories" :xFormatter="memoryXFormatter" :yFormatter="(v: number) => formatBytes(v)" :xNumTicks="4" :yNumTicks="3" curveType="monotoneX" />
                    </UCard>
                </div>

                <!-- Storage Overview Cards -->
                <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
                    <DetailKeyValueCard title="Total Disk Space" icon="i-lucide-hard-drive" :value="formatBytes(stats?.totalBytes || 0)" />
                    <DetailKeyValueCard title="Disk Used" icon="i-lucide-database" :value="formatBytes(stats?.usedBytes || 0)" :subTitle="`${diskUsagePercent}% of total`" />
                    <DetailKeyValueCard title="Available" icon="i-lucide-check-circle" :value="formatBytes(stats?.availableBytes || 0)" :subTitle="`${100 - diskUsagePercent}% free`" />
                    <DetailKeyValueCard title="Alarik Storage" icon="i-lucide-cylinder" :value="formatBytes(stats?.alarikUsedBytes || 0)" :subTitle="`${alarikUsagePercent}% of disk`" />
                </div>

                <!-- Disk Usage Bar -->
                <UCard variant="subtle">
                    <template #header>
                        <CardHeader title="Disk Usage" size="sm" />
                    </template>

                    <div class="space-y-4">
                        <div class="space-y-2">
                            <div class="flex justify-between text-sm">
                                <span class="text-muted">Total Disk Usage</span>
                                <span>{{ formatBytes(stats?.usedBytes || 0) }} / {{ formatBytes(stats?.totalBytes || 0) }}</span>
                            </div>
                            <UProgress v-model="diskUsagePercent" :max="100" size="lg" />
                        </div>

                        <div class="space-y-2">
                            <div class="flex justify-between text-sm">
                                <span class="text-muted">Alarik Storage Usage</span>
                                <span>{{ formatBytes(stats?.alarikUsedBytes || 0) }} of {{ formatBytes(stats?.usedBytes || 0) }} used ({{ alarikOfUsedPercent }}%)</span>
                            </div>
                            <UProgress v-model="alarikOfUsedPercent" :max="100" size="lg" color="primary" />
                        </div>
                    </div>
                </UCard>

                <!-- Resource Counts -->
                <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
                    <DetailKeyValueCard title="Access Keys" icon="i-lucide-key-round" :value="sys?.accessKeyCount ?? 0" />
                    <DetailKeyValueCard title="Shared Links" icon="i-lucide-link" :value="sys?.sharedLinkCount ?? 0" />
                    <DetailKeyValueCard title="OIDC Providers" icon="i-lucide-shield-check" :value="sys?.oidcProviderCount ?? 0" />
                    <DetailKeyValueCard title="Multipart Uploads" icon="i-lucide-layers" :value="sys?.multipartUploadCount ?? 0" subTitle="In progress" />
                </div>

                <div class="grid grid-cols-1 sm:grid-cols-2 gap-6">
                    <UCard variant="subtle">
                        <template #header>
                            <CardHeader title="Buckets" size="sm" />
                        </template>

                        <div class="flex items-center justify-between">
                            <div class="flex items-center gap-3">
                                <div class="flex items-center justify-center p-4 rounded-lg bg-primary/10">
                                    <UIcon name="i-lucide-cylinder" class="w-6 h-6 text-primary" />
                                </div>
                                <div>
                                    <div class="text-3xl font-bold">{{ stats?.bucketCount || 0 }}</div>
                                    <div class="text-sm text-muted">Total buckets</div>
                                </div>
                            </div>
                            <UButton to="/console/admin/buckets" color="neutral" variant="subtle" trailing-icon="i-lucide-arrow-right">Manage</UButton>
                        </div>
                    </UCard>

                    <UCard variant="subtle">
                        <template #header>
                            <CardHeader title="Users" size="sm" />
                        </template>

                        <div class="flex items-center justify-between">
                            <div class="flex items-center gap-3">
                                <div class="flex items-center justify-center p-4 rounded-lg bg-primary/10">
                                    <UIcon name="i-lucide-users" class="w-6 h-6 text-primary" />
                                </div>
                                <div>
                                    <div class="text-3xl font-bold">{{ stats?.userCount || 0 }}</div>
                                    <div class="text-sm text-muted">Registered users</div>
                                </div>
                            </div>
                            <UButton to="/console/admin/users" color="neutral" variant="subtle" trailing-icon="i-lucide-arrow-right">Manage</UButton>
                        </div>
                    </UCard>
                </div>
            </div>
        </template>
    </UDashboardPanel>
</template>
