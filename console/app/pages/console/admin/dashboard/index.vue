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
</script>

<template>
    <UDashboardPanel>
        <template #header>
            <UDashboardNavbar title="Dashboard">
                <template #right>
                    <UButton
                        @click="
                            () => {
                                refresh();
                            }
                        "
                        icon="i-lucide-refresh-cw"
                        color="neutral"
                        variant="ghost"
                        :loading="status === 'pending'"
                    >
                        Refresh
                    </UButton>
                </template>
            </UDashboardNavbar>
        </template>

        <template #body>
            <div class="mx-auto max-w-5xl xl:pt-8 w-full space-y-6">
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
                            <UButton to="/console/buckets" color="neutral" variant="subtle" trailing-icon="i-lucide-arrow-right">Manage</UButton>
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
