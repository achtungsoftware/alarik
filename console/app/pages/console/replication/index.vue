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
    title: "Bucket Replication",
});

const ZERO_UUID = "00000000-0000-0000-0000-000000000000";

const route = useRoute();
const router = useRouter();
const jwtCookie = useJWTCookie();
const toast = useToast();
const { confirm } = useConfirmDialog();
const authHeaders = computed(() => ({ Authorization: `Bearer ${jwtCookie.value}` }));
const apiBase = computed(() => useRuntimeConfig().public.apiBaseUrl);

interface BucketOption {
    label: string;
    value: string;
}

const selectedBucketName = computed({
    get: () => (route.query.bucket as string) ?? "",
    set: (value: string) => {
        router.replace({ query: { ...route.query, bucket: value || undefined } });
    },
});

const bucketSearchTerm = ref("");
const bucketSearchResults = ref<BucketOption[]>([]);
const isSearchingBuckets = ref(false);


const bucketOptions = computed<BucketOption[]>(() => {
    if (!selectedBucketName.value) return bucketSearchResults.value;
    if (bucketSearchResults.value.some((option: BucketOption) => option.value === selectedBucketName.value)) {
        return bucketSearchResults.value;
    }
    return [{ label: selectedBucketName.value, value: selectedBucketName.value }, ...bucketSearchResults.value];
});

async function searchBuckets(term: string) {
    isSearchingBuckets.value = true;
    try {
        const response = await $fetch<Page<Bucket>>(`${apiBase.value}/api/v1/buckets`, {
            params: { page: 1, per: 20, search: term || undefined },
            headers: authHeaders.value,
        });
        bucketSearchResults.value = response.items.map((b: Bucket) => ({ label: b.name, value: b.name }));
    } catch (err) {
        console.error("Failed to search buckets:", err);
    } finally {
        isSearchingBuckets.value = false;
    }
}

let bucketSearchDebounce: ReturnType<typeof setTimeout> | undefined;
watch(bucketSearchTerm, (term) => {
    if (bucketSearchDebounce) clearTimeout(bucketSearchDebounce);
    bucketSearchDebounce = setTimeout(() => searchBuckets(term), 300);
});
searchBuckets("");

// The full Bucket record (for versioningStatus) - there's no single-bucket GET endpoint, so
// this reuses the same search endpoint and picks out the exact match.
const selectedBucket = ref<Bucket | null>(null);
const isLoadingBucket = ref(false);

const isVersioningEnabled = computed(() => selectedBucket.value?.versioningStatus === "Enabled");

// ── Replication data ─────────────────────────────────────────────────────────

const isLoading = ref(false);
const isLoadingTasks = ref(false);
const error = ref("");

const base = computed(() => `${apiBase.value}/api/v1/buckets/${selectedBucketName.value}/replication`);

const targets = ref<ReplicationTarget[]>([]);
const rules = ref<ReplicationRule[]>([]);
const tasks = ref<ReplicationTask[]>([]);

// Below every ref it touches - `immediate: true` runs this synchronously during setup, so
// referencing a ref declared later in the script would be a temporal-dead-zone crash.
watch(
    selectedBucketName,
    async (name) => {
        selectedBucket.value = null;
        targets.value = [];
        rules.value = [];
        tasks.value = [];
        error.value = "";
        if (!name) return;

        isLoadingBucket.value = true;
        try {
            const response = await $fetch<Page<Bucket>>(`${apiBase.value}/api/v1/buckets`, {
                params: { page: 1, per: 5, search: name },
                headers: authHeaders.value,
            });
            selectedBucket.value = response.items.find((b: Bucket) => b.name === name) ?? null;
        } finally {
            isLoadingBucket.value = false;
        }

        await fetchAll();
    },
    { immediate: true }
);

const targetOptions = computed(() => targets.value.map((t: ReplicationTarget) => ({ label: `${t.endpoint} → ${t.targetBucket}`, value: t.id })));

function targetLabel(targetId: string): string {
    const target = targets.value.find((t: ReplicationTarget) => t.id === targetId);
    return target ? `${target.endpoint} → ${target.targetBucket}` : "Unknown target";
}

async function fetchAll() {
    try {
        isLoading.value = true;
        error.value = "";

        const [targetsResponse, rulesResponse] = await Promise.all([
            $fetch<{ targets: ReplicationTarget[] }>(`${base.value}/targets`, { headers: authHeaders.value }),
            $fetch<{ rules: ReplicationRule[] }>(`${base.value}/rules`, { headers: authHeaders.value }),
        ]);

        targets.value = targetsResponse.targets;
        rules.value = rulesResponse.rules;
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isLoading.value = false;
    }
    await fetchTasks();
}

async function fetchTasks() {
    try {
        isLoadingTasks.value = true;
        const response = await $fetch<{ tasks: ReplicationTask[] }>(`${base.value}/tasks`, { headers: authHeaders.value });
        tasks.value = response.tasks;
    } catch (err) {
        // Non-fatal: the targets/rules tables above are the primary content of this page, a
        // failed task-history fetch shouldn't block using it.
        console.error("Failed to fetch replication tasks:", err);
    } finally {
        isLoadingTasks.value = false;
    }
}

// ── Targets: table + slideover editor ───────────────────────────────────────

const isSavingTargets = ref(false);
const targetSlideoverOpen = ref(false);
const editingTargetId = ref<string | null>(null);
const targetDraft = reactive({
    endpoint: "",
    targetBucket: "",
    accessKeyId: "",
    secretAccessKey: "",
    region: "us-east-1",
    enabled: true,
});

function resetTargetDraft() {
    targetDraft.endpoint = "";
    targetDraft.targetBucket = "";
    targetDraft.accessKeyId = "";
    targetDraft.secretAccessKey = "";
    targetDraft.region = "us-east-1";
    targetDraft.enabled = true;
}

function openNewTarget() {
    editingTargetId.value = null;
    resetTargetDraft();
    targetSlideoverOpen.value = true;
}

function closeTargetSlideover() {
    targetSlideoverOpen.value = false;
}

function openEditTarget(target: ReplicationTarget) {
    editingTargetId.value = target.id;
    targetDraft.endpoint = target.endpoint;
    targetDraft.targetBucket = target.targetBucket;
    targetDraft.accessKeyId = target.accessKeyId;
    targetDraft.secretAccessKey = target.secretAccessKey;
    targetDraft.region = target.region;
    targetDraft.enabled = target.enabled;
    targetSlideoverOpen.value = true;
}

async function persistTargets(next: ReplicationTarget[]) {
    isSavingTargets.value = true;
    error.value = "";
    try {
        const payload = {
            targets: next.map((t: ReplicationTarget) => ({
                id: t.id,
                endpoint: t.endpoint.trim(),
                targetBucket: t.targetBucket.trim(),
                accessKeyId: t.accessKeyId.trim(),
                secretAccessKey: t.secretAccessKey.trim(),
                region: t.region.trim() || "us-east-1",
                enabled: t.enabled,
            })),
        };

        const response = await $fetch<{ targets: ReplicationTarget[] }>(`${base.value}/targets`, {
            method: "PUT",
            body: JSON.stringify(payload),
            headers: { "Content-Type": "application/json", ...authHeaders.value },
        });
        targets.value = response.targets;

        // Removing a target auto-disables any rule that referenced it on the server - re-sync.
        const rulesResponse = await $fetch<{ rules: ReplicationRule[] }>(`${base.value}/rules`, { headers: authHeaders.value });
        rules.value = rulesResponse.rules;

        return true;
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
        toast.add({
            title: "Failed to Save Target",
            description: error.value,
            icon: "i-lucide-circle-x",
            color: "error",
        });
        return false;
    } finally {
        isSavingTargets.value = false;
    }
}

async function saveTargetDraft() {
    const next = editingTargetId.value
        ? targets.value.map((t: ReplicationTarget) => (t.id === editingTargetId.value ? { ...t, ...targetDraft } : t))
        : [...targets.value, { id: ZERO_UUID, ...targetDraft }];

    if (!(await persistTargets(next))) return;

    targetSlideoverOpen.value = false;
    toast.add({
        title: "Target saved",
        description: `Targets for "${selectedBucketName.value}" were updated.`,
        icon: "i-lucide-circle-check",
        color: "success",
    });
}

async function deleteTarget(target: ReplicationTarget) {
    const confirmed = await confirm({
        title: "Remove Replication Target",
        message: `Remove the target "${target.endpoint} → ${target.targetBucket}"? Any rule referencing it will be disabled.`,
        confirmLabel: "Remove",
    });
    if (!confirmed) return;

    if (await persistTargets(targets.value.filter((t: ReplicationTarget) => t.id !== target.id))) {
        toast.add({
            title: "Target removed",
            icon: "i-lucide-circle-check",
            color: "success",
        });
    }
}

// ── Rules: table + slideover editor ─────────────────────────────────────────

const isSavingRules = ref(false);
const resyncingId = ref<string | null>(null);
const ruleSlideoverOpen = ref(false);
const editingRuleId = ref<string | null>(null);
const ruleDraft = reactive({
    targetId: "",
    prefix: "",
    replicateDeletes: false,
    replicateExisting: false,
    synchronous: false,
    enabled: true,
});

function resetRuleDraft() {
    ruleDraft.targetId = targetOptions.value[0]?.value ?? "";
    ruleDraft.prefix = "";
    ruleDraft.replicateDeletes = false;
    ruleDraft.replicateExisting = false;
    ruleDraft.synchronous = false;
    ruleDraft.enabled = true;
}

function openNewRule() {
    if (targetOptions.value.length === 0) {
        error.value = "Add a replication target before creating a rule.";
        return;
    }
    editingRuleId.value = null;
    resetRuleDraft();
    ruleSlideoverOpen.value = true;
}

function closeRuleSlideover() {
    ruleSlideoverOpen.value = false;
}

function openEditRule(rule: ReplicationRule) {
    editingRuleId.value = rule.id;
    ruleDraft.targetId = rule.targetId;
    ruleDraft.prefix = rule.prefix ?? "";
    ruleDraft.replicateDeletes = rule.replicateDeletes;
    ruleDraft.replicateExisting = rule.replicateExisting;
    ruleDraft.synchronous = rule.synchronous;
    ruleDraft.enabled = rule.enabled;
    ruleSlideoverOpen.value = true;
}

async function persistRules(next: ReplicationRule[]) {
    isSavingRules.value = true;
    error.value = "";
    try {
        const payload = {
            rules: next.map((r: ReplicationRule) => ({
                id: r.id,
                targetId: r.targetId,
                prefix: r.prefix?.trim() ? r.prefix.trim() : undefined,
                replicateDeletes: r.replicateDeletes,
                replicateExisting: r.replicateExisting,
                synchronous: r.synchronous,
                enabled: r.enabled,
            })),
        };

        const response = await $fetch<{ rules: ReplicationRule[] }>(`${base.value}/rules`, {
            method: "PUT",
            body: JSON.stringify(payload),
            headers: { "Content-Type": "application/json", ...authHeaders.value },
        });
        rules.value = response.rules;
        return true;
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
        toast.add({
            title: "Failed to Save Rule",
            description: error.value,
            icon: "i-lucide-circle-x",
            color: "error",
        });
        return false;
    } finally {
        isSavingRules.value = false;
    }
}

async function saveRuleDraft() {
    const next = editingRuleId.value
        ? rules.value.map((r: ReplicationRule) => (r.id === editingRuleId.value ? { ...r, ...ruleDraft } : r))
        : [...rules.value, { id: ZERO_UUID, ...ruleDraft }];

    if (!(await persistRules(next))) return;

    ruleSlideoverOpen.value = false;
    toast.add({
        title: "Rule saved",
        description: `Rules for "${selectedBucketName.value}" were updated.`,
        icon: "i-lucide-circle-check",
        color: "success",
    });
}

async function deleteRule(rule: ReplicationRule) {
    const confirmed = await confirm({
        title: "Remove Replication Rule",
        message: `Remove this rule (prefix "${rule.prefix || "*"}")? Objects will stop replicating to ${targetLabel(rule.targetId)}.`,
        confirmLabel: "Remove",
    });
    if (!confirmed) return;

    if (await persistRules(rules.value.filter((r: ReplicationRule) => r.id !== rule.id))) {
        toast.add({
            title: "Rule removed",
            icon: "i-lucide-circle-check",
            color: "success",
        });
    }
}

async function resync(rule: ReplicationRule) {
    try {
        resyncingId.value = rule.id;

        await $fetch(`${base.value}/rules/${rule.id}/resync`, {
            method: "POST",
            headers: authHeaders.value,
        });

        // The walk runs in the background - it's not done by the time this returns, just
        // started. Check the Recent Tasks table for progress.
        toast.add({
            title: "Resync started",
            description: "Existing objects are being queued for replication in the background.",
            icon: "i-lucide-refresh-ccw",
            color: "success",
        });

        await fetchTasks();
    } catch (err: any) {
        toast.add({
            title: "Resync Failed",
            description: err.response?._data?.reason ?? "Unknown error",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    } finally {
        resyncingId.value = null;
    }
}

// ── Tasks: table + retry ─────────────────────────────────────────────────────

const retryingId = ref<string | null>(null);

async function retryTask(task: ReplicationTask) {
    try {
        retryingId.value = task.id;

        await $fetch(`${base.value}/tasks/${task.id}/retry`, {
            method: "POST",
            headers: authHeaders.value,
        });

        toast.add({
            title: "Retry queued",
            description: `Replication of ${task.key} to ${task.endpoint} has been queued.`,
            icon: "i-lucide-refresh-ccw",
            color: "success",
        });

        await fetchTasks();
    } catch (err: any) {
        toast.add({
            title: "Retry Failed",
            description: err.response?._data?.reason ?? "Unknown error",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    } finally {
        retryingId.value = null;
    }
}

function relativeTime(iso: string): string {
    const diffMs = Date.now() - new Date(iso).getTime();
    const diffSec = Math.round(diffMs / 1000);
    if (Math.abs(diffSec) < 60) return "just now";
    const diffMin = Math.round(diffSec / 60);
    if (Math.abs(diffMin) < 60) return `${diffMin}m ago`;
    const diffHour = Math.round(diffMin / 60);
    if (Math.abs(diffHour) < 24) return `${diffHour}h ago`;
    const diffDay = Math.round(diffHour / 24);
    return `${diffDay}d ago`;
}

// ── Tables ────────────────────────────────────────────────────────────────────

const targetColumns: TableColumn<ReplicationTarget>[] = [
    {
        id: "status",
        header: "Status",
        cell: ({ row }) =>
            h("span", {
                class: `inline-block w-2 h-2 rounded-full ${row.original.enabled ? "bg-success" : "bg-muted"}`,
                title: row.original.enabled ? "Enabled" : "Disabled",
            }),
    },
    {
        accessorKey: "endpoint",
        header: "Target",
        cell: ({ row }) =>
            h("div", { class: "flex flex-col" }, [
                h("span", { class: "font-medium text-highlighted" }, row.original.endpoint),
                h("span", { class: "text-xs text-muted" }, `→ ${row.original.targetBucket}`),
            ]),
    },
    {
        accessorKey: "region",
        header: "Region",
    },
    {
        id: "actions",
        cell: ({ row }) =>
            h("div", { class: "flex items-center justify-end gap-2" }, [
                h(resolveComponent("UButton"), {
                    label: "Edit",
                    icon: "i-lucide-pencil",
                    variant: "subtle",
                    color: "neutral",
                    size: "xs",
                    onClick: () => openEditTarget(row.original),
                }),
                h(resolveComponent("UButton"), {
                    icon: "i-lucide-trash-2",
                    variant: "subtle",
                    color: "error",
                    size: "xs",
                    "aria-label": "Remove target",
                    onClick: () => deleteTarget(row.original),
                }),
            ]),
    },
];

const ruleColumns: TableColumn<ReplicationRule>[] = [
    {
        id: "status",
        header: "Status",
        cell: ({ row }) =>
            h("span", {
                class: `inline-block w-2 h-2 rounded-full ${row.original.enabled ? "bg-success" : "bg-muted"}`,
                title: row.original.enabled ? "Enabled" : "Disabled",
            }),
    },
    {
        id: "target",
        header: "Target",
        cell: ({ row }) => targetLabel(row.original.targetId),
    },
    {
        accessorKey: "prefix",
        header: "Prefix",
        cell: ({ row }) => row.original.prefix || h("span", { class: "text-muted" }, "*"),
    },
    {
        id: "flags",
        header: "Options",
        cell: ({ row }) => {
            const rule = row.original;
            const badges = [];
            if (rule.replicateDeletes) badges.push(h(resolveComponent("UBadge"), { label: "Deletes", size: "md", color: "neutral", variant: "subtle" }));
            if (rule.replicateExisting) badges.push(h(resolveComponent("UBadge"), { label: "Existing", size: "md", color: "neutral", variant: "subtle" }));
            if (rule.synchronous) badges.push(h(resolveComponent("UBadge"), { label: "Sync", size: "md", color: "primary", variant: "subtle" }));
            return badges.length > 0 ? h("div", { class: "flex flex-wrap gap-1" }, badges) : h("span", { class: "text-muted" }, "—");
        },
    },
    {
        id: "actions",
        cell: ({ row }) => {
            const rule = row.original;
            const buttons = [];
            if (rule.replicateExisting) {
                buttons.push(
                    h(resolveComponent("UButton"), {
                        label: "Resync",
                        icon: "i-lucide-refresh-ccw",
                        variant: "subtle",
                        color: "neutral",
                        size: "xs",
                        loading: resyncingId.value === rule.id,
                        onClick: () => resync(rule),
                    })
                );
            }
            buttons.push(
                h(resolveComponent("UButton"), {
                    label: "Edit",
                    icon: "i-lucide-pencil",
                    variant: "subtle",
                    color: "neutral",
                    size: "xs",
                    onClick: () => openEditRule(rule),
                })
            );
            buttons.push(
                h(resolveComponent("UButton"), {
                    icon: "i-lucide-trash-2",
                    variant: "subtle",
                    color: "error",
                    size: "xs",
                    "aria-label": "Remove rule",
                    onClick: () => deleteRule(rule),
                })
            );
            return h("div", { class: "flex items-center justify-end gap-2" }, buttons);
        },
    },
];

const taskColumns: TableColumn<ReplicationTask>[] = [
    {
        accessorKey: "state",
        header: "State",
        cell: ({ row }) =>
            h(
                resolveComponent("UBadge"),
                { color: row.original.state === "failed" ? "error" : "warning", variant: "subtle", size: "sm" },
                () => (row.original.state === "failed" ? "Failed" : "Pending")
            ),
    },
    {
        accessorKey: "operation",
        header: "Operation",
        cell: ({ row }) => h(resolveComponent("UBadge"), { color: "neutral", variant: "subtle", size: "xs" }, () => row.original.operation),
    },
    {
        accessorKey: "key",
        header: "Key",
        cell: ({ row }) =>
            h("div", { class: "flex flex-col min-w-0" }, [
                h("span", { class: "truncate" }, row.original.key),
                h("span", { class: "text-xs text-muted truncate" }, `→ ${targetLabel(row.original.targetId)}`),
            ]),
    },
    {
        accessorKey: "attempts",
        header: "Attempts",
        cell: ({ row }) => {
            const task = row.original;
            return task.lastError
                ? h("span", { class: "text-muted", title: task.lastError }, `${task.attempts} (${task.lastError.length > 30 ? task.lastError.substring(0, 30) + "…" : task.lastError})`)
                : `${task.attempts}`;
        },
    },
    {
        accessorKey: "createdAt",
        header: "Queued",
        cell: ({ row }) => relativeTime(row.original.createdAt),
    },
    {
        id: "actions",
        cell: ({ row }) =>
            row.original.state === "failed"
                ? h("div", { class: "text-right" }, [
                      h(resolveComponent("UButton"), {
                          label: "Retry",
                          icon: "i-lucide-refresh-ccw",
                          variant: "subtle",
                          color: "neutral",
                          size: "xs",
                          loading: retryingId.value === row.original.id,
                          onClick: () => retryTask(row.original),
                      }),
                  ])
                : null,
    },
];
</script>
<template>
    <UDashboardPanel :ui="{ body: '!p-0' }">
        <template #header>
            <UDashboardNavbar title="Bucket Replication">
                <template #right>
                    <UButton
                        v-if="selectedBucketName"
                        icon="i-lucide-refresh-ccw"
                        color="neutral"
                        variant="subtle"
                        label="Refresh"
                        :loading="isLoading || isLoadingTasks"
                        @click="fetchAll"
                    />
                </template>
            </UDashboardNavbar>

            <UDashboardToolbar :ui="{ left: 'flex-1' }">
                <template #left>
                    <USelectMenu
                        v-model="selectedBucketName"
                        v-model:search-term="bucketSearchTerm"
                        :items="bucketOptions"
                        ignore-filter
                        value-key="value"
                        placeholder="Search buckets…"
                        icon="i-lucide-cylinder"
                        variant="subtle"
                        class="w-full sm:w-72"
                    >
                        <template v-if="isSearchingBuckets" #trailing>
                            <LoadingIndicator :size="14" />
                        </template>
                    </USelectMenu>
                </template>
            </UDashboardToolbar>
        </template>

        <template #body>
            <!-- flex-1 inside the panel's flex-col body centers this vertically in the free space -->
            <div v-if="!selectedBucketName" class="flex-1 flex items-center justify-center">
                <UEmpty
                    title="Select a Bucket"
                    description="Search for a bucket above to view and manage its replication targets, rules, and tasks."
                    icon="i-lucide-repeat"
                    size="lg"
                    variant="naked"
                />
            </div>

            <div v-else class="p-4 space-y-6">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" variant="subtle" />

                <div v-if="isLoadingBucket" class="flex items-center justify-center p-10">
                    <LoadingIndicator />
                </div>

                <template v-else>
                    <UAlert
                        v-if="!isVersioningEnabled"
                        title="Versioning required"
                        description="Replication rules require this bucket's versioning to be Enabled. Enable versioning first, then add rules below."
                        color="warning"
                        variant="subtle"
                        icon="i-lucide-triangle-alert"
                    />

                    <UCard variant="subtle" :ui="{ body: '!p-0' }">
                        <template #header>
                            <CardHeader title="Targets" size="sm" :badge="targets.length > 0 ? targets.length + '' : undefined">
                                <template #rightContent>
                                    <UButton label="Add Target" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" @click="openNewTarget" />
                                </template>
                            </CardHeader>
                        </template>
                        <template #default>
                            <div v-if="isLoading && targets.length === 0" class="flex items-center justify-center p-6">
                                <LoadingIndicator />
                            </div>
                            <UEmpty v-else-if="targets.length === 0" title="No Targets" description="Add a remote S3-compatible destination to replicate to." icon="i-lucide-server" size="sm" variant="naked" class="py-6" />
                            <UTable v-else :data="targets" :columns="targetColumns" :ui="{ th: 'cursor-default' }" />
                        </template>
                    </UCard>

                    <UCard variant="subtle" :ui="{ body: '!p-0' }">
                        <template #header>
                            <CardHeader title="Rules" size="sm" :badge="rules.length > 0 ? rules.length + '' : undefined">
                                <template #rightContent>
                                    <UButton label="Add Rule" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" :disabled="!isVersioningEnabled" @click="openNewRule" />
                                </template>
                            </CardHeader>
                        </template>
                        <template #default>
                            <div v-if="isLoading && rules.length === 0" class="flex items-center justify-center p-6">
                                <LoadingIndicator />
                            </div>
                            <UEmpty v-else-if="rules.length === 0" title="No Rules" description="This bucket has no replication rules yet." icon="i-lucide-repeat" size="sm" variant="naked" class="py-6" />
                            <UTable v-else :data="rules" :columns="ruleColumns" :ui="{ th: 'cursor-default' }" />
                        </template>
                    </UCard>

                    <UCard variant="subtle" :ui="{ body: '!p-0' }">
                        <template #header>
                            <CardHeader title="Recent Tasks" size="sm" :badge="tasks.length > 0 ? tasks.length + '' : undefined">
                                <template #rightContent>
                                    <UButton icon="i-lucide-refresh-ccw" color="neutral" variant="ghost" size="sm" :loading="isLoadingTasks" @click="fetchTasks" />
                                </template>
                            </CardHeader>
                        </template>
                        <template #default>
                            <div v-if="isLoadingTasks && tasks.length === 0" class="flex items-center justify-center p-6">
                                <LoadingIndicator />
                            </div>
                            <UEmpty v-else-if="tasks.length === 0" title="No Tasks Yet" description="Replication activity will show up here once something happens in this bucket." icon="i-lucide-inbox" size="sm" variant="naked" class="py-6" />
                            <UTable v-else :data="tasks" :columns="taskColumns" :ui="{ th: 'cursor-default' }" />
                        </template>
                    </UCard>
                </template>
            </div>
        </template>
    </UDashboardPanel>

    <!-- Target editor -->
    <USlideover v-model:open="targetSlideoverOpen" inset :title="editingTargetId ? 'Edit Target' : 'Add Target'" :ui="{ content: 'w-full max-w-md' }">
        <template #body>
            <div class="space-y-3">
                <div class="flex items-center gap-2">
                    <USwitch v-model="targetDraft.enabled" size="lg" />
                    <span class="text-sm text-muted">{{ targetDraft.enabled ? "Enabled" : "Disabled" }}</span>
                </div>

                <UFormField label="Endpoint">
                    <UInput v-model="targetDraft.endpoint" placeholder="https://remote-endpoint.example.com" variant="subtle" class="w-full" icon="i-lucide-link" />
                </UFormField>

                <UFormField label="Destination bucket">
                    <UInput v-model="targetDraft.targetBucket" placeholder="Destination bucket" variant="subtle" class="w-full" icon="i-lucide-box" />
                </UFormField>

                <UFormField label="Region">
                    <UInput v-model="targetDraft.region" placeholder="Region" variant="subtle" class="w-full" icon="i-lucide-map-pin" />
                </UFormField>

                <UFormField label="Access key ID">
                    <UInput v-model="targetDraft.accessKeyId" placeholder="Access key ID" variant="subtle" class="w-full" icon="i-lucide-key-round" />
                </UFormField>

                <UFormField label="Secret access key">
                    <UInput v-model="targetDraft.secretAccessKey" type="password" placeholder="Secret access key" variant="subtle" class="w-full" icon="i-lucide-key" />
                </UFormField>
            </div>
        </template>

        <template #footer>
            <UButton label="Cancel" color="neutral" variant="subtle" @click="closeTargetSlideover" />
            <UButton label="Save" color="primary" :loading="isSavingTargets" @click="saveTargetDraft" />
        </template>
    </USlideover>

    <!-- Rule editor -->
    <USlideover v-model:open="ruleSlideoverOpen" inset :title="editingRuleId ? 'Edit Rule' : 'Add Rule'" :ui="{ content: 'w-full max-w-md' }">
        <template #body>
            <div class="space-y-3">
                <div class="flex items-center gap-2">
                    <USwitch size="lg" v-model="ruleDraft.enabled" />
                    <span class="text-sm text-muted">{{ ruleDraft.enabled ? "Enabled" : "Disabled" }}</span>
                </div>

                <UFormField label="Target">
                    <USelectMenu v-model="ruleDraft.targetId" :items="targetOptions" value-key="value" placeholder="Select target" variant="subtle" class="w-full" size="lg" />
                </UFormField>

                <UFormField label="Prefix filter">
                    <UInput v-model="ruleDraft.prefix" placeholder="Prefix filter (optional)" variant="subtle" class="w-full" />
                </UFormField>

                <div class="flex flex-wrap gap-4">
                    <label class="flex items-center gap-2 text-sm">
                        <USwitch size="lg" v-model="ruleDraft.replicateDeletes" />
                        Replicate deletes
                    </label>
                    <label class="flex items-center gap-2 text-sm">
                        <USwitch size="lg" v-model="ruleDraft.replicateExisting" />
                        Replicate existing objects
                    </label>
                    <label class="flex items-center gap-2 text-sm">
                        <USwitch size="lg" v-model="ruleDraft.synchronous" />
                        Synchronous
                    </label>
                </div>
                <p v-if="ruleDraft.synchronous" class="text-xs text-muted">
                    Writes to this bucket wait for delivery to this target (up to 20s) before completing, falling back to background retry on failure or timeout.
                </p>
            </div>
        </template>

        <template #footer>
            <UButton label="Cancel" color="neutral" variant="subtle" @click="closeRuleSlideover" />
            <UButton label="Save" color="primary" :loading="isSavingRules" @click="saveRuleDraft" />
        </template>
    </USlideover>
</template>
