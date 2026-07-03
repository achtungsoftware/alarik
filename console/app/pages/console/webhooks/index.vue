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
    title: "Webhooks",
});

const ZERO_UUID = "00000000-0000-0000-0000-000000000000";

const EVENT_OPTIONS = [
    { label: "Object created (any)", value: "s3:ObjectCreated:*" },
    { label: "Object created — Put", value: "s3:ObjectCreated:Put" },
    { label: "Object created — Copy", value: "s3:ObjectCreated:Copy" },
    { label: "Object created — Multipart complete", value: "s3:ObjectCreated:CompleteMultipartUpload" },
    { label: "Object removed (any)", value: "s3:ObjectRemoved:*" },
    { label: "Object removed — Delete", value: "s3:ObjectRemoved:Delete" },
    { label: "Object removed — Delete marker", value: "s3:ObjectRemoved:DeleteMarkerCreated" },
    { label: "Lifecycle expiration (any)", value: "s3:LifecycleExpiration:*" },
];

interface Delivery {
    id: string;
    ruleId: string;
    url: string;
    state: "pending" | "failed";
    attempts: number;
    nextAttemptAt: string;
    lastError?: string;
    createdAt: string;
}

const route = useRoute();
const router = useRouter();
const jwtCookie = useJWTCookie();
const toast = useToast();
const { confirm } = useConfirmDialog();
const authHeaders = computed(() => ({ Authorization: `Bearer ${jwtCookie.value}` }));
const apiBase = computed(() => useRuntimeConfig().public.apiBaseUrl);

// ── Bucket selection - search-as-you-type, server-side ──────────────────────
// A deployment can have far too many buckets to load into a <select> up front, so this
// never fetches "all buckets": it searches `?search=` on the buckets endpoint, debounced,
// and only ever holds a small page of matching results in memory.

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

// Ensures the currently-selected bucket always renders with the right label, even before
// any search has resolved it (e.g. arriving here via a deep link) or if it falls outside
// the current search results.
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

// ── Webhook data ─────────────────────────────────────────────────────────────

const isLoading = ref(false);
const isLoadingDeliveries = ref(false);
const error = ref("");

const base = computed(() => `${apiBase.value}/api/v1/buckets/${selectedBucketName.value}/notifications`);

const rules = ref<NotificationRule[]>([]);
const deliveries = ref<Delivery[]>([]);

// Below every ref it touches - `immediate: true` runs this synchronously during setup, so
// referencing a ref declared later in the script would be a temporal-dead-zone crash.
watch(
    selectedBucketName,
    async (name) => {
        rules.value = [];
        deliveries.value = [];
        error.value = "";
        if (!name) return;
        await fetchAll();
    },
    { immediate: true }
);

async function fetchAll() {
    try {
        isLoading.value = true;
        error.value = "";

        const response = await $fetch<{ rules: NotificationRule[] }>(base.value, { headers: authHeaders.value });
        rules.value = response.rules;
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isLoading.value = false;
    }
    await fetchDeliveries();
}

async function fetchDeliveries() {
    try {
        isLoadingDeliveries.value = true;
        const response = await $fetch<{ deliveries: Delivery[] }>(`${base.value}/deliveries`, { headers: authHeaders.value });
        deliveries.value = response.deliveries;
    } catch (err) {
        // Non-fatal: the rules table above is the primary content of this page, a failed
        // delivery-history fetch shouldn't block using it.
        console.error("Failed to fetch deliveries:", err);
    } finally {
        isLoadingDeliveries.value = false;
    }
}

// ── Rules: table + slideover editor ─────────────────────────────────────────

const isSavingRules = ref(false);
const testingId = ref<string | null>(null);
const ruleSlideoverOpen = ref(false);
const editingRuleId = ref<string | null>(null);
const ruleDraft = reactive({
    url: "",
    secret: "",
    events: ["s3:ObjectCreated:*"] as string[],
    prefix: "",
    suffix: "",
    enabled: true,
});

function resetRuleDraft() {
    ruleDraft.url = "";
    ruleDraft.secret = "";
    ruleDraft.events = ["s3:ObjectCreated:*"];
    ruleDraft.prefix = "";
    ruleDraft.suffix = "";
    ruleDraft.enabled = true;
}

function openNewRule() {
    editingRuleId.value = null;
    resetRuleDraft();
    ruleSlideoverOpen.value = true;
}

function closeRuleSlideover() {
    ruleSlideoverOpen.value = false;
}

function openEditRule(rule: NotificationRule) {
    editingRuleId.value = rule.id;
    ruleDraft.url = rule.url;
    ruleDraft.secret = rule.secret ?? "";
    ruleDraft.events = [...rule.events];
    ruleDraft.prefix = rule.prefix ?? "";
    ruleDraft.suffix = rule.suffix ?? "";
    ruleDraft.enabled = rule.enabled;
    ruleSlideoverOpen.value = true;
}

async function persistRules(next: NotificationRule[]) {
    isSavingRules.value = true;
    error.value = "";
    try {
        // Drop empty optional fields so they serialize as absent, not ""
        const payload = {
            rules: next.map((r: NotificationRule) => ({
                id: r.id,
                url: r.url.trim(),
                secret: r.secret?.trim() ? r.secret.trim() : undefined,
                events: r.events,
                prefix: r.prefix?.trim() ? r.prefix.trim() : undefined,
                suffix: r.suffix?.trim() ? r.suffix.trim() : undefined,
                enabled: r.enabled,
            })),
        };

        const response = await $fetch<{ rules: NotificationRule[] }>(base.value, {
            method: "PUT",
            body: JSON.stringify(payload),
            headers: { "Content-Type": "application/json", ...authHeaders.value },
        });
        rules.value = response.rules;
        return true;
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
        toast.add({
            title: "Failed to Save Webhook",
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
        ? rules.value.map((r: NotificationRule) => (r.id === editingRuleId.value ? { ...r, ...ruleDraft } : r))
        : [...rules.value, { id: ZERO_UUID, ...ruleDraft }];

    if (!(await persistRules(next))) return;

    ruleSlideoverOpen.value = false;
    toast.add({
        title: "Webhook saved",
        description: `Webhooks for "${selectedBucketName.value}" were updated.`,
        icon: "i-lucide-circle-check",
        color: "success",
    });
}

async function deleteRule(rule: NotificationRule) {
    const confirmed = await confirm({
        title: "Remove Webhook",
        message: `Remove the webhook "${rule.url}"? Its queued deliveries will be dropped and events will stop being sent there.`,
        confirmLabel: "Remove",
    });
    if (!confirmed) return;

    if (await persistRules(rules.value.filter((r: NotificationRule) => r.id !== rule.id))) {
        toast.add({
            title: "Webhook removed",
            icon: "i-lucide-circle-check",
            color: "success",
        });
    }
}

async function sendTest(rule: NotificationRule) {
    try {
        testingId.value = rule.id;
        error.value = "";

        await $fetch(`${base.value}/${rule.id}/test`, {
            method: "POST",
            headers: authHeaders.value,
        });

        toast.add({
            title: "Test event queued",
            description: `A test event was sent to ${rule.url}.`,
            icon: "i-lucide-send",
            color: "success",
        });

        await fetchDeliveries();
    } catch (err: any) {
        toast.add({
            title: "Test Failed",
            description: err.response?._data?.reason ?? "Unknown error",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    } finally {
        testingId.value = null;
    }
}

// ── Deliveries: table + retry ────────────────────────────────────────────────

const retryingId = ref<string | null>(null);

async function retryDelivery(delivery: Delivery) {
    try {
        retryingId.value = delivery.id;

        await $fetch(`${base.value}/deliveries/${delivery.id}/retry`, {
            method: "POST",
            headers: authHeaders.value,
        });

        toast.add({
            title: "Retry queued",
            description: `Redelivery to ${delivery.url} has been queued.`,
            icon: "i-lucide-refresh-ccw",
            color: "success",
        });

        await fetchDeliveries();
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

function eventLabel(value: string): string {
    return EVENT_OPTIONS.find((option) => option.value === value)?.label ?? value;
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

const ruleColumns: TableColumn<NotificationRule>[] = [
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
        accessorKey: "url",
        header: "URL",
        cell: ({ row }) =>
            h("div", { class: "flex flex-col min-w-0" }, [
                h("span", { class: "font-medium text-highlighted truncate" }, row.original.url),
                row.original.secret ? h("span", { class: "text-xs text-muted" }, "Signed (HMAC)") : null,
            ]),
    },
    {
        id: "events",
        header: "Events",
        cell: ({ row }) =>
            h(
                "div",
                { class: "flex flex-wrap gap-1" },
                row.original.events.map((event: string) => h(resolveComponent("UBadge"), { label: eventLabel(event), size: "md", color: "neutral", variant: "subtle" }))
            ),
    },
    {
        id: "filters",
        header: "Filters",
        cell: ({ row }) => {
            const rule = row.original;
            const badges = [];
            if (rule.prefix) badges.push(h(resolveComponent("UBadge"), { label: `Prefix: ${rule.prefix}`, size: "md", color: "neutral", variant: "subtle" }));
            if (rule.suffix) badges.push(h(resolveComponent("UBadge"), { label: `Suffix: ${rule.suffix}`, size: "md", color: "neutral", variant: "subtle" }));
            return badges.length > 0 ? h("div", { class: "flex flex-wrap gap-1" }, badges) : h("span", { class: "text-muted" }, "—");
        },
    },
    {
        id: "actions",
        cell: ({ row }) => {
            const rule = row.original;
            return h("div", { class: "flex items-center justify-end gap-2" }, [
                h(resolveComponent("UButton"), {
                    label: "Send test",
                    icon: "i-lucide-send",
                    variant: "subtle",
                    color: "neutral",
                    size: "xs",
                    loading: testingId.value === rule.id,
                    onClick: () => sendTest(rule),
                }),
                h(resolveComponent("UButton"), {
                    label: "Edit",
                    icon: "i-lucide-pencil",
                    variant: "subtle",
                    color: "neutral",
                    size: "xs",
                    onClick: () => openEditRule(rule),
                }),
                h(resolveComponent("UButton"), {
                    icon: "i-lucide-trash-2",
                    variant: "subtle",
                    color: "error",
                    size: "xs",
                    "aria-label": "Remove webhook",
                    onClick: () => deleteRule(rule),
                }),
            ]);
        },
    },
];

const deliveryColumns: TableColumn<Delivery>[] = [
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
        accessorKey: "url",
        header: "URL",
        cell: ({ row }) => h("span", { class: "truncate" }, row.original.url),
    },
    {
        accessorKey: "attempts",
        header: "Attempts",
        cell: ({ row }) => {
            const delivery = row.original;
            return delivery.lastError
                ? h("span", { class: "text-muted", title: delivery.lastError }, `${delivery.attempts} (${delivery.lastError.length > 30 ? delivery.lastError.substring(0, 30) + "…" : delivery.lastError})`)
                : `${delivery.attempts}`;
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
                          onClick: () => retryDelivery(row.original),
                      }),
                  ])
                : null,
    },
];
</script>
<template>
    <UDashboardPanel :ui="{ body: '!p-0' }">
        <template #header>
            <UDashboardNavbar title="Webhooks">
                <template #right>
                    <UButton
                        v-if="selectedBucketName"
                        icon="i-lucide-refresh-ccw"
                        color="neutral"
                        variant="subtle"
                        label="Refresh"
                        :loading="isLoading || isLoadingDeliveries"
                        @click="fetchAll"
                    />
                </template>
            </UDashboardNavbar>

            <!-- The toolbar's left slot container doesn't grow by default (`flex items-center`
                 only), so a `w-full` child collapses to its content width - `flex-1` makes the
                 container take the row, which `w-full` can then actually fill on mobile. -->
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
                    description="Search for a bucket above to view and manage its webhooks and recent deliveries."
                    icon="i-lucide-webhook"
                    size="lg"
                    variant="naked"
                />
            </div>

            <div v-else class="p-4 space-y-6">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" variant="subtle" />

                <UCard variant="subtle" :ui="{ body: '!p-0' }">
                    <template #header>
                        <CardHeader title="Webhooks" size="sm" :badge="rules.length > 0 ? rules.length + '' : undefined">
                            <template #rightContent>
                                <UButton label="Add Webhook" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" @click="openNewRule" />
                            </template>
                        </CardHeader>
                    </template>
                    <template #default>
                        <div v-if="isLoading && rules.length === 0" class="flex items-center justify-center p-6">
                            <LoadingIndicator />
                        </div>
                        <UEmpty v-else-if="rules.length === 0" title="No Webhooks" description="This bucket has no notification rules yet." icon="i-lucide-webhook" size="sm" variant="naked" class="py-6" />
                        <UTable v-else :data="rules" :columns="ruleColumns" :ui="{ th: 'cursor-default' }" />
                    </template>
                </UCard>

                <UCard variant="subtle" :ui="{ body: '!p-0' }">
                    <template #header>
                        <CardHeader title="Recent Deliveries" size="sm" :badge="deliveries.length > 0 ? deliveries.length + '' : undefined">
                            <template #rightContent>
                                <UButton icon="i-lucide-refresh-ccw" color="neutral" variant="ghost" size="sm" :loading="isLoadingDeliveries" @click="fetchDeliveries" />
                            </template>
                        </CardHeader>
                    </template>
                    <template #default>
                        <div v-if="isLoadingDeliveries && deliveries.length === 0" class="flex items-center justify-center p-6">
                            <LoadingIndicator />
                        </div>
                        <UEmpty v-else-if="deliveries.length === 0" title="No Deliveries Yet" description="Events will show up here once something happens in this bucket." icon="i-lucide-inbox" size="sm" variant="naked" class="py-6" />
                        <UTable v-else :data="deliveries" :columns="deliveryColumns" :ui="{ th: 'cursor-default' }" />
                    </template>
                </UCard>
            </div>
        </template>
    </UDashboardPanel>

    <!-- Webhook editor -->
    <USlideover v-model:open="ruleSlideoverOpen" inset :title="editingRuleId ? 'Edit Webhook' : 'Add Webhook'" :ui="{ content: 'w-full max-w-md' }">
        <template #body>
            <div class="space-y-3">
                <div class="flex items-center gap-2">
                    <USwitch size="lg" v-model="ruleDraft.enabled" />
                    <span class="text-sm text-muted">{{ ruleDraft.enabled ? "Enabled" : "Disabled" }}</span>
                </div>

                <UFormField label="URL">
                    <UInput v-model="ruleDraft.url" placeholder="https://example.com/webhook" variant="subtle" class="w-full" icon="i-lucide-link" />
                </UFormField>

                <UFormField label="Events">
                    <USelectMenu v-model="ruleDraft.events" :items="EVENT_OPTIONS" value-key="value" multiple placeholder="Select events" variant="subtle" class="w-full" size="lg" />
                </UFormField>

                <UFormField label="Prefix filter">
                    <UInput v-model="ruleDraft.prefix" placeholder="Prefix filter (optional)" variant="subtle" class="w-full" />
                </UFormField>

                <UFormField label="Suffix filter">
                    <UInput v-model="ruleDraft.suffix" placeholder="Suffix filter (optional)" variant="subtle" class="w-full" />
                </UFormField>

                <UFormField label="Signing secret" help="When set, deliveries carry an X-Alarik-Signature-256 HMAC header the receiver can verify.">
                    <UInput v-model="ruleDraft.secret" type="password" placeholder="Signing secret (optional)" variant="subtle" class="w-full" icon="i-lucide-key" />
                </UFormField>
            </div>
        </template>

        <template #footer>
            <UButton label="Cancel" color="neutral" variant="subtle" @click="closeRuleSlideover" />
            <UButton label="Save" color="primary" :loading="isSavingRules" @click="saveRuleDraft" />
        </template>
    </USlideover>
</template>
