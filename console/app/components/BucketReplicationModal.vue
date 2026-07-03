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

const props = withDefaults(
    defineProps<{
        open: boolean;
        bucket: Bucket;
    }>(),
    {
        open: false,
    }
);

const ZERO_UUID = "00000000-0000-0000-0000-000000000000";

const isLoading = ref(false);
const isSavingTargets = ref(false);
const isSavingRules = ref(false);
const resyncingId = ref<string | null>(null);
const error = ref("");
const emit = defineEmits(["update:open", "close", "saved"]);
const open = ref(props.open);
const jwtCookie = useJWTCookie();
const toast = useToast();

const base = computed(() => `${useRuntimeConfig().public.apiBaseUrl}/api/v1/buckets/${props.bucket.name}/replication`);
const authHeaders = computed(() => ({ Authorization: `Bearer ${jwtCookie.value}` }));

const isVersioningEnabled = computed(() => props.bucket.versioningStatus === "Enabled");

const targets = ref<ReplicationTarget[]>([]);
const rules = ref<ReplicationRule[]>([]);
const tasks = ref<ReplicationTask[]>([]);
const isLoadingTasks = ref(false);
const retryingId = ref<string | null>(null);

const targetOptions = computed(() =>
    targets.value
        .filter((t: any) => t.id !== ZERO_UUID)
        .map((t: any) => ({ label: `${t.endpoint} → ${t.targetBucket}`, value: t.id }))
);

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val) {
            fetchAll();
        }
    },
    { immediate: true }
);

watch(open, (val) => {
    emit("update:open", val);
});

async function fetchAll() {
    try {
        isLoading.value = true;
        error.value = "";

        const [targetsResponse, rulesResponse] = await Promise.all([
            $fetch<{ targets: ReplicationTarget[] }>(`${base.value}/targets`, { headers: authHeaders.value }),
            $fetch<{ rules: ReplicationRule[] }>(`${base.value}/rules`, { headers: authHeaders.value }),
        ]);

        targets.value = targetsResponse.targets.map((t) => ({ ...t }));
        rules.value = rulesResponse.rules.map((r) => ({ ...r }));
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isLoading.value = false;
    }
    await fetchTasks();
}

function addTarget() {
    targets.value.push({
        id: ZERO_UUID,
        endpoint: "",
        targetBucket: "",
        accessKeyId: "",
        secretAccessKey: "",
        region: "us-east-1",
        enabled: true,
    });
}

function removeTarget(index: number) {
    targets.value.splice(index, 1);
}

async function saveTargets() {
    try {
        isSavingTargets.value = true;
        error.value = "";

        const payload = {
            targets: targets.value.map((t: any) => ({
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

        // Re-sync with server-assigned ids so newly added targets are selectable by rules
        targets.value = response.targets.map((t) => ({ ...t }));

        toast.add({
            title: "Replication targets saved",
            description: `Targets for "${props.bucket.name}" were updated.`,
            icon: "i-lucide-circle-check",
            color: "success",
        });

        emit("saved");
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isSavingTargets.value = false;
    }
}

function addRule() {
    if (targetOptions.value.length === 0) {
        error.value = "Add and save a replication target before creating a rule.";
        return;
    }
    rules.value.push({
        id: ZERO_UUID,
        targetId: targetOptions.value[0]!.value,
        prefix: "",
        replicateDeletes: false,
        replicateExisting: false,
        synchronous: false,
        enabled: true,
    });
}

function removeRule(index: number) {
    rules.value.splice(index, 1);
}

async function saveRules() {
    try {
        isSavingRules.value = true;
        error.value = "";

        const payload = {
            rules: rules.value.map((r: any) => ({
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

        rules.value = response.rules.map((r) => ({ ...r }));

        toast.add({
            title: "Replication rules saved",
            description: `Rules for "${props.bucket.name}" were updated.`,
            icon: "i-lucide-circle-check",
            color: "success",
        });

        emit("saved");
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isSavingRules.value = false;
    }
}

async function resync(rule: ReplicationRule) {
    if (rule.id === ZERO_UUID) {
        error.value = "Save the rule before triggering a resync.";
        return;
    }
    try {
        resyncingId.value = rule.id;
        error.value = "";

        await $fetch(`${base.value}/rules/${rule.id}/resync`, {
            method: "POST",
            headers: authHeaders.value,
        });

        // The walk runs in the background - it's not done by the time this returns, just
        // started. Check the Recent Tasks section below for progress.
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

async function fetchTasks() {
    try {
        isLoadingTasks.value = true;

        const response = await $fetch<{ tasks: ReplicationTask[] }>(`${base.value}/tasks`, { headers: authHeaders.value });
        tasks.value = response.tasks;
    } catch (err: any) {
        // Non-fatal: the targets/rules editors above are the primary content of this modal, a
        // failed task-history fetch shouldn't block using it.
        console.error("Failed to fetch replication tasks:", err);
    } finally {
        isLoadingTasks.value = false;
    }
}

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

function targetLabel(targetId: string): string {
    const target = targets.value.find((t: any) => t.id === targetId);
    return target ? `${target.endpoint} → ${target.targetBucket}` : "Unknown target";
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
</script>
<template>
    <UModal v-model:open="open" :title="`Replication — ${bucket.name}`" :ui="{ footer: 'justify-end', content: 'max-w-2xl' }">
        <slot />

        <template #body>
            <div class="space-y-4">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" variant="subtle" />

                <UAlert
                    v-if="!isVersioningEnabled"
                    title="Versioning required"
                    description="Replication rules require this bucket's versioning to be Enabled. Enable versioning first, then add rules below."
                    color="warning"
                    variant="subtle"
                    icon="i-lucide-triangle-alert"
                />

                <UAlert description="Alarik pushes a copy of each written or deleted object to the target buckets below, matched by rule prefix. Deletes and pre-existing objects are opt-in per rule." color="info" variant="subtle" />

                <div v-if="isLoading" class="flex items-center justify-center p-6">
                    <LoadingIndicator />
                </div>

                <div v-else class="space-y-6">
                    <div class="space-y-3">
                        <CardHeader title="Targets" size="sm" />

                        <UCard v-for="(target, index) in targets" :key="index" variant="subtle">
                            <div class="space-y-3">
                                <div class="flex items-center gap-2">
                                    <USwitch v-model="target.enabled" />
                                    <span class="text-sm text-muted">{{ target.enabled ? "Enabled" : "Disabled" }}</span>
                                    <div class="flex-1" />
                                    <UButton icon="i-lucide-trash-2" color="error" variant="subtle" size="xs" aria-label="Remove target" @click="removeTarget(index)" />
                                </div>

                                <UInput v-model="target.endpoint" placeholder="https://remote-endpoint.example.com" variant="subtle" class="w-full" icon="i-lucide-link" />

                                <div class="flex gap-2">
                                    <UInput v-model="target.targetBucket" placeholder="Destination bucket" variant="subtle" class="flex-1" icon="i-lucide-box" />
                                    <UInput v-model="target.region" placeholder="Region" variant="subtle" class="flex-1" icon="i-lucide-map-pin" />
                                </div>

                                <div class="flex gap-2">
                                    <UInput v-model="target.accessKeyId" placeholder="Access key ID" variant="subtle" class="flex-1" icon="i-lucide-key-round" />
                                    <UInput v-model="target.secretAccessKey" type="password" placeholder="Secret access key" variant="subtle" class="flex-1" icon="i-lucide-key" />
                                </div>
                            </div>
                        </UCard>

                        <UButton label="Add Target" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" @click="addTarget" />
                        <UEmpty v-if="targets.length === 0" title="No Targets" description="Add a remote S3-compatible destination to replicate to." icon="i-lucide-server" size="sm" variant="naked" />

                        <div class="flex justify-end">
                            <UButton label="Save Targets" :loading="isSavingTargets" color="primary" size="sm" @click="saveTargets" />
                        </div>
                    </div>

                    <div class="space-y-3">
                        <CardHeader title="Rules" size="sm" />

                        <UCard v-for="(rule, index) in rules" :key="index" variant="subtle">
                            <div class="space-y-3">
                                <div class="flex items-center gap-2">
                                    <USwitch v-model="rule.enabled" />
                                    <span class="text-sm text-muted">{{ rule.enabled ? "Enabled" : "Disabled" }}</span>
                                    <div class="flex-1" />
                                    <UButton
                                        v-if="rule.replicateExisting"
                                        label="Resync now"
                                        icon="i-lucide-refresh-ccw"
                                        variant="subtle"
                                        color="neutral"
                                        size="xs"
                                        :loading="resyncingId === rule.id"
                                        @click="resync(rule)"
                                    />
                                    <UButton icon="i-lucide-trash-2" color="error" variant="subtle" size="xs" aria-label="Remove rule" @click="removeRule(index)" />
                                </div>

                                <USelectMenu
                                    v-model="rule.targetId"
                                    :items="targetOptions"
                                    value-key="value"
                                    placeholder="Select target"
                                    variant="subtle"
                                    class="w-full"
                                    size="lg"
                                />

                                <UInput v-model="rule.prefix" placeholder="Prefix filter (optional)" variant="subtle" class="w-full" />

                                <div class="flex flex-wrap gap-4">
                                    <label class="flex items-center gap-2 text-sm">
                                        <USwitch v-model="rule.replicateDeletes" />
                                        Replicate deletes
                                    </label>
                                    <label class="flex items-center gap-2 text-sm">
                                        <USwitch v-model="rule.replicateExisting" />
                                        Replicate existing objects
                                    </label>
                                    <label class="flex items-center gap-2 text-sm">
                                        <USwitch v-model="rule.synchronous" />
                                        Synchronous
                                    </label>
                                </div>
                                <p v-if="rule.synchronous" class="text-xs text-muted">
                                    Writes to this bucket wait for delivery to this target (up to 20s) before completing, falling back to background retry on failure or timeout.
                                </p>
                            </div>
                        </UCard>

                        <UButton label="Add Rule" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" :disabled="!isVersioningEnabled" @click="addRule" />
                        <UEmpty v-if="rules.length === 0" title="No Rules" description="This bucket has no replication rules yet." icon="i-lucide-repeat" size="sm" variant="naked" />

                        <div class="flex justify-end">
                            <UButton label="Save Rules" :loading="isSavingRules" color="primary" size="sm" :disabled="!isVersioningEnabled" @click="saveRules" />
                        </div>
                    </div>
                </div>

                <UCard variant="subtle">
                    <template #header>
                        <CardHeader title="Recent Tasks" size="sm" :badge="tasks.length > 0 ? tasks.length + '' : undefined">
                            <template #rightContent>
                                <UButton icon="i-lucide-refresh-ccw" color="neutral" variant="ghost" size="sm" :loading="isLoadingTasks" @click="fetchTasks" />
                            </template>
                        </CardHeader>
                    </template>
                    <template #default>
                        <div v-if="isLoadingTasks && tasks.length === 0" class="flex items-center justify-center p-4">
                            <LoadingIndicator />
                        </div>
                        <UEmpty v-else-if="tasks.length === 0" title="No Tasks Yet" description="Replication activity will show up here once something happens in this bucket." icon="i-lucide-inbox" size="sm" variant="naked" />
                        <div v-else class="divide-y divide-default -mx-4">
                            <div v-for="task in tasks" :key="task.id" class="px-4 py-2 flex items-start justify-between gap-2">
                                <div class="min-w-0 flex-1">
                                    <div class="flex items-center gap-2">
                                        <UBadge :color="task.state === 'failed' ? 'error' : 'warning'" variant="subtle" size="xs">
                                            {{ task.state === "failed" ? "Failed" : "Pending" }}
                                        </UBadge>
                                        <UBadge color="neutral" variant="subtle" size="xs">{{ task.operation }}</UBadge>
                                        <span class="text-xs text-muted truncate">{{ task.key }} → {{ targetLabel(task.targetId) }}</span>
                                    </div>
                                    <div class="text-xs text-muted mt-1">
                                        {{ relativeTime(task.createdAt) }} · {{ task.attempts }} attempt{{ task.attempts === 1 ? "" : "s" }}
                                        <span v-if="task.lastError" :title="task.lastError"> · {{ task.lastError.length > 40 ? task.lastError.substring(0, 40) + "…" : task.lastError }}</span>
                                    </div>
                                </div>
                                <UButton
                                    v-if="task.state === 'failed'"
                                    label="Retry"
                                    icon="i-lucide-refresh-ccw"
                                    variant="subtle"
                                    color="neutral"
                                    size="xs"
                                    :loading="retryingId === task.id"
                                    @click="retryTask(task)"
                                />
                            </div>
                        </div>
                    </template>
                </UCard>
            </div>
        </template>

        <template #footer="{ close }">
            <UButton label="Close" color="neutral" variant="subtle" @click="close" />
        </template>
    </UModal>
</template>
