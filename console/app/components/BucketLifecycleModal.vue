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

/// Editor for the bucket's lifecycle rules - the same rule shape the S3 `?lifecycle` API
/// stores, saved as a whole set like PutBucketLifecycleConfiguration.
const props = withDefaults(
    defineProps<{
        open: boolean;
        bucket: Bucket;
    }>(),
    {
        open: false,
    }
);

const isLoading = ref(false);
const isSaving = ref(false);
const isDeleting = ref(false);
const error = ref("");
const emit = defineEmits(["update:open", "close", "saved"]);
const open = ref(props.open);
const toast = useToast();
const { confirm } = useConfirmDialog();

const endpoint = computed(() => `/buckets/${props.bucket.name}/lifecycle`);

type RuleRow = {
    id: string;
    enabled: boolean;
    prefix: string;
    expirationDays: number | null;
    noncurrentVersionExpirationDays: number | null;
    abortIncompleteMultipartUploadDays: number | null;
};

const rows = ref<RuleRow[]>([]);
const hadRules = ref(false);

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val) fetchRules();
    },
    { immediate: true }
);

watch(open, (val) => {
    emit("update:open", val);
});

function toRow(rule: LifecycleRule): RuleRow {
    return {
        id: rule.id,
        enabled: rule.enabled,
        prefix: rule.prefix,
        expirationDays: rule.expirationDays ?? null,
        noncurrentVersionExpirationDays: rule.noncurrentVersionExpirationDays ?? null,
        abortIncompleteMultipartUploadDays: rule.abortIncompleteMultipartUploadDays ?? null,
    };
}

async function fetchRules() {
    try {
        isLoading.value = true;
        error.value = "";
        const response = await useApi()<{ rules: LifecycleRule[] }>(endpoint.value);
        rows.value = response.rules.map(toRow);
        hadRules.value = rows.value.length > 0;
    } catch (err) {
        error.value = errorMessage(err, "Unknown error");
    } finally {
        isLoading.value = false;
    }
}

function addRow() {
    rows.value.push({
        id: "",
        enabled: true,
        prefix: "",
        expirationDays: 30,
        noncurrentVersionExpirationDays: null,
        abortIncompleteMultipartUploadDays: null,
    });
}

function removeRow(index: number) {
    rows.value.splice(index, 1);
}

/// A number input clears to "" - store that as "no action", not as 0.
function days(value: number | null): number | null {
    return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/// Mirrors the server's checks so the common mistakes are explained inline, before a request.
const validationMessage = computed(() => {
    if (rows.value.length === 0) return "";
    const ids = new Set<string>();
    for (const [index, row] of rows.value.entries()) {
        const label = row.id.trim() || `rule ${index + 1}`;
        const actions = [days(row.expirationDays), days(row.noncurrentVersionExpirationDays), days(row.abortIncompleteMultipartUploadDays)].filter((d): d is number => d !== null);
        if (actions.length === 0) return `${label}: set at least one action.`;
        if (actions.some((d) => d <= 0 || !Number.isInteger(d))) return `${label}: days must be whole numbers above 0.`;
        if (row.id.trim() && ids.has(row.id.trim())) return `Rule ID "${row.id.trim()}" is used twice.`;
        if (row.id.trim()) ids.add(row.id.trim());
    }
    return "";
});

async function save() {
    if (validationMessage.value) {
        error.value = validationMessage.value;
        return;
    }
    if (rows.value.length === 0) {
        await removeAll(false);
        return;
    }
    try {
        isSaving.value = true;
        error.value = "";

        const rules: LifecycleRule[] = rows.value.map((row) => ({
            id: row.id.trim(),
            enabled: row.enabled,
            prefix: row.prefix,
            expirationDays: days(row.expirationDays),
            noncurrentVersionExpirationDays: days(row.noncurrentVersionExpirationDays),
            abortIncompleteMultipartUploadDays: days(row.abortIncompleteMultipartUploadDays),
        }));

        await useApi()(endpoint.value, {
            method: "PUT",
            body: { rules },
        });

        toast.add({
            title: "Lifecycle saved",
            description: `${rules.length} rule${rules.length === 1 ? "" : "s"} for "${props.bucket.name}" saved.`,
            icon: "i-lucide-circle-check",
            color: "success",
        });

        emit("saved");
        open.value = false;
    } catch (err) {
        error.value = errorMessage(err, "Unknown error");
    } finally {
        isSaving.value = false;
    }
}

async function removeAll(ask = true) {
    if (ask) {
        const confirmed = await confirm({
            title: "Remove Lifecycle Rules",
            message: `Do you really want to remove every lifecycle rule for "${props.bucket.name}"? Objects stop expiring automatically.`,
            confirmLabel: "Remove",
        });
        if (!confirmed) return;
    }

    try {
        isDeleting.value = true;
        error.value = "";

        await useApi()(endpoint.value, { method: "DELETE" });

        rows.value = [];
        toast.add({
            title: "Lifecycle removed",
            description: `All lifecycle rules for "${props.bucket.name}" were removed.`,
            icon: "i-lucide-circle-check",
            color: "success",
        });

        emit("saved");
        open.value = false;
    } catch (err) {
        error.value = errorMessage(err, "Unknown error");
    } finally {
        isDeleting.value = false;
    }
}
</script>
<template>
    <UModal v-model:open="open" :title="`Lifecycle — ${bucket.name}`" description="Expire objects, prune old versions and abandoned multipart uploads automatically." :ui="{ footer: 'justify-between', content: 'max-w-3xl' }">
        <slot />

        <template #body>
            <div class="space-y-4">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" variant="subtle" />

                <div v-if="isLoading" class="flex items-center justify-center p-6">
                    <LoadingIndicator />
                </div>

                <div v-else class="space-y-3">
                    <UCard v-for="(row, index) in rows" :key="index" variant="subtle">
                        <div class="space-y-3">
                            <div class="flex items-center gap-2">
                                <UInput v-model="row.id" placeholder="Rule ID (optional)" variant="subtle" class="flex-1" />
                                <UInput v-model="row.prefix" placeholder="Prefix, e.g. logs/ (empty = whole bucket)" variant="subtle" class="flex-1" />
                                <USwitch v-model="row.enabled" label="Enabled" />
                                <UButton icon="i-lucide-x" color="neutral" variant="subtle" aria-label="Remove rule" @click="removeRow(index)" />
                            </div>
                            <div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
                                <UFormField label="Expire objects after (days)" size="xs">
                                    <UInputNumber v-model="row.expirationDays" :min="1" placeholder="—" variant="subtle" class="w-full" />
                                </UFormField>
                                <UFormField label="Expire old versions after (days)" size="xs">
                                    <UInputNumber v-model="row.noncurrentVersionExpirationDays" :min="1" placeholder="—" variant="subtle" class="w-full" />
                                </UFormField>
                                <UFormField label="Abort unfinished uploads after (days)" size="xs">
                                    <UInputNumber v-model="row.abortIncompleteMultipartUploadDays" :min="1" placeholder="—" variant="subtle" class="w-full" />
                                </UFormField>
                            </div>
                        </div>
                    </UCard>

                    <p v-if="validationMessage" class="text-xs text-error">{{ validationMessage }}</p>

                    <UButton label="Add Rule" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" @click="addRow" />

                    <UEmpty v-if="rows.length === 0" title="No Lifecycle Rules" description="Objects in this bucket are kept until deleted." icon="i-lucide-timer" size="sm" variant="naked" />
                </div>
            </div>
        </template>

        <template #footer="{ close }">
            <UButton label="Remove All" color="error" variant="subtle" :loading="isDeleting" :disabled="!hadRules" @click="removeAll()" />
            <div class="flex gap-2">
                <UButton label="Cancel" color="neutral" variant="subtle" @click="close" />
                <UButton label="Save" :loading="isSaving" color="primary" :disabled="!!validationMessage" @click="save" />
            </div>
        </template>
    </UModal>
</template>
