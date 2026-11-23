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

/// Editor for the bucket's CORS rules - what browsers on other origins may do against the S3
/// API of this bucket. Saved as a whole set like PutBucketCors.
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

const endpoint = computed(() => `/buckets/${props.bucket.name}/cors`);

const methodOptions = ["GET", "PUT", "HEAD", "POST", "DELETE"];

// Kept out of the template: a `//` inside a static attribute breaks vue-tsc's generated code.
const originPlaceholder = "https://app.example.com";
const originsHelp = "One per line. A lone * matches any origin; one * inside a value matches any run of characters (any subdomain, for example).";
const headersHelp = "One per line, * allowed once per entry (x-amz-*).";
const wildcard = "*";

/// Lists are edited as one-per-line text; the rows keep the server's array shape otherwise.
type RuleRow = {
    id: string;
    allowedOrigins: string;
    allowedMethods: string[];
    allowedHeaders: string;
    exposeHeaders: string;
    maxAgeSeconds: number | null;
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

function toRow(rule: CORSRule): RuleRow {
    return {
        id: rule.id ?? "",
        allowedOrigins: rule.allowedOrigins.join("\n"),
        allowedMethods: [...rule.allowedMethods],
        allowedHeaders: rule.allowedHeaders.join("\n"),
        exposeHeaders: rule.exposeHeaders.join("\n"),
        maxAgeSeconds: rule.maxAgeSeconds ?? null,
    };
}

function lines(text: string): string[] {
    return text
        .split(/[\n,]/)
        .map((line) => line.trim())
        .filter((line) => line !== "");
}

function toRule(row: RuleRow): CORSRule {
    return {
        id: row.id.trim() || null,
        allowedOrigins: lines(row.allowedOrigins),
        allowedMethods: row.allowedMethods,
        allowedHeaders: lines(row.allowedHeaders),
        exposeHeaders: lines(row.exposeHeaders),
        maxAgeSeconds: typeof row.maxAgeSeconds === "number" && Number.isFinite(row.maxAgeSeconds) ? row.maxAgeSeconds : null,
    };
}

async function fetchRules() {
    try {
        isLoading.value = true;
        error.value = "";
        const response = await useApi()<{ rules: CORSRule[] }>(endpoint.value);
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
        allowedOrigins: "",
        allowedMethods: ["GET", "HEAD"],
        allowedHeaders: "*",
        exposeHeaders: "ETag",
        maxAgeSeconds: 3000,
    });
}

function removeRow(index: number) {
    rows.value.splice(index, 1);
}

/// Mirrors the server's checks so the common mistakes are explained inline, before a request.
const validationMessage = computed(() => {
    for (const [index, row] of rows.value.entries()) {
        const rule = toRule(row);
        const label = rule.id || `rule ${index + 1}`;
        if (rule.allowedOrigins.length === 0) return `${label}: add at least one allowed origin.`;
        if (rule.allowedMethods.length === 0) return `${label}: pick at least one method.`;
        const doubleStar = [...rule.allowedOrigins, ...rule.allowedHeaders].find((value) => value.split("*").length > 2);
        if (doubleStar) return `${label}: "${doubleStar}" may contain one * at most.`;
        if (rule.maxAgeSeconds !== null && rule.maxAgeSeconds !== undefined && rule.maxAgeSeconds < 0) return `${label}: max age cannot be negative.`;
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

        const rules = rows.value.map(toRule);
        await useApi()(endpoint.value, {
            method: "PUT",
            body: { rules },
        });

        toast.add({
            title: "CORS saved",
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
            title: "Remove CORS Rules",
            message: `Do you really want to remove every CORS rule for "${props.bucket.name}"? Browsers on other origins lose access immediately.`,
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
            title: "CORS removed",
            description: `All CORS rules for "${props.bucket.name}" were removed.`,
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
    <UModal v-model:open="open" :title="`CORS — ${bucket.name}`" description="Which web origins may call this bucket's S3 API from the browser. Rules apply in order; the first match wins." :ui="{ footer: 'justify-between', content: 'max-w-3xl' }">
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
                                <UButton icon="i-lucide-x" color="neutral" variant="subtle" aria-label="Remove rule" @click="removeRow(index)" />
                            </div>
                            <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
                                <UFormField label="Allowed origins" :help="originsHelp" size="xs">
                                    <UTextarea v-model="row.allowedOrigins" :rows="3" variant="subtle" class="w-full" :placeholder="originPlaceholder" />
                                </UFormField>
                                <UFormField label="Allowed headers" :help="headersHelp" size="xs">
                                    <UTextarea v-model="row.allowedHeaders" :rows="3" variant="subtle" class="w-full" :placeholder="wildcard" />
                                </UFormField>
                                <UFormField label="Allowed methods" size="xs">
                                    <UCheckboxGroup v-model="row.allowedMethods" :items="methodOptions" orientation="horizontal" />
                                </UFormField>
                                <UFormField label="Exposed headers" help="Response headers scripts may read, one per line." size="xs">
                                    <UTextarea v-model="row.exposeHeaders" :rows="2" variant="subtle" class="w-full" placeholder="ETag" />
                                </UFormField>
                                <UFormField label="Preflight cache (seconds)" size="xs">
                                    <UInputNumber v-model="row.maxAgeSeconds" :min="0" placeholder="—" variant="subtle" class="w-full" />
                                </UFormField>
                            </div>
                        </div>
                    </UCard>

                    <p v-if="validationMessage" class="text-xs text-error">{{ validationMessage }}</p>

                    <UButton label="Add Rule" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" @click="addRow" />

                    <UEmpty v-if="rows.length === 0" title="No CORS Rules" description="Browsers on other origins cannot call this bucket directly." icon="i-lucide-globe" size="sm" variant="naked" />
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
