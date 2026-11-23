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
        // Manage the policy via the site-wide admin endpoints (any bucket) instead of the
        // self-service ones (only buckets you own). Defaults to self-service, since every
        // user - not just admins - needs to manage policies on their own buckets.
        admin?: boolean;
    }>(),
    {
        open: false,
        admin: false,
    }
);

const isLoading = ref(false);
const isDeleting = ref(false);
const error = ref("");
const emit = defineEmits(["update:open", "close", "saved"]);
const open = ref(props.open);
const toast = useToast();
const { confirm } = useConfirmDialog();

const policyEndpoint = computed(
    () => `/${props.admin ? "admin/" : ""}buckets/${props.bucket.name}/policy`
);

const policyText = ref(props.bucket.policy ?? "");

/// Caught here so a typo is explained inline, not as a server rejection after a round trip.
const jsonError = computed(() => {
    if (policyText.value.trim() === "") return "";
    try {
        const parsed: unknown = JSON.parse(policyText.value);
        if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return "A policy must be a JSON object.";
        if (!("Statement" in parsed)) return "A policy needs a Statement array.";
        return "";
    } catch (err) {
        return `Invalid JSON: ${err instanceof Error ? err.message : String(err)}`;
    }
});

function formatPolicy() {
    if (jsonError.value) return;
    policyText.value = JSON.stringify(JSON.parse(policyText.value), null, 4);
}

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val) {
            // Reset to the bucket's current policy each time the modal is opened
            policyText.value = props.bucket.policy ?? "";
            error.value = "";
        }
    }
);

watch(open, (val) => {
    emit("update:open", val);
});

function fillPublicReadTemplate() {
    policyText.value = JSON.stringify(
        {
            Version: "2012-10-17",
            Statement: [
                {
                    Sid: "PublicRead",
                    Effect: "Allow",
                    Principal: "*",
                    Action: "s3:GetObject",
                    Resource: `arn:aws:s3:::${props.bucket.name}/*`,
                },
            ],
        },
        null,
        4
    );
}

async function save() {
    if (jsonError.value) {
        error.value = jsonError.value;
        return;
    }
    try {
        isLoading.value = true;
        error.value = "";

        await useApi()(policyEndpoint.value, {
            method: "PUT",
            body: JSON.stringify({ policy: policyText.value }),
            headers: {
                "Content-Type": "application/json",
            },
        });

        toast.add({
            title: "Policy saved",
            description: `The bucket policy for "${props.bucket.name}" was updated.`,
            icon: "i-lucide-circle-check",
            color: "success",
        });

        emit("saved");
        open.value = false;
    } catch (err) {
        error.value = errorMessage(err, "Unknown error");
    } finally {
        isLoading.value = false;
    }
}

async function removePolicy() {
    const confirmed = await confirm({
        title: "Remove Bucket Policy",
        message: `Do you really want to remove the policy for "${props.bucket.name}"? Any public access it currently grants will stop working immediately.`,
        confirmLabel: "Remove",
    });
    if (!confirmed) return;

    try {
        isDeleting.value = true;
        error.value = "";

        await useApi()(policyEndpoint.value, {
            method: "DELETE",
        });

        policyText.value = "";
        toast.add({
            title: "Policy removed",
            description: `The bucket policy for "${props.bucket.name}" was removed.`,
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
    <UModal v-model:open="open" :title="`Bucket Policy — ${bucket.name}`" :ui="{ footer: 'justify-between', content: 'max-w-2xl' }">
        <slot />

        <template #body>
            <div class="space-y-4">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" variant="subtle" />

                <UAlert
                    title="Anonymous public read access only"
                    description="A bucket policy can grant unauthenticated GetObject/ListBucket access. Only Effect: Allow, Principal: &quot;*&quot;, and the GetObject / GetObjectVersion / ListBucket actions are currently supported - anything else is rejected when saving."
                    color="info"
                    variant="subtle"
                    icon="i-lucide-info"
                />

                <div class="flex justify-end gap-2">
                    <UButton label="Format JSON" variant="subtle" color="neutral" size="sm" icon="i-lucide-braces" :disabled="!!jsonError || policyText.trim() === ''" @click="formatPolicy" />
                    <UButton label="Make bucket public (read-only)" variant="subtle" color="neutral" size="sm" icon="i-lucide-globe" @click="fillPublicReadTemplate" />
                </div>

                <UTextarea
                    v-model="policyText"
                    placeholder="Paste or write a bucket policy JSON document here..."
                    :rows="14"
                    variant="subtle"
                    :ui="{ base: 'font-mono text-xs' }"
                    class="w-full"
                />
                <p v-if="jsonError" class="text-xs text-error">{{ jsonError }}</p>
            </div>
        </template>

        <template #footer="{ close }">
            <UButton label="Remove Policy" color="error" variant="subtle" :loading="isDeleting" :disabled="!bucket.policy" @click="removePolicy" />
            <div class="flex gap-2">
                <UButton label="Cancel" color="neutral" variant="subtle" @click="close" />
                <UButton label="Save" :loading="isLoading" color="primary" :disabled="!!jsonError || policyText.trim() === ''" @click="save" />
            </div>
        </template>
    </UModal>
</template>
