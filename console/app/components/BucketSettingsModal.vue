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

// Self-service bucket settings for the owner of the bucket - the endpoints below are the
// non-admin ("only buckets you own") ones, so this modal is for a user managing their own bucket,
// not for an admin acting on someone else's. Deliberately a general "settings" modal rather than a
// single-purpose one so future per-bucket options can be added as extra sections.
const props = withDefaults(
    defineProps<{
        open: boolean;
        bucket: Bucket;
    }>(),
    {
        open: false,
    }
);

const emit = defineEmits(["update:open", "close", "saved"]);
const open = ref(props.open);
const jwtCookie = useJWTCookie();
const toast = useToast();

const isSaving = ref(false);
const error = ref("");

// --- Versioning section ------------------------------------------------------
// S3 semantics: a bucket is "Disabled" only until versioning is first touched; after that the two
// states are "Enabled" and "Suspended". So the switch maps on -> Enabled, off -> Suspended, and
// "Disabled" only ever shows as the initial state (turning the switch off on a Disabled bucket is
// a no-op we don't submit).
const versioningEnabled = ref(props.bucket.versioningStatus === "Enabled");

const versioningEndpoint = computed(
    () => `${useRuntimeConfig().public.apiBaseUrl}/api/v1/buckets/${props.bucket.name}/versioning`
);

const versioningChanged = computed(() => {
    const current = props.bucket.versioningStatus;
    if (!versioningEnabled.value && current === "Disabled") return false; // already off, nothing to do
    const target = versioningEnabled.value ? "Enabled" : "Suspended";
    return target !== current;
});

const hasChanges = computed(() => versioningChanged.value);

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val) {
            // Reset each section from the bucket's current state every time the modal opens.
            versioningEnabled.value = props.bucket.versioningStatus === "Enabled";
            error.value = "";
        }
    }
);

watch(open, (val) => emit("update:open", val));

async function save() {
    try {
        isSaving.value = true;
        error.value = "";

        if (versioningChanged.value) {
            await $fetch(versioningEndpoint.value, {
                method: "PUT",
                body: JSON.stringify({ status: versioningEnabled.value ? "Enabled" : "Suspended" }),
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${jwtCookie.value}`,
                },
            });
        }

        toast.add({
            title: "Settings saved",
            description: `Settings for "${props.bucket.name}" were updated.`,
            icon: "i-lucide-circle-check",
            color: "success",
        });

        emit("saved");
        open.value = false;
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isSaving.value = false;
    }
}
</script>
<template>
    <UModal v-model:open="open" :title="`Bucket Settings — ${bucket.name}`" :ui="{ footer: 'justify-end', content: 'max-w-xl' }">
        <slot />

        <template #body>
            <div class="space-y-6">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" variant="subtle" />

                <!-- Versioning -->
                <div class="flex items-start justify-between gap-4">
                    <div class="space-y-1">
                        <div class="flex items-center gap-2">
                            <p class="text-sm font-medium">Object versioning</p>
                            <UBadge
                                :label="bucket.versioningStatus"
                                size="sm"
                                variant="subtle"
                                :color="bucket.versioningStatus === 'Enabled' ? 'success' : 'neutral'"
                            />
                        </div>
                        <p class="text-xs text-muted">
                            Keep every version of an object on overwrite and delete. Turning it off later
                            <span class="font-medium">suspends</span> versioning - existing versions are retained, new
                            writes stop creating them.
                        </p>
                    </div>
                    <USwitch v-model="versioningEnabled" />
                </div>
            </div>
        </template>

        <template #footer="{ close }">
            <UButton label="Cancel" color="neutral" variant="subtle" @click="close" />
            <UButton label="Save" :loading="isSaving" :disabled="!hasChanges" color="primary" @click="save" />
        </template>
    </UModal>
</template>
