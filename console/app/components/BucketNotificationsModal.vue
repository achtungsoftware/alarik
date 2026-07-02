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

const isLoading = ref(false);
const isSaving = ref(false);
const testingId = ref<string | null>(null);
const error = ref("");
const emit = defineEmits(["update:open", "close", "saved"]);
const open = ref(props.open);
const jwtCookie = useJWTCookie();
const toast = useToast();

const endpoint = computed(() => `${useRuntimeConfig().public.apiBaseUrl}/api/v1/buckets/${props.bucket.name}/notifications`);

const rules = ref<NotificationRule[]>([]);

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val) {
            fetchRules();
        }
    },
    { immediate: true }
);

watch(open, (val) => {
    emit("update:open", val);
});

async function fetchRules() {
    try {
        isLoading.value = true;
        error.value = "";

        const response = await $fetch<{ rules: NotificationRule[] }>(endpoint.value, {
            headers: { Authorization: `Bearer ${jwtCookie.value}` },
        });

        rules.value = response.rules.map((r) => ({ ...r, events: [...r.events] }));
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isLoading.value = false;
    }
}

function addRule() {
    rules.value.push({
        id: ZERO_UUID,
        url: "",
        secret: "",
        events: ["s3:ObjectCreated:*"],
        prefix: "",
        suffix: "",
        enabled: true,
    });
}

function removeRule(index: number) {
    rules.value.splice(index, 1);
}

async function save() {
    try {
        isSaving.value = true;
        error.value = "";

        // Drop empty optional fields so they serialize as absent, not ""
        const payload = {
            rules: rules.value.map((r: any) => ({
                id: r.id,
                url: r.url.trim(),
                secret: r.secret?.trim() ? r.secret.trim() : undefined,
                events: r.events,
                prefix: r.prefix?.trim() ? r.prefix.trim() : undefined,
                suffix: r.suffix?.trim() ? r.suffix.trim() : undefined,
                enabled: r.enabled,
            })),
        };

        const response = await $fetch<{ rules: NotificationRule[] }>(endpoint.value, {
            method: "PUT",
            body: JSON.stringify(payload),
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${jwtCookie.value}`,
            },
        });

        // Re-sync with server-assigned ids so "Send test" works without re-opening
        rules.value = response.rules.map((r) => ({ ...r, events: [...r.events] }));

        toast.add({
            title: "Webhooks saved",
            description: `Notification rules for "${props.bucket.name}" were updated.`,
            icon: "i-lucide-circle-check",
            color: "success",
        });

        emit("saved");
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isSaving.value = false;
    }
}

async function sendTest(rule: NotificationRule) {
    if (rule.id === ZERO_UUID) {
        error.value = "Save the rule before sending a test event.";
        return;
    }
    try {
        testingId.value = rule.id;
        error.value = "";

        await $fetch(`${endpoint.value}/${rule.id}/test`, {
            method: "POST",
            headers: { Authorization: `Bearer ${jwtCookie.value}` },
        });

        toast.add({
            title: "Test event queued",
            description: `A test event was sent to ${rule.url}.`,
            icon: "i-lucide-send",
            color: "success",
        });
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        testingId.value = null;
    }
}
</script>
<template>
    <UModal v-model:open="open" :title="`Webhooks — ${bucket.name}`" :ui="{ footer: 'justify-end', content: 'max-w-2xl' }">
        <slot />

        <template #body>
            <div class="space-y-4">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" variant="subtle" />

                <p class="text-sm text-muted">
                    Alarik POSTs an S3-compatible event to each enabled URL when matching objects change. Add a secret to receive an
                    <span class="font-mono">X-Alarik-Signature-256</span> HMAC header for verification.
                </p>

                <div v-if="isLoading" class="flex items-center justify-center p-6">
                    <LoadingIndicator />
                </div>

                <div v-else class="space-y-4">
                    <UCard v-for="(rule, index) in rules" :key="index" variant="subtle">
                        <div class="space-y-3">
                            <div class="flex items-center gap-2">
                                <USwitch v-model="rule.enabled" />
                                <span class="text-sm text-muted">{{ rule.enabled ? "Enabled" : "Disabled" }}</span>
                                <div class="flex-1" />
                                <UButton
                                    label="Send test"
                                    icon="i-lucide-send"
                                    variant="subtle"
                                    color="neutral"
                                    size="xs"
                                    :loading="testingId === rule.id"
                                    @click="sendTest(rule)"
                                />
                                <UButton icon="i-lucide-trash-2" color="error" variant="subtle" size="xs" aria-label="Remove rule" @click="removeRule(index)" />
                            </div>

                            <UInput v-model="rule.url" placeholder="https://example.com/webhook" variant="subtle" class="w-full" icon="i-lucide-link" />

                            <USelectMenu v-model="rule.events" :items="EVENT_OPTIONS" value-key="value" multiple placeholder="Select events" variant="subtle" class="w-full" />

                            <div class="flex gap-2">
                                <UInput v-model="rule.prefix" placeholder="Prefix filter (optional)" variant="subtle" class="flex-1" />
                                <UInput v-model="rule.suffix" placeholder="Suffix filter (optional)" variant="subtle" class="flex-1" />
                            </div>

                            <UInput v-model="rule.secret" type="password" placeholder="Signing secret (optional)" variant="subtle" class="w-full" icon="i-lucide-key" />
                        </div>
                    </UCard>

                    <UButton label="Add Webhook" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" @click="addRule" />

                    <UEmpty v-if="rules.length === 0" title="No Webhooks" description="This bucket has no notification rules yet." icon="i-lucide-webhook" size="sm" variant="naked" />
                </div>
            </div>
        </template>

        <template #footer="{ close }">
            <UButton label="Cancel" color="neutral" variant="subtle" @click="close" />
            <UButton label="Save" :loading="isSaving" color="primary" @click="save" />
        </template>
    </UModal>
</template>
