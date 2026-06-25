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
        bucket: string;
        item: BrowserItem | null;
    }>(),
    {
        open: false,
    }
);

const isLoading = ref(false);
const error = ref("");
const emit = defineEmits(["update:open", "close"]);
const open = ref(props.open);
const jwtCookie = useJWTCookie();
const toast = useToast();

const expiresInSeconds = ref(3600);
const generatedURL = ref("");
const expiresAt = ref<Date | null>(null);

const presets = [
    { label: "1 hour", seconds: 3600 },
    { label: "1 day", seconds: 86400 },
    { label: "3 days", seconds: 259200 },
    { label: "7 days", seconds: 604800 },
];

const fileName = computed(() => props.item?.key.split("/").filter(Boolean).pop() ?? "");

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val) {
            // Reset every time the modal is reopened, for a new or the same file
            generatedURL.value = "";
            expiresAt.value = null;
            error.value = "";
            expiresInSeconds.value = 3600;
        }
    }
);

watch(open, (val) => {
    emit("update:open", val);
});

async function generateLink() {
    if (!props.item) return;

    try {
        isLoading.value = true;
        error.value = "";

        const response = await $fetch<{ url: string; expiresAt: string }>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/share`, {
            method: "POST",
            body: JSON.stringify({
                bucket: props.bucket,
                key: props.item.key,
                expiresInSeconds: expiresInSeconds.value,
            }),
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${jwtCookie.value}`,
            },
        });

        generatedURL.value = response.url;
        expiresAt.value = new Date(response.expiresAt);
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isLoading.value = false;
    }
}

async function copyLink() {
    if (!generatedURL.value) return;
    await navigator.clipboard.writeText(generatedURL.value);
    toast.add({
        title: "Copied",
        description: "The link was copied to your clipboard.",
        icon: "i-lucide-circle-check",
        color: "success",
    });
}
</script>
<template>
    <UModal v-model:open="open" title="Share File" :ui="{ footer: 'justify-end' }">
        <slot />

        <template #body>
            <div class="space-y-4">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" variant="subtle" />

                <p class="text-sm text-muted">Generate a temporary public link to <span class="font-medium text-highlighted">{{ fileName }}</span>. Anyone with the link can download the file until it expires - no account needed.</p>

                <div v-if="!generatedURL">
                    <p class="text-sm font-medium mb-2">Expires in</p>
                    <div class="flex gap-2">
                        <UButton v-for="preset in presets" :key="preset.seconds" :label="preset.label" :variant="expiresInSeconds === preset.seconds ? 'solid' : 'subtle'" :color="expiresInSeconds === preset.seconds ? 'primary' : 'neutral'" size="sm" @click="expiresInSeconds = preset.seconds" />
                    </div>
                </div>

                <div v-else class="space-y-2">
                    <p class="text-sm font-medium">Shareable link</p>
                    <div class="flex gap-2">
                        <UInput :model-value="generatedURL" readonly class="w-full" variant="subtle" />
                        <UButton icon="i-lucide-copy" color="neutral" variant="subtle" aria-label="Copy link" @click="copyLink" />
                    </div>
                    <p v-if="expiresAt" class="text-xs text-muted">Expires {{ expiresAt.toLocaleString() }}</p>
                </div>
            </div>
        </template>

        <template #footer="{ close }">
            <UButton label="Close" color="neutral" variant="subtle" @click="close" />
            <UButton v-if="!generatedURL" label="Generate Link" :loading="isLoading" color="primary" @click="generateLink" />
            <UButton v-else label="Generate New Link" :loading="isLoading" color="primary" variant="subtle" @click="generateLink" />
        </template>
    </UModal>
</template>
