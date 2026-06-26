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

const isLoading = ref(false);
const isSaving = ref(false);
const isDeleting = ref(false);
const error = ref("");
const emit = defineEmits(["update:open", "close", "saved"]);
const open = ref(props.open);
const jwtCookie = useJWTCookie();
const toast = useToast();
const { confirm } = useConfirmDialog();

const tagsEndpoint = computed(() => `${useRuntimeConfig().public.apiBaseUrl}/api/v1/buckets/${props.bucket.name}/tags`);

const rows = ref<{ key: string; value: string }[]>([]);

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val) {
            fetchTags();
        }
    },
    { immediate: true }
);

watch(open, (val) => {
    emit("update:open", val);
});

async function fetchTags() {
    try {
        isLoading.value = true;
        error.value = "";

        const response = await $fetch<{ tags: Record<string, string> }>(tagsEndpoint.value, {
            headers: {
                Authorization: `Bearer ${jwtCookie.value}`,
            },
        });

        rows.value = Object.entries(response.tags).map(([key, value]) => ({ key, value }));
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isLoading.value = false;
    }
}

function addRow() {
    rows.value.push({ key: "", value: "" });
}

function removeRow(index: number) {
    rows.value.splice(index, 1);
}

async function save() {
    try {
        isSaving.value = true;
        error.value = "";

        const tags: Record<string, string> = {};
        for (const row of rows.value) {
            if (row.key.trim() === "") continue;
            tags[row.key.trim()] = row.value;
        }

        await $fetch(tagsEndpoint.value, {
            method: "PUT",
            body: JSON.stringify({ tags }),
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${jwtCookie.value}`,
            },
        });

        toast.add({
            title: "Tags saved",
            description: `The tags for "${props.bucket.name}" were updated.`,
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

async function removeAll() {
    const confirmed = await confirm({
        title: "Remove All Tags",
        message: `Do you really want to remove all tags for "${props.bucket.name}"?`,
        confirmLabel: "Remove",
    });
    if (!confirmed) return;

    try {
        isDeleting.value = true;
        error.value = "";

        await $fetch(tagsEndpoint.value, {
            method: "DELETE",
            headers: {
                Authorization: `Bearer ${jwtCookie.value}`,
            },
        });

        rows.value = [];
        toast.add({
            title: "Tags removed",
            description: `All tags for "${props.bucket.name}" were removed.`,
            icon: "i-lucide-circle-check",
            color: "success",
        });

        emit("saved");
        open.value = false;
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isDeleting.value = false;
    }
}
</script>
<template>
    <UModal v-model:open="open" :title="`Bucket Tags — ${bucket.name}`" :ui="{ footer: 'justify-between', content: 'max-w-xl' }">
        <slot />

        <template #body>
            <div class="space-y-4">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" variant="subtle" />

                <div v-if="isLoading" class="flex items-center justify-center p-6">
                    <LoadingIndicator />
                </div>

                <div v-else class="space-y-2">
                    <div v-for="(row, index) in rows" :key="index" class="flex gap-2">
                        <UInput v-model="row.key" placeholder="Key" variant="subtle" class="flex-1" />
                        <UInput v-model="row.value" placeholder="Value" variant="subtle" class="flex-1" />
                        <UButton icon="i-lucide-x" color="neutral" variant="subtle" aria-label="Remove tag" @click="removeRow(index)" />
                    </div>

                    <UButton label="Add Tag" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" @click="addRow" />

                    <UEmpty v-if="rows.length === 0" title="No Tags" description="This bucket has no tags yet." icon="i-lucide-tags" size="sm" variant="naked" />
                </div>
            </div>
        </template>

        <template #footer="{ close }">
            <UButton label="Remove All" color="error" variant="subtle" :loading="isDeleting" :disabled="rows.length === 0" @click="removeAll" />
            <div class="flex gap-2">
                <UButton label="Cancel" color="neutral" variant="subtle" @click="close" />
                <UButton label="Save" :loading="isSaving" color="primary" @click="save" />
            </div>
        </template>
    </UModal>
</template>
