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
import type { FormSubmitEvent } from "@nuxt/ui";

export type ObjectCopyMode = "copy" | "move" | "rename";

/// Copy, move and rename share one dialog and one server call (`POST /objects/copy`): the mode
/// only decides which fields show and whether the source is removed afterwards.
const props = withDefaults(
    defineProps<{
        open: boolean;
        bucket: string;
        item: BrowserItem;
        mode: ObjectCopyMode;
    }>(),
    {
        open: false,
    }
);

const emit = defineEmits<{
    "update:open": [value: boolean];
    done: [item: BrowserItem];
}>();

const open = computed({
    get: () => props.open,
    set: (value: boolean) => emit("update:open", value),
});

const toast = useToast();
const { copyObject } = useObjectService();

const isSaving = ref(false);
const error = ref("");
const buckets = ref<string[]>([]);

const fileName = computed(() => props.item.key.split("/").pop() ?? props.item.key);
const folder = computed(() => props.item.key.slice(0, props.item.key.length - fileName.value.length));

const state = reactive({
    destinationBucket: props.bucket,
    destinationKey: props.item.key,
    newName: fileName.value,
});

const titles: Record<ObjectCopyMode, { title: string; action: string }> = {
    copy: { title: "Copy Object", action: "Copy" },
    move: { title: "Move Object", action: "Move" },
    rename: { title: "Rename Object", action: "Rename" },
};

const targetKey = computed(() => (props.mode === "rename" ? folder.value + state.newName.trim() : state.destinationKey.trim()));

const validationMessage = computed(() => {
    if (!targetKey.value || targetKey.value.endsWith("/")) return "Enter a file name, not a folder.";
    if (state.destinationBucket === props.bucket && targetKey.value === props.item.key) {
        return props.mode === "copy" ? "Choose a different destination to copy to." : "The name is unchanged.";
    }
    return "";
});

watch(
    () => props.open,
    async (val) => {
        if (!val) return;
        error.value = "";
        state.destinationBucket = props.bucket;
        state.destinationKey = props.item.key;
        state.newName = fileName.value;
        if (props.mode !== "rename") await fetchBuckets();
    },
    { immediate: true }
);

async function fetchBuckets() {
    try {
        const page = await useApi()<Page<Bucket>>(`/buckets`, { params: { page: 1, per: 100 } });
        buckets.value = page.items.map((bucket: Bucket) => bucket.name);
        if (!buckets.value.includes(props.bucket)) buckets.value.unshift(props.bucket);
    } catch (err) {
        // The current bucket is always a valid destination; the picker just stays short.
        logFailure("Failed to load buckets for the copy dialog:", err);
        buckets.value = [props.bucket];
    }
}

async function submitForm(_event: FormSubmitEvent<typeof state>) {
    if (validationMessage.value) {
        error.value = validationMessage.value;
        return;
    }
    try {
        isSaving.value = true;
        error.value = "";
        const result = await copyObject({
            sourceBucket: props.bucket,
            sourceKey: props.item.key,
            destinationBucket: state.destinationBucket,
            destinationKey: targetKey.value,
            deleteSource: props.mode !== "copy",
        });
        toast.add({
            title: `${titles[props.mode].title.split(" ")[0]} complete`,
            description: `"${fileName.value}" → ${state.destinationBucket}/${targetKey.value}`,
            icon: "i-lucide-circle-check",
            color: "success",
        });
        emit("done", result);
        open.value = false;
    } catch (err) {
        error.value = errorMessage(err, "The operation failed");
    } finally {
        isSaving.value = false;
    }
}
</script>

<template>
    <FormModal v-model:open="open" :title="titles[mode].title" :description="item.key" :submit-label="titles[mode].action" :loading="isSaving" :error="error" :state="state" @submit="submitForm">
        <template v-if="mode === 'rename'">
            <UFormField label="New name" name="newName" :help="folder ? `Stays in ${folder}` : undefined">
                <UInput v-model="state.newName" class="w-full" variant="subtle" autofocus />
            </UFormField>
        </template>
        <template v-else>
            <UFormField label="Destination bucket" name="destinationBucket">
                <USelect v-model="state.destinationBucket" :items="buckets" class="w-full" variant="subtle" />
            </UFormField>
            <UFormField label="Destination key" name="destinationKey" help="The full key, folders included - e.g. archive/2024/report.pdf">
                <UInput v-model="state.destinationKey" class="w-full" variant="subtle" />
            </UFormField>
        </template>
        <UAlert
            v-if="mode !== 'copy'"
            color="neutral"
            variant="subtle"
            icon="i-lucide-info"
            description="The original is removed once the copy is stored. On a versioned bucket it gets a delete marker, so its history stays available."
        />
    </FormModal>
</template>
