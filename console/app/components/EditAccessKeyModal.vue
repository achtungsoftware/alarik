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

const props = withDefaults(
    defineProps<{
        open: boolean;
        accessKey: AccessKey;
    }>(),
    {
        open: false,
    }
);

const isLoading = ref(false);
const error = ref("");
const emit = defineEmits(["update:open", "saved"]);
const open = ref(props.open);

watch(
    () => props.open,
    (val) => {
        open.value = val;
    }
);

watch(open, (val) => {
    emit("update:open", val);
});

const state = reactive({
    description: props.accessKey.description ?? "",
});

async function submitForm(event: FormSubmitEvent<typeof state>) {
    event.preventDefault();
    try {
        isLoading.value = true;
        error.value = "";

        await useApi()(`/users/accessKeys/${props.accessKey.id}`, {
            method: "PATCH",
            body: JSON.stringify({ description: state.description.trim() }),
            headers: {
                "Content-Type": "application/json",
            },
        });

        emit("saved");
        open.value = false;
    } catch (err) {
        error.value = errorMessage(err, "Unknown error");
    } finally {
        isLoading.value = false;
    }
}
</script>
<template>
    <FormModal v-model:open="open" title="Edit Description" :description="accessKey.accessKey" submit-label="Save" :loading="isLoading" :error="error" :state="state" @submit="submitForm">
        <template #trigger>
            <slot />
        </template>

        <UFormField label="Description" name="description" help="Optional note to help identify what this key is used for. Leave empty to remove it.">
            <UTextarea placeholder="e.g. Backup server" v-model="state.description" :maxlength="500" class="w-full" size="lg" variant="subtle" />
        </UFormField>
    </FormModal>
</template>
