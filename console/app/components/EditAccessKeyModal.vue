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
const form = useTemplateRef("form");
const jwtCookie = useJWTCookie();

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

async function submitForm(event: FormSubmitEvent<any>) {
    event.preventDefault();
    try {
        isLoading.value = true;
        error.value = "";

        await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/users/accessKeys/${props.accessKey.id}`, {
            method: "PATCH",
            body: JSON.stringify({ description: state.description.trim() }),
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${jwtCookie.value}`,
            },
        });

        emit("saved");
        open.value = false;
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isLoading.value = false;
    }
}
</script>
<template>
    <UModal v-model:open="open" title="Edit Description" :description="accessKey.accessKey" :ui="{ footer: 'justify-end' }">
        <slot />

        <template #body>
            <UForm ref="form" :state="state" @submit="submitForm" class="space-y-4">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" variant="subtle" />

                <UFormField label="Description" name="description" help="Optional note to help identify what this key is used for. Leave empty to remove it.">
                    <UTextarea placeholder="e.g. Backup server" v-model="state.description" :maxlength="500" class="w-full" size="lg" variant="subtle" />
                </UFormField>
            </UForm>
        </template>

        <template #footer="{ close }">
            <UButton label="Cancel" color="neutral" variant="subtle" @click="close" />
            <UButton label="Save" :loading="isLoading" color="primary" @click="form?.submit()" />
        </template>
    </UModal>
</template>
