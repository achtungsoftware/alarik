<!--
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
-->
<script setup lang="ts" generic="T extends Record<string, unknown>">
import type { FormSubmitEvent } from "@nuxt/ui";

/// The shell every create/edit dialog shares - the trigger slot, a form with an error alert,
/// and Cancel plus a submit button that shows the saving state - so a modal only declares its
/// fields and what to do on submit.
const props = withDefaults(
    defineProps<{
        open: boolean;
        title: string;
        description?: string;
        submitLabel?: string;
        loading?: boolean;
        error?: string;
        state: T;
    }>(),
    {
        description: undefined,
        submitLabel: "Save",
        loading: false,
        error: "",
    }
);

const emit = defineEmits<{
    "update:open": [value: boolean];
    submit: [event: FormSubmitEvent<T>];
}>();

const open = computed({
    get: () => props.open,
    set: (value: boolean) => emit("update:open", value),
});
const form = useTemplateRef("form");
</script>

<template>
    <UModal v-model:open="open" :title="title" :description="description" :ui="{ footer: 'justify-end' }">
        <slot name="trigger" />

        <template #body>
            <UForm ref="form" :state="state" @submit="emit('submit', $event)" class="space-y-4">
                <UAlert v-if="error" title="Error" :description="error" color="error" variant="subtle" />
                <slot />
            </UForm>
        </template>

        <template #footer="{ close }">
            <UButton label="Cancel" color="neutral" variant="subtle" @click="close" />
            <UButton :label="submitLabel" :loading="loading" color="primary" @click="form?.submit()" />
        </template>
    </UModal>
</template>
