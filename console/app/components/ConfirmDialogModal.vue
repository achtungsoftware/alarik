<script setup lang="ts">
import type { ButtonProps } from "@nuxt/ui";
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

defineProps<{
    title?: string;
    message: string;
    confirmLabel?: string;
    confirmColor?: ButtonProps["color"];
}>();

const emit = defineEmits<{
    close: [confirmed: boolean];
}>();

const open = ref(true);

function handleConfirm() {
    emit("close", true);
}

function handleCancel() {
    emit("close", false);
}
</script>

<template>
    <UModal v-model:open="open" :title="title ?? 'Are you sure?'" :ui="{ footer: 'justify-end' }" @update:open="(val) => !val && handleCancel()">
        <template #body>
            <p>{{ message }}</p>
        </template>

        <template #footer>
            <UButton label="Cancel" color="neutral" variant="subtle" @click="handleCancel" />
            <UButton @click="handleConfirm" :label="confirmLabel ?? 'Continue'" :color="confirmColor ?? 'error'" />
        </template>
    </UModal>
</template>
