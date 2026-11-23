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
import { CalendarDate } from "@internationalized/date";

const props = withDefaults(
    defineProps<{
        open: boolean;
    }>(),
    {
        open: false,
    }
);

const isLoading = ref(false);
const error = ref("");
const emit = defineEmits(["update:open", "close"]);
const open = ref(props.open);
const inputDateRef = useTemplateRef("inputDateRef");
const expirationDate = shallowRef<CalendarDate | null>(null);

// Fetch generated access key
const {
    data: accessKeyGeneratorResponse,
    status: accessKeyGeneratorStatus,
    refresh,
} = await useFetch<{ accessKeyId: string; secretAccessKey: string }>(`/accessKeyGenerator`, {
    $fetch: useApi(),
    default: () => ({ accessKeyId: "", secretAccessKey: "" }),
});

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
    accessKey: accessKeyGeneratorResponse?.value.accessKeyId ?? "",
    secretKey: accessKeyGeneratorResponse?.value.secretAccessKey ?? "",
    description: "",
});

async function submitForm(event: FormSubmitEvent<typeof state>) {
    event.preventDefault();
    try {
        isLoading.value = true;
        error.value = "";

        const response = await useApi()<{ token: string }>(`/users/accessKeys`, {
            method: "POST",
            body: JSON.stringify({
                ...state,
                description: state.description.trim(),
                expirationDate: expirationDate.value ? expirationDate.value.toDate("UTC").toISOString() : null,
            }),
            headers: {
                "Content-Type": "application/json",
            },
        });

        window.location.reload();
    } catch (err) {
        error.value = errorMessage(err, "Unknown error");
    } finally {
        isLoading.value = false;
    }
}
</script>
<template>
    <FormModal v-model:open="open" title="Create Access Key" submit-label="Create" :loading="isLoading" :error="error" :state="state" @submit="submitForm">
        <template #trigger>
            <slot />
        </template>

        <UFormField required label="Access Key" name="accessKey">
            <UInput placeholder="Access Key" v-model="state.accessKey" class="w-full" size="lg" variant="subtle" />
        </UFormField>

        <UFormField required label="Secret Key" name="secretKey">
            <UInput placeholder="Secret Key" v-model="state.secretKey" class="w-full" size="lg" variant="subtle" />
        </UFormField>
        <UFormField label="Description" name="description" help="Optional note to help identify what this key is used for.">
            <UTextarea placeholder="e.g. Backup server" v-model="state.description" :maxlength="500" class="w-full" size="lg" variant="subtle" />
        </UFormField>
        <UFormField label="Expiration Date" name="expirationDate" help="The Access Key will be deleted automatically if you set a date.">
            <UInputDate ref="inputDateRef" v-model="expirationDate" variant="subtle" size="lg" class="w-full">
                <template #trailing>
                    <UPopover :reference="inputDateRef?.inputsRef[3]?.$el">
                        <UButton color="neutral" variant="link" size="sm" icon="i-lucide-calendar" aria-label="Select a date" class="px-0" />

                        <template #content>
                            <UCalendar v-model="expirationDate" class="p-2" />
                        </template>
                    </UPopover>
                </template>
            </UInputDate>
        </UFormField>
    </FormModal>
</template>
