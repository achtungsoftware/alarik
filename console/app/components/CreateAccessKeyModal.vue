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
const form = useTemplateRef("form");
const jwtCookie = useJWTCookie();
const inputDateRef = useTemplateRef("inputDateRef");
const expirationDate = shallowRef<CalendarDate | null>(null);

// Fetch generated access key
const {
    data: accessKeyGeneratorResponse,
    status: accessKeyGeneratorStatus,
    refresh,
} = await useFetch<{ accessKeyId: string; secretAccessKey: string }>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/accessKeyGenerator`, {
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
});

async function submitForm(event: FormSubmitEvent<any>) {
    event.preventDefault();
    try {
        isLoading.value = true;
        error.value = "";

        const response = await $fetch<{ token: string }>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/users/accessKeys`, {
            method: "POST",
            body: JSON.stringify({
                ...state,
                expirationDate: expirationDate.value ? expirationDate.value.toDate("UTC").toISOString() : null,
            }),
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${jwtCookie.value}`,
            },
        });

        window.location.reload();
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Unknown error";
    } finally {
        isLoading.value = false;
    }
}
</script>
<template>
    <UModal v-model:open="open" title="Create Access Key" :ui="{ footer: 'justify-end' }">
        <slot />

        <template #body>
            <UForm ref="form" :state="state" @submit="submitForm" class="space-y-4">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" class="" variant="subtle" />

                <UFormField required label="Access Key" name="accessKey">
                    <UInput placeholder="Access Key" v-model="state.accessKey" class="w-full" size="lg" variant="subtle" />
                </UFormField>

                <UFormField required label="Secret Key" name="secretKey">
                    <UInput placeholder="Secret Key" v-model="state.secretKey" class="w-full" size="lg" variant="subtle" />
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
            </UForm>
        </template>

        <template #footer="{ close }">
            <UButton label="Cancel" color="neutral" variant="subtle" @click="close" />
            <UButton label="Create" :loading="isLoading" color="primary" @click="form?.submit()" />
        </template>
    </UModal>
</template>
