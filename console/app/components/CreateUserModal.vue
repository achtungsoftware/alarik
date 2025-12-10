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
    name: "",
    username: "",
    password: "",
    isAdmin: false
});

async function submitForm(event: FormSubmitEvent<any>) {
    event.preventDefault();
    try {
        isLoading.value = true;
        error.value = "";

        const response = await $fetch<{ token: string }>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/admin/users`, {
            method: "POST",
            body: JSON.stringify(state),
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
    <UModal v-model:open="open" title="Create User" :ui="{ footer: 'justify-end' }">
        <slot />

        <template #body>
            <UForm ref="form" :state="state" @submit="submitForm" class="space-y-4">
                <UAlert v-if="error != ''" title="Error" :description="error" color="error" class="" variant="subtle" />

                <UFormField required label="Name">
                    <UInput placeholder="Name" v-model="state.name" class="w-full" size="lg" variant="subtle" />
                </UFormField>

                <UFormField required label="Username">
                    <UInput autocomplete="off" :autocorrect="false" placeholder="Username" v-model="state.username" class="w-full" size="lg" variant="subtle" />
                </UFormField>
                <UFormField required label="Password">
                    <UInput autocomplete="off" :autocorrect="false" placeholder="Password" type="password" v-model="state.password" class="w-full" size="lg" variant="subtle" />
                </UFormField>

                <USwitch v-model="state.isAdmin" size="lg" label="Admin" description="Do you want to create an Admin User?" />
            </UForm>
        </template>

        <template #footer="{ close }">
            <UButton label="Cancel" color="neutral" variant="subtle" @click="close" />
            <UButton label="Create" :loading="isLoading" color="primary" @click="form?.submit()" />
        </template>
    </UModal>
</template>
