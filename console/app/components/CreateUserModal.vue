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

async function submitForm(event: FormSubmitEvent<typeof state>) {
    event.preventDefault();
    try {
        isLoading.value = true;
        error.value = "";

        const response = await useApi()<{ token: string }>(`/admin/users`, {
            method: "POST",
            body: JSON.stringify(state),
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
    <FormModal v-model:open="open" title="Create User" submit-label="Create" :loading="isLoading" :error="error" :state="state" @submit="submitForm">
        <template #trigger>
            <slot />
        </template>

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
    </FormModal>
</template>
