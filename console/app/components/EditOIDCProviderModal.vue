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
        provider: OIDCProvider;
    }>(),
    {
        open: false,
    }
);

const isLoading = ref(false);
const error = ref("");
const emit = defineEmits(["update:open", "close"]);
const open = ref(props.open);
const callbackURL = `/auth/oidc/callback`;

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
    id: props.provider.id,
    name: props.provider.name,
    issuerURL: props.provider.issuerURL,
    clientId: props.provider.clientId,
    clientSecret: "",
    enabled: props.provider.enabled,
});

async function submitForm(event: FormSubmitEvent<typeof state>) {
    event.preventDefault();
    try {
        isLoading.value = true;
        error.value = "";

        // A blank clientSecret is sent as-is - the server already treats an empty value as
        // "keep the existing secret".
        await useApi()(`/admin/oidcProviders`, {
            method: "PUT",
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
    <FormModal v-model:open="open" title="Edit OIDC Provider" submit-label="Save" :loading="isLoading" :error="error" :state="state" @submit="submitForm">
        <template #trigger>
            <slot />
        </template>

        <UFormField required label="Name">
            <UInput placeholder="Name" v-model="state.name" class="w-full" size="lg" variant="subtle" />
        </UFormField>

        <UFormField required label="Issuer URL">
            <UInput autocomplete="off" :autocorrect="false" placeholder="Issuer URL" v-model="state.issuerURL" class="w-full" size="lg" variant="subtle" />
        </UFormField>

        <UFormField required label="Client ID">
            <UInput autocomplete="off" :autocorrect="false" placeholder="Client ID" v-model="state.clientId" class="w-full" size="lg" variant="subtle" />
        </UFormField>

        <UFormField label="Client Secret" description="Leave blank to keep the existing secret.">
            <UInput autocomplete="off" :autocorrect="false" placeholder="New Client Secret (optional)" type="password" v-model="state.clientSecret" class="w-full" size="lg" variant="subtle" />
        </UFormField>

        <UFormField label="Redirect URL" description="Register this exact URL with your identity provider.">
            <UInput :model-value="callbackURL" readonly class="w-full" size="lg" variant="subtle" />
        </UFormField>

        <USwitch v-model="state.enabled" size="lg" label="Enabled" description="Show this provider as a sign-in option." />
    </FormModal>
</template>
