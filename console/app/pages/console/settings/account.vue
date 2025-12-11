<script setup lang="ts">
definePageMeta({
    layout: "dashboard",
});

useHead({
    title: `Account Settings`,
});

import type { FormSubmitEvent } from "@nuxt/ui";

const user = useUser();
const error = ref("");
const isLoading = ref(false);
const form = useTemplateRef("form");
const jwtCookie = useJWTCookie();

const state = reactive({
    name: user.value.name,
    username: user.value.username,
});

async function submitForm(event: FormSubmitEvent<any>) {
    event.preventDefault();
    try {
        isLoading.value = true;
        error.value = "";

        const response = await $fetch<{ token: string }>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/users`, {
            method: "PUT",
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
    <UDashboardPanel>
        <template #header>
            <UDashboardNavbar title="Settings" />
        </template>

        <template #body>
            <div class="mx-auto max-w-3xl lg:pt-12 w-full">
                <UCard variant="subtle">
                    <template #header>
                        <CardHeader
                            title="Account"
                            :breadCrumbItems="[
                                {
                                    label: 'Settings',
                                },
                                {
                                    label: 'Account',
                                    to: '/console/settings/account',
                                },
                            ]"
                        >
                            <template #rightContent>
                                <UButton :loading="isLoading" @click="form?.submit()" icon="i-lucide-save" color="primary">Save</UButton>
                            </template>
                        </CardHeader>
                    </template>

                    <UForm ref="form" @submit="submitForm" class="space-y-4">
                        <UAlert v-if="error != ''" title="Error" :description="error" color="error" class="" variant="subtle" />

                        <UFormField name="name" label="Name" description="Your Name" required class="flex flex-col sm:flex-row justify-between items-start sm:gap-4">
                            <UInput v-model="state.name" placeholder="Name" size="lg" variant="subtle" />
                        </UFormField>

                        <UFormField name="username" label="Username" description="Your Username" required class="flex flex-col sm:flex-row justify-between items-start sm:gap-4">
                            <UInput v-model="state.username" placeholder="Username" size="lg" variant="subtle" />
                        </UFormField>
                    </UForm>
                </UCard>
            </div>
        </template>
    </UDashboardPanel>
</template>
