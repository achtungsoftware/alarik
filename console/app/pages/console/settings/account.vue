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
const router = useRouter();
const { confirm } = useConfirmDialog();

const state = reactive({
    name: user.value.name,
    username: user.value.username,
    currentPassword: "",
    newPassword: "",
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

async function deleteAccount() {
    const confirmed = await confirm({
        title: "Delete Account",
        message: "Are you sure you want to delete your account? This will permanently delete all your buckets, access keys, and data. This action cannot be undone.",
        confirmLabel: "Delete Account",
        confirmColor: "error",
    });

    if (!confirmed) return;

    const confirmedAgain = await confirm({
        title: "Are you absolutely sure?",
        message: "This is your last chance to cancel. Your account and all data will be permanently deleted.",
        confirmLabel: "Yes, delete my account",
        confirmColor: "error",
    });

    if (!confirmedAgain) return;

    try {
        await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/users`, {
            method: "DELETE",
            headers: {
                Authorization: `Bearer ${jwtCookie.value}`,
            },
        });

        jwtCookie.value = null;
        window.location.reload();
    } catch (err: any) {
        error.value = err.response?._data?.reason ?? "Failed to delete account";
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

                        <UFormField name="currentPassword" label="Current Password" description="Required to change your password" class="flex flex-col sm:flex-row justify-between items-start sm:gap-4">
                            <UInput v-model="state.currentPassword" type="password" placeholder="Current Password" size="lg" variant="subtle" />
                        </UFormField>

                        <UFormField name="newPassword" label="New Password" description="Leave empty to keep current password" class="flex flex-col sm:flex-row justify-between items-start sm:gap-4">
                            <UInput v-model="state.newPassword" type="password" placeholder="New Password" size="lg" variant="subtle" />
                        </UFormField>
                    </UForm>
                </UCard>

                <UCard variant="subtle" class="mt-6">
                    <template #header>
                        <CardHeader title="Danger Zone" color="error" />
                    </template>

                    <UFormField label="Delete Account" description="Permanently delete your account and all associated data including buckets and access keys." class="flex flex-col sm:flex-row justify-between items-start sm:gap-4">
                        <UButton @click="deleteAccount" color="error" variant="soft" icon="i-lucide-trash-2">Delete</UButton>
                    </UFormField>
                </UCard>
            </div>
        </template>
    </UDashboardPanel>
</template>
