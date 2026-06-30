<script setup lang="ts">
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

import { h, resolveComponent } from "vue";
import type { DropdownMenuItem, TableColumn } from "@nuxt/ui";

definePageMeta({
    layout: "dashboard",
});

useHead({
    title: `OIDC Providers`,
});

const page = ref(1);
const itemsPerPage = ref(10);
const UBadge = resolveComponent("UBadge");
const jwtCookie = useJWTCookie();
const openCreateModal = ref(false);
const toast = useToast();
const UDropdownMenu = resolveComponent("UDropdownMenu");
const UButton = resolveComponent("UButton");
const { confirm } = useConfirmDialog();

const selectedProviderForEdit = ref<OIDCProvider | null>(null);
const openEditModal = ref(false);

const {
    data: fetchResponse,
    status,
    refresh,
} = await useFetch<Page<OIDCProvider>>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/admin/oidcProviders`, {
    params: {
        page: page,
        per: itemsPerPage,
    },
    headers: {
        Authorization: `Bearer ${jwtCookie.value}`,
    },
    default: () => ({ items: [], metadata: { page: 1, per: 12, total: 0 } }),
});

const columns: TableColumn<OIDCProvider>[] = [
    {
        accessorKey: "name",
        header: "Name",
        cell: ({ row }) => row.original.name,
    },
    {
        accessorKey: "issuerURL",
        header: "Issuer URL",
        cell: ({ row }) => row.original.issuerURL,
    },
    {
        accessorKey: "enabled",
        header: "Status",
        cell: ({ row }) =>
            h(
                UBadge,
                { color: row.original.enabled ? "success" : "neutral", variant: "subtle" },
                () => (row.original.enabled ? "Enabled" : "Disabled")
            ),
    },
    {
        id: "actions",
        cell: ({ row }) => {
            return h(
                "div",
                { class: "text-right" },
                h(
                    UDropdownMenu,
                    {
                        content: {
                            align: "end",
                        },
                        items: [
                            [
                                {
                                    label: "Edit Provider",
                                    icon: "i-lucide-square-pen",
                                    onSelect() {
                                        selectedProviderForEdit.value = row.original;
                                        openEditModal.value = true;
                                    },
                                },
                            ],
                            [
                                {
                                    label: "Delete Provider",
                                    icon: "i-lucide-trash-2",
                                    color: "error" as const,
                                    onSelect() {
                                        deleteProvider(row.original);
                                    },
                                },
                            ],
                        ] as DropdownMenuItem[][],
                        "aria-label": "Action Menu",
                    },
                    () =>
                        h(UButton, {
                            icon: "i-lucide-ellipsis-vertical",
                            color: "neutral",
                            variant: "ghost",
                            class: "ml-auto",
                            "aria-label": "Action Menu",
                        })
                )
            );
        },
    },
];

async function deleteProvider(provider: OIDCProvider) {
    const confirmed = await confirm({
        title: "Delete OIDC Provider",
        message: `Do you really want to delete '${provider.name}'? Any users linked to it will fall back to local login. This action cannot be undone.`,
        confirmLabel: "Delete",
    });

    if (!confirmed) return;

    try {
        await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/admin/oidcProviders/${provider.id}`, {
            method: "DELETE",
            headers: {
                Authorization: `Bearer ${jwtCookie.value}`,
            },
        });

        await refresh();

        toast.add({
            title: "Deletion Successful",
            description: `Provider "${provider.name}" deleted successfully`,
            icon: "i-lucide-circle-check",
            color: "success",
        });
    } catch (error: any) {
        console.error(`Failed to delete provider ${provider.name}:`, error);
        toast.add({
            title: "Deletion Failed",
            description: error.response?._data?.reason ?? "Failed to delete provider",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    }
}
</script>
<template>
    <EditOIDCProviderModal v-if="selectedProviderForEdit && openEditModal" v-model:open="openEditModal" :provider="selectedProviderForEdit" />

    <UDashboardPanel
        :ui="{
            body: '!p-0',
        }"
    >
        <template #header>
            <UDashboardNavbar title="OIDC Providers">
                <template #right>
                    <CreateOIDCProviderModal v-model:open="openCreateModal">
                        <UButton icon="i-lucide-plus" color="primary">Create</UButton>
                    </CreateOIDCProviderModal>
                </template>
            </UDashboardNavbar>
        </template>

        <template #body>
            <div class="flex flex-col">
                <UTable
                    :data="fetchResponse?.items"
                    :columns="columns"
                    :loading="status === 'pending'"
                    loadingAnimation="elastic"
                    :ui="{
                        tr: 'cursor-pointer',
                        th: 'cursor-default',
                    }"
                >
                    <template #empty>
                        <UEmpty title="No OIDC Providers" description="No SSO providers configured yet." icon="i-lucide-key-round" size="lg" variant="naked" />
                    </template>
                </UTable>
                <div v-if="fetchResponse?.metadata.total > itemsPerPage" class="flex justify-end p-4 border-t border-default">
                    <UPagination v-model:page="page" show-edges :items-per-page="itemsPerPage" :total="fetchResponse.metadata.total" variant="ghost" active-variant="solid" active-color="primary" color="neutral" size="sm" />
                </div>
            </div>
        </template>
    </UDashboardPanel>
</template>
