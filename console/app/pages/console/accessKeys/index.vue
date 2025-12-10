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
import type { TableColumn, TableRow } from "@nuxt/ui";

definePageMeta({
    layout: "dashboard",
});

useHead({
    title: `Access Keys`,
});

const page = ref(1);
const itemsPerPage = ref(10);
const UCheckbox = resolveComponent("UCheckbox");
const UBadge = resolveComponent("UBadge");
const rowSelection = ref<Record<string, boolean>>({});
const openDeletionModal = ref(false);
const jwtCookie = useJWTCookie();
const openAccessKeyCreateModal = ref(false);
const isDeleting = ref(false);
const toast = useToast();

const {
    data: fetchResponse,
    status,
    refresh,
} = await useFetch<Page<AccessKey>>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/users/accessKeys`, {
    params: {
        page: page,
        per: itemsPerPage,
    },
    headers: {
        Authorization: `Bearer ${jwtCookie.value}`,
    },
    default: () => ({ items: [], metadata: { page: 1, per: 12, total: 0 } }),
});

const columns: TableColumn<AccessKey>[] = [
    {
        id: "select",
        header: ({ table }) =>
            h(UCheckbox, {
                modelValue: table.getIsSomePageRowsSelected() ? "indeterminate" : table.getIsAllPageRowsSelected(),
                "onUpdate:modelValue": (value: boolean | "indeterminate") => table.toggleAllPageRowsSelected(!!value),
                ariaLabel: "Select all",
            }),
        cell: ({ row }) =>
            h(UCheckbox, {
                modelValue: row.getIsSelected(),
                "onUpdate:modelValue": (value: boolean | "indeterminate") => row.toggleSelected(!!value),
                ariaLabel: "Select row",
            }),
    },
    {
        accessorKey: "accessKey",
        header: "Access Key",
        cell: ({ row }) => {
            return row.original.accessKey;
        },
    },
    {
        accessorKey: "expirationDate",
        header: "Expiration Date",
        cell: ({ row }) => {
            return row.original.expirationDate ? new Date(row.original.expirationDate).toLocaleString() : "never";
        },
    },
    {
        accessorKey: "createdAt",
        header: "Created at",
        cell: ({ row }) => {
            return new Date(row.original.createdAt).toLocaleString();
        },
    },
];

const selectedItems = computed(() => {
    return Object.entries(rowSelection.value)
        .filter(([_, selected]) => selected)
        .map(([index]) => fetchResponse.value?.items?.[Number(index)])
        .filter((item): item is AccessKey => item !== undefined);
});

function onSelect(e: Event, row: TableRow<AccessKey>) {
    //router.push(`/console/members/${row.original.name}`);
}

async function deleteMany() {
    const items = selectedItems.value;
    if (items.length === 0) {
        return;
    }

    isDeleting.value = true;
    let successCount = 0;
    let errorCount = 0;

    try {
        for (const item of items) {
            try {
                await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/users/accessKeys/${item.id}`, {
                    method: "DELETE",
                    headers: {
                        Authorization: `Bearer ${jwtCookie.value}`,
                    },
                });
                successCount++;
            } catch (error) {
                console.error(`Failed to delete access key ${item.accessKey}:`, error);
                errorCount++;
            }
        }

        // Refresh the list
        await refresh();

        // Clear selection
        rowSelection.value = {};

        // Show appropriate toast based on results
        if (errorCount === 0) {
            toast.add({
                title: "Deletion Successful",
                description: `${successCount} access key${successCount !== 1 ? "s" : ""} deleted successfully`,
                icon: "i-lucide-circle-check",
                color: "success",
            });
        } else if (successCount === 0) {
            toast.add({
                title: "Deletion Failed",
                description: `All ${errorCount} access key${errorCount !== 1 ? "s" : ""} failed to delete`,
                icon: "i-lucide-circle-x",
                color: "error",
            });
        } else {
            toast.add({
                title: "Deletion Partially Successful",
                description: `${successCount} succeeded, ${errorCount} failed`,
                icon: "i-lucide-alert-triangle",
                color: "warning",
            });
        }
    } finally {
        isDeleting.value = false;
    }
}
</script>
<template>
    <ConfirmationDialog confirmLabel="Delete" v-model:isShowing="openDeletionModal" :title="`Delete ${selectedItems.length} Access Key${selectedItems.length !== 1 ? 's' : ''}`" :onConfirm="deleteMany" :message="`Do you really want to delete ${selectedItems.length} access key${selectedItems.length !== 1 ? 's' : ''}? This action cannot be undone.`" />

    <UDashboardPanel
        :ui="{
            body: '!p-0',
        }"
    >
        <template #header>
            <UDashboardNavbar title="Access Keys">
                <template #right>
                    <UButton @click="openDeletionModal = !openDeletionModal" v-if="!Object.values(rowSelection).every((selected) => !selected)" color="error">
                        <template #trailing>
                            <UBadge color="neutral" variant="subtle" size="sm">{{ Object.values(rowSelection).length }}</UBadge>
                        </template>
                        Delete
                    </UButton>
                    <CreateAccessKeyModal v-model:open="openAccessKeyCreateModal">
                        <UButton icon="i-lucide-plus" color="primary">Create</UButton>
                    </CreateAccessKeyModal>
                </template>
            </UDashboardNavbar>
        </template>

        <template #body>
            <div class="flex flex-col">
                <UTable
                    v-model:row-selection="rowSelection"
                    @select="onSelect"
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
                        <UEmpty title="No Access Keys" description="There are no access keys yet." icon="i-lucide-key-round" size="lg" variant="naked" />
                    </template>
                </UTable>
                <div v-if="fetchResponse?.metadata.total > itemsPerPage" class="flex justify-end p-4 border-t border-default">
                    <UPagination v-model:page="page" show-edges :items-per-page="itemsPerPage" :total="fetchResponse.metadata.total" variant="ghost" active-variant="solid" active-color="primary" color="neutral" size="sm" />
                </div>
            </div>
        </template>
    </UDashboardPanel>
</template>
