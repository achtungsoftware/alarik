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
import type { DropdownMenuItem, TableColumn, TableRow } from "@nuxt/ui";

definePageMeta({
    layout: "dashboard",
});

useHead({
    title: `Users`,
});

const page = ref(1);
const itemsPerPage = ref(10);
const UCheckbox = resolveComponent("UCheckbox");
const UBadge = resolveComponent("UBadge");
const rowSelection = ref<Record<string, boolean>>({});
const openUserCreateModal = ref(false);
const isDeleting = ref(false);
const toast = useToast();
const UDropdownMenu = resolveComponent("UDropdownMenu");
const UButton = resolveComponent("UButton");
const { confirm } = useConfirmDialog();

const selectedUserForEdit = ref<User | null>(null);
const openUserEditModal = ref(false);

const {
    data: fetchResponse,
    status,
    refresh,
} = await useFetch<Page<User>>(`/admin/users`, {
    $fetch: useApi(),
    params: {
        page: page,
        per: itemsPerPage,
    },
    default: () => ({ items: [], metadata: { page: 1, per: 12, total: 0 } }),
});

const columns: TableColumn<User>[] = [
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
        accessorKey: "name",
        header: "Name",
        cell: ({ row }) => {
            return row.original.name;
        },
    },
    {
        accessorKey: "username",
        header: "Username",
        cell: ({ row }) => {
            return row.original.username;
        },
    },
    {
        accessorKey: "isAdmin",
        header: "Admin",
        cell: ({ row }) => {
            return row.original.isAdmin ? "yes" : "no";
        },
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
                                    label: "Edit User",
                                    icon: "i-lucide-square-pen",
                                    onSelect() {
                                        selectedUserForEdit.value = row.original;
                                        openUserEditModal.value = true;
                                    },
                                },
                            ],
                            [
                                {
                                    label: "Delete User",
                                    icon: "i-lucide-trash-2",
                                    color: "error" as const,
                                    onSelect() {
                                        deleteSingleUser(row.original);
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

// Keyed by `id` (see `getRowId`), not row index, so a refresh cannot move the selection.
const selectedItems = computed(() => {
    return (fetchResponse.value?.items ?? []).filter((item: User) => rowSelection.value[item.id]);
});

function onSelect(e: Event, row: TableRow<User>) {}

async function deleteMany() {
    const items = selectedItems.value;
    if (items.length === 0) return;

    const confirmed = await confirm({
        title: `Delete ${items.length} User${items.length !== 1 ? "s" : ""}`,
        message: `Do you really want to delete ${items.length} user${items.length !== 1 ? "s" : ""}? All data from the user will be deleted. This action cannot be undone.`,
        confirmLabel: "Delete",
    });

    if (!confirmed) return;

    isDeleting.value = true;
    let successCount = 0;
    let errorCount = 0;

    try {
        for (const item of items) {
            try {
                await useApi()(`/admin/users/${item.id}`, {
                    method: "DELETE",
                });
                successCount++;
            } catch (error) {
                logFailure(`Failed to delete user ${item.name}:`, error);
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
                description: `${successCount} user${successCount !== 1 ? "s" : ""} deleted successfully`,
                icon: "i-lucide-circle-check",
                color: "success",
            });
        } else if (successCount === 0) {
            toast.add({
                title: "Deletion Failed",
                description: `All ${errorCount} user${errorCount !== 1 ? "s" : ""} failed to delete`,
                icon: "i-lucide-circle-x",
                color: "error",
            });
        } else {
            toast.add({
                title: "Deletion Partially Successful",
                description: `${successCount} deleted, ${errorCount} failed`,
                icon: "i-lucide-alert-triangle",
                color: "warning",
            });
        }
    } finally {
        isDeleting.value = false;
    }
}

async function deleteSingleUser(user: User) {
    const confirmed = await confirm({
        title: "Delete User",
        message: `Do you really want to delete '${user.name}'? All buckets and data from this user will be deleted. This action cannot be undone.`,
        confirmLabel: "Delete",
    });

    if (!confirmed) return;

    isDeleting.value = true;

    try {
        await useApi()(`/admin/users/${user.id}`, {
            method: "DELETE",
        });

        await refresh();

        toast.add({
            title: "Deletion Successful",
            description: `User "${user.name}" deleted successfully`,
            icon: "i-lucide-circle-check",
            color: "success",
        });
    } catch (error) {
        logFailure(`Failed to delete user ${user.name}:`, error);
        toast.add({
            title: "Deletion Failed",
            description: errorMessage(error, "Failed to delete user"),
            icon: "i-lucide-circle-x",
            color: "error",
        });
    } finally {
        isDeleting.value = false;
    }
}
</script>
<template>
    <EditUserModal v-if="selectedUserForEdit && openUserEditModal" v-model:open="openUserEditModal" :user="selectedUserForEdit" />

    <UDashboardPanel
        :ui="{
            body: '!p-0',
        }"
    >
        <template #header>
            <UDashboardNavbar title="Users">
                <template #right>
                    <UButton @click="deleteMany" v-if="!Object.values(rowSelection).every((selected) => !selected)" color="error" :loading="isDeleting">
                        <template #trailing>
                            <UBadge color="neutral" variant="subtle" size="sm">{{ Object.values(rowSelection).length }}</UBadge>
                        </template>
                        Delete
                    </UButton>
                    <CreateUserModal v-model:open="openUserCreateModal">
                        <UButton icon="i-lucide-plus" color="primary">Create</UButton>
                    </CreateUserModal>
                </template>
            </UDashboardNavbar>
        </template>

        <template #body>
            <div class="flex flex-col">
                <UTable
                    v-model:row-selection="rowSelection"
                    :get-row-id="(row: User) => row.id"
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
                        <UEmpty title="No Users" description="There are no users yet." icon="i-lucide-users" size="lg" variant="naked" />
                    </template>
                </UTable>
                <div v-if="fetchResponse?.metadata.total > itemsPerPage" class="flex justify-end p-4 border-t border-default">
                    <UPagination v-model:page="page" show-edges :items-per-page="itemsPerPage" :total="fetchResponse.metadata.total" variant="ghost" active-variant="solid" active-color="primary" color="neutral" size="sm" />
                </div>
            </div>
        </template>
    </UDashboardPanel>
</template>
