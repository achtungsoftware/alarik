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
import type { TableColumn } from "@nuxt/ui";

definePageMeta({
    layout: "dashboard",
});

useHead({
    title: `Shared Links`,
});

const page = ref(1);
const itemsPerPage = ref(10);
const UCheckbox = resolveComponent("UCheckbox");
const rowSelection = ref<Record<string, boolean>>({});
const jwtCookie = useJWTCookie();
const isDeleting = ref(false);
const toast = useToast();
const { confirm } = useConfirmDialog();

const {
    data: fetchResponse,
    status,
    refresh,
} = await useFetch<Page<SharedLink>>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/share`, {
    params: {
        page: page,
        per: itemsPerPage,
    },
    headers: {
        Authorization: `Bearer ${jwtCookie.value}`,
    },
    default: () => ({ items: [], metadata: { page: 1, per: 12, total: 0 } }),
});

async function copyLink(url: string) {
    await navigator.clipboard.writeText(url);
    toast.add({
        title: "Copied",
        description: "The link was copied to your clipboard.",
        icon: "i-lucide-circle-check",
        color: "success",
    });
}

async function deleteLink(item: SharedLink) {
    try {
        await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/share/${item.id}`, {
            method: "DELETE",
            headers: {
                Authorization: `Bearer ${jwtCookie.value}`,
            },
        });
        await refresh();
        toast.add({
            title: "Link Revoked",
            description: "The shared link no longer works.",
            icon: "i-lucide-circle-check",
            color: "success",
        });
    } catch (error: any) {
        toast.add({
            title: "Failed to Revoke Link",
            description: error.response?._data?.reason ?? "Unknown error",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    }
}

const columns: TableColumn<SharedLink>[] = [
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
        accessorKey: "bucketName",
        header: "Bucket",
    },
    {
        accessorKey: "key",
        header: "File",
    },
    {
        accessorKey: "expiresAt",
        header: "Expires",
        cell: ({ row }) =>
            row.original.expiresAt
                ? new Date(row.original.expiresAt).toLocaleString()
                : h(resolveComponent("UBadge"), { label: "Never", size: "md", color: "neutral", variant: "subtle" }),
    },
    {
        accessorKey: "createdAt",
        header: "Created at",
        cell: ({ row }) => new Date(row.original.createdAt).toLocaleString(),
    },
    {
        id: "actions",
        cell: ({ row }) => {
            const item = row.original;
            return h("div", { class: "flex flex-row items-center justify-end gap-2" }, [
                h(resolveComponent("UButton"), {
                    label: "Copy Link",
                    variant: "subtle",
                    color: "neutral",
                    size: "sm",
                    icon: "i-lucide-copy",
                    onClick: (e: Event) => {
                        e.stopPropagation();
                        copyLink(item.url);
                    },
                }),
                h(resolveComponent("UButton"), {
                    label: "Revoke",
                    variant: "subtle",
                    color: "error",
                    size: "sm",
                    icon: "i-lucide-trash-2",
                    onClick: (e: Event) => {
                        e.stopPropagation();
                        deleteLink(item);
                    },
                }),
            ]);
        },
    },
];

const selectedItems = computed(() => {
    return Object.entries(rowSelection.value)
        .filter(([_, selected]) => selected)
        .map(([index]) => fetchResponse.value?.items?.[Number(index)])
        .filter((item): item is SharedLink => item !== undefined);
});

async function deleteMany() {
    const items = selectedItems.value;
    if (items.length === 0) return;

    const confirmed = await confirm({
        title: `Revoke ${items.length} Shared Link${items.length !== 1 ? "s" : ""}`,
        message: `Do you really want to revoke ${items.length} shared link${items.length !== 1 ? "s" : ""}? Anyone with the link will immediately lose access.`,
        confirmLabel: "Revoke",
    });

    if (!confirmed) return;

    isDeleting.value = true;
    let successCount = 0;
    let errorCount = 0;

    try {
        for (const item of items) {
            try {
                await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/share/${item.id}`, {
                    method: "DELETE",
                    headers: {
                        Authorization: `Bearer ${jwtCookie.value}`,
                    },
                });
                successCount++;
            } catch (error) {
                console.error(`Failed to revoke shared link ${item.id}:`, error);
                errorCount++;
            }
        }

        await refresh();
        rowSelection.value = {};

        if (errorCount === 0) {
            toast.add({
                title: "Revocation Successful",
                description: `${successCount} shared link${successCount !== 1 ? "s" : ""} revoked successfully`,
                icon: "i-lucide-circle-check",
                color: "success",
            });
        } else if (successCount === 0) {
            toast.add({
                title: "Revocation Failed",
                description: `All ${errorCount} shared link${errorCount !== 1 ? "s" : ""} failed to revoke`,
                icon: "i-lucide-circle-x",
                color: "error",
            });
        } else {
            toast.add({
                title: "Revocation Partially Successful",
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
    <UDashboardPanel
        :ui="{
            body: '!p-0',
        }"
    >
        <template #header>
            <UDashboardNavbar title="Shared Links">
                <template #right>
                    <UButton @click="deleteMany" v-if="!Object.values(rowSelection).every((selected) => !selected)" color="error" :loading="isDeleting">
                        <template #trailing>
                            <UBadge color="neutral" variant="subtle" size="sm">{{ Object.values(rowSelection).length }}</UBadge>
                        </template>
                        Revoke
                    </UButton>
                </template>
            </UDashboardNavbar>
        </template>

        <template #body>
            <div class="flex flex-col">
                <UTable
                    v-model:row-selection="rowSelection"
                    :data="fetchResponse?.items"
                    :columns="columns"
                    :loading="status === 'pending'"
                    loadingAnimation="elastic"
                    :ui="{
                        th: 'cursor-default',
                    }"
                >
                    <template #empty>
                        <UEmpty title="No Shared Links" description="There are no shared links yet. Share a file from the Object Browser to create one." icon="i-lucide-share-2" size="lg" variant="naked" />
                    </template>
                </UTable>
                <div v-if="fetchResponse?.metadata.total > itemsPerPage" class="flex justify-end p-4 border-t border-default">
                    <UPagination v-model:page="page" show-edges :items-per-page="itemsPerPage" :total="fetchResponse.metadata.total" variant="ghost" active-variant="solid" active-color="primary" color="neutral" size="sm" />
                </div>
            </div>
        </template>
    </UDashboardPanel>
</template>
