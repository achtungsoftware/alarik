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

import type { BreadcrumbItem, TableColumn, TableRow } from "@nuxt/ui";

definePageMeta({
    layout: "dashboard",
});

useHead({
    title: `Object Browser`,
});

const openBucketCreateModal = ref(false);
const page = ref(1);
const itemsPerPage = ref(100);
const rowSelection = ref<Record<string, boolean>>({});
const openDeletionModal = ref(false);
const jwtCookie = useJWTCookie();
const fileInput = ref<HTMLInputElement | null>(null);
const folderInput = ref<HTMLInputElement | null>(null);
const isUploading = ref(false);
const isDeleting = ref(false);
const isDownloading = ref(false);
const toast = useToast();
const openDetailModal = ref(false);
const selectedObject = ref<BrowserItem | null>(null);

// Navigation state
const currentBucket = ref<string>("");
const currentPrefix = ref<string>("");

// Reset page when navigation changes
watch([currentBucket, currentPrefix], () => {
    page.value = 1;
    rowSelection.value = {};
});

// Get selected items for deletion
const selectedItems = computed(() => {
    return displayItems.value.filter((_, index) => rowSelection.value[index]);
});

// Count files and folders for deletion dialog
const deletionCounts = computed(() => {
    const items = selectedItems.value.filter((item) => !item.isBucket);
    const fileCount = items.filter((item) => !item.isFolder).length;
    const folderCount = items.filter((item) => item.isFolder).length;
    return { fileCount, folderCount, total: items.length };
});

// Breadcrumb data structure for navigation
interface BreadcrumbNav {
    label: string;
    icon: string;
    onClick: () => void;
}

// Computed breadcrumb navigation items
const breadcrumbNavItems = computed<BreadcrumbNav[]>(() => {
    const items: BreadcrumbNav[] = [
        {
            label: "Buckets",
            icon: "i-lucide-home",
            onClick: () => {
                currentBucket.value = "";
                currentPrefix.value = "";
            },
        },
    ];

    if (currentBucket.value) {
        items.push({
            label: currentBucket.value,
            icon: "i-lucide-database",
            onClick: () => {
                currentPrefix.value = "";
            },
        });

        if (currentPrefix.value) {
            const folderParts = currentPrefix.value.split("/").filter((p) => p.length > 0);
            folderParts.forEach((part, index) => {
                items.push({
                    label: part,
                    icon: "i-lucide-folder",
                    onClick: () => {
                        const folders = folderParts.slice(0, index + 1);
                        currentPrefix.value = folders.join("/") + "/";
                    },
                });
            });
        }
    }

    return items;
});

// Convert to BreadcrumbItem format (without click handlers, we'll use slots)
const breadcrumbItems = computed<BreadcrumbItem[]>(() => {
    return breadcrumbNavItems.value.map((item) => ({
        label: item.label,
        icon: item.icon,
    }));
});

// Fetch buckets (with pagination)
const {
    data: bucketsResponse,
    status: bucketsStatus,
    refresh: refreshBuckets,
} = await useFetch<Page<Bucket>>(`${useRuntimeConfig().public.API_BASE_URL}/api/v1/buckets`, {
    params: { page: page, per: itemsPerPage },
    headers: { Authorization: `Bearer ${jwtCookie.value}` },
    watch: [page],
    default: () => ({ items: [], metadata: { page: 1, per: 100, total: 0 } }),
});

// Fetch objects when inside a bucket
const {
    data: objectsResponse,
    status: objectsStatus,
    refresh,
} = await useFetch<Page<BrowserItem>>(`${useRuntimeConfig().public.API_BASE_URL}/api/v1/objects`, {
    params: {
        bucket: currentBucket,
        prefix: currentPrefix,
        page: page,
        per: itemsPerPage,
    },
    headers: {
        Authorization: `Bearer ${jwtCookie.value}`,
    },
    watch: [currentBucket, currentPrefix, page],
    immediate: false,
    default: () => ({ items: [], metadata: { page: 1, per: 100, total: 0 } }),
});

// Combined data: show buckets at root, or objects when inside a bucket
const displayItems = computed<BrowserItem[]>(() => {
    if (!currentBucket.value) {
        // Show buckets as folders
        return (
            bucketsResponse.value?.items?.map((bucket: Bucket) => ({
                key: bucket.name,
                size: 0,
                contentType: "application/x-directory",
                etag: "",
                lastModified: bucket.creationDate || new Date().toISOString(),
                isFolder: true,
                isBucket: true,
            })) || []
        );
    } else {
        // Show objects/folders inside bucket
        return objectsResponse.value?.items || [];
    }
});

const status = computed(() => {
    return !currentBucket.value ? bucketsStatus.value : objectsStatus.value;
});

// Pagination metadata
const paginationMetadata = computed(() => {
    return !currentBucket.value ? bucketsResponse.value?.metadata : objectsResponse.value?.metadata;
});

const showPagination = computed(() => {
    return (paginationMetadata.value?.total || 0) > itemsPerPage.value;
});

const columns: TableColumn<BrowserItem>[] = [
    {
        id: "select",
        header: ({ table }) =>
            h(resolveComponent("UCheckbox"), {
                modelValue: table.getIsSomePageRowsSelected() ? "indeterminate" : table.getIsAllPageRowsSelected(),
                "onUpdate:modelValue": (value: boolean | "indeterminate") => table.toggleAllPageRowsSelected(!!value),
                ariaLabel: "Select all",
            }),
        cell: ({ row }) =>
            h(resolveComponent("UCheckbox"), {
                modelValue: row.getIsSelected(),
                "onUpdate:modelValue": (value: boolean | "indeterminate") => row.toggleSelected(!!value),
                ariaLabel: "Select row",
            }),
    },
    {
        accessorKey: "key",
        header: "Name",
        cell: ({ row }) => {
            const item = row.original;
            let displayName = item.key;

            if (item.isBucket) {
                displayName = item.key;
            } else if (item.isFolder) {
                displayName =
                    item.key
                        .split("/")
                        .filter((p: any) => p)
                        .pop() + "/";
            } else {
                displayName = item.key.split("/").pop() || item.key;
            }

            const icon = getFileIcon(item.key, item.isFolder || false, item.isBucket || false);

            return h("div", { class: "flex items-center gap-2" }, [h(resolveComponent("UIcon"), { name: icon, class: "w-6 h-6" }), h("span", displayName)]);
        },
    },
    {
        accessorKey: "size",
        header: "Size",
        cell: ({ row }) => {
            if (row.original.isFolder || row.original.isBucket) return "-";
            return formatBytes(row.original.size);
        },
    },
    {
        accessorKey: "lastModified",
        header: "Last Modified",
        cell: ({ row }) => {
            if (row.original.isBucket) {
                return new Date(row.original.lastModified).toLocaleString();
            }
            if (row.original.isFolder) return "-";
            return new Date(row.original.lastModified).toLocaleString();
        },
    },
];

function onSelect(e: Event, row: TableRow<BrowserItem>) {
    const item = row.original;

    if (item.isBucket) {
        // Navigate into bucket
        currentBucket.value = item.key;
        currentPrefix.value = "";
        return;
    } else if (item.isFolder) {
        // Navigate into folder
        currentPrefix.value = item.key;
        return;
    }

    // Item is File
    openDetailModal.value = true;
    selectedObject.value = row.original;
}

async function deleteMany() {
    const items = selectedItems.value;
    if (items.length === 0) {
        return;
    }

    isDeleting.value = true;
    let successCount = 0;
    let errorCount = 0;
    let skippedBuckets = 0;

    try {
        for (const item of items) {
            // Skip buckets - they should be deleted through bucket management
            if (item.isBucket) {
                skippedBuckets++;
                continue;
            }

            try {
                await $fetch(`${useRuntimeConfig().public.API_BASE_URL}/api/v1/objects`, {
                    method: "DELETE",
                    headers: {
                        Authorization: `Bearer ${jwtCookie.value}`,
                    },
                    params: {
                        bucket: currentBucket.value,
                        key: item.key,
                    },
                });
                successCount++;
            } catch (error) {
                console.error(`Failed to delete ${item.key}:`, error);
                errorCount++;
            }
        }

        // Refresh the object list
        await refresh();

        // Clear selection
        rowSelection.value = {};

        // Show appropriate toast based on results
        if (skippedBuckets > 0) {
            toast.add({
                title: "Buckets Cannot Be Deleted Here",
                description: `${skippedBuckets} bucket${skippedBuckets !== 1 ? "s" : ""} skipped. Use bucket management to delete buckets.`,
                icon: "i-lucide-alert-triangle",
                color: "warning",
            });
        } else if (errorCount === 0) {
            toast.add({
                title: "Deletion Successful",
                description: `${successCount} item${successCount !== 1 ? "s" : ""} deleted successfully`,
                icon: "i-lucide-circle-check",
                color: "success",
            });
        } else if (successCount === 0) {
            toast.add({
                title: "Deletion Failed",
                description: `All ${errorCount} item${errorCount !== 1 ? "s" : ""} failed to delete`,
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

async function downloadSelected() {
    const items = selectedItems.value.filter((item) => !item.isBucket);
    if (items.length === 0) {
        return;
    }

    isDownloading.value = true;

    try {
        const keys = items.map((item) => item.key);

        const response = await fetch(`${useRuntimeConfig().public.API_BASE_URL}/api/v1/objects/download`, {
            method: "POST",
            headers: {
                Authorization: `Bearer ${jwtCookie.value}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                bucket: currentBucket.value,
                keys: keys,
            }),
        });

        if (!response.ok) {
            throw new Error(`Download failed: ${response.statusText}`);
        }

        // Get the blob from the response
        const blob = await response.blob();

        // Get filename from Content-Disposition header or use default
        const contentDisposition = response.headers.get("Content-Disposition");
        let filename = "download";
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?(.+?)"?$/);
            if (filenameMatch && filenameMatch[1]) {
                filename = filenameMatch[1];
            }
        }

        // Create a download link and trigger it
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);

        // Clear selection
        rowSelection.value = {};

        toast.add({
            title: "Download Started",
            description: `Downloading ${items.length} item${items.length !== 1 ? "s" : ""}`,
            icon: "i-lucide-download",
            color: "success",
        });
    } catch (err: any) {
        toast.add({
            title: "Download Failed",
            description: err.data?.reason ?? "Unknown error",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    } finally {
        isDownloading.value = false;
    }
}

function triggerFileUpload() {
    fileInput.value?.click();
}

function triggerFolderUpload() {
    folderInput.value?.click();
}

async function handleFileUpload(event: Event) {
    const input = event.target as HTMLInputElement;
    const files = input.files;

    if (!files || files.length === 0) return;

    isUploading.value = true;
    let successCount = 0;
    let errorCount = 0;

    try {
        for (const file of Array.from(files)) {
            try {
                const formData = new FormData();
                formData.append("data", file, file.name);

                await $fetch(`${useRuntimeConfig().public.API_BASE_URL}/api/v1/objects`, {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${jwtCookie.value}`,
                    },
                    params: {
                        bucket: currentBucket.value,
                        ...(currentPrefix.value.length > 0 && { prefix: currentPrefix.value }),
                    },
                    body: formData,
                });
                successCount++;
            } catch (fileError) {
                console.error(`Failed to upload ${file.name}:`, fileError);
                errorCount++;
            }
        }

        // Refresh the object list
        await refresh();

        // Show appropriate toast based on results
        if (errorCount === 0) {
            toast.add({
                title: "Upload Successful",
                description: `${successCount} file${successCount !== 1 ? "s" : ""} uploaded successfully`,
                icon: "i-lucide-circle-check",
                color: "success",
            });
        } else if (successCount === 0) {
            toast.add({
                title: "Upload Failed",
                description: `All ${errorCount} file${errorCount !== 1 ? "s" : ""} failed to upload`,
                icon: "i-lucide-circle-x",
                color: "error",
            });
        } else {
            toast.add({
                title: "Upload Partially Successful",
                description: `${successCount} succeeded, ${errorCount} failed`,
                icon: "i-lucide-alert-triangle",
                color: "warning",
            });
        }

        // Clear the file input
        if (input) input.value = "";
    } finally {
        isUploading.value = false;
    }
}

async function handleFolderUpload(event: Event) {
    const input = event.target as HTMLInputElement;
    const files = input.files;

    if (!files || files.length === 0) return;

    isUploading.value = true;
    let successCount = 0;
    let errorCount = 0;

    try {
        for (const file of Array.from(files)) {
            try {
                // Get the relative path from the file's webkitRelativePath
                const relativePath = (file as any).webkitRelativePath || file.name;

                const formData = new FormData();
                formData.append("data", file, file.name);

                // Combine current prefix with the file's relative path
                const fullPrefix = currentPrefix.value + relativePath.substring(0, relativePath.lastIndexOf("/") + 1);

                await $fetch(`${useRuntimeConfig().public.API_BASE_URL}/api/v1/objects`, {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${jwtCookie.value}`,
                    },
                    params: {
                        bucket: currentBucket.value,
                        prefix: fullPrefix,
                    },
                    body: formData,
                });
                successCount++;
            } catch (fileError) {
                console.error(`Failed to upload ${file.name}:`, fileError);
                errorCount++;
            }
        }

        // Refresh the object list
        await refresh();

        // Show appropriate toast based on results
        if (errorCount === 0) {
            toast.add({
                title: "Upload Successful",
                description: `${successCount} file${successCount !== 1 ? "s" : ""} uploaded successfully`,
                icon: "i-lucide-circle-check",
                color: "success",
            });
        } else if (successCount === 0) {
            toast.add({
                title: "Upload Failed",
                description: `All ${errorCount} file${errorCount !== 1 ? "s" : ""} failed to upload`,
                icon: "i-lucide-circle-x",
                color: "error",
            });
        } else {
            toast.add({
                title: "Upload Partially Successful",
                description: `${successCount} succeeded, ${errorCount} failed`,
                icon: "i-lucide-alert-triangle",
                color: "warning",
            });
        }

        // Clear the folder input
        if (input) input.value = "";
    } finally {
        isUploading.value = false;
    }
}
</script>
<template>
    <ObjectDetailModal v-model:open="openDetailModal" :item="selectedObject" />

    <ConfirmationDialog confirmLabel="Delete" v-model:isShowing="openDeletionModal" :title="`Delete ${deletionCounts.total} Item${deletionCounts.total !== 1 ? 's' : ''}`" :onConfirm="deleteMany" :message="deletionCounts.fileCount > 0 && deletionCounts.folderCount > 0 ? `Do you really want to delete ${deletionCounts.fileCount} file${deletionCounts.fileCount !== 1 ? 's' : ''} and ${deletionCounts.folderCount} folder${deletionCounts.folderCount !== 1 ? 's' : ''}? This action cannot be undone.` : deletionCounts.folderCount > 0 ? `Do you really want to delete ${deletionCounts.folderCount} folder${deletionCounts.folderCount !== 1 ? 's' : ''} and all files within? This action cannot be undone.` : `Do you really want to delete ${deletionCounts.fileCount} file${deletionCounts.fileCount !== 1 ? 's' : ''}? This action cannot be undone.`" />

    <UDashboardPanel
        :ui="{
            body: '!p-0',
        }"
    >
        <template #header>
            <UDashboardNavbar title="Object Browser">
                <template #right>
                    <UButton @click="openDeletionModal = !openDeletionModal" v-if="!Object.values(rowSelection).every((selected) => !selected)" icon="i-lucide-trash" color="error">
                        <template #trailing>
                            <UBadge color="neutral" variant="subtle" size="sm">{{ Object.values(rowSelection).length }}</UBadge>
                        </template>
                        Delete
                    </UButton>

                    <UButton @click="downloadSelected" v-if="!Object.values(rowSelection).every((selected) => !selected)" icon="i-lucide-download" color="neutral" variant="subtle" :loading="isDownloading">
                        <template #trailing>
                            <UBadge color="neutral" variant="subtle" size="sm">{{ Object.values(rowSelection).length }}</UBadge>
                        </template>
                        Download
                    </UButton>

                    <UButton
                        @click="
                            () => {
                                refreshBuckets();
                                refresh();
                            }
                        "
                        label="Refresh"
                        icon="i-lucide-refresh-ccw"
                        color="neutral"
                        variant="subtle"
                    />

                    <UDropdownMenu
                        v-if="currentBucket != ''"
                        :items="[
                            {
                                label: 'File',
                                icon: 'i-lucide-file',
                                onSelect: triggerFileUpload,
                            },
                            {
                                label: 'Folder',
                                icon: 'i-lucide-folder',
                                onSelect: triggerFolderUpload,
                            },
                        ]"
                    >
                        <UButton label="Upload" icon="i-lucide-upload" color="neutral" variant="subtle" :loading="isUploading" />
                    </UDropdownMenu>

                    <!-- Hidden file input -->
                    <input ref="fileInput" type="file" multiple style="display: none" @change="handleFileUpload" />

                    <!-- Hidden folder input -->
                    <input ref="folderInput" type="file" webkitdirectory directory multiple style="display: none" @change="handleFolderUpload" />

                    <CreateBucketModal v-if="currentBucket == ''" v-model:open="openBucketCreateModal">
                        <UButton icon="i-lucide-plus" color="primary">Bucket</UButton>
                    </CreateBucketModal>
                </template>
            </UDashboardNavbar>

            <UDashboardToolbar v-if="breadcrumbItems.length > 1">
                <template #left>
                    <UBreadcrumb :items="breadcrumbItems">
                        <template #item="{ item, index }">
                            <button @click="breadcrumbNavItems[index]?.onClick()" class="flex items-center gap-1.5 hover:text-primary transition-colors">
                                <UIcon v-if="item.icon" :name="item.icon" class="w-4 h-4" />
                                <span>{{ item.label }}</span>
                            </button>
                        </template>
                    </UBreadcrumb>
                </template>
            </UDashboardToolbar>
        </template>

        <template #body>
            <div class="flex flex-col">
                <!-- File browser table -->
                <UTable
                    v-model:row-selection="rowSelection"
                    @select="onSelect"
                    :data="displayItems"
                    :columns="columns"
                    :loading="status === 'pending'"
                    loadingAnimation="elastic"
                    :ui="{
                        tr: 'cursor-pointer',
                        th: 'cursor-default',
                    }"
                >
                    <template #empty>
                        <UEmpty v-if="!currentBucket" title="No Buckets" description="There are no buckets yet." icon="i-lucide-cylinder" size="lg" variant="naked" />
                        <UEmpty v-else title="No Objects" description="This folder is empty." icon="i-lucide-folder-open" size="lg" variant="naked" />
                    </template>
                </UTable>

                <!-- Pagination -->
                <div v-if="showPagination" class="flex justify-end p-4 border-t border-default">
                    <UPagination v-model:page="page" show-edges :items-per-page="itemsPerPage" :total="paginationMetadata?.total || 0" variant="ghost" active-variant="solid" active-color="primary" color="neutral" size="sm" />
                </div>
            </div>
        </template>
    </UDashboardPanel>
</template>
