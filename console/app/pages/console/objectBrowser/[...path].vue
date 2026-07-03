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

const route = useRoute();

const openBucketCreateModal = ref(false);
const page = ref(1);
const itemsPerPage = ref(100);
const rowSelection = ref<Record<string, boolean>>({});
const jwtCookie = useJWTCookie();
const fileInput = ref<HTMLInputElement | null>(null);
const folderInput = ref<HTMLInputElement | null>(null);
const openDetailModal = ref(false);
const selectedObject = ref<BrowserItem | null>(null);
const openPreviewModal = ref(false);
const previewObject = ref<BrowserItem | null>(null);
const openBucketPolicyModal = ref(false);
const selectedBucketForPolicy = ref<Bucket | null>(null);
const openBucketTagsModal = ref(false);
const selectedBucketForTags = ref<Bucket | null>(null);
const openShareModal = ref(false);
const shareObject = ref<BrowserItem | null>(null);

const searchInput = ref("");
const searchQuery = ref("");
let searchDebounceTimer: ReturnType<typeof setTimeout> | undefined;
watch(searchInput, (val) => {
    if (searchDebounceTimer) clearTimeout(searchDebounceTimer);
    searchDebounceTimer = setTimeout(() => {
        searchQuery.value = val.trim();
    }, 300);
});
watch(searchQuery, () => {
    page.value = 1;
    rowSelection.value = {};
});

const { isDeleting, isDownloading, deleteObjects, downloadObjects, downloadSingleObject } = useObjectService();
const { addToQueue, addFolderToQueue, isUploading, openSlideover, onBatchComplete } = useUploadQueue();
const { filesFromDataTransfer } = useDragDropFiles();

// Refresh the list when uploads complete
onBatchComplete(() => {
    refresh();
});
const { confirm } = useConfirmDialog();
const toast = useToast();

// Navigation derived from route
const currentBucket = computed(() => {
    const pathSegments = route.params.path as string[] | undefined;
    return pathSegments?.[0] ?? "";
});

const currentPrefix = computed(() => {
    const pathSegments = route.params.path as string[] | undefined;
    if (!pathSegments || pathSegments.length <= 1) return "";
    return pathSegments.slice(1).join("/") + "/";
});

// Reset page and search when navigation changes
watch([currentBucket, currentPrefix], () => {
    page.value = 1;
    rowSelection.value = {};
    searchInput.value = "";
    searchQuery.value = "";
});

const selectedItems = computed(() => {
    return displayItems.value.filter((_, index) => rowSelection.value[index]);
});

// Count files, folders, and buckets for deletion dialog
const deletionCounts = computed(() => {
    const items = selectedItems.value;
    const bucketCount = items.filter((item) => item.isBucket).length;
    const nonBucketItems = items.filter((item) => !item.isBucket);
    const fileCount = nonBucketItems.filter((item) => !item.isFolder).length;
    const folderCount = nonBucketItems.filter((item) => item.isFolder).length;
    return { fileCount, folderCount, bucketCount, total: items.length };
});

interface BreadcrumbNav {
    label: string;
    icon: string;
    to: string;
}

// Computed breadcrumb navigation items
const breadcrumbNavItems = computed<BreadcrumbNav[]>(() => {
    const items: BreadcrumbNav[] = [
        {
            label: "Buckets",
            icon: "i-lucide-home",
            to: "/console/objectBrowser",
        },
    ];

    if (currentBucket.value) {
        items.push({
            label: currentBucket.value,
            icon: "i-lucide-cylinder",
            to: `/console/objectBrowser/${currentBucket.value}`,
        });

        if (currentPrefix.value) {
            const folderParts = currentPrefix.value.split("/").filter((p) => p.length > 0);
            folderParts.forEach((part, index) => {
                const folders = folderParts.slice(0, index + 1);
                items.push({
                    label: part,
                    icon: "i-lucide-folder",
                    to: `/console/objectBrowser/${currentBucket.value}/${folders.join("/")}`,
                });
            });
        }
    }

    return items;
});

// Convert to BreadcrumbItem format
const breadcrumbItems = computed<BreadcrumbItem[]>(() => {
    return breadcrumbNavItems.value.map((item) => ({
        label: item.label,
        icon: item.icon,
        to: item.to,
    }));
});

// Fetch buckets (with pagination)
const {
    data: bucketsResponse,
    status: bucketsStatus,
    refresh: refreshBuckets,
} = await useFetch<Page<Bucket>>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/buckets`, {
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
} = await useFetch<Page<BrowserItem>>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects`, {
    params: {
        bucket: currentBucket,
        prefix: currentPrefix,
        search: searchQuery,
        page: page,
        per: itemsPerPage,
    },
    headers: {
        Authorization: `Bearer ${jwtCookie.value}`,
    },
    immediate: false,
    default: () => ({ items: [], metadata: { page: 1, per: 100, total: 0 } }),
});

// Fetch objects when navigating into a bucket, or when the search query changes
watch(
    [currentBucket, currentPrefix, page, searchQuery],
    () => {
        if (currentBucket.value) {
            refresh();
        }
    },
    { immediate: true }
);

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

// Bucket/folder sizes aren't in the list response (a recursive directory walk per row on
// every page load would make listing slow to scale) - instead fetched lazily, one small
// request per bucket/folder already visible on the current page. Keyed by item.key, which is
// unique within a single listing (root = bucket names, inside a bucket = folder keys).
interface EntryStats {
    sizeBytes: number;
    objectCount: number;
}
const entryStats = ref<Record<string, EntryStats | "loading" | "error">>({});

watch(
    displayItems,
    (items) => {
        for (const item of items) {
            if (!item.isFolder) continue;
            if (entryStats.value[item.key]) continue;

            entryStats.value[item.key] = "loading";
            const params = item.isBucket ? { bucket: item.key } : { bucket: currentBucket.value, prefix: item.key };
            $fetch<EntryStats>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/stats`, {
                headers: { Authorization: `Bearer ${jwtCookie.value}` },
                params,
            })
                .then((stats) => {
                    entryStats.value[item.key] = stats;
                })
                .catch(() => {
                    entryStats.value[item.key] = "error";
                });
        }
    },
    { immediate: true }
);

// Cheap invalidation on navigation - stale folder stats from a different bucket/prefix must
// never bleed into a fresh listing that happens to reuse the same folder name.
watch([currentBucket, currentPrefix], () => {
    entryStats.value = {};
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
        accessorKey: "name",
        header: "Name",
        cell: ({ row }) => {
            const item = row.original;
            let displayName = item.key;

            if (item.isBucket) {
                displayName = item.key;
            } else if (item.isFolder) {
                displayName = item.key
                    .split("/")
                    .filter((p: any) => p)
                    .pop();
            } else {
                displayName = item.key.split("/").pop() || item.key;
            }

            const icon = getFileIcon(item.key, item.isFolder || false, item.isBucket || false);
            const iconColorClass = item.isBucket ? "text-primary" : item.isFolder ? "text-green-500" : "text-secondary-500";
            const bgColorClass = item.isBucket ? "bg-primary/20" : item.isFolder ? "bg-green-500/20" : "bg-secondary-500/20";

            let subtitle;
            if (item.isBucket) {
                const bucket = bucketsResponse.value?.items?.find((b: Bucket) => b.name === item.key);
                subtitle = h("div", { class: "flex items-center gap-1.5" }, [
                    h("span", { class: "text-xs text-muted" }, "Bucket"),
                    bucket
                        ? h(resolveComponent("UBadge"), {
                              label: `Versioning ${bucket.versioningStatus}`,
                              size: "xs",
                              variant: bucket.versioningStatus === "Enabled" ? "solid" : "subtle",
                              color: bucket.versioningStatus === "Enabled" ? "primary" : bucket.versioningStatus === "Suspended" ? "warning" : "neutral",
                          })
                        : null,
                ]);
            } else if (item.isFolder) {
                subtitle = h("span", { class: "text-xs text-muted" }, "Folder");
            } else {
                subtitle = h("span", { class: "text-xs text-muted" }, item.contentType || "File");
            }

            return h("div", { class: "flex items-center gap-3" }, [h("div", { class: `flex items-center justify-center w-9 h-9 rounded-lg ${bgColorClass}` }, [h(resolveComponent("UIcon"), { name: icon, class: `w-5 h-5 ${iconColorClass}` })]), h("div", { class: "flex flex-col" }, [h("span", { class: "font-medium text-highlighted" }, displayName), subtitle])]);
        },
    },
    {
        accessorKey: "size",
        header: "Size",
        cell: ({ row }) => {
            const item = row.original;
            if (!item.isFolder) return formatBytes(item.size);

            const stats = entryStats.value[item.key];
            if (!stats || stats === "loading") return h(resolveComponent("LoadingIndicator"), { size: 14 });
            if (stats === "error") return h("span", { class: "text-muted" }, "—");
            return formatBytes(stats.sizeBytes);
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
    {
        accessorKey: "actionButtons",
        header: "Actions",
        cell: ({ row }) => {
            const item = row.original;

            if (item.isBucket) {
                const bucket = bucketsResponse.value?.items?.find((b: Bucket) => b.name === item.key);
                if (!bucket) return;

                return h("div", { class: "flex flex-row items-center gap-2" }, [
                    h(resolveComponent("UButton"), {
                        label: "Policy",
                        variant: "subtle",
                        color: "neutral",
                        size: "sm",
                        icon: "i-lucide-shield",
                        onClick: (e: Event) => {
                            e.stopPropagation();
                            selectedBucketForPolicy.value = bucket;
                            openBucketPolicyModal.value = true;
                        },
                    }),
                    h(resolveComponent("UButton"), {
                        label: "Tags",
                        variant: "subtle",
                        color: "neutral",
                        size: "sm",
                        icon: "i-lucide-tags",
                        onClick: (e: Event) => {
                            e.stopPropagation();
                            selectedBucketForTags.value = bucket;
                            openBucketTagsModal.value = true;
                        },
                    }),
                    h(resolveComponent("UButton"), {
                        label: "Webhooks",
                        variant: "subtle",
                        color: "neutral",
                        size: "sm",
                        icon: "i-lucide-webhook",
                        onClick: (e: Event) => {
                            e.stopPropagation();
                            navigateTo(`/console/webhooks?bucket=${bucket.name}`);
                        },
                    }),
                    h(resolveComponent("UButton"), {
                        label: "Replication",
                        variant: "subtle",
                        color: "neutral",
                        size: "sm",
                        icon: "i-lucide-repeat",
                        onClick: (e: Event) => {
                            e.stopPropagation();
                            navigateTo(`/console/replication?bucket=${bucket.name}`);
                        },
                    }),
                ]);
            }

            if (item.isFolder) {
                return;
            }

            return h("div", { class: "flex flex-row items-center gap-2" }, [
                h(resolveComponent("UButton"), {
                    label: "Preview",
                    variant: "subtle",
                    color: "neutral",
                    size: "sm",
                    icon: "i-lucide-eye",
                    onClick: (e: Event) => {
                        e.stopPropagation();
                        previewObject.value = item;
                        openPreviewModal.value = true;
                    },
                }),
                h(resolveComponent("UButton"), {
                    label: "Download",
                    variant: "subtle",
                    color: "neutral",
                    size: "sm",
                    icon: "i-lucide-download",
                    onClick: async (e: Event) => {
                        e.stopPropagation();
                        await downloadSingleObject(currentBucket.value, item.key);
                    },
                }),
                h(resolveComponent("UButton"), {
                    label: "Share",
                    variant: "subtle",
                    color: "neutral",
                    size: "sm",
                    icon: "i-lucide-share-2",
                    onClick: (e: Event) => {
                        e.stopPropagation();
                        shareObject.value = item;
                        openShareModal.value = true;
                    },
                }),
                h(resolveComponent("UButton"), {
                    label: "Delete",
                    variant: "subtle",
                    color: "error",
                    size: "sm",
                    icon: "i-lucide-trash",
                    onClick: (e: Event) => {
                        e.stopPropagation();
                        handleSingleDelete(item);
                    },
                }),
            ]);
        },
    },
];

function onSelect(e: Event, row: TableRow<BrowserItem>) {
    const item = row.original;

    if (item.isBucket) {
        navigateTo(`/console/objectBrowser/${item.key}`);
        return;
    } else if (item.isFolder) {
        const folderPath = item.key.endsWith("/") ? item.key.slice(0, -1) : item.key;
        navigateTo(`/console/objectBrowser/${currentBucket.value}/${folderPath}`);
        return;
    }

    // Item is File
    openDetailModal.value = true;
    selectedObject.value = row.original;
}

async function deleteMany() {
    const items = selectedItems.value;
    if (items.length === 0) return;

    const { fileCount, folderCount, bucketCount, total } = deletionCounts.value;

    // Build message based on what's being deleted
    const parts: string[] = [];
    if (bucketCount > 0) parts.push(`${bucketCount} bucket${bucketCount !== 1 ? "s" : ""}`);
    if (folderCount > 0) parts.push(`${folderCount} folder${folderCount !== 1 ? "s" : ""}`);
    if (fileCount > 0) parts.push(`${fileCount} file${fileCount !== 1 ? "s" : ""}`);

    let message = `Do you really want to delete ${parts.join(" and ")}?`;
    if (bucketCount > 0) message += " All Objects inside the Bucket will be deleted.";
    message += " This action cannot be undone.";

    const confirmed = await confirm({
        title: `Delete ${total} Item${total !== 1 ? "s" : ""}`,
        message,
        confirmLabel: "Delete",
    });

    if (!confirmed) return;

    // Delete buckets
    if (bucketCount > 0) {
        const buckets = items.filter((item) => item.isBucket);
        for (const bucket of buckets) {
            await deleteBucket(bucket.key);
        }
        await refreshBuckets();
    }

    // Delete files/folders
    const nonBucketItems = items.filter((item) => !item.isBucket);
    if (nonBucketItems.length > 0) {
        await deleteObjects(currentBucket.value, nonBucketItems);
        await refresh();
    }

    rowSelection.value = {};
}

async function downloadSelected() {
    const items = selectedItems.value.filter((item) => !item.isBucket);
    if (items.length === 0) return;

    const keys = items.map((item) => item.key);
    const success = await downloadObjects(currentBucket.value, keys);
    if (success) {
        rowSelection.value = {};
    }
}

function clearSearch() {
    searchInput.value = "";
}

// Drag-and-drop upload - only active while browsing inside a bucket. Uses an enter/leave
// counter (not a boolean) because dragenter/dragleave fire on every child element as the
// pointer crosses them, not just once for the drop zone as a whole.
const isDraggingOver = ref(false);
let dragCounter = 0;

function onDragEnter() {
    if (!currentBucket.value) return;
    dragCounter++;
    isDraggingOver.value = true;
}

function onDragLeave() {
    if (!currentBucket.value) return;
    dragCounter--;
    if (dragCounter <= 0) {
        dragCounter = 0;
        isDraggingOver.value = false;
    }
}

async function onDrop(event: DragEvent) {
    dragCounter = 0;
    isDraggingOver.value = false;
    if (!currentBucket.value || !event.dataTransfer) return;

    const { files, hasFolders } = await filesFromDataTransfer(event.dataTransfer);
    if (files.length === 0) return;

    // addFolderToQueue preserves structure via webkitRelativePath when present and falls back
    // to a flat root placement otherwise, so it's correct for both folder and plain file drops
    // (including a mixed drop of both).
    if (hasFolders) {
        addFolderToQueue(currentBucket.value, currentPrefix.value, files);
    } else {
        addToQueue(currentBucket.value, currentPrefix.value, files);
    }
}

function triggerFileUpload() {
    fileInput.value?.click();
}

function triggerFolderUpload() {
    folderInput.value?.click();
}

function handleFileUpload(event: Event) {
    const input = event.target as HTMLInputElement;
    const files = input.files;
    if (!files || files.length === 0) return;

    addToQueue(currentBucket.value, currentPrefix.value, files);
    if (input) input.value = "";
}

function handleFolderUpload(event: Event) {
    const input = event.target as HTMLInputElement;
    const files = input.files;
    if (!files || files.length === 0) return;

    addFolderToQueue(currentBucket.value, currentPrefix.value, files);
    if (input) input.value = "";
}

async function handleSingleDelete(item: BrowserItem) {
    const fileName = item.key.split("/").pop() || item.key;
    const confirmed = await confirm({
        title: "Delete File",
        message: `Do you really want to delete "${fileName}"? This action cannot be undone.`,
        confirmLabel: "Delete",
    });

    if (!confirmed) return;

    await deleteObjects(currentBucket.value, [item]);
    await refresh();
}

async function deleteBucket(bucketName: string): Promise<boolean> {
    try {
        await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/buckets/${bucketName}`, {
            method: "DELETE",
            headers: { Authorization: `Bearer ${jwtCookie.value}` },
        });
        return true;
    } catch (error: any) {
        const message = error?.data?.reason || error?.message || "Failed to delete bucket";
        toast.add({
            title: "Failed to Delete Bucket",
            description: message,
            icon: "i-lucide-circle-x",
            color: "error",
        });
        return false;
    }
}
</script>
<template>
    <ObjectDetailModal v-model:open="openDetailModal" :item="selectedObject" :bucketName="currentBucket" @versionDeleted="refresh" @saved="refresh" />
    <BucketPolicyModal v-if="selectedBucketForPolicy && openBucketPolicyModal" v-model:open="openBucketPolicyModal" :bucket="selectedBucketForPolicy" @saved="refreshBuckets" />
    <BucketTagsModal v-if="selectedBucketForTags && openBucketTagsModal" v-model:open="openBucketTagsModal" :bucket="selectedBucketForTags" @saved="refreshBuckets" />
    <ShareFileModal v-if="shareObject && openShareModal" v-model:open="openShareModal" :bucket="currentBucket" :item="shareObject" />
    <FilePreviewModal v-model:open="openPreviewModal" :bucket="currentBucket" :item="previewObject" @saved="refresh" />
    <UploadProgressSlideover />

    <UDashboardPanel
        :ui="{
            body: '!p-0',
        }"
    >
        <template #header>
            <UDashboardNavbar title="Object Browser">
                <template #right>
                    <UButton @click="deleteMany" v-if="!Object.values(rowSelection).every((selected) => !selected)" icon="i-lucide-trash" color="error" :loading="isDeleting">
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
                        v-if="currentBucket != '' && !isUploading"
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
                    <UButton v-else-if="isUploading" icon="i-lucide-list" color="neutral" variant="subtle" label="Upload Progress" @click="openSlideover" />

                    <!-- Hidden file input -->
                    <input ref="fileInput" type="file" multiple style="display: none" @change="handleFileUpload" />

                    <!-- Hidden folder input -->
                    <input ref="folderInput" type="file" webkitdirectory directory multiple style="display: none" @change="handleFolderUpload" />

                    <CreateBucketModal v-if="currentBucket == ''" v-model:open="openBucketCreateModal">
                        <UButton icon="i-lucide-plus" color="primary">Bucket</UButton>
                    </CreateBucketModal>
                </template>
            </UDashboardNavbar>

            <UDashboardToolbar v-if="breadcrumbItems.length > 1 || currentBucket">
                <template #left>
                    <UBreadcrumb :items="breadcrumbItems" class="min-w-0 shrink truncate">
                        <template #separator>
                            <span class="mx-2 text-muted">/</span>
                        </template>
                    </UBreadcrumb>
                </template>
                <template #right>
                    <UInput
                        v-if="currentBucket"
                        v-model="searchInput"
                        placeholder="Search…"
                        icon="i-lucide-search"
                        variant="subtle"
                        size="sm"
                        class="hidden sm:block sm:w-64"
                        :trailing="searchInput.length > 0"
                    >
                        <template v-if="searchInput.length > 0" #trailing>
                            <UButton icon="i-lucide-x" color="neutral" variant="link" size="xs" aria-label="Clear search" @click="clearSearch" />
                        </template>
                    </UInput>
                </template>
            </UDashboardToolbar>

            <div v-if="currentBucket" class="px-4 py-2 border-b border-default sm:hidden">
                <UInput
                    v-model="searchInput"
                    placeholder="Search this bucket…"
                    icon="i-lucide-search"
                    variant="subtle"
                    size="sm"
                    class="w-full"
                    :trailing="searchInput.length > 0"
                >
                    <template v-if="searchInput.length > 0" #trailing>
                        <UButton icon="i-lucide-x" color="neutral" variant="link" size="xs" aria-label="Clear search" @click="clearSearch" />
                    </template>
                </UInput>
            </div>
        </template>

        <template #body>
            <div class="flex flex-col relative" @dragenter.prevent="onDragEnter" @dragover.prevent @dragleave.prevent="onDragLeave" @drop.prevent="onDrop">
                <div v-if="isDraggingOver" class="absolute inset-0 z-10 flex items-center justify-center bg-primary/10 border-2 border-dashed border-primary rounded-lg pointer-events-none m-2">
                    <div class="flex flex-col items-center gap-2 text-primary">
                        <UIcon name="i-lucide-upload-cloud" class="w-10 h-10" />
                        <span class="font-medium">Drop files or folders to upload</span>
                    </div>
                </div>

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
