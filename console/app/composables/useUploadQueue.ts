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

export type UploadStatus = "pending" | "uploading" | "processing" | "completed" | "error" | "cancelled";

export interface UploadItem {
    id: string;
    file: File;
    bucket: string;
    prefix: string;
    status: UploadStatus;
    progress: number;
    error?: string;
    abortController?: AbortController;
}

const uploadQueue = ref<UploadItem[]>([]);
const isSlideoverOpen = ref(false);
const activeUploads = ref(0);
const onCompleteCallbacks = ref<Set<() => void>>(new Set());

const BATCH_SIZE = 50;
const BATCH_DELAY = 10;

const ACTIVE_STATUSES: UploadStatus[] = ["pending", "uploading", "processing"];
const FAILED_STATUSES: UploadStatus[] = ["error", "cancelled"];

export function useUploadQueue() {
    const config = useRuntimeConfig();
    const jwtCookie = useJWTCookie();
    const toast = useToast();

    const apiBaseUrl = config.public.apiBaseUrl;

    const isUploading = computed(() => uploadQueue.value.some((item) => ACTIVE_STATUSES.includes(item.status)));
    const uploadingCount = computed(() => uploadQueue.value.filter((item) => item.status === "uploading" || item.status === "processing").length);
    const pendingCount = computed(() => uploadQueue.value.filter((item) => item.status === "pending").length);
    const completedCount = computed(() => uploadQueue.value.filter((item) => item.status === "completed").length);
    const errorCount = computed(() => uploadQueue.value.filter((item) => item.status === "error").length);
    const cancelledCount = computed(() => uploadQueue.value.filter((item) => item.status === "cancelled").length);
    const failedOrCancelledCount = computed(() => errorCount.value + cancelledCount.value);
    const totalCount = computed(() => uploadQueue.value.length);

    const overallProgress = computed(() => {
        const items = uploadQueue.value;
        if (items.length === 0) return 0;
        const totalProgress = items.reduce((sum, item) => {
            if (item.status === "completed") return sum + 100;
            if (FAILED_STATUSES.includes(item.status)) return sum;
            return sum + item.progress;
        }, 0);
        return Math.round(totalProgress / items.length);
    });

    function generateId(): string {
        return `${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    }

    async function addToQueue(bucket: string, prefix: string, files: FileList | File[], preserveFolderStructure = false) {
        const fileArray = Array.from(files);
        isSlideoverOpen.value = true;

        for (let i = 0; i < fileArray.length; i += BATCH_SIZE) {
            const batch = fileArray.slice(i, i + BATCH_SIZE);
            const newItems: UploadItem[] = batch.map((file) => {
                let itemPrefix = prefix;
                if (preserveFolderStructure) {
                    const relativePath = (file as any).webkitRelativePath || file.name;
                    itemPrefix = prefix + relativePath.substring(0, relativePath.lastIndexOf("/") + 1);
                }
                return {
                    id: generateId(),
                    file,
                    bucket,
                    prefix: itemPrefix,
                    status: "pending" as UploadStatus,
                    progress: 0,
                };
            });

            uploadQueue.value.push(...newItems);

            if (i + BATCH_SIZE < fileArray.length) {
                await new Promise((resolve) => setTimeout(resolve, BATCH_DELAY));
            }
        }

        processQueue();
    }

    function addFolderToQueue(bucket: string, prefix: string, files: FileList | File[]) {
        return addToQueue(bucket, prefix, files, true);
    }

    function processQueue() {
        const pendingItems = uploadQueue.value.filter((item) => item.status === "pending");

        while (activeUploads.value < MAX_CONCURRENT_UPLOADS && pendingItems.length > 0) {
            const item = pendingItems.shift();
            if (item) {
                uploadItem(item);
            }
        }
    }

    function updateItem(id: string, updates: Partial<UploadItem>) {
        const index = uploadQueue.value.findIndex((i) => i.id === id);
        if (index === -1) return;
        const current = uploadQueue.value[index]!;
        uploadQueue.value[index] = { ...current, ...updates } as UploadItem;
        triggerRef(uploadQueue);
    }

    function uploadItem(item: UploadItem) {
        if (!uploadQueue.value.find((i) => i.id === item.id)) return;

        const abortController = new AbortController();
        updateItem(item.id, { status: "uploading", abortController });
        activeUploads.value++;

        const formData = new FormData();
        formData.append("data", item.file, item.file.name);

        const url = new URL(`${apiBaseUrl}/api/v1/objects`);
        url.searchParams.set("bucket", item.bucket);
        if (item.prefix.length > 0) {
            url.searchParams.set("prefix", item.prefix);
        }

        const xhr = new XMLHttpRequest();

        xhr.upload.onprogress = (event) => {
            if (event.lengthComputable) {
                const progress = Math.round((event.loaded / event.total) * 100);
                updateItem(item.id, { progress });
                if (progress === 100) {
                    updateItem(item.id, { status: "processing" });
                }
            }
        };

        xhr.onload = () => {
            if (xhr.status >= 200 && xhr.status < 300) {
                updateItem(item.id, { status: "completed", progress: 100 });
            } else {
                let errorMsg = `Upload failed (${xhr.status})`;
                try {
                    const response = JSON.parse(xhr.responseText);
                    errorMsg = response?.reason || errorMsg;
                } catch {
                    // Keep default error message
                }
                updateItem(item.id, { status: "error", error: errorMsg });
            }
            onUploadFinished();
        };

        xhr.onerror = () => {
            updateItem(item.id, { status: "error", error: "Network error" });
            onUploadFinished();
        };

        xhr.onabort = () => {
            updateItem(item.id, { status: "cancelled", error: "Cancelled" });
            onUploadFinished();
        };

        abortController.signal.addEventListener("abort", () => xhr.abort());

        xhr.open("POST", url.toString());
        xhr.setRequestHeader("Authorization", `Bearer ${jwtCookie.value}`);
        xhr.send(formData);
    }

    function onUploadFinished() {
        activeUploads.value--;
        processQueue();

        const stillProcessing = uploadQueue.value.some((i) => ACTIVE_STATUSES.includes(i.status));
        if (!stillProcessing) {
            showCompletionToast();
            onCompleteCallbacks.value.forEach((cb) => cb());
        }
    }

    function showCompletionToast() {
        const completed = completedCount.value;
        const errors = errorCount.value;
        const cancelled = cancelledCount.value;

        if (cancelled > 0 && completed === 0 && errors === 0) return;

        if (errors === 0 && completed > 0) {
            toast.add({
                title: "Upload Complete",
                description: `${completed} file${completed !== 1 ? "s" : ""} uploaded successfully`,
                icon: "i-lucide-circle-check",
                color: "success",
            });
        } else if (completed === 0 && errors > 0) {
            toast.add({
                title: "Upload Failed",
                description: `All ${errors} file${errors !== 1 ? "s" : ""} failed to upload`,
                icon: "i-lucide-circle-x",
                color: "error",
            });
        } else if (completed > 0 && errors > 0) {
            toast.add({
                title: "Upload Partially Complete",
                description: `${completed} succeeded, ${errors} failed`,
                icon: "i-lucide-alert-triangle",
                color: "warning",
            });
        }
    }

    function cancelItem(id: string) {
        const item = uploadQueue.value.find((i) => i.id === id);
        if (!item) return;

        if (item.status === "uploading" && item.abortController) {
            item.abortController.abort();
        } else if (item.status === "pending") {
            updateItem(id, { status: "cancelled", error: "Cancelled" });
        }
    }

    function stopAll() {
        uploadQueue.value.forEach((item) => {
            if (item.status === "uploading" && item.abortController) {
                item.abortController.abort();
            } else if (item.status === "pending") {
                updateItem(item.id, { status: "cancelled", error: "Cancelled" });
            }
        });
    }

    function retryItem(id: string) {
        const item = uploadQueue.value.find((i) => i.id === id);
        if (item && FAILED_STATUSES.includes(item.status)) {
            updateItem(id, { status: "pending", progress: 0, error: undefined, abortController: undefined });
            processQueue();
        }
    }

    function retryAllFailed() {
        uploadQueue.value
            .filter((item) => FAILED_STATUSES.includes(item.status))
            .forEach((item) => {
                updateItem(item.id, { status: "pending", progress: 0, error: undefined, abortController: undefined });
            });
        processQueue();
    }

    function removeItem(id: string) {
        const item = uploadQueue.value.find((i) => i.id === id);
        if (!item) return;

        if (item.status === "uploading" && item.abortController) {
            item.abortController.abort();
        }
        uploadQueue.value = uploadQueue.value.filter((i) => i.id !== id);
    }

    function clearCompleted() {
        uploadQueue.value = uploadQueue.value.filter((item) => item.status !== "completed");
    }

    function clearAll() {
        stopAll();
        uploadQueue.value = [];
    }

    function openSlideover() {
        isSlideoverOpen.value = true;
    }

    function closeSlideover() {
        isSlideoverOpen.value = false;
    }

    function onBatchComplete(callback: () => void) {
        onCompleteCallbacks.value.add(callback);
        onScopeDispose(() => {
            onCompleteCallbacks.value.delete(callback);
        });
    }

    return {
        // State
        uploadQueue,
        isSlideoverOpen,
        isUploading,
        uploadingCount,
        pendingCount,
        completedCount,
        errorCount,
        cancelledCount,
        failedOrCancelledCount,
        totalCount,
        overallProgress,

        // Actions
        addToQueue,
        addFolderToQueue,
        cancelItem,
        stopAll,
        retryItem,
        retryAllFailed,
        removeItem,
        clearCompleted,
        clearAll,
        openSlideover,
        closeSlideover,
        onBatchComplete,
    };
}
