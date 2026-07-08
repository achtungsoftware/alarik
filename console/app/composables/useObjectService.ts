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

export interface DeleteResult {
    successCount: number;
    errorCount: number;
    skippedBuckets: number;
}

export function useObjectService() {
    const config = useRuntimeConfig();
    const jwtCookie = useJWTCookie();
    const toast = useToast();

    const isDeleting = ref(false);
    const isDownloading = ref(false);

    const apiBaseUrl = config.public.apiBaseUrl;

    async function deleteObject(bucket: string, key: string): Promise<boolean> {
        try {
            await $fetch(`${apiBaseUrl}/api/v1/objects`, {
                method: "DELETE",
                headers: { Authorization: `Bearer ${jwtCookie.value}` },
                params: { bucket, key },
            });
            return true;
        } catch (error) {
            console.error(`Failed to delete ${key}:`, error);
            return false;
        }
    }

    async function deleteObjects(bucket: string, items: BrowserItem[]): Promise<DeleteResult> {
        const result: DeleteResult = { successCount: 0, errorCount: 0, skippedBuckets: 0 };

        if (items.length === 0) return result;

        isDeleting.value = true;

        try {
            for (const item of items) {
                if (item.isBucket) {
                    result.skippedBuckets++;
                    continue;
                }

                const success = await deleteObject(bucket, item.key);
                if (success) {
                    result.successCount++;
                } else {
                    result.errorCount++;
                }
            }

            showDeleteResultToast(result);
        } finally {
            isDeleting.value = false;
        }

        return result;
    }

    function showDeleteResultToast(result: DeleteResult) {
        if (result.skippedBuckets > 0) {
            toast.add({
                title: "Buckets Cannot Be Deleted Here",
                description: `${result.skippedBuckets} bucket${result.skippedBuckets !== 1 ? "s" : ""} skipped. Use the delete button in the Actions column to delete buckets.`,
                icon: "i-lucide-alert-triangle",
                color: "warning",
            });
        } else if (result.errorCount === 0) {
            toast.add({
                title: "Deletion Successful",
                description: `${result.successCount} item${result.successCount !== 1 ? "s" : ""} deleted successfully`,
                icon: "i-lucide-circle-check",
                color: "success",
            });
        } else if (result.successCount === 0) {
            toast.add({
                title: "Deletion Failed",
                description: `All ${result.errorCount} item${result.errorCount !== 1 ? "s" : ""} failed to delete`,
                icon: "i-lucide-circle-x",
                color: "error",
            });
        } else {
            toast.add({
                title: "Deletion Partially Successful",
                description: `${result.successCount} succeeded, ${result.errorCount} failed`,
                icon: "i-lucide-alert-triangle",
                color: "warning",
            });
        }
    }

    /// Fetches `url` and reports live progress as bytes arrive, instead of the browser giving
    /// no feedback at all until `response.blob()` resolves - which for a large object could be
    /// minutes of the UI looking completely idle. `total` is null when the response has no
    /// Content-Length (e.g. the ZIP multi-file download, whose size isn't known upfront).
    async function fetchWithProgress(
        url: string,
        options: RequestInit,
        onProgress: (loaded: number, total: number | null) => void
    ): Promise<{ blob: Blob; response: Response }> {
        const response = await fetch(url, options);
        if (!response.ok) {
            throw new Error(`Download failed: ${response.statusText}`);
        }

        const contentLength = response.headers.get("Content-Length");
        const total = contentLength ? parseInt(contentLength, 10) : null;

        if (!response.body) {
            // Streaming reads aren't available in this environment - fall back to a single
            // await with no progress reporting, rather than failing the download entirely.
            const blob = await response.blob();
            onProgress(blob.size, total);
            return { blob, response };
        }

        const reader = response.body.getReader();
        const chunks: BlobPart[] = [];
        let loaded = 0;

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            if (value) {
                chunks.push(value as BlobPart);
                loaded += value.length;
                onProgress(loaded, total);
            }
        }

        return { blob: new Blob(chunks), response };
    }

    /// One line of human-readable progress, shown live in the download toast - a percentage
    /// when the total size is known, otherwise just the running byte count.
    function formatProgress(loaded: number, total: number | null): string {
        if (total && total > 0) {
            const percent = Math.min(100, Math.round((loaded / total) * 100));
            return `${percent}% • ${formatBytes(loaded)} / ${formatBytes(total)}`;
        }
        return `${formatBytes(loaded)} downloaded`;
    }

    async function downloadObjects(bucket: string, keys: string[]): Promise<boolean> {
        if (keys.length === 0) return false;

        isDownloading.value = true;

        const label = keys.length === 1 ? (keys[0]?.split("/").pop() ?? keys[0]!) : `${keys.length} items`;
        const toastId = toast.add({
            title: `Downloading ${label}`,
            description: "Starting…",
            icon: "i-lucide-download",
            color: "primary",
            duration: 0,
        }).id;
        let lastUpdate = 0;

        try {
            const { blob, response } = await fetchWithProgress(
                `${apiBaseUrl}/api/v1/objects/download`,
                {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${jwtCookie.value}`,
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({ bucket, keys }),
                },
                (loaded, total) => {
                    // Progress arrives in a tight loop on fast connections - updating the
                    // toast on every chunk would just churn re-renders for no visible benefit.
                    const now = Date.now();
                    if (now - lastUpdate < 150 && loaded !== total) return;
                    lastUpdate = now;
                    toast.update(toastId, { description: formatProgress(loaded, total) });
                }
            );

            const filename = extractFilename(response) || "download";
            triggerBrowserDownload(blob, filename);

            toast.update(toastId, {
                title: "Download Complete",
                description: `${keys.length} item${keys.length !== 1 ? "s" : ""} downloaded successfully`,
                icon: "i-lucide-circle-check",
                color: "success",
                duration: undefined,
            });

            return true;
        } catch (err: any) {
            toast.update(toastId, {
                title: "Download Failed",
                description: err.data?.reason ?? err.message ?? "Unknown error",
                icon: "i-lucide-circle-x",
                color: "error",
                duration: undefined,
            });
            return false;
        } finally {
            isDownloading.value = false;
        }
    }

    async function downloadSingleObject(bucket: string, key: string): Promise<boolean> {
        isDownloading.value = true;

        const displayName = key.split("/").pop() || key;
        const toastId = toast.add({
            title: `Downloading ${displayName}`,
            description: "Starting…",
            icon: "i-lucide-download",
            color: "primary",
            duration: 0,
        }).id;
        let lastUpdate = 0;

        try {
            const { blob, response } = await fetchWithProgress(
                `${apiBaseUrl}/api/v1/objects/download`,
                {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${jwtCookie.value}`,
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({ bucket, keys: [key] }),
                },
                (loaded, total) => {
                    const now = Date.now();
                    if (now - lastUpdate < 150 && loaded !== total) return;
                    lastUpdate = now;
                    toast.update(toastId, { description: formatProgress(loaded, total) });
                }
            );

            const filename = extractFilename(response) || displayName;
            triggerBrowserDownload(blob, filename);

            toast.update(toastId, {
                title: "Download Complete",
                description: filename,
                icon: "i-lucide-circle-check",
                color: "success",
                duration: undefined,
            });

            return true;
        } catch (err: any) {
            toast.update(toastId, {
                title: "Download Failed",
                description: err.data?.reason ?? err.message ?? "Unknown error",
                icon: "i-lucide-circle-x",
                color: "error",
                duration: undefined,
            });
            return false;
        } finally {
            isDownloading.value = false;
        }
    }

    function extractFilename(response: Response): string | null {
        const contentDisposition = response.headers.get("Content-Disposition");
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?(.+?)"?$/);
            if (filenameMatch && filenameMatch[1]) {
                return filenameMatch[1];
            }
        }
        return null;
    }

    function triggerBrowserDownload(blob: Blob, filename: string) {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
    }

    return {
        // State
        isDeleting: readonly(isDeleting),
        isDownloading: readonly(isDownloading),

        // Delete operations
        deleteObject,
        deleteObjects,

        // Download operations
        downloadObjects,
        downloadSingleObject,
    };
}
