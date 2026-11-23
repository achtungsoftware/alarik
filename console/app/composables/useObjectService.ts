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

    /// Asks the API for a download ticket and returns the URL the browser can fetch on its own.
    /// The browser streams the bytes itself - native progress, no copy of the object in memory,
    /// previews that can seek - which a fetch-then-blob flow could not do.
    async function downloadTicketUrl(request: {
        bucket: string;
        keys: string[];
        versionId?: string;
        inline?: boolean;
        downloadName?: string;
    }): Promise<string> {
        const ticket = await $fetch<DownloadTicket>(`${apiBaseUrl}/api/v1/objects/download-ticket`, {
            method: "POST",
            headers: { Authorization: `Bearer ${jwtCookie.value}` },
            body: request,
        });
        return `${apiBaseUrl}/api/v1/objects/download?ticket=${encodeURIComponent(ticket.ticket)}`;
    }

    /// Hands a ticket URL to the browser as a download. The server's Content-Disposition names
    /// the file, so this works across origins where the `download` attribute would be ignored.
    function triggerBrowserDownload(url: string) {
        const a = document.createElement("a");
        a.href = url;
        a.rel = "noopener";
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    }

    async function startDownload(bucket: string, keys: string[], label: string): Promise<boolean> {
        isDownloading.value = true;
        try {
            triggerBrowserDownload(await downloadTicketUrl({ bucket, keys }));
            toast.add({
                title: "Download Started",
                description: `${label} - your browser shows the progress`,
                icon: "i-lucide-download",
                color: "success",
            });
            return true;
        } catch (err) {
            toast.add({
                title: "Download Failed",
                description: errorMessage(err),
                icon: "i-lucide-circle-x",
                color: "error",
            });
            return false;
        } finally {
            isDownloading.value = false;
        }
    }

    async function downloadObjects(bucket: string, keys: string[]): Promise<boolean> {
        if (keys.length === 0) return false;
        const label = keys.length === 1 ? (keys[0]?.split("/").pop() ?? keys[0]!) : `${keys.length} items`;
        return startDownload(bucket, keys, label);
    }

    async function downloadSingleObject(bucket: string, key: string): Promise<boolean> {
        return startDownload(bucket, [key], key.split("/").pop() || key);
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
        downloadTicketUrl,
        triggerBrowserDownload,
    };
}
