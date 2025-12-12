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

export interface UploadResult {
    successCount: number;
    errorCount: number;
}

export function useObjectService() {
    const config = useRuntimeConfig();
    const jwtCookie = useJWTCookie();
    const toast = useToast();

    const isDeleting = ref(false);
    const isDownloading = ref(false);
    const isUploading = ref(false);

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

    async function downloadObjects(bucket: string, keys: string[]): Promise<boolean> {
        if (keys.length === 0) return false;

        isDownloading.value = true;

        try {
            const response = await fetch(`${apiBaseUrl}/api/v1/objects/download`, {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${jwtCookie.value}`,
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ bucket, keys }),
            });

            if (!response.ok) {
                throw new Error(`Download failed: ${response.statusText}`);
            }

            const blob = await response.blob();
            const filename = extractFilename(response) || "download";

            triggerBrowserDownload(blob, filename);

            toast.add({
                title: "Download Started",
                description: `Downloading ${keys.length} item${keys.length !== 1 ? "s" : ""}`,
                icon: "i-lucide-download",
                color: "success",
            });

            return true;
        } catch (err: any) {
            toast.add({
                title: "Download Failed",
                description: err.data?.reason ?? err.message ?? "Unknown error",
                icon: "i-lucide-circle-x",
                color: "error",
            });
            return false;
        } finally {
            isDownloading.value = false;
        }
    }

    async function downloadSingleObject(bucket: string, key: string): Promise<boolean> {
        isDownloading.value = true;

        try {
            const response = await fetch(`${apiBaseUrl}/api/v1/objects/download`, {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${jwtCookie.value}`,
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ bucket, keys: [key] }),
            });

            if (!response.ok) {
                throw new Error(`Download failed: ${response.statusText}`);
            }

            const blob = await response.blob();
            const filename = extractFilename(response) || key.split("/").pop() || "download";

            triggerBrowserDownload(blob, filename);

            toast.add({
                title: "Download Started",
                description: `Downloading ${filename}`,
                icon: "i-lucide-download",
                color: "success",
            });

            return true;
        } catch (err: any) {
            toast.add({
                title: "Download Failed",
                description: err.data?.reason ?? err.message ?? "Unknown error",
                icon: "i-lucide-circle-x",
                color: "error",
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

    async function uploadFile(bucket: string, prefix: string, file: File): Promise<boolean> {
        try {
            const formData = new FormData();
            formData.append("data", file, file.name);

            await $fetch(`${apiBaseUrl}/api/v1/objects`, {
                method: "POST",
                headers: { Authorization: `Bearer ${jwtCookie.value}` },
                params: {
                    bucket,
                    ...(prefix.length > 0 && { prefix }),
                },
                body: formData,
            });
            return true;
        } catch (error) {
            console.error(`Failed to upload ${file.name}:`, error);
            return false;
        }
    }

    async function uploadFiles(bucket: string, prefix: string, files: FileList | File[]): Promise<UploadResult> {
        const result: UploadResult = { successCount: 0, errorCount: 0 };

        if (!files || files.length === 0) return result;

        isUploading.value = true;

        try {
            for (const file of Array.from(files)) {
                const success = await uploadFile(bucket, prefix, file);
                if (success) {
                    result.successCount++;
                } else {
                    result.errorCount++;
                }
            }

            showUploadResultToast(result);
        } finally {
            isUploading.value = false;
        }

        return result;
    }

    async function uploadFolder(bucket: string, currentPrefix: string, files: FileList | File[]): Promise<UploadResult> {
        const result: UploadResult = { successCount: 0, errorCount: 0 };

        if (!files || files.length === 0) return result;

        isUploading.value = true;

        try {
            for (const file of Array.from(files)) {
                const relativePath = (file as any).webkitRelativePath || file.name;
                const fullPrefix = currentPrefix + relativePath.substring(0, relativePath.lastIndexOf("/") + 1);

                const success = await uploadFile(bucket, fullPrefix, file);
                if (success) {
                    result.successCount++;
                } else {
                    result.errorCount++;
                }
            }

            showUploadResultToast(result);
        } finally {
            isUploading.value = false;
        }

        return result;
    }

    function showUploadResultToast(result: UploadResult) {
        if (result.errorCount === 0) {
            toast.add({
                title: "Upload Successful",
                description: `${result.successCount} file${result.successCount !== 1 ? "s" : ""} uploaded successfully`,
                icon: "i-lucide-circle-check",
                color: "success",
            });
        } else if (result.successCount === 0) {
            toast.add({
                title: "Upload Failed",
                description: `All ${result.errorCount} file${result.errorCount !== 1 ? "s" : ""} failed to upload`,
                icon: "i-lucide-circle-x",
                color: "error",
            });
        } else {
            toast.add({
                title: "Upload Partially Successful",
                description: `${result.successCount} succeeded, ${result.errorCount} failed`,
                icon: "i-lucide-alert-triangle",
                color: "warning",
            });
        }
    }

    return {
        // State
        isDeleting: readonly(isDeleting),
        isDownloading: readonly(isDownloading),
        isUploading: readonly(isUploading),

        // Delete operations
        deleteObject,
        deleteObjects,

        // Download operations
        downloadObjects,
        downloadSingleObject,

        // Upload operations
        uploadFile,
        uploadFiles,
        uploadFolder,
    };
}
