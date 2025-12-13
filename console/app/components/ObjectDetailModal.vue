<script lang="ts" setup>
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

const props = withDefaults(
    defineProps<{
        open: boolean;
        item: BrowserItem | null;
        bucketName: string;
    }>(),
    {
        open: false,
        bucketName: "",
    }
);

const emit = defineEmits(["update:open", "close", "versionDeleted"]);
const open = ref(props.open);
const jwtCookie = useJWTCookie();
const toast = useToast();
const versions = ref<BrowserItem[]>([]);
const loadingVersions = ref(false);
const deletingVersionId = ref<string | null>(null);
const openPreviewModal = ref(false);
const previewObject = ref<BrowserItem | null>(null);

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val && props.item && props.bucketName) {
            fetchVersions();
        }
    }
);

watch(open, (val) => {
    emit("update:open", val);
    if (!val) {
        versions.value = [];
    }
});

async function fetchVersions() {
    if (!props.item || !props.bucketName) return;

    loadingVersions.value = true;
    try {
        const response = await $fetch<BrowserItem[]>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/versions`, {
            headers: { Authorization: `Bearer ${jwtCookie.value}` },
            params: {
                bucket: props.bucketName,
                key: props.item.key,
            },
        });
        versions.value = response || [];
    } catch (error) {
        console.error("Failed to fetch versions:", error);
        versions.value = [];
    } finally {
        loadingVersions.value = false;
    }
}

async function deleteVersion(versionId: string) {
    if (!props.item || !props.bucketName) return;

    deletingVersionId.value = versionId;
    try {
        await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/version`, {
            method: "DELETE",
            headers: { Authorization: `Bearer ${jwtCookie.value}` },
            params: {
                bucket: props.bucketName,
                key: props.item.key,
                versionId: versionId,
            },
        });

        toast.add({
            title: "Version Deleted",
            description: `Version ${versionId.substring(0, 8)}... deleted successfully`,
            icon: "i-lucide-circle-check",
            color: "success",
        });

        // Refresh versions list
        await fetchVersions();

        // Emit event so parent can refresh if needed
        emit("versionDeleted");
    } catch (error: any) {
        toast.add({
            title: "Delete Failed",
            description: error.data?.reason || "Failed to delete version",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    } finally {
        deletingVersionId.value = null;
    }
}

async function downloadVersion(versionId: string) {
    if (!props.item || !props.bucketName) return;

    try {
        const response = await fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/download`, {
            method: "POST",
            headers: {
                Authorization: `Bearer ${jwtCookie.value}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                bucket: props.bucketName,
                keys: [props.item.key],
                versionId: versionId,
            }),
        });

        if (!response.ok) {
            throw new Error(`Download failed: ${response.statusText}`);
        }

        const blob = await response.blob();
        const originalFilename = props.item.key.split("/").pop() || "download";

        // Build versioned filename: name_versionId.ext or name_versionId if no extension
        let downloadFilename: string;
        const lastDotIndex = originalFilename.lastIndexOf(".");
        if (lastDotIndex > 0) {
            const name = originalFilename.substring(0, lastDotIndex);
            const ext = originalFilename.substring(lastDotIndex);
            downloadFilename = `${name}_${versionId.substring(0, 8)}${ext}`;
        } else {
            downloadFilename = `${originalFilename}_${versionId.substring(0, 8)}`;
        }

        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = downloadFilename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);

        toast.add({
            title: "Download Started",
            description: `Downloading ${downloadFilename}`,
            icon: "i-lucide-download",
            color: "success",
        });
    } catch (error: any) {
        toast.add({
            title: "Download Failed",
            description: error.message || "Failed to download version",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    }
}

function getVersionStatusBadge(version: BrowserItem) {
    if (version.isDeleteMarker) {
        return { label: "Delete Marker", color: "error" as const };
    }
    if (version.isLatest) {
        return { label: "Latest", color: "success" as const };
    }
    return null;
}
</script>
<template>
    <FilePreviewModal v-if="open" v-model:open="openPreviewModal" :bucket="props.bucketName" :item="previewObject" @saved="fetchVersions" />
    
    <USlideover v-model:open="open" :title="props.item?.key">
        <slot />

        <template #body>
            <div v-if="props.item" class="space-y-6">
                <!-- Object Details Card -->
                <UCard
                    variant="subtle"
                    :ui="{
                        body: '!p-0',
                    }"
                >
                    <template #header>
                        <CardHeader title="Object Details" size="sm" />
                    </template>
                    <template #default>
                        <div>
                            <NameValueLabel name="Key" :value="props.item.key" />
                            <NameValueLabel name="ETag" :value="props.item.etag" />
                            <NameValueLabel name="Content-Type" :value="props.item.contentType" />
                            <NameValueLabel name="Size" :value="formatBytes(props.item.size)" />
                            <NameValueLabel name="Last Modified" :value="new Date(props.item.lastModified).toLocaleString()" />
                            <NameValueLabel name="Version Id" v-if="props.item.versionId" :value="props.item.versionId" />
                            <NameValueLabel name="Is Latest" v-if="props.item.isLatest !== undefined" :value="props.item.isLatest ? 'Yes' : 'No'" />
                            <NameValueLabel name="Is Delete Marker" v-if="props.item.isDeleteMarker" :value="'Yes'" />
                        </div>
                    </template>
                </UCard>

                <UCard
                    v-if="versions.length > 0 || loadingVersions"
                    variant="subtle"
                    :ui="{
                        body: '!p-0',
                    }"
                >
                    <template #header>
                        <CardHeader title="Versions" size="sm" :badge="versions.length > 0 ? versions.length : undefined">
                            <template #rightContent>
                                <UButton v-if="!loadingVersions" icon="i-lucide-refresh-ccw" color="neutral" variant="ghost" size="sm" @click="fetchVersions" />
                            </template>
                        </CardHeader>
                    </template>
                    <template #default>
                        <div v-if="loadingVersions" class="p-6 flex items-center justify-center">
                            <LoadingIndicator />
                        </div>
                        <div v-else class="divide-y divide-default">
                            <div v-for="version in versions" :key="version.versionId" class="p-3 hover:bg-elevated/50 transition-colors">
                                <div class="flex items-start justify-between gap-2">
                                    <div class="flex-1 min-w-0">
                                        <div class="flex items-center gap-2">
                                            <span class="text-sm font-mono truncate" :title="version.versionId"> {{ version.versionId?.substring(0, 12) }}... </span>
                                            <UBadge v-if="getVersionStatusBadge(version)" :color="getVersionStatusBadge(version)!.color" variant="subtle" size="xs">
                                                {{ getVersionStatusBadge(version)!.label }}
                                            </UBadge>
                                        </div>
                                        <div class="text-xs text-muted mt-1">
                                            {{ new Date(version.lastModified).toLocaleString() }}
                                            <span v-if="!version.isDeleteMarker"> · {{ formatBytes(version.size) }}</span>
                                        </div>
                                    </div>
                                    <div class="flex items-center gap-1">
                                        <UButton
                                            v-if="!version.isDeleteMarker"
                                            icon="i-lucide-eye"
                                            color="neutral"
                                            variant="ghost"
                                            size="xs"
                                            title="Preview this version"
                                            @click="
                                                () => {
                                                    previewObject = version;
                                                    openPreviewModal = true;
                                                }
                                            "
                                        />
                                        <UButton v-if="!version.isDeleteMarker" icon="i-lucide-download" color="neutral" variant="ghost" size="xs" title="Download this version" @click="downloadVersion(version.versionId!)" />
                                        <UButton icon="i-lucide-trash-2" color="error" variant="ghost" size="xs" title="Delete this version permanently" :loading="deletingVersionId === version.versionId" @click="deleteVersion(version.versionId!)" />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </template>
                </UCard>

                <UCard v-else-if="!loadingVersions && props.bucketName" variant="subtle">
                    <template #default>
                        <UEmpty title="Versions" description="No version history available. Enable versioning on this bucket to track changes." icon="i-lucide-file-stack" size="md" variant="naked" />
                    </template>
                </UCard>
            </div>
        </template>
    </USlideover>
</template>
