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

const emit = defineEmits(["update:open", "close", "versionDeleted", "saved"]);
const open = ref(props.open);
const jwtCookie = useJWTCookie();
const toast = useToast();
const versions = ref<BrowserItem[]>([]);
const loadingVersions = ref(false);
const deletingVersionId = ref<string | null>(null);
const openPreviewModal = ref(false);
const previewObject = ref<BrowserItem | null>(null);
const tagRows = ref<{ key: string; value: string }[]>([]);
const loadingTags = ref(false);
const savingTags = ref(false);

const contentTypeInput = ref("");
const metadataRows = ref<{ key: string; value: string }[]>([]);
const loadingMetadata = ref(false);
const savingMetadata = ref(false);

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val && props.item && props.bucketName) {
            fetchVersions();
            fetchTags();
            fetchMetadata();
        }
    }
);

watch(open, (val) => {
    emit("update:open", val);
    if (!val) {
        versions.value = [];
        tagRows.value = [];
        metadataRows.value = [];
    }
});

async function fetchMetadata() {
    if (!props.item || !props.bucketName) return;

    loadingMetadata.value = true;
    try {
        const response = await $fetch<{ contentType: string; metadata: Record<string, string> }>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/metadata`, {
            headers: { Authorization: `Bearer ${jwtCookie.value}` },
            params: {
                bucket: props.bucketName,
                key: props.item.key,
            },
        });
        contentTypeInput.value = response.contentType;
        metadataRows.value = Object.entries(response.metadata).map(([key, value]) => ({ key, value }));
    } catch (error) {
        console.error("Failed to fetch metadata:", error);
        contentTypeInput.value = props.item.contentType;
        metadataRows.value = [];
    } finally {
        loadingMetadata.value = false;
    }
}

function addMetadataRow() {
    metadataRows.value.push({ key: "", value: "" });
}

function removeMetadataRow(index: number) {
    metadataRows.value.splice(index, 1);
}

async function saveMetadata() {
    if (!props.item || !props.bucketName) return;
    if (contentTypeInput.value.trim() === "") {
        toast.add({
            title: "Save Failed",
            description: "Content-Type cannot be empty.",
            icon: "i-lucide-circle-x",
            color: "error",
        });
        return;
    }

    savingMetadata.value = true;
    try {
        const metadata: Record<string, string> = {};
        for (const row of metadataRows.value) {
            if (row.key.trim() === "") continue;
            metadata[row.key.trim()] = row.value;
        }

        await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/metadata`, {
            method: "PUT",
            body: JSON.stringify({ contentType: contentTypeInput.value.trim(), metadata }),
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${jwtCookie.value}`,
            },
            params: {
                bucket: props.bucketName,
                key: props.item.key,
            },
        });

        toast.add({
            title: "Metadata saved",
            description: `Content-Type and metadata for "${props.item.key}" were updated.`,
            icon: "i-lucide-circle-check",
            color: "success",
        });

        emit("saved");
    } catch (error: any) {
        toast.add({
            title: "Save Failed",
            description: error.data?.reason ?? "Failed to save metadata",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    } finally {
        savingMetadata.value = false;
    }
}

async function fetchTags() {
    if (!props.item || !props.bucketName) return;

    loadingTags.value = true;
    try {
        const response = await $fetch<{ tags: Record<string, string> }>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/tags`, {
            headers: { Authorization: `Bearer ${jwtCookie.value}` },
            params: {
                bucket: props.bucketName,
                key: props.item.key,
            },
        });
        tagRows.value = Object.entries(response.tags).map(([key, value]) => ({ key, value }));
    } catch (error) {
        console.error("Failed to fetch tags:", error);
        tagRows.value = [];
    } finally {
        loadingTags.value = false;
    }
}

function addTagRow() {
    tagRows.value.push({ key: "", value: "" });
}

function removeTagRow(index: number) {
    tagRows.value.splice(index, 1);
}

async function saveTags() {
    if (!props.item || !props.bucketName) return;

    savingTags.value = true;
    try {
        const tags: Record<string, string> = {};
        for (const row of tagRows.value) {
            if (row.key.trim() === "") continue;
            tags[row.key.trim()] = row.value;
        }

        await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/tags`, {
            method: "PUT",
            body: JSON.stringify({ tags }),
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${jwtCookie.value}`,
            },
            params: {
                bucket: props.bucketName,
                key: props.item.key,
            },
        });

        toast.add({
            title: "Tags saved",
            description: `The tags for "${props.item.key}" were updated.`,
            icon: "i-lucide-circle-check",
            color: "success",
        });
    } catch (error: any) {
        toast.add({
            title: "Save Failed",
            description: error.data?.reason ?? "Failed to save tags",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    } finally {
        savingTags.value = false;
    }
}

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
    
    <USlideover inset v-model:open="open" :title="props.item?.key">
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
                            <NameValueLabel name="Size" :value="formatBytes(props.item.size)" />
                            <NameValueLabel name="Last Modified" :value="new Date(props.item.lastModified).toLocaleString()" />
                            <NameValueLabel name="Version Id" v-if="props.item.versionId" :value="props.item.versionId" />
                            <NameValueLabel name="Is Latest" v-if="props.item.isLatest !== undefined" :value="props.item.isLatest ? 'Yes' : 'No'" />
                            <NameValueLabel name="Is Delete Marker" v-if="props.item.isDeleteMarker" :value="'Yes'" />
                        </div>
                    </template>
                </UCard>

                <UCard variant="subtle">
                    <template #header>
                        <CardHeader title="Content-Type & Metadata" size="sm" />
                    </template>
                    <template #default>
                        <div v-if="loadingMetadata" class="flex items-center justify-center p-4">
                            <LoadingIndicator />
                        </div>
                        <div v-else class="space-y-3">
                            <UInput v-model="contentTypeInput" placeholder="Content-Type" variant="subtle" size="sm" class="w-full" />

                            <div class="space-y-2">
                                <div v-for="(row, index) in metadataRows" :key="index" class="flex gap-2">
                                    <UInput v-model="row.key" placeholder="x-amz-meta-key" variant="subtle" size="sm" class="flex-1" />
                                    <UInput v-model="row.value" placeholder="Value" variant="subtle" size="sm" class="flex-1" />
                                    <UButton icon="i-lucide-x" color="neutral" variant="subtle" size="sm" aria-label="Remove metadata entry" @click="removeMetadataRow(index)" />
                                </div>

                                <UButton label="Add Metadata" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" @click="addMetadataRow" />
                            </div>

                            <div class="flex justify-end">
                                <UButton label="Save" :loading="savingMetadata" color="primary" size="sm" @click="saveMetadata" />
                            </div>
                        </div>
                    </template>
                </UCard>

                <UCard variant="subtle">
                    <template #header>
                        <CardHeader title="Tags" size="sm" :badge="tagRows.length > 0 ? tagRows.length + '' : undefined" />
                    </template>
                    <template #default>
                        <div v-if="loadingTags" class="flex items-center justify-center p-4">
                            <LoadingIndicator />
                        </div>
                        <div v-else class="space-y-2">
                            <div v-for="(row, index) in tagRows" :key="index" class="flex gap-2">
                                <UInput v-model="row.key" placeholder="Key" variant="subtle" size="sm" class="flex-1" />
                                <UInput v-model="row.value" placeholder="Value" variant="subtle" size="sm" class="flex-1" />
                                <UButton icon="i-lucide-x" color="neutral" variant="subtle" size="sm" aria-label="Remove tag" @click="removeTagRow(index)" />
                            </div>

                            <div class="flex items-center justify-between">
                                <UButton label="Add Tag" icon="i-lucide-plus" variant="subtle" color="neutral" size="sm" @click="addTagRow" />
                                <UButton label="Save Tags" :loading="savingTags" color="primary" size="sm" @click="saveTags" />
                            </div>
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
