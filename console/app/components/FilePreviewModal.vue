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

const props = withDefaults(
    defineProps<{
        open: boolean;
        bucket: string;
        item: BrowserItem | null;
    }>(),
    {
        open: false,
    }
);

const emit = defineEmits(["update:open", "saved"]);
const open = ref(props.open);
const jwtCookie = useJWTCookie();
const toast = useToast();
const loading = ref(false);
const saving = ref(false);
const error = ref<string | null>(null);
const previewUrl = ref<string | null>(null);
const textContent = ref<string | null>(null);
const editedContent = ref<string | null>(null);
const isEditing = ref(false);

const fileName = computed(() => props.item?.key.split("/").pop() || props.item?.key || "");

const previewType = computed(() => {
    const type = props.item?.contentType?.toLowerCase() || "";
    const ext = fileName.value.split(".").pop()?.toLowerCase() || "";

    // Images
    if (type.startsWith("image/") || ["jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "ico"].includes(ext)) {
        return "image";
    }

    // Text files
    if (type.startsWith("text/") || type === "application/json" || type === "application/xml" || type === "application/javascript" || ["txt", "md", "json", "xml", "html", "css", "js", "ts", "vue", "jsx", "tsx", "yaml", "yml", "toml", "ini", "cfg", "conf", "sh", "bash", "py", "rb", "go", "rs", "java", "kt", "c", "cpp", "h", "hpp", "cs", "sql", "log", "env", "swift"].includes(ext)) {
        return "text";
    }

    // Videos
    if (type.startsWith("video/") || ["mp4", "webm", "ogg", "mov"].includes(ext)) {
        return "video";
    }

    // Audio
    if (type.startsWith("audio/") || ["mp3", "wav", "ogg", "m4a", "flac"].includes(ext)) {
        return "audio";
    }

    // PDF
    if (type === "application/pdf" || ext === "pdf") {
        return "pdf";
    }

    return "unsupported";
});

const canPreview = computed(() => previewType.value !== "unsupported");
const canEdit = computed(() => previewType.value === "text" && (props.item?.isLatest !== false));
const hasChanges = computed(() => isEditing.value && editedContent.value !== textContent.value);

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val && props.bucket && props.item?.key) {
            loadPreview();
        }
    }
);

watch(open, (val) => {
    emit("update:open", val);
    if (!val) {
        cleanup();
    }
});

function cleanup() {
    if (previewUrl.value) {
        URL.revokeObjectURL(previewUrl.value);
        previewUrl.value = null;
    }
    textContent.value = null;
    editedContent.value = null;
    isEditing.value = false;
    error.value = null;
}

function startEditing() {
    editedContent.value = textContent.value;
    isEditing.value = true;
}

function cancelEditing() {
    editedContent.value = null;
    isEditing.value = false;
}

async function saveChanges() {
    if (!props.bucket || !props.item?.key || editedContent.value === null) return;

    saving.value = true;
    try {
        // Get the prefix (everything except the filename)
        const lastSlashIndex = props.item.key.lastIndexOf("/");
        const prefix = lastSlashIndex >= 0 ? props.item.key.substring(0, lastSlashIndex + 1) : "";

        // Create a File from the edited content
        const contentType = props.item.contentType || "text/plain";
        const blob = new Blob([editedContent.value], { type: contentType });
        const file = new File([blob], fileName.value, { type: contentType });

        const formData = new FormData();
        formData.append("data", file, file.name);

        await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects`, {
            method: "POST",
            headers: { Authorization: `Bearer ${jwtCookie.value}` },
            params: {
                bucket: props.bucket,
                ...(prefix.length > 0 && { prefix }),
            },
            body: formData,
        });

        // Update the original content to match saved content
        textContent.value = editedContent.value;
        isEditing.value = false;
        editedContent.value = null;

        toast.add({
            title: "File Saved",
            description: "Changes have been saved successfully",
            icon: "i-lucide-circle-check",
            color: "success",
        });

        emit("saved");
    } catch (err: any) {
        toast.add({
            title: "Save Failed",
            description: err.message || "Failed to save changes",
            icon: "i-lucide-circle-x",
            color: "error",
        });
    } finally {
        saving.value = false;
    }
}

async function loadPreview() {
    if (!props.bucket || !props.item?.key) return;

    cleanup();
    loading.value = true;
    error.value = null;

    try {
        const response = await fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/objects/download`, {
            method: "POST",
            headers: {
                Authorization: `Bearer ${jwtCookie.value}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                bucket: props.bucket,
                keys: [props.item.key],
                ...(props.item.versionId && { versionId: props.item.versionId }),
            }),
        });

        if (!response.ok) {
            throw new Error(`Failed to load file: ${response.statusText}`);
        }

        const blob = await response.blob();

        if (previewType.value === "text") {
            textContent.value = await blob.text();
        } else {
            previewUrl.value = URL.createObjectURL(blob);
        }
    } catch (err: any) {
        error.value = err.message || "Failed to load preview";
    } finally {
        loading.value = false;
    }
}

onUnmounted(() => {
    cleanup();
});

const title = computed(() => {
    if (props.item && props.item.versionId) {
        return `${fileName.value} (${props.item.versionId.substring(0, 12)}...)`
    }
    return fileName.value
});
</script>

<template>
    <UModal v-model:open="open" :title="title" :ui="{ content: 'max-w-5xl', footer: 'justify-end' }">
        <template #body>
            <!-- Loading State -->
            <div v-if="loading" class="flex items-center justify-center bg-elevated/50 rounded-lg min-h-64">
                <div class="flex flex-col items-center gap-4 p-8">
                    <LoadingIndicator />
                    <span class="text-muted">Loading preview...</span>
                </div>
            </div>

            <!-- Error State -->
            <div v-else-if="error" class="flex items-center justify-center bg-elevated/50 rounded-lg min-h-64">
                <UEmpty title="Failed to load preview" :description="error" icon="i-lucide-alert-circle" size="lg" variant="naked">
                    <template #actions>
                        <UButton label="Retry" variant="subtle" color="neutral" @click="loadPreview" />
                    </template>
                </UEmpty>
            </div>

            <!-- Unsupported Type -->
            <div v-else-if="!canPreview" class="flex items-center justify-center bg-elevated/50 rounded-lg min-h-64">
                <UEmpty title="Preview not available" :description="`This file type cannot be previewed. Content-Type: ${item?.contentType || 'unknown'}`" icon="i-lucide-file-question" size="lg" variant="naked" />
            </div>

            <!-- Image Preview -->
            <div v-else-if="previewType === 'image' && previewUrl" class="flex items-center justify-center bg-elevated/50 rounded-lg">
                <img :src="previewUrl" :alt="fileName" class="max-w-full max-h-[70vh] object-contain" />
            </div>

            <!-- Video Preview -->
            <div v-else-if="previewType === 'video' && previewUrl" class="flex items-center justify-center bg-elevated/50 rounded-lg">
                <video :src="previewUrl" controls class="max-w-full max-h-[70vh]">Your browser does not support video playback.</video>
            </div>

            <!-- Audio Preview -->
            <div v-else-if="previewType === 'audio' && previewUrl" class="flex flex-col items-center gap-4 p-8 bg-elevated/50 rounded-lg">
                <UIcon name="i-lucide-music" class="w-16 h-16 text-muted" />
                <p class="font-medium">{{ fileName }}</p>
                <audio :src="previewUrl" controls class="w-full max-w-md">Your browser does not support audio playback.</audio>
            </div>

            <!-- PDF Preview -->
            <div v-else-if="previewType === 'pdf' && previewUrl" class="bg-elevated/50 rounded-lg overflow-hidden">
                <iframe :src="previewUrl" class="w-full h-[70vh] border-0" :title="fileName" />
            </div>

            <!-- Text Preview / Edit -->
            <div v-else-if="previewType === 'text' && textContent !== null" class="bg-elevated/50 rounded-lg max-h-[70vh] overflow-y-auto overflow-x-hidden">
                <textarea
                    v-if="isEditing"
                    v-model="editedContent"
                    class="w-full h-[60vh] p-4 text-sm font-mono bg-transparent border-0 leading-relaxed resize-none focus:outline-none focus:ring-0"
                    spellcheck="false"
                />
                <pre v-else class="p-4 text-sm font-mono whitespace-pre-wrap break-words leading-relaxed">{{ textContent }}</pre>
            </div>
        </template>

        <template v-if="canEdit && textContent !== null" #footer>
            <div class="flex justify-end gap-2">
                <template v-if="isEditing">
                    <UButton label="Cancel" variant="ghost" color="neutral" :disabled="saving" @click="cancelEditing" />
                    <UButton label="Save" icon="i-lucide-save" :loading="saving" :disabled="!hasChanges" @click="saveChanges" />
                </template>
                <UButton v-else label="Edit File" icon="i-lucide-square-pen" color="neutral" variant="subtle" @click="startEditing" />
            </div>
        </template>
    </UModal>
</template>
