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
        objectKey: string;
        contentType?: string;
    }>(),
    {
        open: false,
    }
);

const emit = defineEmits(["update:open"]);
const open = ref(props.open);
const jwtCookie = useJWTCookie();
const loading = ref(false);
const error = ref<string | null>(null);
const previewUrl = ref<string | null>(null);
const textContent = ref<string | null>(null);

const fileName = computed(() => props.objectKey.split("/").pop() || props.objectKey);

const previewType = computed(() => {
    const type = props.contentType?.toLowerCase() || "";
    const ext = fileName.value.split(".").pop()?.toLowerCase() || "";

    // Images
    if (type.startsWith("image/") || ["jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "ico"].includes(ext)) {
        return "image";
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

    // Text files
    if (type.startsWith("text/") || type === "application/json" || type === "application/xml" || type === "application/javascript" || ["txt", "md", "json", "xml", "html", "css", "js", "ts", "vue", "jsx", "tsx", "yaml", "yml", "toml", "ini", "cfg", "conf", "sh", "bash", "py", "rb", "go", "rs", "java", "kt", "c", "cpp", "h", "hpp", "cs", "sql", "log", "env"].includes(ext)) {
        return "text";
    }

    return "unsupported";
});

const canPreview = computed(() => previewType.value !== "unsupported");

watch(
    () => props.open,
    (val) => {
        open.value = val;
        if (val && props.bucket && props.objectKey) {
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
    error.value = null;
}

async function loadPreview() {
    if (!props.bucket || !props.objectKey) return;

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
                keys: [props.objectKey],
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
</script>

<template>
    <UModal v-model:open="open" :title="fileName" :ui="{ content: 'max-w-5xl' }">
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
                <UEmpty title="Preview not available" :description="`This file type cannot be previewed. Content-Type: ${contentType || 'unknown'}`" icon="i-lucide-file-question" size="lg" variant="naked" />
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

            <!-- Text Preview -->
            <div v-else-if="previewType === 'text' && textContent !== null" class="bg-elevated/50 rounded-lg max-h-[70vh] overflow-y-auto overflow-x-hidden">
                <pre class="p-4 text-sm font-mono whitespace-pre-wrap break-words leading-relaxed">{{ textContent }}</pre>
            </div>
        </template>
    </UModal>
</template>
