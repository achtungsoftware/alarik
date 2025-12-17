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

import type { UploadStatus } from "~/composables/useUploadQueue";

type ProgressColor = "error" | "neutral" | "primary" | "secondary" | "success" | "warning" | "info";

interface StatusConfig {
    icon: string;
    color: string;
    bgColor: string;
    progressColor: ProgressColor;
}

const { uploadQueue, isSlideoverOpen, isUploading, uploadingCount, pendingCount, completedCount, errorCount, cancelledCount, failedOrCancelledCount, totalCount, overallProgress, cancelItem, stopAll, retryItem, retryAllFailed, removeItem, clearCompleted, clearAll, closeSlideover } = useUploadQueue();

const MAX_VISIBLE_ITEMS = 50;

const STATUS_PRIORITY: Record<UploadStatus, number> = {
    uploading: 0,
    processing: 1,
    pending: 2,
    error: 3,
    cancelled: 4,
    completed: 5,
};

const sortedItems = computed(() => {
    return [...uploadQueue.value].sort((a, b) => STATUS_PRIORITY[a.status] - STATUS_PRIORITY[b.status]);
});

const displayedItems = computed(() => sortedItems.value.slice(0, MAX_VISIBLE_ITEMS));
const hasMoreItems = computed(() => uploadQueue.value.length > MAX_VISIBLE_ITEMS);

const STATUS_CONFIG: Record<UploadStatus, StatusConfig> = {
    pending: { icon: "i-lucide-clock", color: "text-muted", bgColor: "bg-muted/20", progressColor: "neutral" },
    uploading: { icon: "i-lucide-upload", color: "text-primary", bgColor: "bg-primary/20", progressColor: "primary" },
    processing: { icon: "i-lucide-loader-2", color: "text-info", bgColor: "bg-info/20", progressColor: "secondary" },
    completed: { icon: "i-lucide-circle-check", color: "text-success", bgColor: "bg-success/20", progressColor: "success" },
    error: { icon: "i-lucide-circle-x", color: "text-error", bgColor: "bg-error/20", progressColor: "error" },
    cancelled: { icon: "i-lucide-ban", color: "text-warning", bgColor: "bg-warning/20", progressColor: "warning" },
};

function canCancel(status: UploadStatus): boolean {
    return status === "uploading" || status === "pending";
}

function canRetry(status: UploadStatus): boolean {
    return status === "error" || status === "cancelled";
}

function canRemove(status: UploadStatus): boolean {
    return status === "completed" || status === "error" || status === "cancelled";
}
</script>

<template>
    <USlideover v-model:open="isSlideoverOpen" title="Uploads" :ui="{ content: 'w-full max-w-md' }">
        <template #body>
            <div class="flex flex-col h-full">
                <!-- Overall Progress -->
                <div v-if="isUploading" class="pb-4 border-b border-default">
                    <div class="flex items-center justify-between mb-2">
                        <span class="text-sm font-medium">Overall Progress</span>
                        <span class="text-sm text-muted">{{ overallProgress }}%</span>
                    </div>
                    <UProgress :model-value="overallProgress" color="primary" size="sm" />
                    <p class="text-xs text-muted mt-2">{{ uploadingCount }} uploading, {{ pendingCount }} waiting</p>
                </div>

                <!-- Summary Stats -->
                <div v-if="uploadQueue.length > 0" class="flex items-center gap-4 py-3 border-b border-default">
                    <div v-if="pendingCount > 0 || uploadingCount > 0" class="flex items-center gap-1.5">
                        <UIcon name="i-lucide-upload" class="w-4 h-4 text-primary" />
                        <span class="text-sm text-muted">{{ pendingCount + uploadingCount }} active</span>
                    </div>
                    <div class="flex items-center gap-1.5">
                        <UIcon name="i-lucide-circle-check" class="w-4 h-4 text-success" />
                        <span class="text-sm text-muted">{{ completedCount }} done</span>
                    </div>
                    <div v-if="errorCount > 0" class="flex items-center gap-1.5">
                        <UIcon name="i-lucide-circle-x" class="w-4 h-4 text-error" />
                        <span class="text-sm text-muted">{{ errorCount }} failed</span>
                    </div>
                    <div v-if="cancelledCount > 0" class="flex items-center gap-1.5">
                        <UIcon name="i-lucide-ban" class="w-4 h-4 text-warning" />
                        <span class="text-sm text-muted">{{ cancelledCount }} cancelled</span>
                    </div>
                </div>

                <!-- Action Buttons -->
                <div v-if="uploadQueue.length > 0" class="flex items-center gap-2 py-3 border-b border-default">
                    <UButton v-if="isUploading" size="xs" color="error" variant="soft" icon="i-lucide-square" @click="stopAll">Stop All</UButton>
                    <UButton v-if="failedOrCancelledCount > 0" size="xs" color="warning" variant="soft" icon="i-lucide-refresh-ccw" @click="retryAllFailed">Retry All</UButton>
                    <UButton v-if="completedCount > 0" size="xs" color="neutral" variant="soft" icon="i-lucide-check-check" @click="clearCompleted">Clear Done</UButton>
                    <UButton v-if="totalCount > 0 && !isUploading" size="xs" color="neutral" variant="ghost" icon="i-lucide-trash-2" @click="clearAll">Clear All</UButton>
                </div>

                <!-- Upload List -->
                <div class="flex-1 overflow-y-auto -mx-4 px-4">
                    <div v-if="uploadQueue.length === 0" class="flex items-center justify-center h-full">
                        <UEmpty title="No Uploads" description="Upload files to see them here." icon="i-lucide-upload" size="md" variant="naked" />
                    </div>

                    <div v-else class="divide-y divide-default">
                        <div v-for="item in displayedItems" :key="item.id" class="py-3 first:pt-4">
                            <div class="flex items-start gap-3">
                                <!-- Status Icon -->
                                <div :class="['flex items-center justify-center w-8 h-8 rounded-lg shrink-0', STATUS_CONFIG[item.status].bgColor]">
                                    <UIcon :name="STATUS_CONFIG[item.status].icon" :class="['w-4 h-4', STATUS_CONFIG[item.status].color, item.status === 'processing' ? 'animate-spin' : '']" />
                                </div>

                                <!-- File Info -->
                                <div class="flex-1 min-w-0">
                                    <div class="flex items-center gap-2">
                                        <p class="text-sm font-medium truncate flex-1" :title="item.file.name">
                                            {{ item.file.name }}
                                        </p>
                                        <span v-if="item.status === 'uploading'" class="text-xs text-primary shrink-0">{{ item.progress }}%</span>
                                        <span v-else-if="item.status === 'processing'" class="text-xs text-info shrink-0">Processing...</span>
                                    </div>

                                    <!-- Progress Bar -->
                                    <div v-if="item.status === 'uploading' || item.status === 'processing' || item.status === 'completed'" class="mt-1.5">
                                        <UProgress :model-value="item.progress" :color="STATUS_CONFIG[item.status].progressColor" size="xs" />
                                    </div>

                                    <p class="text-xs text-muted mt-1">
                                        {{ formatBytes(item.file.size) }}
                                    </p>
                                    <p v-if="item.error && item.status !== 'cancelled'" class="text-xs text-error mt-0.5">
                                        {{ item.error }}
                                    </p>
                                </div>

                                <!-- Actions -->
                                <div class="flex items-center gap-1 shrink-0">
                                    <UButton v-if="canCancel(item.status)" icon="i-lucide-x" color="neutral" variant="ghost" size="xs" title="Cancel upload" @click="cancelItem(item.id)" />
                                    <UButton v-if="canRetry(item.status)" icon="i-lucide-refresh-ccw" color="warning" variant="ghost" size="xs" title="Retry upload" @click="retryItem(item.id)" />
                                    <UButton v-if="canRemove(item.status)" icon="i-lucide-trash-2" color="neutral" variant="ghost" size="xs" title="Remove from list" @click="removeItem(item.id)" />
                                </div>
                            </div>
                        </div>

                        <!-- More items indicator -->
                        <div v-if="hasMoreItems" class="py-4 text-center">
                            <p class="text-sm text-muted">+ {{ totalCount - MAX_VISIBLE_ITEMS }} more files</p>
                        </div>
                    </div>
                </div>
            </div>
        </template>

        <template #footer>
            <div class="flex justify-end">
                <UButton color="neutral" variant="soft" @click="closeSlideover">Close</UButton>
            </div>
        </template>
    </USlideover>
</template>
