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
        title: string;
        icon: string;
        subTitle?: string;
        value: string | number;
    }>(),
    {}
);
</script>

<template>
    <UCard variant="subtle">
        <div class="flex flex-col gap-2">
            <div class="flex items-center gap-2 text-secondary text-sm">
                <UIcon :name="props.icon" class="w-4 h-4" />
                {{ props.title }}
            </div>
            <div class="text-2xl font-semibold">
                {{ props.value }}
            </div>
            <div v-if="props.subTitle" class="text-xs text-muted">{{ props.subTitle }}</div>
        </div>
    </UCard>
</template>
