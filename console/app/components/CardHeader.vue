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

import type { BreadcrumbItem } from "@nuxt/ui";

const props = withDefaults(
    defineProps<{
        title: String;
        badge?: String;
        breadCrumbItems?: BreadcrumbItem[];
        size?: "sm" | "md" | "lg";
        color?: "default" | "error";
    }>(),
    {
        size: "md",
        color: "default"
    }
);

const slots = useSlots();
</script>

<template>
    <div class="flex flex-col md:flex-row gap-4 md:items-center justify-between">
        <div class="flex flex-col gap-0.5">
            <h2
                :class="{
                    'font-medium flex flex-row items-center gap-3': true,
                    'text-sm': props.size == 'sm',
                    'text-lg': props.size == 'md',
                    'text-xl': props.size == 'lg',
                    'text-default': props.color == 'default',
                    'text-error': props.color == 'error',
                }"
            >
                {{ props.title }}<UBadge v-if="props.badge" size="sm" color="neutral" variant="subtle">{{ props.badge }}</UBadge>
            </h2>
            <UBreadcrumb v-if="props.breadCrumbItems" :items="props.breadCrumbItems" />
        </div>
        <div v-if="slots.rightContent" class="flex flex-row gap-4 items-center">
            <slot name="rightContent" />
        </div>
    </div>
</template>
