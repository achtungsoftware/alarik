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

import type { DropdownMenuItem, NavigationMenuItem } from "@nuxt/ui";

const route = useRoute();
const openSidebar = ref(false);
const user = useUser();
const jwtCookie = useJWTCookie();
const colorMode = useColorMode();

const items = computed<NavigationMenuItem[][]>(() => {
    return [
        [
            {
                label: "User",
                type: "label",
            },
            {
                label: "Object Browser",
                icon: "i-lucide-folder-tree",
                to: "/console/objectBrowser",
                active: route.path.startsWith("/console/objectBrowser"),
            },
            {
                label: "Access Keys",
                icon: "i-lucide-key-round",
                to: "/console/accessKeys",
                active: route.path.startsWith("/console/accessKeys"),
            },
            {
                label: "Administrator",
                type: "label",
            },
            {
                label: "Buckets",
                icon: "i-lucide-cylinder",
                to: "/console/buckets",
                active: route.path.startsWith("/console/buckets"),
            },
            {
                label: "Policies",
                icon: "i-lucide-shield-user",
                to: "/console",
                active: route.path.startsWith("/console/policies"),
            },
        ],
        [
            {
                label: "Configuration",
                type: "label",
            },

            {
                label: "Settings",
                icon: "i-lucide-cog",
                to: "/console",
                active: route.path.startsWith("/console/settings"),
            },
        ],
    ];
});

const userMenuItems = computed<DropdownMenuItem[][]>(() => [
    [
        {
            label: "Appearance",
            icon: "i-lucide-sun-moon",
            children: [
                {
                    label: "Light",
                    icon: "i-lucide-sun",
                    type: "checkbox",
                    checked: colorMode.preference === "light",
                    onSelect(e: Event) {
                        e.preventDefault();
                        colorMode.preference = "light";
                    },
                },
                {
                    label: "Dark",
                    icon: "i-lucide-moon",
                    type: "checkbox",
                    checked: colorMode.preference === "dark",
                    onUpdateChecked(checked: boolean) {
                        if (checked) {
                            colorMode.preference = "dark";
                        }
                    },
                    onSelect(e: Event) {
                        e.preventDefault();
                    },
                },
                {
                    label: "System",
                    icon: "i-lucide-laptop-minimal",
                    type: "checkbox",
                    checked: colorMode.preference === "system",
                    onUpdateChecked(checked: boolean) {
                        if (checked) {
                            colorMode.preference = "system";
                        }
                    },
                    onSelect(e: Event) {
                        e.preventDefault();
                    },
                },
            ],
        },
        {
            type: "separator",
        },
        {
            label: "Log Out",
            icon: "i-lucide-log-out",
            color: "error",
            onClick: async () => {
                jwtCookie.value = "";
                window.location.reload();
            },
        },
    ],
]);
</script>
<template>
    <UDashboardGroup unit="rem">
        <UDashboardSidebar v-model:open="openSidebar" :resizable="false" :ui="{ footer: 'border-t border-default' }">
            <template #header>
                <Logo size="sm" />
            </template>

            <template #default>
                <UNavigationMenu :items="items[0]" orientation="vertical" />
                <UNavigationMenu :items="items[1]" orientation="vertical" class="mt-auto" />
            </template>

            <template #footer>
                <UDropdownMenu :items="userMenuItems" :content="{ align: 'center', collisionPadding: 12 }" :ui="{ content: 'w-(--reka-dropdown-menu-trigger-width)' }">
                    <UButton :label="user.name" color="neutral" variant="ghost" block :trailing-icon="'i-lucide-chevrons-up-down'" />
                </UDropdownMenu>
            </template>
        </UDashboardSidebar>

        <slot />
    </UDashboardGroup>
</template>
