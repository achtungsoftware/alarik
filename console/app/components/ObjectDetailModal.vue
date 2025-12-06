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
    }>(),
    {
        open: false,
    }
);

const emit = defineEmits(["update:open", "close"]);
const open = ref(props.open);

watch(
    () => props.open,
    (val) => {
        open.value = val;
    }
);

watch(open, (val) => {
    emit("update:open", val);
});
</script>
<template>
    <USlideover v-model:open="open" :title="props.item?.key">
        <slot />

        <template #body>
            <div v-if="props.item">
                <UCard
                    variant="subtle"
                    :ui="{
                        body: '!p-0',
                    }"
                >
                    <template #default>
                        <div>
                            <NameValueLabel name="Key" :value="props.item.key" />
                            <NameValueLabel name="ETag" :value="props.item.etag" />
                            <NameValueLabel name="Content-Type" :value="props.item.contentType" />
                            <NameValueLabel name="Size" :value="formatBytes(props.item.size)" />
                            <NameValueLabel name="Last Modified" :value="new Date(props.item.lastModified).toLocaleString()" />
                        </div>
                    </template>
                </UCard>
            </div>
        </template>
    </USlideover>
</template>
