<script setup lang="ts">
const props = defineProps({
    isShowing: {
        type: Boolean,
        default: false,
    },
    onConfirm: {
        type: Function,
        default: () => {},
    },
    title: {
        type: String,
        default: "Are you sure?"
    },
    message: {
        type: String,
        required: true,
    },
    confirmLabel: {
        type: String,
        default: "Continue",
    },
});

const emit = defineEmits(["update:isShowing"]);
const open = ref(props.isShowing);

function hideModal() {
    emit("update:isShowing", false);
}

watch(
    () => props.isShowing,
    (val) => {
        open.value = val;
    }
);
</script>
<template>
    <UModal v-model:open="open" :title="props.title" :ui="{ footer: 'justify-end' }">
        <template #body>
            <p>{{ props.message }}</p>
        </template>

        <template #footer>
            <UButton label="Cancel" color="neutral" variant="subtle" @click="hideModal" />
            <UButton
                @click="
                    () => {
                        props.onConfirm();
                        hideModal();
                    }
                "
                :label="props.confirmLabel"
                color="error"
            />
        </template>
    </UModal>
</template>
