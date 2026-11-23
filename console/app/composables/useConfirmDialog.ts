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

import type { ButtonProps } from "@nuxt/ui";

import ConfirmDialogModal from "~/components/ConfirmDialogModal.vue";

export interface ConfirmDialogOptions {
    title?: string;
    message: string;
    confirmLabel?: string;
    confirmColor?: ButtonProps["color"];
}

export function useConfirmDialog() {
    const overlay = useOverlay();

    async function confirm(options: ConfirmDialogOptions): Promise<boolean> {
        const modal = overlay.create(ConfirmDialogModal, {
            props: {
                title: options.title,
                message: options.message,
                confirmLabel: options.confirmLabel,
                confirmColor: options.confirmColor,
            },
        });

        const result = await modal.open();
        return result === true;
    }

    return {
        confirm,
    };
}
