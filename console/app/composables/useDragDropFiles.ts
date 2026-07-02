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

/// Walks a dropped `DataTransfer` (files and/or folders) into a flat `File[]`, recursing into
/// directories via the WebKit File System Entry API. Files that came from inside a dropped
/// folder get a `webkitRelativePath` stamped on them (folder + subpath), matching what
/// `<input webkitdirectory>` sets natively, so the result can be handed directly to
/// `useUploadQueue().addFolderToQueue` without that composable needing to know files can also
/// arrive via drag-and-drop.
export function useDragDropFiles() {
    async function readFileEntry(entry: FileSystemFileEntry): Promise<File> {
        return new Promise((resolve, reject) => entry.file(resolve, reject));
    }

    async function readDirectoryEntries(reader: FileSystemDirectoryReader): Promise<FileSystemEntry[]> {
        // readEntries() does not guarantee returning every entry in one call - per spec it must
        // be called repeatedly until it resolves with an empty array.
        const all: FileSystemEntry[] = [];
        for (;;) {
            const batch = await new Promise<FileSystemEntry[]>((resolve, reject) => reader.readEntries(resolve, reject));
            if (batch.length === 0) break;
            all.push(...batch);
        }
        return all;
    }

    async function walk(entry: FileSystemEntry, path: string, out: File[]): Promise<void> {
        if (entry.isFile) {
            const file = await readFileEntry(entry as FileSystemFileEntry);
            const relativePath = path + file.name;
            Object.defineProperty(file, "webkitRelativePath", {
                value: relativePath,
                writable: false,
                configurable: true,
            });
            out.push(file);
        } else if (entry.isDirectory) {
            const reader = (entry as FileSystemDirectoryEntry).createReader();
            const children = await readDirectoryEntries(reader);
            for (const child of children) {
                await walk(child, `${path}${entry.name}/`, out);
            }
        }
    }

    /// Resolves a drop event's DataTransfer into files, plus whether any folder was involved
    /// (callers use this to decide whether to preserve folder structure on upload).
    async function filesFromDataTransfer(dataTransfer: DataTransfer): Promise<{ files: File[]; hasFolders: boolean }> {
        const items = Array.from(dataTransfer.items ?? []);
        const entries = items.map((item) => item.webkitGetAsEntry?.()).filter((e): e is FileSystemEntry => !!e);

        // Fallback for browsers without webkitGetAsEntry - plain flat file list, no folders
        if (entries.length === 0) {
            return { files: Array.from(dataTransfer.files ?? []), hasFolders: false };
        }

        const out: File[] = [];
        let hasFolders = false;
        for (const entry of entries) {
            if (entry.isDirectory) hasFolders = true;
            await walk(entry, "", out);
        }
        return { files: out, hasFolders };
    }

    return { filesFromDataTransfer };
}
