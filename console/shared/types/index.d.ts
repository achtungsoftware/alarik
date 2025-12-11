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

declare global {
    export interface Bucket {
        name: string;
        creationDate: string;
        versioningStatus: string;
    }

    // ObjectMeta.ResponseDTO
    export interface BrowserItem {
        key: string;
        size: number;
        contentType: string;
        etag: string;
        lastModified: string;
        isFolder: boolean;
        isBucket?: boolean;
        versionId?: string;
        isLatest?: boolean;
        isDeleteMarker?: boolean;
    }

    export interface AccessKey {
        id: string;
        accessKey: string;
        createdAt: string;
        expirationDate?: string;
    }

    export interface User {
        id: string;
        name: string;
        username: string;
        isAdmin: boolean;
    }

    // Fluent Page
    export interface Page<T> {
        items: T[];
        metadata: {
            page: number;
            per: number;
            total: number;
        };
    }
}
