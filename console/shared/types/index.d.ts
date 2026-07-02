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
    export interface StorageStats {
        totalBytes: number;
        availableBytes: number;
        usedBytes: number;
        alarikUsedBytes: number;
        bucketCount: number;
        userCount: number;
    }

    export interface MetricsMinuteBucket {
        // ISO string or unix seconds, depending on the server's date encoder
        timestamp: string | number;
        bytesIn: number;
        bytesOut: number;
        requests: number;
        errors: number;
        cpuPercent?: number;
        memoryBytes?: number;
    }

    export interface SystemStats {
        metrics: {
            uptimeSeconds: number;
            totalBytesIn: number;
            totalBytesOut: number;
            totalRequests: number;
            totalErrors: number;
            processCPUPercent?: number;
            systemCPUPercent?: number;
            processMemoryBytes?: number;
            systemMemoryTotalBytes?: number;
            systemMemoryAvailableBytes?: number;
            loadAverage1?: number;
            loadAverage5?: number;
            loadAverage15?: number;
            coreCount: number;
            history: MetricsMinuteBucket[];
        };
        accessKeyCount: number;
        sharedLinkCount: number;
        oidcProviderCount: number;
        multipartUploadCount: number;
    }

    export interface NotificationRule {
        id: string;
        url: string;
        secret?: string;
        events: string[];
        prefix?: string;
        suffix?: string;
        enabled: boolean;
    }

    export interface Bucket {
        name: string;
        creationDate: string;
        versioningStatus: string;
        policy?: string;
        user?: User;
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

    export interface SharedLink {
        id: string;
        bucketName: string;
        key: string;
        url: string;
        expiresAt: string;
        createdAt: string;
    }

    export interface User {
        id: string;
        name: string;
        username: string;
        isAdmin: boolean;
    }

    export interface OIDCProvider {
        id: string;
        name: string;
        issuerURL: string;
        clientId: string;
        enabled: boolean;
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
