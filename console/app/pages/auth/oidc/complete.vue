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

useHead({
    title: `Signing In - Alarik`,
});

const jwtCookie = useJWTCookie();

onMounted(() => {
    // The backend hands the token/error back in the URL fragment rather than a query param -
    // fragments aren't sent to servers or logged by reverse proxies, unlike query strings.
    const params = new URLSearchParams(window.location.hash.replace(/^#/, ""));
    const token = params.get("token");
    const error = params.get("error");

    if (token) {
        jwtCookie.value = token;
        window.location.href = "/console/objectBrowser";
    } else {
        window.location.href = `/?oidcError=${encodeURIComponent(error ?? "unknown_error")}`;
    }
});
</script>

<template>
    <div class="min-h-screen flex justify-center items-center">
        <div class="flex flex-col items-center gap-3">
            <UIcon name="i-lucide-loader-2" class="w-8 h-8 animate-spin text-muted" />
            <p class="text-sm text-muted">Signing you in...</p>
        </div>
    </div>
</template>
