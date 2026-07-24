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

export default defineNuxtConfig({
    compatibilityDate: "2025-07-15",
    devtools: { enabled: false },
    modules: ["@nuxt/ui", "nuxt-charts"],
    css: ["~/assets/css/main.css"],
    nitro: {
        // Suppresses the post-build `.output` file tree listing, whose `generateFSTree` leaks
        // badly enough to exhaust Node's default ~2 GB heap right after the bundle is finished
        // (nuxt/nuxt#27141 - closed as upstream, still unfixed). Cosmetic output only; the build
        // itself needs a fraction of the memory the listing does.
        logLevel: 1,
    },
    routeRules: {
        // renders only on client-side
        "/**": { ssr: false, prerender: false },
        "/console": {
            redirect: {
                to: "/console/objectBrowser",
                statusCode: 301,
            },
        },
    },
    runtimeConfig: {
        public: {
            appVersion: "1.0.0-beta-9", // Updated by publish.sh
            apiBaseUrl: "http://localhost:8080",
            consoleBaseUrl: "http://localhost:3000",
            allowAccountCreation: false,
        },
    },
});
