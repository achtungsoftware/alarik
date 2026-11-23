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

/// The one `$fetch` for the console API. Every request gets the base URL and the session
/// token here, and an expired or revoked session is handled in one place: the token is dropped
/// and the user lands on the login page, instead of each page meeting the same 401 on its own.
/// Requests from the login and sign-up pages are left alone - a 401 there is the answer, not a
/// lost session.
export function useApi() {
    const config = useRuntimeConfig();
    const jwtCookie = useJWTCookie();
    return $fetch.create({
        baseURL: `${config.public.apiBaseUrl}/api/v1`,
        onRequest({ options }) {
            // `useFetch` hands its options through with `baseURL` present but undefined, which
            // ofetch takes as "no base" rather than falling back to the instance default.
            if (!options.baseURL) {
                options.baseURL = `${config.public.apiBaseUrl}/api/v1`;
            }
            if (jwtCookie.value) {
                options.headers.set("Authorization", `Bearer ${jwtCookie.value}`);
            }
        },
        onResponseError({ response }) {
            if (response.status !== 401) return;
            const path = useRoute().path;
            if (!path.startsWith("/console")) return;
            jwtCookie.value = null;
            navigateTo("/");
        },
    });
}
