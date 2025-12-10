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

export default defineNuxtRouteMiddleware(async (to, from) => {
    const jwtCookie = useJWTCookie();
    const user = useUser();

    if (jwtCookie.value != null) {
        if (jwtCookie.value !== "" && !user.value.isLoggedIn) {
            await $fetch(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/users/auth`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${jwtCookie.value}`,
                },
            })
                .then((res) => {
                    user.value = res;
                    user.value.isLoggedIn = true;
                })
                .catch((e) => {
                    jwtCookie.value = "";
                });
        }
    }

    // Check if to.name exists before calling startsWith
    if (to.name && to.name == "index" && user.value.isLoggedIn) {
        return navigateTo("/console/objectBrowser");
    }

    if (to.name && to.name.startsWith("console") && !user.value.isLoggedIn) {
        return navigateTo("/");
    }
});
