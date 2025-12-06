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
    title: `Log In - Alarik`,
});

const allowAccountCreation = useRuntimeConfig().public.ALLOW_ACCOUNT_CREATION;
const jwtCookie = useJWTCookie();
const loginError = ref("");
const isLoadingLogin = ref(false);
const loginState = reactive({
    username: "",
    password: "",
});

async function login(e: Event) {
    e.preventDefault();

    try {
        isLoadingLogin.value = true;
        loginError.value = "";

        const response = await $fetch<{ token: string }>(`${useRuntimeConfig().public.API_BASE_URL}/api/v1/users/login`, {
            method: "POST",
            body: JSON.stringify(loginState),
            headers: {
                "Content-Type": "application/json",
            },
        });

        if (response.token) {
            jwtCookie.value = response.token;
            window.location.reload();
        }
    } catch (error: any) {
        loginError.value = error.data?.reason ?? "Unknown error";
    } finally {
        isLoadingLogin.value = false;
    }
}
</script>

<template>
    <div class="sm:min-h-screen flex justify-center items-center">
        <div class="grid sm:grid-cols-2 sm:min-h-screen w-full">
                <div class="bg-black dark:bg-elevated/50 w-full h-full">
                    <div class="dark h-full w-full hidden sm:block relative overflow-hidden">
                        <div class="grid-background"></div>
                        <div class="flex h-full flex-col justify-between items-start p-8 relative z-10">
                            <Logo />
                            <div class="text-default font-medium text-lg">The open-source storage they didn't want you to have.</div>
                        </div>
                    </div>
                </div>

            <div class="h-full flex justify-center items-center">
                <div class="flex-1 p-6 sm:p-8 max-w-lg">
                    <Logo class="sm:hidden block pb-6" />
                    <h1 class="pb-2 text-2xl font-medium">Sign In to Alarik</h1>
                    <p class="pb-4 text-sm text-muted">Alarik is an open source, high performance S3 compatible storage solution.</p>
                    <UForm :state="loginState" @submit="login">
                        <UAlert v-if="loginError != ''" title="Error" :description="loginError" color="error" class="mb-4" />

                        <UFormField required label="Username">
                            <UInput placeholder="Username" v-model="loginState.username" class="w-full mb-4" size="xl" variant="subtle" />
                        </UFormField>
                        <UFormField required label="Password">
                            <UInput placeholder="Password" type="password" v-model="loginState.password" class="w-full" size="xl" variant="subtle" />
                        </UFormField>
                        <div class="mt-6 flex flex-col gap-3">
                            <UButton :loading="isLoadingLogin" label="Log In" type="submit" block size="xl" />
                            <USeparator v-if="allowAccountCreation" label="or" />
                            <UButton v-if="allowAccountCreation" to="/createAccount" label="Create Account" block size="xl" color="neutral" variant="subtle" />
                        </div>
                    </UForm>
                    <div class="flex flex-row gap-4 items-center justify-center pt-4">
                        <UButton icon="grommet-icons:github" to="https://github.com" target="_blank" color="primary" variant="ghost" />
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>
