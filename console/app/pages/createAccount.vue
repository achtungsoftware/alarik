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
    title: `Create Account - Alarik`,
});

const router = useRouter();
const jwtCookie = useJWTCookie();
const createAccountError = ref("");
const isLoadingCreateAccount = ref(false);
const createAccountState = reactive({
    name: "",
    username: "",
    password: "",
});

async function createAccount(e: Event) {
    e.preventDefault();

    try {
        isLoadingCreateAccount.value = true;
        createAccountError.value = "";

        const response = await $fetch<{ token: string }>(`${useRuntimeConfig().public.apiBaseUrl}/api/v1/users`, {
            method: "POST",
            body: JSON.stringify(createAccountState),
            headers: {
                "Content-Type": "application/json",
            },
        });

        router.push("/");
    } catch (error: any) {
        createAccountError.value = error.data?.reason ?? "Unknown error";
    } finally {
        isLoadingCreateAccount.value = false;
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
                        <div class="text-default font-medium text-lg">Because your data shouldn’t depend on someone’s business model.</div>
                    </div>
                </div>
            </div>

            <div class="h-full flex justify-center items-center">
                <div class="flex-1 p-6 sm:p-8 max-w-lg">
                    <Logo class="sm:hidden block pb-6" />
                    <h1 class="pb-4 text-2xl font-medium">Create Account</h1>
                    <UForm :state="createAccountState" @submit="createAccount">
                        <UAlert v-if="createAccountError != ''" title="Error" :description="createAccountError" color="error" class="mb-4" />
                        <UAlert title="Warning" description="For safety reasons, public account creation should be disabled in production." color="warning" class="mb-4" variant="subtle" />

                        <UFormField required label="Your Name">
                            <UInput placeholder="Name" v-model="createAccountState.name" class="w-full mb-4" size="xl" variant="subtle" />
                        </UFormField>

                        <UFormField required label="Username">
                            <UInput placeholder="Username" v-model="createAccountState.username" class="w-full mb-4" size="xl" variant="subtle" />
                        </UFormField>
                        <UFormField required label="Password">
                            <UInput placeholder="Password" type="password" v-model="createAccountState.password" class="w-full" size="xl" variant="subtle" />
                        </UFormField>
                        <div class="mt-6 flex flex-col gap-3">
                            <UButton label="Create Account" type="submit" block size="xl" />
                            <USeparator label="or" />
                            <UButton to="/" label="Log In" block size="xl" color="neutral" variant="subtle" />
                        </div>
                    </UForm>
                </div>
            </div>
        </div>
    </div>
</template>
