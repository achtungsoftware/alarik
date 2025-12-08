<script setup>
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

const props = defineProps({
    size: {
        type: String,
        default: "md",
    },
});

function logoWidth() {
    switch (props.size) {
        case "sm":
            return "140px";
        case "md":
            return "160px";
        case "lg":
            return "300px";
        default:
            return "200px";
    }
}
</script>

<template>
    <NuxtLink :prefetch="false" to="/" class="flex-shrink-0" aria-label="Alarik Logo">
        <svg :width="logoWidth()" height="100%" viewBox="0 0 4267 1067" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" xml:space="preserve" xmlns:serif="http://www.serif.com/" style="fill-rule: evenodd; clip-rule: evenodd; stroke-linejoin: round; stroke-miterlimit: 2">
            <g transform="matrix(0.25,0,0,0.25,585.05,533.333)">
                <g transform="matrix(1,0,0,1,-2133.33,-2133.33)">
                    <g transform="matrix(12.6194,0,0,12.6194,-23640.4,-24809)">
                        <path d="M2005,2047L1971,2007C1971,2007 2106.23,2007.97 2133.33,2024C2157,2038 2154,2053 2154,2053C2154,2053 2180,2057.5 2193,2077C2199,2086 2188,2111 2188,2111C2188,2111 2185,2097 2171,2095C2156.59,2092.94 2131.67,2092.26 2122,2100C2098.87,2118.51 2055.06,2219.78 2051,2221C2041,2224 2000,2234 2000,2234L2022,2167L1971,2241L1890,2263L2005,2047ZM2129,2049C2118.5,2046.17 2114.52,2043.88 2076,2042C2078,2044 2089,2059 2111,2063C2120.28,2064.69 2139,2059 2139,2059C2139,2059 2136,2050.89 2129,2049Z" style="fill: var(--color-primary-500)" />
                        <path d="M2093,2161L2067,2213C2067,2213 2062,2219 2069,2222C2085.18,2228.93 2159,2261 2159,2261L2093,2161Z" style="fill: var(--color-primary-500)" />
                    </g>
                </g>
            </g>
            <g transform="matrix(1,0,0,1,-113,-21.234)">
                <path d="M1813.7,823L1695.78,823L1648.9,701.052L1434.3,701.052L1389.99,823L1275,823L1484.11,286.135L1598.73,286.135L1813.7,823ZM1614.11,610.598L1540.14,411.379L1467.63,610.598L1614.11,610.598Z" style="fill: var(--color-primary-500); fill-rule: nonzero" />
                <path d="M1874.12,823L1874.12,290.529L1982.52,290.529L1982.52,732.546L2252.05,732.546L2252.05,823L1874.12,823Z" style="fill: var(--color-primary-500); fill-rule: nonzero" />
                <path d="M2813.45,823L2695.53,823L2648.66,701.052L2434.06,701.052L2389.75,823L2274.76,823L2483.86,286.135L2598.49,286.135L2813.45,823ZM2613.87,610.598L2539.89,411.379L2467.38,610.598L2613.87,610.598Z" style="fill: var(--color-primary-500); fill-rule: nonzero" />
                <path
                    d="M2871.31,823L2871.31,286.135L3099.46,286.135C3156.84,286.135 3198.52,290.957 3224.52,300.6C3250.53,310.244 3271.34,327.395 3286.96,352.053C3302.59,376.711 3310.4,404.909 3310.4,436.647C3310.4,476.931 3298.56,510.195 3274.88,536.44C3251.2,562.685 3215.8,579.226 3168.68,586.062C3192.11,599.733 3211.46,614.748 3226.72,631.105C3241.98,647.463 3262.55,676.516 3288.43,718.264L3353.98,823L3224.34,823L3145.97,706.179C3118.14,664.431 3099.1,638.125 3088.84,627.26C3078.59,616.396 3067.72,608.95 3056.25,604.921C3044.78,600.893 3026.59,598.879 3001.68,598.879L2979.71,598.879L2979.71,823L2871.31,823ZM2979.71,513.186L3059.91,513.186C3111.91,513.186 3144.39,510.988 3157.32,506.594C3170.26,502.199 3180.4,494.631 3187.72,483.889C3195.04,473.146 3198.71,459.719 3198.71,443.605C3198.71,425.539 3193.88,410.952 3184.24,399.843C3174.6,388.735 3160.99,381.716 3143.41,378.786C3134.62,377.565 3108.25,376.955 3064.31,376.955L2979.71,376.955L2979.71,513.186Z"
                    style="fill: var(--color-primary-500); fill-rule: nonzero"
                />
                <rect x="3409.28" y="286.135" width="108.398" height="536.865" style="fill: var(--color-primary-500); fill-rule: nonzero" />
                <path d="M3622.41,823L3622.41,286.135L3730.81,286.135L3730.81,524.538L3949.81,286.135L4095.56,286.135L3893.41,495.241L4106.54,823L3966.28,823L3818.7,571.047L3730.81,660.769L3730.81,823L3622.41,823Z" style="fill: var(--color-primary-500); fill-rule: nonzero" />
            </g>
        </svg>
    </NuxtLink>
</template>
