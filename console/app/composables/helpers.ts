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

export const MAX_CONCURRENT_UPLOADS = 3;

export function formatBytes(bytes: number): string {
    if (bytes === 0) return "0 Bytes";
    const k = 1024;
    const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

export function getFileIcon(filename: string, isFolder: boolean, isBucket: boolean): string {
    if (isBucket) return "i-lucide-cylinder";
    if (isFolder) return "i-lucide-folder";

    // Get file extension
    const ext = filename.split(".").pop()?.toLowerCase() || "";

    // Map file extensions to VSCode icons
    const iconMap: Record<string, string> = {
        // Documents
        pdf: "vscode-icons:file-type-pdf2",
        doc: "vscode-icons:file-type-word",
        docx: "vscode-icons:file-type-word",
        odt: "vscode-icons:file-type-word",
        rtf: "vscode-icons:file-type-word",
        xls: "vscode-icons:file-type-excel",
        xlsx: "vscode-icons:file-type-excel",
        ods: "vscode-icons:file-type-excel",
        ppt: "vscode-icons:file-type-powerpoint",
        pptx: "vscode-icons:file-type-powerpoint",
        odp: "vscode-icons:file-type-powerpoint",
        txt: "vscode-icons:file-type-text",
        csv: "vscode-icons:file-type-text",
        md: "vscode-icons:file-type-markdown",
        markdown: "vscode-icons:file-type-markdown",

        // Images
        jpg: "vscode-icons:file-type-image",
        jpeg: "vscode-icons:file-type-image",
        png: "vscode-icons:file-type-image",
        gif: "vscode-icons:file-type-image",
        bmp: "vscode-icons:file-type-image",
        tiff: "vscode-icons:file-type-image",
        tif: "vscode-icons:file-type-image",
        svg: "vscode-icons:file-type-svg",
        webp: "vscode-icons:file-type-image",
        ico: "vscode-icons:file-type-image",
        heic: "vscode-icons:file-type-image",
        heif: "vscode-icons:file-type-image",
        raw: "vscode-icons:file-type-image",
        cr2: "vscode-icons:file-type-image",
        nef: "vscode-icons:file-type-image",
        psd: "vscode-icons:file-type-photoshop",
        ai: "vscode-icons:file-type-ai",
        eps: "vscode-icons:file-type-ai",
        sketch: "vscode-icons:file-type-sketch",

        // Videos
        mp4: "vscode-icons:file-type-video",
        m4v: "vscode-icons:file-type-video",
        mov: "vscode-icons:file-type-video",
        avi: "vscode-icons:file-type-video",
        mkv: "vscode-icons:file-type-video",
        webm: "vscode-icons:file-type-video",
        flv: "vscode-icons:file-type-video",
        wmv: "vscode-icons:file-type-video",
        mpg: "vscode-icons:file-type-video",
        mpeg: "vscode-icons:file-type-video",
        "3gp": "vscode-icons:file-type-video",
        ogv: "vscode-icons:file-type-video",

        // Audio
        mp3: "vscode-icons:file-type-audio",
        wav: "vscode-icons:file-type-audio",
        flac: "vscode-icons:file-type-audio",
        m4a: "vscode-icons:file-type-audio",
        aac: "vscode-icons:file-type-audio",
        ogg: "vscode-icons:file-type-audio",
        wma: "vscode-icons:file-type-audio",
        opus: "vscode-icons:file-type-audio",
        aiff: "vscode-icons:file-type-audio",
        alac: "vscode-icons:file-type-audio",

        // Archives
        zip: "vscode-icons:file-type-zip",
        rar: "vscode-icons:file-type-zip",
        "7z": "vscode-icons:file-type-zip",
        tar: "vscode-icons:file-type-zip",
        gz: "vscode-icons:file-type-zip",
        tgz: "vscode-icons:file-type-zip",
        bz2: "vscode-icons:file-type-zip",
        xz: "vscode-icons:file-type-zip",
        iso: "vscode-icons:file-type-zip",
        dmg: "vscode-icons:file-type-zip",
        pkg: "vscode-icons:file-type-zip",
        deb: "vscode-icons:file-type-zip",
        rpm: "vscode-icons:file-type-zip",

        // Programming Languages
        js: "vscode-icons:file-type-js",
        mjs: "vscode-icons:file-type-js",
        cjs: "vscode-icons:file-type-js",
        ts: "vscode-icons:file-type-typescript",
        mts: "vscode-icons:file-type-typescript",
        cts: "vscode-icons:file-type-typescript",
        jsx: "vscode-icons:file-type-reactjs",
        tsx: "vscode-icons:file-type-reactts",
        vue: "vscode-icons:file-type-vue",
        svelte: "vscode-icons:file-type-svelte",
        py: "vscode-icons:file-type-python",
        pyc: "vscode-icons:file-type-python",
        pyo: "vscode-icons:file-type-python",
        pyw: "vscode-icons:file-type-python",
        pyx: "vscode-icons:file-type-python",
        java: "vscode-icons:file-type-java",
        class: "vscode-icons:file-type-java",
        jar: "vscode-icons:file-type-jar",
        kt: "vscode-icons:file-type-kotlin",
        kts: "vscode-icons:file-type-kotlin",
        scala: "vscode-icons:file-type-scala",
        sc: "vscode-icons:file-type-scala",
        cpp: "vscode-icons:file-type-cpp",
        cc: "vscode-icons:file-type-cpp",
        cxx: "vscode-icons:file-type-cpp",
        "c++": "vscode-icons:file-type-cpp",
        hpp: "vscode-icons:file-type-cpp",
        hxx: "vscode-icons:file-type-cpp",
        h: "vscode-icons:file-type-c",
        c: "vscode-icons:file-type-c",
        cs: "vscode-icons:file-type-csharp",
        go: "vscode-icons:file-type-go",
        rs: "vscode-icons:file-type-rust",
        php: "vscode-icons:file-type-php",
        rb: "vscode-icons:file-type-ruby",
        erb: "vscode-icons:file-type-ruby",
        swift: "vscode-icons:file-type-swift",
        m: "vscode-icons:file-type-objectivec",
        mm: "vscode-icons:file-type-objectivecpp",
        r: "vscode-icons:file-type-r",
        rmd: "vscode-icons:file-type-r",
        dart: "vscode-icons:file-type-dartlang",
        lua: "vscode-icons:file-type-lua",
        pl: "vscode-icons:file-type-perl",
        pm: "vscode-icons:file-type-perl",
        sh: "vscode-icons:file-type-shell",
        bash: "vscode-icons:file-type-shell",
        zsh: "vscode-icons:file-type-shell",
        fish: "vscode-icons:file-type-shell",
        bat: "vscode-icons:file-type-bat",
        cmd: "vscode-icons:file-type-bat",
        ps1: "vscode-icons:file-type-powershell",
        psm1: "vscode-icons:file-type-powershell",

        // Web & Markup
        html: "vscode-icons:file-type-html",
        htm: "vscode-icons:file-type-html",
        xhtml: "vscode-icons:file-type-html",
        css: "vscode-icons:file-type-css",
        scss: "vscode-icons:file-type-scss",
        sass: "vscode-icons:file-type-sass",
        less: "vscode-icons:file-type-less",
        styl: "vscode-icons:file-type-stylus",
        json: "vscode-icons:file-type-json",
        jsonc: "vscode-icons:file-type-json",
        json5: "vscode-icons:file-type-json",
        xml: "vscode-icons:file-type-xml",
        yaml: "vscode-icons:file-type-yaml",
        yml: "vscode-icons:file-type-yaml",
        toml: "vscode-icons:file-type-toml",
        ini: "vscode-icons:file-type-ini",
        cfg: "vscode-icons:file-type-config",
        conf: "vscode-icons:file-type-config",

        // Build & Config Files
        dockerfile: "vscode-icons:file-type-docker",
        docker: "vscode-icons:file-type-docker",
        cmake: "vscode-icons:file-type-cmake",
        gradle: "vscode-icons:file-type-gradle",
        npm: "vscode-icons:file-type-npm",
        yarn: "vscode-icons:file-type-yarn",

        // Database
        sql: "vscode-icons:file-type-sql",
        db: "vscode-icons:file-type-db",
        sqlite: "vscode-icons:file-type-sqlite",
        mdb: "vscode-icons:file-type-access",

        // Fonts
        ttf: "vscode-icons:file-type-font",
        otf: "vscode-icons:file-type-font",
        woff: "vscode-icons:file-type-font",
        woff2: "vscode-icons:file-type-font",
        eot: "vscode-icons:file-type-font",

        // 3D Models
        blend: "vscode-icons:file-type-blender",

        // eBooks
        epub: "vscode-icons:file-type-epub",
        mobi: "vscode-icons:file-type-epub",
        azw: "vscode-icons:file-type-epub",
        azw3: "vscode-icons:file-type-epub",

        // Other
        env: "vscode-icons:file-type-dotenv",
        log: "vscode-icons:file-type-log",
        gitignore: "vscode-icons:file-type-git",
        git: "vscode-icons:file-type-git",
        exe: "vscode-icons:file-type-binary",
        dll: "vscode-icons:file-type-binary",
        so: "vscode-icons:file-type-binary",
        dylib: "vscode-icons:file-type-binary",
        app: "vscode-icons:file-type-binary",
        key: "vscode-icons:file-type-key",
        pub: "vscode-icons:file-type-key",
        afdesign: "vscode-icons:file-type-affinitydesigner",
        afphoto: "vscode-icons:file-type-affinityphoto",
    };

    return iconMap[ext] || "vscode-icons:default-file";
}
