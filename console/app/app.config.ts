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

export default defineAppConfig({
    // https://ui3.nuxt.dev/getting-started/theme#design-system
    ui: {
        colors: {
            secondary: "blue",
            neutral: "zinc",
        },
        icons: {
            light: "i-lucide-moon",
            dark: "i-lucide-sun",
        },
        breadcrumb: {
            slots: {
                root: "relative min-w-0",
                list: "flex items-center gap-1.5",
                item: "flex min-w-0",
                link: "group relative flex items-center gap-1.5 text-sm min-w-0 focus-visible:outline-primary cursor-pointer [&>button]:cursor-pointer",
                linkLeadingIcon: "shrink-0 size-5",
                linkLeadingAvatar: "shrink-0",
                linkLeadingAvatarSize: "2xs",
                linkLabel: "truncate",
                separator: "flex",
                separatorIcon: "shrink-0 size-5 text-muted",
            },
            variants: {
                active: {
                    true: {
                        link: "text-primary font-semibold",
                    },
                    false: {
                        link: "text-muted font-medium",
                    },
                },
                disabled: {
                    true: {
                        link: "cursor-not-allowed opacity-75",
                    },
                },
                to: {
                    true: "",
                },
            },
            compoundVariants: [
                {
                    disabled: false,
                    active: false,
                    to: true,
                    class: {
                        link: ["hover:text-default", "transition-colors"],
                    },
                },
            ],
        },
        header: {
            slots: {
                container: "flex items-center justify-between gap-3 h-full max-w-[90rem]",
            },
        },
        prose: {
            a: {
                base: "text-secondary font-medium border-b border-transparent hover:border-secondary",
            },
        },
        dashboardSidebar: {
            slots: {
                root: "relative hidden lg:flex flex-col min-h-svh min-w-16 w-[16rem] shrink-0 bg-elevated/40",
            },
        },
        avatar: {
            variants: {
                size: {
                    "3xs": {
                        root: "size-4 text-[8px]",
                    },
                    "2xs": {
                        root: "size-5 text-[9px]",
                    },
                    xs: {
                        root: "size-6 text-xs",
                    },
                    sm: {
                        root: "size-7 text-xs",
                    },
                    md: {
                        root: "size-8 text-xs",
                    },
                    lg: {
                        root: "size-9 text-md",
                    },
                    xl: {
                        root: "size-10 text-md",
                    },
                    "2xl": {
                        root: "size-11 text-[22px]",
                    },
                    "3xl": {
                        root: "size-12 text-2xl",
                    },
                },
            },
            defaultVariants: {
                size: "md",
            },
        },
        table: {
            slots: {
                tr: "data-[selected=true]:bg-elevated/50",
                th: "px-4 py-3.5 text-sm text-left rtl:text-right font-medium [&:has([role=checkbox])]:pe-0",
                td: "p-4 text-sm text-default whitespace-nowrap [&:has([role=checkbox])]:pe-0",
                separator: "h-[1px] border-b border-default bg-transparent",
            },
        },
        switch: {
            slots: {
                root: "relative flex items-start",
                base: ["cursor-pointer inline-flex items-center shrink-0 rounded-full border-2 border-transparent focus-visible:outline-2 focus-visible:outline-offset-2 data-[state=unchecked]:bg-accented", "transition-[background] duration-200"],
            },
            variants: {
                required: {
                    true: {
                        label: "after:content-['*'] after:ms-0.5 after:text-secondary-500",
                    },
                },
            },
        },
        pageHero: {
            slots: {
                root: "relative isolate",
                container: "flex flex-col lg:grid py-24 sm:py-32 lg:py-40 gap-16 sm:gap-y-24",
                wrapper: "",
                header: "",
                headline: "mb-4",
                title: "text-5xl sm:text-6xl text-pretty tracking-tight font-bold text-highlighted",
                description: "text-lg sm:text-lg/6 text-muted",
                body: "mt-10",
                footer: "mt-10",
                links: "flex flex-wrap gap-x-6 gap-y-3",
            },
            variants: {
                orientation: {
                    horizontal: {
                        container: "lg:grid-cols-2 lg:items-center",
                        description: "text-pretty",
                    },
                    vertical: {
                        container: "",
                        headline: "justify-center",
                        wrapper: "text-center",
                        description: "text-balance",
                        links: "justify-center",
                    },
                },
                reverse: {
                    true: {
                        wrapper: "order-last",
                    },
                },
                title: {
                    true: {
                        description: "mt-6",
                    },
                },
            },
        },
        tabs: {
            slots: {
                trigger: ["group cursor-pointer relative inline-flex items-center shrink-0 min-w-0 data-[state=inactive]:text-(--ui-text-muted) hover:data-[state=inactive]:not-disabled:text-(--ui-text) font-medium rounded-[calc(var(--ui-radius)*1.5)] disabled:cursor-not-allowed disabled:opacity-75", "transition-colors"],
            },
        },
        accordion: {
            slots: {
                trigger: "group flex-1 flex items-center gap-1.5 font-medium text-sm py-3.5 focus-visible:outline-(--ui-primary) min-w-0 cursor-pointer",
            },
        },
        formField: {
            variants: {
                required: {
                    true: {
                        label: "after:content-['*'] after:ms-0.5 after:text-(--ui-secondary)",
                    },
                },
            },
        },
        dropdownMenu: {
            slots: {
                content: "min-w-32 bg-default shadow-lg rounded-lg ring ring-default overflow-hidden data-[state=open]:animate-[scale-in_100ms_ease-out] data-[state=closed]:animate-[scale-out_100ms_ease-in] origin-(--reka-dropdown-menu-content-transform-origin) flex flex-col",
                viewport: "relative divide-y divide-default scroll-py-1 overflow-y-auto flex-1",
                arrow: "fill-default",
                group: "p-1.5 isolate",
                label: "w-full flex items-center font-semibold text-highlighted",
                separator: "-mx-1 my-1 h-px bg-border",
                item: "group cursor-pointer relative w-full flex items-center select-none outline-none before:absolute before:z-[-1] before:inset-px before:rounded-md data-disabled:cursor-not-allowed data-disabled:opacity-75",
                itemLeadingIcon: "shrink-0",
                itemLeadingAvatar: "shrink-0",
                itemLeadingAvatarSize: "",
                itemTrailing: "ms-auto inline-flex gap-1.5 items-center",
                itemTrailingIcon: "shrink-0",
                itemTrailingKbds: "hidden lg:inline-flex items-center shrink-0",
                itemTrailingKbdsSize: "",
                itemLabel: "truncate",
                itemLabelExternalIcon: "inline-block size-3 align-top text-default",
            },
            variants: {
                color: {
                    primary: "",
                    secondary: "",
                    success: "",
                    info: "",
                    warning: "",
                    error: "",
                    neutral: "",
                },
                active: {
                    true: {
                        item: "text-highlighted before:bg-elevated",
                        itemLeadingIcon: "text-default",
                    },
                    false: {
                        item: ["text-default data-highlighted:text-highlighted data-[state=open]:text-highlighted data-highlighted:before:bg-elevated/50 data-[state=open]:before:bg-elevated/50", "transition-colors before:transition-colors"],
                        itemLeadingIcon: ["text-default group-data-highlighted:text-default group-data-[state=open]:text-default", "transition-colors"],
                    },
                },
                loading: {
                    true: {
                        itemLeadingIcon: "animate-spin",
                    },
                },
                size: {
                    xs: {
                        label: "p-1.5 text-xs gap-1",
                        item: "p-1.5 text-xs gap-1",
                        itemLeadingIcon: "size-4",
                        itemLeadingAvatarSize: "3xs",
                        itemTrailingIcon: "size-4",
                        itemTrailingKbds: "gap-0.5",
                        itemTrailingKbdsSize: "sm",
                    },
                    sm: {
                        label: "p-2 text-xs gap-1.5",
                        item: "p-2 text-xs gap-1.5",
                        itemLeadingIcon: "size-4",
                        itemLeadingAvatarSize: "3xs",
                        itemTrailingIcon: "size-4",
                        itemTrailingKbds: "gap-0.5",
                        itemTrailingKbdsSize: "sm",
                    },
                    md: {
                        label: "p-2 text-sm gap-1.5",
                        item: "p-2 text-sm gap-1.5",
                        itemLeadingIcon: "size-5",
                        itemLeadingAvatarSize: "2xs",
                        itemTrailingIcon: "size-5",
                        itemTrailingKbds: "gap-0.5",
                        itemTrailingKbdsSize: "md",
                    },
                    lg: {
                        label: "p-2 text-sm gap-2",
                        item: "p-2 text-sm gap-2",
                        itemLeadingIcon: "size-5",
                        itemLeadingAvatarSize: "2xs",
                        itemTrailingIcon: "size-5",
                        itemTrailingKbds: "gap-1",
                        itemTrailingKbdsSize: "md",
                    },
                    xl: {
                        label: "p-2 text-base gap-2",
                        item: "p-2 text-base gap-2",
                        itemLeadingIcon: "size-6",
                        itemLeadingAvatarSize: "xs",
                        itemTrailingIcon: "size-6",
                        itemTrailingKbds: "gap-1",
                        itemTrailingKbdsSize: "lg",
                    },
                },
            },
            compoundVariants: [
                {
                    color: "primary",
                    active: false,
                    class: {
                        item: "text-primary data-highlighted:text-primary data-highlighted:before:bg-primary/10 data-[state=open]:before:bg-primary/10",
                        itemLeadingIcon: "text-primary/75 group-data-highlighted:text-primary group-data-[state=open]:text-primary",
                    },
                },
                {
                    color: "primary",
                    active: true,
                    class: {
                        item: "text-primary before:bg-primary/10",
                        itemLeadingIcon: "text-primary",
                    },
                },
            ],
            defaultVariants: {
                size: "md",
            },
        },
        select: {
            slots: {
                base: ["relative cursor-pointer group rounded-[calc(var(--ui-radius)*1.5)] inline-flex items-center focus:outline-none disabled:cursor-not-allowed disabled:opacity-75"],
                item: ["group !cursor-pointer relative w-full flex items-center select-none outline-none before:absolute before:z-[-1] before:inset-px before:rounded-[calc(var(--ui-radius)*1.5)] data-disabled:cursor-not-allowed data-disabled:opacity-75 text-(--ui-text) data-highlighted:text-(--ui-text-highlighted) data-highlighted:before:bg-(--ui-bg-elevated)/50", "transition-colors before:transition-colors"],
            },
        },
        selectMenu: {
            slots: {
                content: "max-h-60 w-(--reka-popper-anchor-width) bg-(--ui-bg) dark:bg-(--ui-bg-muted) shadow-xl rounded-[calc(var(--ui-radius)*1.5)] ring ring-(--ui-border) overflow-hidden data-[state=open]:animate-[scale-in_100ms_ease-out] data-[state=closed]:animate-[scale-out_100ms_ease-in] pointer-events-auto",
                base: ["relative cursor-pointer group rounded-[calc(var(--ui-radius)*1.5)] inline-flex items-center focus:outline-none disabled:cursor-not-allowed disabled:opacity-75", "transition-colors"],
                item: ["group !cursor-pointer relative w-full flex items-center select-none outline-none before:absolute before:z-[-1] before:inset-px before:rounded-[calc(var(--ui-radius)*1.5)] data-disabled:cursor-not-allowed data-disabled:opacity-75 text-(--ui-text) data-highlighted:text-(--ui-text-highlighted) data-highlighted:before:bg-(--ui-bg-elevated)/50", "transition-colors before:transition-colors"],
            },
        },
        inputMenu: {
            slots: {
                item: ["group !cursor-pointer relative w-full flex items-center gap-1.5 p-1.5 text-sm select-none outline-none before:absolute before:z-[-1] before:inset-px before:rounded-[calc(var(--ui-radius)*1.5)] data-disabled:cursor-not-allowed data-disabled:opacity-75 text-(--ui-text) data-highlighted:text-(--ui-text-highlighted) data-highlighted:before:bg-(--ui-bg-elevated)/50", "transition-colors before:transition-colors"],
            },
        },
        input: {
            defaultVariants: {
                size: "lg",
                color: "primary",
                variant: "outline",
            },
        },
        button: {
            slots: {
                base: ["rounded-[calc(var(--ui-radius)*1.5)] cursor-pointer font-medium inline-flex items-center focus:outline-hidden disabled:cursor-not-allowed aria-disabled:cursor-not-allowed disabled:opacity-75 aria-disabled:opacity-75", "transition-colors"],
                label: "truncate",
                leadingIcon: "shrink-0",
                leadingAvatar: "shrink-0",
                leadingAvatarSize: "",
                trailingIcon: "shrink-0",
            },
            defaultVariants: {
                color: "primary",
                variant: "solid",
                size: "md",
            },
            variants: {
                size: {
                    xs: {
                        base: "px-2 py-1 text-xs gap-1",
                        leadingIcon: "size-4",
                        leadingAvatarSize: "3xs",
                        trailingIcon: "size-4",
                    },
                    sm: {
                        base: "px-2.5 py-1.5 text-xs gap-1.5",
                        leadingIcon: "size-4",
                        leadingAvatarSize: "3xs",
                        trailingIcon: "size-4",
                    },
                    md: {
                        base: "px-2.5 py-1.5 text-sm gap-1.5",
                        leadingIcon: "size-5",
                        leadingAvatarSize: "2xs",
                        trailingIcon: "size-5",
                    },
                    lg: {
                        base: "px-3 py-2 text-sm gap-2",
                        leadingIcon: "size-5",
                        leadingAvatarSize: "2xs",
                        trailingIcon: "size-5",
                    },
                    xl: {
                        base: "px-3.5 py-2 text-base gap-2",
                        leadingIcon: "size-5",
                        leadingAvatarSize: "xs",
                        trailingIcon: "size-5",
                    },
                },
            },
        },
        modal: {
            variants: {
                overlay: {
                    true: {
                        overlay: "bg-black/50 backdrop-blur-sm",
                    },
                },
            },
        },
        drawer: {
            slots: {
                overlay: "fixed inset-0 bg-black/50 backdrop-blur-sm",
            },
        },
        slideover: {
            slots: {
                overlay: "fixed inset-0 bg-black/50 backdrop-blur-sm",
            },
        },
        checkbox: {
            slots: {
                base: "cursor-pointer shrink-0 flex items-center justify-center rounded-(--ui-radius) text-(--ui-bg) ring ring-inset ring-(--ui-border-accented) focus-visible:outline-2 focus-visible:outline-offset-2",
                label: "block font-medium text-(--ui-text) cursor-pointer",
            },
        },
        error: {
            slots: {
                root: "min-h-[calc(100vh-var(--ui-header-height))] flex flex-col items-center justify-center text-center",
                statusCode: "text-base",
                statusMessage: "mt-2 text-4xl sm:text-5xl font-bold text-highlighted text-balance",
                message: "mt-4 text-lg text-muted text-balance",
                links: "mt-8 flex items-center justify-center gap-6",
            },
        },
        contentNavigation: {
            slots: {
                link: "cursor-pointer group relative w-full px-2.5 py-2 before:inset-y-px before:inset-x-0 flex items-center gap-1.5 text-sm before:absolute before:z-[-1] before:rounded-md focus:outline-none focus-visible:outline-none focus-visible:before:ring-inset focus-visible:before:ring-2",
            },
            variants: {
                active: {
                    true: {
                        link: "",
                    },
                    false: {
                        link: "text-default",
                        linkLeadingIcon: "text-default",
                    },
                },
            },
        },
        navigationMenu: {
            slots: {
                root: "relative flex gap-1.5 [&>div]:min-w-0",
                list: "isolate min-w-0",
                label: "w-full flex items-center gap-1.5 font-semibold text-xs/5 text-highlighted px-2.5 py-1.5",
                item: "min-w-0",
                link: "group relative w-full flex items-center gap-1.5 font-medium text-sm before:absolute before:z-[-1] before:rounded-md focus:outline-none focus-visible:outline-none dark:focus-visible:outline-none focus-visible:before:ring-inset focus-visible:before:ring-2",
                linkLeadingIcon: "shrink-0 size-5",
                linkLeadingAvatar: "shrink-0",
                linkLeadingAvatarSize: "2xs",
                linkTrailing: "group ms-auto inline-flex gap-1.5 items-center",
                linkTrailingBadge: "shrink-0",
                linkTrailingBadgeSize: "sm",
                linkTrailingIcon: "size-5 transform shrink-0 group-data-[state=open]:rotate-180 transition-transform duration-200",
                linkLabel: "truncate",
                linkLabelExternalIcon: "inline-block size-3 align-top",
                childList: "isolate",
                childLabel: "text-xs text-highlighted",
                childItem: "",
                childLink: "group relative size-full flex items-start text-start text-sm before:absolute before:z-[-1] before:rounded-md focus:outline-none focus-visible:outline-none dark:focus-visible:outline-none focus-visible:before:ring-inset focus-visible:before:ring-2",
                childLinkWrapper: "min-w-0",
                childLinkIcon: "size-5 shrink-0",
                childLinkLabel: "truncate",
                childLinkLabelExternalIcon: "inline-block size-3 align-top",
                childLinkDescription: "text-muted",
                separator: "px-2 h-px bg-border",
                viewportWrapper: "absolute top-full left-0 flex w-full",
                viewport: "relative overflow-hidden bg-default shadow-lg rounded-md ring ring-default h-(--reka-navigation-menu-viewport-height) w-full transition-[width,height,left] duration-200 origin-[top_center] data-[state=open]:animate-[scale-in_100ms_ease-out] data-[state=closed]:animate-[scale-out_100ms_ease-in] z-[1]",
                content: "",
                indicator: "absolute data-[state=visible]:animate-[fade-in_100ms_ease-out] data-[state=hidden]:animate-[fade-out_100ms_ease-in] data-[state=hidden]:opacity-0 bottom-0 z-[2] w-(--reka-navigation-menu-indicator-size) translate-x-(--reka-navigation-menu-indicator-position) flex h-2.5 items-end justify-center overflow-hidden transition-[translate,width] duration-200",
                arrow: "relative top-[50%] size-2.5 rotate-45 border border-default bg-default z-[1] rounded-xs",
            },
            variants: {
                color: {
                    primary: {
                        link: "focus-visible:before:ring-primary",
                        childLink: "focus-visible:before:ring-primary",
                    },
                    secondary: {
                        link: "focus-visible:before:ring-secondary",
                        childLink: "focus-visible:before:ring-secondary",
                    },
                    success: {
                        link: "focus-visible:before:ring-success",
                        childLink: "focus-visible:before:ring-success",
                    },
                    info: {
                        link: "focus-visible:before:ring-info",
                        childLink: "focus-visible:before:ring-info",
                    },
                    warning: {
                        link: "focus-visible:before:ring-warning",
                        childLink: "focus-visible:before:ring-warning",
                    },
                    error: {
                        link: "focus-visible:before:ring-error",
                        childLink: "focus-visible:before:ring-error",
                    },
                    neutral: {
                        link: "focus-visible:before:ring-inverted",
                        childLink: "focus-visible:before:ring-inverted",
                    },
                },
                highlightColor: {
                    primary: "",
                    secondary: "",
                    success: "",
                    info: "",
                    warning: "",
                    error: "",
                    neutral: "",
                },
                variant: {
                    pill: "",
                    link: "",
                },
                orientation: {
                    horizontal: {
                        root: "items-center justify-between",
                        list: "flex items-center",
                        item: "py-2",
                        link: "px-2.5 py-1.5 before:inset-x-px before:inset-y-0 cursor-pointer",
                        childList: "grid p-2",
                        childLink: "px-3 py-2 gap-2 before:inset-x-px before:inset-y-0",
                        childLinkLabel: "font-medium",
                        content: "absolute top-0 left-0 w-full max-h-[70vh] overflow-y-auto",
                    },
                    vertical: {
                        root: "flex-col",
                        link: "flex-row px-2.5 py-2 before:inset-y-px before:inset-x-0 cursor-pointer",
                        childLabel: "px-1.5 py-0.5",
                        childLink: "p-1.5 gap-1.5 before:inset-y-px before:inset-x-0",
                    },
                },
                contentOrientation: {
                    horizontal: {
                        viewportWrapper: "justify-center",
                        content: "data-[motion=from-start]:animate-[enter-from-left_200ms_ease] data-[motion=from-end]:animate-[enter-from-right_200ms_ease] data-[motion=to-start]:animate-[exit-to-left_200ms_ease] data-[motion=to-end]:animate-[exit-to-right_200ms_ease]",
                    },
                    vertical: {
                        viewport: "sm:w-(--reka-navigation-menu-viewport-width) left-(--reka-navigation-menu-viewport-left)",
                    },
                },
                active: {
                    true: {
                        childLink: "before:bg-elevated text-highlighted",
                        childLinkIcon: "text-default",
                    },
                    false: {
                        link: "text-default",
                        linkLeadingIcon: "text-default",
                        childLink: ["hover:before:bg-elevated/50 text-default hover:text-highlighted", "transition-colors before:transition-colors"],
                        childLinkIcon: ["text-default", "transition-colors"],
                    },
                },
                disabled: {
                    true: {
                        link: "cursor-not-allowed opacity-50",
                    },
                },
                highlight: {
                    true: "",
                },
                level: {
                    true: "",
                },
                collapsed: {
                    true: "",
                },
            },
            compoundVariants: [
                {
                    orientation: "horizontal",
                    contentOrientation: "horizontal",
                    class: {
                        childList: "grid-cols-2 gap-2",
                    },
                },
                {
                    orientation: "horizontal",
                    contentOrientation: "vertical",
                    class: {
                        childList: "gap-1",
                        content: "w-60",
                    },
                },
                {
                    orientation: "vertical",
                    collapsed: false,
                    class: {
                        childList: "ms-5 border-s border-default",
                        childItem: "ps-1.5 -ms-px",
                        content: "data-[state=open]:animate-[collapsible-down_200ms_ease-out] data-[state=closed]:animate-[collapsible-up_200ms_ease-out] overflow-hidden",
                    },
                },
                {
                    orientation: "vertical",
                    collapsed: true,
                    class: {
                        link: "px-1.5",
                        content: "shadow-sm rounded-sm min-h-6 p-1",
                    },
                },
                {
                    orientation: "horizontal",
                    highlight: true,
                    class: {
                        link: ["after:absolute after:-bottom-2 after:inset-x-2.5 after:block after:h-px after:rounded-full", "after:transition-colors"],
                    },
                },
                {
                    orientation: "vertical",
                    highlight: true,
                    level: true,
                    class: {
                        link: ["after:absolute after:-start-1.5 after:inset-y-0.5 after:block after:w-px after:rounded-full", "after:transition-colors"],
                    },
                },
                {
                    disabled: false,
                    active: false,
                    variant: "pill",
                    class: {
                        link: ["hover:text-highlighted hover:before:bg-elevated/50", "transition-colors before:transition-colors"],
                        linkLeadingIcon: ["group-hover:text-default", "transition-colors"],
                    },
                },
                {
                    disabled: false,
                    active: false,
                    variant: "pill",
                    orientation: "horizontal",
                    class: {
                        link: "data-[state=open]:text-highlighted",
                        linkLeadingIcon: "group-data-[state=open]:text-default",
                    },
                },
                {
                    disabled: false,
                    variant: "pill",
                    highlight: true,
                    orientation: "horizontal",
                    class: {
                        link: "data-[state=open]:before:bg-elevated/50",
                    },
                },
                {
                    disabled: false,
                    variant: "pill",
                    highlight: false,
                    active: false,
                    orientation: "horizontal",
                    class: {
                        link: "data-[state=open]:before:bg-elevated/50",
                    },
                },
                {
                    color: "primary",
                    variant: "pill",
                    active: true,
                    class: {
                        link: "text-primary",
                        linkLeadingIcon: "text-primary group-data-[state=open]:text-primary",
                    },
                },
                {
                    color: "neutral",
                    variant: "pill",
                    active: true,
                    class: {
                        link: "text-highlighted",
                        linkLeadingIcon: "text-highlighted group-data-[state=open]:text-highlighted",
                    },
                },
                {
                    variant: "pill",
                    active: true,
                    highlight: false,
                    class: {
                        link: "before:bg-elevated",
                    },
                },
                {
                    variant: "pill",
                    active: true,
                    highlight: true,
                    disabled: false,
                    class: {
                        link: ["hover:before:bg-elevated/50", "before:transition-colors"],
                    },
                },
                {
                    disabled: false,
                    active: false,
                    variant: "link",
                    class: {
                        link: ["hover:text-highlighted", "transition-colors"],
                        linkLeadingIcon: ["group-hover:text-default", "transition-colors"],
                    },
                },
                {
                    disabled: false,
                    active: false,
                    variant: "link",
                    orientation: "horizontal",
                    class: {
                        link: "data-[state=open]:text-highlighted",
                        linkLeadingIcon: "group-data-[state=open]:text-default",
                    },
                },
                {
                    color: "primary",
                    variant: "link",
                    active: true,
                    class: {
                        link: "text-primary",
                        linkLeadingIcon: "text-primary group-data-[state=open]:text-primary",
                    },
                },
                {
                    color: "neutral",
                    variant: "link",
                    active: true,
                    class: {
                        link: "text-highlighted",
                        linkLeadingIcon: "text-highlighted group-data-[state=open]:text-highlighted",
                    },
                },
                {
                    highlightColor: "primary",
                    highlight: true,
                    level: true,
                    active: true,
                    class: {
                        link: "after:bg-primary",
                    },
                },
                {
                    highlightColor: "neutral",
                    highlight: true,
                    level: true,
                    active: true,
                    class: {
                        link: "after:bg-inverted",
                    },
                },
            ],
            defaultVariants: {
                color: "primary",
                highlightColor: "primary",
                variant: "pill",
            },
        },
        commandPalette: {
            slots: {
                item: "cursor-pointer group relative w-full flex items-center gap-1.5 p-1.5 text-sm select-none outline-none before:absolute before:z-[-1] before:inset-px before:rounded-md data-disabled:cursor-not-allowed data-disabled:opacity-75",
            },
        },
    },
});
