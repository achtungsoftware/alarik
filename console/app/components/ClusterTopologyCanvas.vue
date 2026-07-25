<script lang="ts" setup>
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

interface StorageInfo {
    nodeId: string;
    sizeBytes: number;
    objectCount: number;
}

const props = withDefaults(
    defineProps<{
        nodes: ClusterNode[];
        storage: StorageInfo[];
        nodeColor: (id: string) => string;
        selectedId: string | null;
        // Non-zero draws a subtle "flow" along the edges touching a draining node, so an
        // in-progress migration is visible rather than implied.
        rebalancePending?: number;
        animated?: boolean;
    }>(),
    { rebalancePending: 0, animated: true }
);

const emit = defineEmits<{ (e: "select", id: string | null): void }>();

const container = ref<HTMLDivElement | null>(null);
const canvas = ref<HTMLCanvasElement | null>(null);
const hoveredId = ref<string | null>(null);
const prefersReducedMotion = ref(false);

interface NodePos {
    id: string;
    x: number;
    y: number;
    r: number;
}

let positions: NodePos[] = [];
let cssWidth = 0;
let cssHeight = 0;
let raf = 0;
let resizeObserver: ResizeObserver | undefined;
let themeObserver: MutationObserver | undefined;

type ThemeKey = "success" | "warning" | "error" | "neutral" | "text" | "muted" | "border";
// Each entry is a text-colour utility class whose computed `color` is the value we want.
const themeClass: Record<ThemeKey, string> = {
    success: "text-success",
    warning: "text-warning",
    error: "text-error",
    neutral: "text-dimmed",
    text: "text-highlighted",
    muted: "text-muted",
    border: "text-muted", // overridden below by the actual border var; placeholder for typing
};
let theme: Record<ThemeKey, string> = { success: "", warning: "", error: "", neutral: "", text: "", muted: "", border: "" };

function resolveFromProbe(apply: (el: HTMLSpanElement) => void, read: (style: CSSStyleDeclaration) => string): string {
    if (typeof document === "undefined") return "";
    const probe = document.createElement("span");
    probe.style.position = "absolute";
    probe.style.opacity = "0";
    probe.style.pointerEvents = "none";
    apply(probe);
    document.body.appendChild(probe);
    const value = read(getComputedStyle(probe));
    document.body.removeChild(probe);
    return value;
}

function refreshTheme() {
    for (const key of ["success", "warning", "error", "neutral", "text", "muted"] as ThemeKey[]) {
        theme[key] = resolveFromProbe((el) => (el.className = themeClass[key]), (s) => s.color);
    }
    // Edge/mesh colour: the accented border var IS a real CSS variable, so read it directly.
    theme.border = resolveFromProbe((el) => (el.style.color = "var(--ui-border-accented)"), (s) => s.color);
}

function diskUsedFraction(node: ClusterNode): number | null {
    if (node.totalBytes == null || node.availableBytes == null || node.totalBytes <= 0) return null;
    return Math.min(1, Math.max(0, 1 - node.availableBytes / node.totalBytes));
}

function healthColor(node: ClusterNode): string {
    if (node.status === "draining") return theme.warning;
    if (node.status === "removed") return theme.neutral;
    return node.isHealthy ? theme.success : theme.error;
}

// Elliptical layout, deterministic and stable
const LEGEND_BAND = 30;

let centerX = 0;
let centerY = 0;

function computeLayout() {
    const n = props.nodes.length;
    positions = [];
    if (n === 0 || cssWidth === 0 || cssHeight === 0) return;

    const drawH = Math.max(0, cssHeight - LEGEND_BAND);
    const cx = cssWidth / 2;
    const cy = drawH / 2;
    centerX = cx;
    centerY = cy;

    const minDim = Math.min(cssWidth, drawH);
    const nodeR = Math.max(12, Math.min(28, minDim / 8, 150 / Math.max(3, n)));
    // Horizontal margin leaves room for the widest label; vertical for the label under the lowest
    // node (which sits ~nodeR + 20 below the node centre).
    const marginX = nodeR + 56;
    const marginY = nodeR + 28;
    const rx = Math.max(0, cx - marginX);
    const ry = Math.max(0, cy - marginY);

    if (n === 1) {
        positions.push({ id: props.nodes[0]!.id, x: cx, y: cy, r: nodeR });
        return;
    }
    for (let i = 0; i < n; i++) {
        const angle = -Math.PI / 2 + (i / n) * Math.PI * 2;
        positions.push({
            id: props.nodes[i]!.id,
            x: cx + rx * Math.cos(angle),
            y: cy + ry * Math.sin(angle),
            r: nodeR,
        });
    }
}

function nodeById(id: string): ClusterNode | undefined {
    return props.nodes.find((node) => node.id === id);
}

function draw(now: number) {
    const cv = canvas.value;
    const ctx = cv?.getContext("2d");
    if (!cv || !ctx) return;

    const dpr = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, cssWidth, cssHeight);

    const animate = props.animated && !prefersReducedMotion.value;
    const t = animate ? now : 0;
    const activeId = hoveredId.value ?? props.selectedId;

    // ── Edges (mesh) ─────────────────────────────────────────────────────────
    for (let i = 0; i < positions.length; i++) {
        for (let j = i + 1; j < positions.length; j++) {
            const a = positions[i]!;
            const b = positions[j]!;
            const na = nodeById(a.id);
            const nb = nodeById(b.id);
            const touchesActive = activeId != null && (a.id === activeId || b.id === activeId);
            const touchesDraining = na?.status === "draining" || nb?.status === "draining";

            ctx.beginPath();
            ctx.moveTo(a.x, a.y);
            ctx.lineTo(b.x, b.y);

            if (touchesDraining && props.rebalancePending > 0) {
                ctx.strokeStyle = theme.warning;
                ctx.globalAlpha = 0.55;
                ctx.lineWidth = 1.5;
                ctx.setLineDash([4, 6]);
                ctx.lineDashOffset = animate ? -(t / 45) % 10 : 0;
            } else {
                ctx.strokeStyle = theme.border;
                ctx.globalAlpha = touchesActive ? 0.95 : 0.4;
                ctx.lineWidth = touchesActive ? 1.5 : 1;
                ctx.setLineDash([]);
            }
            ctx.stroke();
        }
    }
    ctx.globalAlpha = 1;
    ctx.setLineDash([]);

    // ── Nodes ────────────────────────────────────────────────────────────────
    for (const p of positions) {
        const node = nodeById(p.id);
        if (!node) continue;

        const isSelected = props.selectedId === p.id;
        const isHovered = hoveredId.value === p.id;
        const health = healthColor(node);
        const identity = props.nodeColor(p.id);

        const pulse =
            animate && node.isHealthy && node.status === "active"
                ? 0.5 + 0.5 * Math.sin(t / 900 + p.x)
                : 0;
        const r = p.r * (isHovered ? 1.12 : 1) + pulse * 1.2;

        if (isSelected || isHovered) {
            ctx.beginPath();
            ctx.arc(p.x, p.y, r + 8, 0, Math.PI * 2);
            ctx.fillStyle = health;
            ctx.globalAlpha = 0.12;
            ctx.fill();
            ctx.globalAlpha = 1;
        }

        // Disk-usage arc (outer), from the top, clockwise. Warning-toned when near-full,
        // otherwise the node's own identity colour.
        const used = diskUsedFraction(node);
        if (used != null) {
            ctx.beginPath();
            ctx.arc(p.x, p.y, r + 5, -Math.PI / 2, -Math.PI / 2 + used * Math.PI * 2);
            ctx.strokeStyle = node.isNearFull ? theme.warning : identity;
            ctx.globalAlpha = 0.85;
            ctx.lineWidth = 3;
            ctx.lineCap = "round";
            ctx.stroke();
            ctx.globalAlpha = 1;
        }

        // Main disc (identity colour - the one place a distinct per-node hue is genuinely needed,
        // shared with the storage donut and placement chips).
        ctx.beginPath();
        ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
        ctx.fillStyle = identity;
        ctx.globalAlpha = node.status === "removed" ? 0.35 : 1;
        ctx.fill();
        ctx.globalAlpha = 1;

        // Health ring: dashed for draining, solid otherwise.
        ctx.beginPath();
        ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
        ctx.strokeStyle = health;
        ctx.lineWidth = isSelected ? 3 : 2;
        if (node.status === "draining") {
            ctx.setLineDash([3, 3]);
            ctx.lineDashOffset = animate ? -(t / 60) % 6 : 0;
        } else {
            ctx.setLineDash([]);
        }
        ctx.stroke();
        ctx.setLineDash([]);

        // Label below.
        ctx.font = `${isSelected || isHovered ? "600 " : ""}12px ui-sans-serif, system-ui, sans-serif`;
        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        ctx.fillStyle = isSelected || isHovered ? theme.text : theme.muted;
        ctx.fillText(shortAddress(node.address), p.x, p.y + r + 8);
    }

    // ── Centre summary ───────────────────────────────────────────────────────
    if (positions.length > 1) {
        const healthy = props.nodes.filter((node) => node.isHealthy).length;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillStyle = theme.text;
        ctx.font = "600 15px ui-sans-serif, system-ui, sans-serif";
        ctx.fillText(`${props.nodes.length} nodes`, centerX, centerY - 8);
        ctx.fillStyle = theme.muted;
        ctx.font = "12px ui-sans-serif, system-ui, sans-serif";
        ctx.fillText(`${healthy}/${props.nodes.length} healthy`, centerX, centerY + 10);
    }

    if (animate) raf = requestAnimationFrame(draw);
}

// Requests a redraw. When animated, `draw` re-schedules itself, so this both kicks off and
// coalesces into the single running loop (the prior version early-returned here on the assumption
// a loop was already running, but nothing had started it - which left the canvas blank).
function requestDraw() {
    cancelAnimationFrame(raf);
    raf = requestAnimationFrame(draw);
}

function resizeCanvas() {
    const cv = canvas.value;
    const el = container.value;
    if (!cv || !el) return;
    const rect = el.getBoundingClientRect();
    cssWidth = rect.width;
    cssHeight = rect.height;
    const dpr = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
    cv.width = Math.round(cssWidth * dpr);
    cv.height = Math.round(cssHeight * dpr);
    cv.style.width = `${cssWidth}px`;
    cv.style.height = `${cssHeight}px`;
    computeLayout();
    requestDraw();
}

function pointerToNode(evt: MouseEvent): string | null {
    const cv = canvas.value;
    if (!cv) return null;
    const rect = cv.getBoundingClientRect();
    const px = evt.clientX - rect.left;
    const py = evt.clientY - rect.top;
    for (const p of positions) {
        const dx = px - p.x;
        const dy = py - p.y;
        if (dx * dx + dy * dy <= (p.r + 6) * (p.r + 6)) return p.id;
    }
    return null;
}

function onMove(evt: MouseEvent) {
    const id = pointerToNode(evt);
    if (id !== hoveredId.value) {
        hoveredId.value = id;
        if (canvas.value) canvas.value.style.cursor = id ? "pointer" : "default";
        requestDraw();
    }
}

function onLeave() {
    if (hoveredId.value !== null) {
        hoveredId.value = null;
        requestDraw();
    }
}

function onClick(evt: MouseEvent) {
    const id = pointerToNode(evt);
    emit("select", id === props.selectedId ? null : id);
}

onMounted(() => {
    refreshTheme();

    if (typeof window !== "undefined" && window.matchMedia) {
        const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
        prefersReducedMotion.value = mq.matches;
        mq.addEventListener?.("change", (e) => {
            prefersReducedMotion.value = e.matches;
            requestDraw();
        });
    }

    // Re-resolve theme colours whenever the app toggles light/dark (Nuxt Color Mode flips a class
    // / attribute on <html>).
    themeObserver = new MutationObserver(() => {
        refreshTheme();
        requestDraw();
    });
    themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ["class", "style", "data-theme"] });

    resizeObserver = new ResizeObserver(() => resizeCanvas());
    if (container.value) resizeObserver.observe(container.value);
    resizeCanvas();
});

onUnmounted(() => {
    cancelAnimationFrame(raf);
    resizeObserver?.disconnect();
    themeObserver?.disconnect();
});

watch(() => props.nodes.length, () => { computeLayout(); requestDraw(); });
watch(
    () => [props.nodes, props.storage, props.selectedId, props.rebalancePending, props.animated],
    () => requestDraw(),
    { deep: true }
);
</script>

<template>
    <div ref="container" class="relative w-full h-[clamp(300px,42vh,460px)]">
        <canvas ref="canvas" class="block w-full h-full" @mousemove="onMove" @mouseleave="onLeave" @click="onClick" />
        <!-- Legend: single line (never wraps into the reserved band), theme utility colours only.
             The lowest-priority item is dropped on narrow screens. -->
        <div class="absolute bottom-2 left-2 right-2 flex flex-nowrap items-center gap-x-3 overflow-hidden text-xs text-muted select-none pointer-events-none">
            <span class="flex items-center gap-1 shrink-0"><span class="inline-block w-2 h-2 rounded-full bg-success" /> Healthy</span>
            <span class="flex items-center gap-1 shrink-0"><span class="inline-block w-2 h-2 rounded-full bg-warning" /> Draining</span>
            <span class="flex items-center gap-1 shrink-0"><span class="inline-block w-2 h-2 rounded-full bg-error" /> Unreachable</span>
            <span class="hidden sm:flex items-center gap-1 shrink-0"><span class="inline-block w-2.5 h-2.5 rounded-full border-2 border-default" /> ring = disk used</span>
        </div>
    </div>
</template>
