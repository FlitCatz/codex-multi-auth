import { expect, it, vi } from "vitest";
import { mkdtemp } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { removeWithRetry } from "./helpers/remove-with-retry.js";
import { AccountManager } from "../lib/accounts.js";
import { saveApiRoutes } from "../lib/api-route-store.js";
import { startRuntimeRotationProxy } from "../lib/runtime-rotation-proxy.js";
import { createAutomaticApiCheck } from "../lib/runtime/automatic-api-checks.js";
import { discoverModelInventory } from "../lib/runtime/model-discovery-status.js";
it("shares paid probe results between production live catalogs, daily checks, and restarted discovery", async () => {
    const root = await mkdtemp(join(tmpdir(), "shared-api-probes-"));
    const previous = process.env.CODEX_MULTI_AUTH_DIR;
    process.env.CODEX_MULTI_AUTH_DIR = root;
    const realFetch = globalThis.fetch;
    let release!: () => void, started!: () => void;
    const gate = new Promise<void>(r => { release = r; }), first = new Promise<void>(r => { started = r; });
    let programPosts = 0, posts = 0;
    const fetcher: typeof fetch = async (input, init) => {
        if (String(input).startsWith("http://127.0.0.1:"))
            return realFetch(input, init);
        if (String(input).includes("/models"))
            return Response.json({ data: [{ id: "gpt-6-test-shared" }], models: [] });
        if (init?.method !== "POST")
            return new Response("missing", { status: 404 });
        const body = JSON.parse(String(init.body));
        posts++;
        if (posts === 1) {
            started();
            await gate;
        }
        if (body.access_programs)
            programPosts++;
        return Response.json({ status: "completed", reasoning: body.reasoning, access_programs: body.access_programs, service_tier: body.service_tier ?? "default", output: [{ type: "function_call", name: "capability_probe" }] });
    };
    const mock = vi.spyOn(globalThis, "fetch").mockImplementation(fetcher);
    const route = { id: "shared", label: "shared", kind: "api" as const, apiKey: "fixture-secret", enabled: true, priority: 0, probeCapabilities: true, visibleModels: ["gpt-6-test-shared"], knownModels: ["gpt-6-test-shared"] };
    const manager = new AccountManager(undefined, { version: 3, activeIndex: 0, accounts: [] });
    let proxy: Awaited<ReturnType<typeof startRuntimeRotationProxy>> | undefined;
    try {
        await saveApiRoutes([route]);
        proxy = await startRuntimeRotationProxy({ accountManager: manager, fetchImpl: fetcher, nativeOpenai: true, clientApiKey: "fixture-local" });
        const live = realFetch(`${proxy.baseUrl}/models`, { headers: { authorization: "Bearer fixture-local" } });
        await first;
        const daily = createAutomaticApiCheck()(new AbortController().signal);
        release();
        await (await live).text();
        await daily;
        expect(programPosts).toBe(2);
        const before = posts;
        await discoverModelInventory(null, [route], { fetchImpl: fetcher, persistProbes: true, updateApiDiscovery: async () => route });
        expect(posts).toBe(before);
        await discoverModelInventory(null, [route], { fetchImpl: fetcher, persistProbes: true, forceProbes: true, updateApiDiscovery: async () => route });
        expect(programPosts).toBe(4);
    }
    finally {
        release();
        await proxy?.close();
        await manager.flushPendingSave();
        mock.mockRestore();
        if (previous === undefined)
            delete process.env.CODEX_MULTI_AUTH_DIR;
        else
            process.env.CODEX_MULTI_AUTH_DIR = previous;
        await removeWithRetry(root, { recursive: true, force: true });
    }
});
it("does not put non-probing inference metadata behind a paid probe lease", async () => {
    const { enrichPersistedApiCapabilities } = await import("../lib/runtime/persisted-api-capabilities.js");
    const { apiPickerModel } = await import("../lib/runtime/api-model-runtime.js");
    const root = await mkdtemp(join(tmpdir(), "shared-api-nonprobe-")), previous = process.env.CODEX_MULTI_AUTH_DIR;
    process.env.CODEX_MULTI_AUTH_DIR = root;
    let release!: () => void, started!: () => void;
    const gate = new Promise<void>(r => { release = r; }), first = new Promise<void>(r => { started = r; });
    const route = { id: "shared", label: "shared", kind: "api" as const, apiKey: "fixture-secret", enabled: true, priority: 0, probeCapabilities: true, visibleModels: ["fixture"] };
    const fetcher: typeof fetch = async (_input, init) => { if (init?.method !== "POST")
        return new Response("missing", { status: 404 }); started(); await gate; return Response.json({ output: [] }); };
    let paid: Promise<unknown> | undefined;
    let hangGuard: ReturnType<typeof setTimeout> | undefined;
    try {
        paid = enrichPersistedApiCapabilities([apiPickerModel("fixture")], false, route, false, { fetchImpl: fetcher });
        await first;
        // A generous hang guard only: success must arrive before releasing the paid gate.
        const result = await Promise.race([
            enrichPersistedApiCapabilities([apiPickerModel("fixture")], false, { ...route, probeCapabilities: false }, false, { fetchImpl: fetcher }),
            new Promise<never>((_, reject) => { hangGuard = setTimeout(() => reject(Error("non-probing metadata blocked behind paid lease")), 10000); hangGuard.unref(); }),
        ]);
        expect(result).toHaveLength(1);
    }
    finally {
        clearTimeout(hangGuard);
        release();
        await paid;
        if (previous === undefined)
            delete process.env.CODEX_MULTI_AUTH_DIR;
        else
            process.env.CODEX_MULTI_AUTH_DIR = previous;
        await removeWithRetry(root, { recursive: true, force: true });
    }
}, 15000);
it("caps paid probes across independent routes at four", async () => {
    const { enrichPersistedApiCapabilities } = await import("../lib/runtime/persisted-api-capabilities.js");
    const { apiPickerModel } = await import("../lib/runtime/api-model-runtime.js");
    const root = await mkdtemp(join(tmpdir(), "shared-api-limit-")), previous = process.env.CODEX_MULTI_AUTH_DIR;
    process.env.CODEX_MULTI_AUTH_DIR = root;
    let active = 0, maximum = 0;
    const fetcher: typeof fetch = async (_input, init) => {
        if (init?.method !== "POST")
            return new Response("missing", { status: 404 });
        active++;
        maximum = Math.max(maximum, active);
        await new Promise(resolve => setTimeout(resolve, 5));
        active--;
        return Response.json({ output: [] });
    };
    try {
        await Promise.all(Array.from({ length: 3 }, (_, i) => enrichPersistedApiCapabilities([apiPickerModel(`fixture-${i}`)], false, { id: `route-${i}`, label: "fixture", kind: "api", apiKey: "fixture", enabled: true, priority: 0, probeCapabilities: true }, false, { fetchImpl: fetcher })));
        expect(maximum).toBeLessThanOrEqual(4);
        expect(maximum).toBeGreaterThan(1);
    }
    finally {
        if (previous === undefined)
            delete process.env.CODEX_MULTI_AUTH_DIR;
        else
            process.env.CODEX_MULTI_AUTH_DIR = previous;
        await removeWithRetry(root, { recursive: true, force: true });
    }
});
it("does not overwrite unrelated evidence updated at the same timestamp", async () => {
    const { enrichPersistedApiCapabilities } = await import("../lib/runtime/persisted-api-capabilities.js");
    const { apiPickerModel } = await import("../lib/runtime/api-model-runtime.js");
    const { writeFile, readFile } = await import("node:fs/promises");
    const root = await mkdtemp(join(tmpdir(), "shared-api-merge-")), previous = process.env.CODEX_MULTI_AUTH_DIR;
    process.env.CODEX_MULTI_AUTH_DIR = root;
    const path = join(root, "api-capability-probes.json"), id = "a".repeat(64), at = 1000;
    const old = { at, levels: [], tiers: [], status: { responses: "unsupported" } }, updated = { ...old, status: { responses: "verified" } };
    await writeFile(path, JSON.stringify({ version: 1, entries: [[id, old]] }));
    let changed = false;
    const fetcher: typeof fetch = async (_input, init) => {
        if (init?.method !== "POST")
            return new Response("missing", { status: 404 });
        if (!changed) {
            changed = true;
            await writeFile(path, JSON.stringify({ version: 1, entries: [[id, updated]] }));
        }
        return Response.json({ output: [] });
    };
    try {
        await enrichPersistedApiCapabilities([apiPickerModel("fixture")], false, { id: "route", label: "fixture", kind: "api", apiKey: "fixture", enabled: true, priority: 0, probeCapabilities: true }, false, { fetchImpl: fetcher, now: () => at });
        expect(new Map(JSON.parse(await readFile(path, "utf8")).entries).get(id)).toEqual(updated);
    }
    finally {
        if (previous === undefined)
            delete process.env.CODEX_MULTI_AUTH_DIR;
        else
            process.env.CODEX_MULTI_AUTH_DIR = previous;
        await removeWithRetry(root, { recursive: true, force: true });
    }
});
it("returns documentation without paid probes when another lease times out", async () => {
    const locks = await import("../lib/storage/file-lock.js");
    const { enrichPersistedApiCapabilities } = await import("../lib/runtime/persisted-api-capabilities.js");
    const { apiPickerModel } = await import("../lib/runtime/api-model-runtime.js");
    const lock = vi.spyOn(locks, "withFileTransactionLock").mockRejectedValue(Object.assign(new Error("held"), { code: "ELOCKED" }));
    let posts = 0;
    const fetcher: typeof fetch = async (_input, init) => { if (init?.method === "POST")
        posts++; return new Response("missing", { status: 404 }); };
    try {
        const result = await enrichPersistedApiCapabilities([apiPickerModel("fixture")], false, { id: "route", label: "fixture", kind: "api", apiKey: "fixture", enabled: true, priority: 0, probeCapabilities: true }, false, { fetchImpl: fetcher });
        expect(result).toHaveLength(1);
        expect(posts).toBe(0);
    }
    finally {
        lock.mockRestore();
    }
});
