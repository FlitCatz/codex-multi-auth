import { afterEach, expect, it, vi } from "vitest";
import { mkdtemp } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { removeWithRetry } from "./helpers/remove-with-retry.js";
import { AccountManager } from "../lib/accounts.js";
import { saveApiRoutes } from "../lib/api-route-store.js";
import { startRuntimeRotationProxy } from "../lib/runtime-rotation-proxy.js";
import { createAutomaticApiCheck } from "../lib/runtime/automatic-api-checks.js";
import { discoverModelInventory } from "../lib/runtime/model-discovery-status.js";
import {resetPersistedApiCapabilitiesForTests} from "../lib/runtime/persisted-api-capabilities.js";
afterEach(()=>{vi.restoreAllMocks();resetPersistedApiCapabilitiesForTests();});
it("preserves non-probing live catalog reasoning through a documentation outage", async () => {
    const root = await mkdtemp(join(tmpdir(), "shared-api-documentation-"));
    const previous = process.env.CODEX_MULTI_AUTH_DIR;
    process.env.CODEX_MULTI_AUTH_DIR = root;
    const realFetch = globalThis.fetch;
    let outage = false, posts = 0;
    const fetcher: typeof fetch = async (input, init) => {
        if (String(input).startsWith("http://127.0.0.1:")) return realFetch(input, init);
        if (String(input).startsWith("https://developers.openai.com/")) {
            expect(new Headers(init?.headers).has("authorization")).toBe(false);
            return outage ? new Response("unavailable", { status: 503 })
                : new Response("Model ID: `fixture`\nReasoning.effort supports: low, medium, high.");
        }
        if (init?.method !== "POST") return Response.json({ data: [{ id: "fixture" }], models: [] });
        posts++;
        return Response.json({ status: "completed", output: [] });
    };
    const mock = vi.spyOn(globalThis, "fetch").mockImplementation(fetcher);
    const route = { id: "docs", label: "fixture", kind: "api" as const, apiKey: "fixture-secret", enabled: true, priority: 0, probeCapabilities: false, visibleModels: ["fixture"], knownModels: ["fixture"] };
    const manager = new AccountManager(undefined, { version: 3, activeIndex: 0, accounts: [] });
    let proxy: Awaited<ReturnType<typeof startRuntimeRotationProxy>> | undefined;
    const headers = { authorization: "Bearer fixture-local", "content-type": "application/json" };
    try {
        await saveApiRoutes([route]);
        proxy = await startRuntimeRotationProxy({ accountManager: manager, fetchImpl: fetcher, nativeOpenai: true, clientApiKey: "fixture-local" });
        const catalog = async () => {
            const response = await realFetch(`${proxy!.baseUrl}/models?refresh_capabilities=1`, { headers });
            expect(response.status).toBe(200);
            const body = await response.json();
            return body.models.find((model: { slug: string }) => model.slug === "api/fixture");
        };
        const first = await catalog();
        expect(first.supported_reasoning_levels.map((level: { effort: string }) => level.effort)).toContain("high");
        outage = true;
        const second = await catalog();
        expect(second.supported_reasoning_levels).toEqual(first.supported_reasoning_levels);
        expect(posts).toBe(0);
        const response = await realFetch(`${proxy.baseUrl}/responses`, {
            headers, method: "POST", body: JSON.stringify({ model: "api/fixture", reasoning: { effort: "high" }, input: "fixture", stream: false }),
        });
        expect(response.status).toBe(200);
        await response.text();
        expect(posts).toBe(1);
    } finally {
        await proxy?.close();
        await manager.flushPendingSave();
        mock.mockRestore();
        if (previous === undefined) delete process.env.CODEX_MULTI_AUTH_DIR;
        else process.env.CODEX_MULTI_AUTH_DIR = previous;
        await removeWithRetry(root, { recursive: true, force: true });
    }
});
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
    let release!:()=>void, started!:()=>void;
    const gate=new Promise<void>(resolve=>{release=resolve;}), saturated=new Promise<void>(resolve=>{started=resolve;});
    const fetcher: typeof fetch = async (_input, init) => {
        if (init?.method !== "POST")
            return new Response("missing", { status: 404 });
        active++;
        maximum = Math.max(maximum, active);
        if(active===4)started();
        await gate;
        active--;
        return Response.json({ output: [] });
    };
    try {
        const pending = Promise.all(Array.from({ length: 3 }, (_, i) => enrichPersistedApiCapabilities([apiPickerModel(`fixture-${i}`)], false, { id: `route-${i}`, label: "fixture", kind: "api", apiKey: "fixture", enabled: true, priority: 0, probeCapabilities: true }, false, { fetchImpl: fetcher })));
        try { await saturated; expect(()=>resetPersistedApiCapabilitiesForTests()).toThrow("Cannot reset active"); }
        finally { release(); await pending; }
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

it.each(["corrupt", "EACCES"])("preserves unreadable %s cache without paid probes", async (failure) => {
    const { promises: fs } = await import("node:fs");
    const { enrichPersistedApiCapabilities } = await import("../lib/runtime/persisted-api-capabilities.js");
    const root = await mkdtemp(join(tmpdir(), "probe-read-failure-")), previous = process.env.CODEX_MULTI_AUTH_DIR;
    process.env.CODEX_MULTI_AUTH_DIR = root;
    const path = join(root, "api-capability-probes.json");
    const original = failure === "corrupt" ? "{broken" : JSON.stringify({ version: 1, entries: [] });
    await fs.writeFile(path, original);
    const read = fs.readFile.bind(fs);
    const spy = vi.spyOn(fs, "readFile").mockImplementation(async (...args) => {
        if (String(args[0]) === path && failure === "EACCES")
            throw Object.assign(Error("denied"), { code: "EACCES" });
        return read(...args);
    });
    let posts = 0;
    try {
        const result = await enrichPersistedApiCapabilities([{ slug: "fixture" }], false, { id: "fixture", kind: "api", label: "fixture", apiKey: "fixture", enabled: true, priority: 0, probeCapabilities: true, visibleModels: ["fixture"] }, false, { fetchImpl: async (_url, init) => { if (init?.method === "POST")
                posts++; return new Response("missing", { status: 404 }); } });
        expect(result).toHaveLength(1);
        expect(posts).toBe(0);
        expect(await read(path, "utf8")).toBe(original);
    }
    finally {
        spy.mockRestore();
        if (previous === undefined)
            delete process.env.CODEX_MULTI_AUTH_DIR;
        else
            process.env.CODEX_MULTI_AUTH_DIR = previous;
        await removeWithRetry(root, { recursive: true, force: true });
    }
});
it("returns completed evidence after ENOSPC and avoids immediate duplicate probes", async () => {
    const { promises: fs } = await import("node:fs");
    const { enrichPersistedApiCapabilities } = await import("../lib/runtime/persisted-api-capabilities.js");
    const root = await mkdtemp(join(tmpdir(), "probe-write-failure-")), previous = process.env.CODEX_MULTI_AUTH_DIR;
    process.env.CODEX_MULTI_AUTH_DIR = root;
    const rename = fs.rename.bind(fs);
    const spy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => { if (String(to).endsWith("api-capability-probes.json"))
        throw Object.assign(Error("full"), { code: "ENOSPC" }); return rename(from, to); });
    let posts = 0;
    const route = { id: "fixture", kind: "api" as const, label: "fixture", apiKey: "fixture", enabled: true, priority: 0, probeCapabilities: true, visibleModels: ["fixture"] };
    const options = { fetchImpl: async (_url: unknown, init?: RequestInit) => { if (init?.method !== "POST")
            return new Response("missing", { status: 404 }); posts++; const body = JSON.parse(String(init.body)); return Response.json({ reasoning: body.reasoning, service_tier: body.service_tier, output: [{ type: "function_call", name: "capability_probe" }] }); } };
    try {
        const [first] = await enrichPersistedApiCapabilities([{ slug: "fixture" }], false, route, false, options);
        expect(first.capability_probe_status).toMatchObject({ responses: "verified" });
        const before = posts;
        const [second] = await enrichPersistedApiCapabilities([{ slug: "fixture" }], false, route, false, options);
        expect(second.capability_probe_status).toMatchObject({ responses: "verified" });
        expect(posts).toBe(before);
    }
    finally {
        spy.mockRestore();
        if (previous === undefined)
            delete process.env.CODEX_MULTI_AUTH_DIR;
        else
            process.env.CODEX_MULTI_AUTH_DIR = previous;
        await removeWithRetry(root, { recursive: true, force: true });
    }
});
it.each(["fresh", "expired", "future", "changed-programs", "changed-credential"])("keeps only fresh matching retained evidence after write then read failure: %s", async (scenario) => {
    const { promises: fs } = await import("node:fs");
    const { enrichPersistedApiCapabilities } = await import("../lib/runtime/persisted-api-capabilities.js");
    const root = await mkdtemp(join(tmpdir(), "probe-write-read-failure-"));
    const previous = process.env.CODEX_MULTI_AUTH_DIR;
    process.env.CODEX_MULTI_AUTH_DIR = root;
    const path = join(root, "api-capability-probes.json");
    const rename = fs.rename.bind(fs), read = fs.readFile.bind(fs);
    let readFailed = false, now = 1000, posts = 0;
    vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
        if (String(to) === path) throw Object.assign(Error("full"), { code: "ENOSPC" });
        return rename(from, to);
    });
    vi.spyOn(fs, "readFile").mockImplementation(async (...args) => {
        if (readFailed && String(args[0]) === path) throw Object.assign(Error("unavailable"), { code: "EIO" });
        return read(...args);
    });
    const route = { id: "fixture", kind: "api" as const, label: "fixture", apiKey: "fixture-key", enabled: true, priority: 0, probeCapabilities: true, accessPrograms: { cyber: ["standard"] } };
    const options = { now: () => now, fetchImpl: async (_url: unknown, init?: RequestInit) => {
        if (init?.method !== "POST") return new Response("missing", { status: 404 });
        posts++;
        const body = JSON.parse(String(init.body));
        return Response.json({ status: "completed", reasoning: body.reasoning, service_tier: body.service_tier, access_programs: body.access_programs, output: [{ type: "function_call", name: "capability_probe" }] });
    } };
    try {
        const [first] = await enrichPersistedApiCapabilities([{ slug: "fixture" }], false, route, false, options);
        expect(first.capability_probe_status).toMatchObject({ responses: "verified", "program:cyber:standard": "verified" });
        const count = posts;
        readFailed = true;
        if (scenario === "expired") now += 900_001;
        else if (scenario === "future") now--;
        const current = scenario === "changed-programs" ? { ...route, accessPrograms: { cyber: ["daybreak_blue"] } }
            : scenario === "changed-credential" ? { ...route, apiKey: "new-fixture-key" } : route;
        const [second] = await enrichPersistedApiCapabilities([scenario === "fresh" ? { slug: "fixture" } : first], true, current, true, options);
        if (scenario === "fresh") {
            expect(second.capability_probe_status).toEqual(first.capability_probe_status);
            expect(second.service_tiers).toEqual(first.service_tiers);
            expect(second.capability_checked_at).toBe(1000);
            expect(second.program_checked_at).toBe(1000);
        } else {
            expect(second.capability_probe_status).toBeUndefined();
            expect(second.capability_checked_at).toBeUndefined();
            expect(second.program_checked_at).toBeUndefined();
            expect(second.service_tiers).toEqual([]);
        }
        expect(posts).toBe(count);
    } finally {
        vi.restoreAllMocks();
        if (previous === undefined) delete process.env.CODEX_MULTI_AUTH_DIR;
        else process.env.CODEX_MULTI_AUTH_DIR = previous;
        await removeWithRetry(root, { recursive: true, force: true });
    }
});
it("retries a transient locked cache read without losing existing entries", async () => {
    const { promises: fs } = await import("node:fs");
    const { enrichPersistedApiCapabilities } = await import("../lib/runtime/persisted-api-capabilities.js");
    const root = await mkdtemp(join(tmpdir(), "probe-read-retry-")), previous = process.env.CODEX_MULTI_AUTH_DIR;
    process.env.CODEX_MULTI_AUTH_DIR = root;
    const path = join(root, "api-capability-probes.json"), id = "f".repeat(64), entry = { at: 1, levels: [], tiers: [], status: { responses: "verified" } };
    await fs.writeFile(path, JSON.stringify({ version: 1, entries: [[id, entry]] }));
    const read = fs.readFile.bind(fs);
    let reads = 0;
    const spy = vi.spyOn(fs, "readFile").mockImplementation(async (...args) => { if (String(args[0]) === path && ++reads === 1)
        throw Object.assign(Error("busy"), { code: "EBUSY" }); return read(...args); });
    let posts = 0;
    vi.useFakeTimers();
    try {
        const pending = enrichPersistedApiCapabilities([{ slug: "fixture" }], false, { id: "fixture", kind: "api", label: "fixture", apiKey: "fixture", enabled: true, priority: 0, probeCapabilities: true, visibleModels: ["fixture"] }, false, { fetchImpl: async (_url, init) => { if (init?.method === "POST")
                posts++; return new Response("missing", { status: 404 }); } });
        await vi.waitFor(() => expect(reads).toBeGreaterThan(0));
        await vi.runAllTimersAsync();
        await pending;
        expect(reads).toBeGreaterThanOrEqual(3);
        expect(posts).toBeGreaterThan(0);
        expect(new Map(JSON.parse(await read(path, "utf8")).entries).get(id)).toEqual(entry);
    }
    finally {
        vi.useRealTimers();
        spy.mockRestore();
        if (previous === undefined)
            delete process.env.CODEX_MULTI_AUTH_DIR;
        else
            process.env.CODEX_MULTI_AUTH_DIR = previous;
        await removeWithRetry(root, { recursive: true, force: true });
    }
});

it.each(["EACCES", "EROFS", "ENOSPC"])("keeps API and ZDR metadata available when lease storage fails with %s", async (code) => {
    const locks = await import("../lib/storage/file-lock.js");
    const { enrichPersistedApiCapabilities } = await import("../lib/runtime/persisted-api-capabilities.js");
    const root = await mkdtemp(join(tmpdir(), "probe-lease-unavailable-"));
    const previous = process.env.CODEX_MULTI_AUTH_DIR;
    process.env.CODEX_MULTI_AUTH_DIR = root;
    const originalLock = locks.withFileTransactionLock;
    const lock = vi.spyOn(locks, "withFileTransactionLock").mockImplementation(async (...args) => {
        if (String(args[0]).includes("api-probe-locks")) throw Object.assign(new Error("fixture lease unavailable"), { code });
        return originalLock(...args);
    });
    let posts = 0;
    const fetcher: typeof fetch = async (_input, init) => {
        if (init?.method === "POST") {
            posts++;
            throw Error("Paid probe must not run without its lease");
        }
        expect(new Headers(init?.headers).has("authorization")).toBe(false);
        return new Response("Model ID: `fixture`\nReasoning.effort supports: low, medium, high.");
    };
    try {
        for (const kind of ["api", "zdr"] as const) {
            const [model] = await enrichPersistedApiCapabilities([{ slug: "fixture" }], false, {
                id: kind, label: "fixture", kind, apiKey: "fixture-key", enabled: true,
                priority: 0, probeCapabilities: true,
            }, false, { fetchImpl: fetcher });
            expect(model.supported_reasoning_levels).toEqual(expect.arrayContaining([expect.objectContaining({ effort: "high" })]));
        }
        expect(posts).toBe(0);
    } finally {
        lock.mockRestore();
        if (previous === undefined) delete process.env.CODEX_MULTI_AUTH_DIR;
        else process.env.CODEX_MULTI_AUTH_DIR = previous;
        await removeWithRetry(root, { recursive: true, force: true });
    }
});

it.each([false, true])("replaces future-dated own evidence without repeating paid probes (save failure=%s)", async (saveFails) => {
	const { promises: fs } = await import("node:fs");
	const { createHash } = await import("node:crypto");
	const { enrichPersistedApiCapabilities } = await import("../lib/runtime/persisted-api-capabilities.js");
	const root = await mkdtemp(join(tmpdir(), "future-probes-"));
	const previous = process.env.CODEX_MULTI_AUTH_DIR;
	process.env.CODEX_MULTI_AUTH_DIR = root;
	const path = join(root, "api-capability-probes.json");
	const key = createHash("sha256").update("fixture\0fixture-key\0fixture").digest("hex");
	const future = { at: 2000, levels: [], tiers: [], status: { responses: "unsupported" } };
	await fs.writeFile(path, JSON.stringify({ version: 1, entries: [[key, future]] }));
	const rename = fs.rename.bind(fs);
	const spy = vi.spyOn(fs, "rename").mockImplementation(async (from, to) => {
		if (saveFails && String(to) === path) throw Object.assign(Error("full"), { code: "ENOSPC" });
		return rename(from, to);
	});
	let posts = 0;
	const route = { id: "fixture", label: "fixture", kind: "api" as const, apiKey: "fixture-key", enabled: true, priority: 0, probeCapabilities: true };
	const options = { now: () => 1000, fetchImpl: async (_url: unknown, init?: RequestInit) => {
		if (init?.method !== "POST") return new Response("missing", { status: 404 });
		posts++;
		const body = JSON.parse(String(init.body));
		return Response.json({ reasoning: body.reasoning, service_tier: body.service_tier, output: [{ type: "function_call", name: "capability_probe" }] });
	} };
	try {
		await enrichPersistedApiCapabilities([{ slug: "fixture" }], false, route, false, options);
		const firstPosts = posts;
		expect(firstPosts).toBeGreaterThan(0);
		await enrichPersistedApiCapabilities([{ slug: "fixture" }], false, route, false, options);
		expect(posts).toBe(firstPosts);
		spy.mockRestore();
		await enrichPersistedApiCapabilities([{ slug: "fixture" }], false, route, false, options);
		expect(posts).toBe(firstPosts);
		expect(new Map(JSON.parse(await fs.readFile(path, "utf8")).entries).get(key)).toMatchObject({ at: 1000, status: { responses: "verified" } });
	} finally {
		spy.mockRestore();
		if (previous === undefined) delete process.env.CODEX_MULTI_AUTH_DIR;
		else process.env.CODEX_MULTI_AUTH_DIR = previous;
		await removeWithRetry(root, { recursive: true, force: true });
	}
});
