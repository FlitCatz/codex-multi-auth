import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import { dirname, join } from "node:path";
import { z } from "zod";
import { logWarn } from "../logger.js";
import { getCodexMultiAuthDir } from "../runtime-paths.js";
import { withFileTransactionLock } from "../storage/file-lock.js";
import { withRetry } from "../fs-retry.js";
import { tempPathFor } from "../temp-path.js";
import { mapWithConcurrency } from "../concurrency.js";
import { getApiRoutesPath, loadApiRoutes, type ApiRouteCredential } from "../api-route-store.js";
import type { RouteModel } from "../model-route-policy.js";
import { ApiModelCapabilities, type ProbeResult } from "./api-model-capabilities.js";

const printable = z.string().max(256).regex(/^[^\x00-\x1f\x7f]*$/);
const probeCacheSchema = z.object({
	version: z.literal(1),
	entries: z.array(z.tuple([
		z.string().regex(/^[0-9a-f]{64}$/),
		z.object({
			programRetry: z.boolean().optional(),
			programCheckedAt: z.number().finite().nonnegative().max(8.64e15).optional(),
			at: z.number(),
			levels: z.array(printable).max(100),
			tiers: z.array(printable).max(100),
			status: z.record(printable, printable),
		}),
	])).max(1000),
});

let activeProbes = 0;
let activeEnrichments = 0;
const retainedProbes = new Map<string, ProbeResult>();
const retainedOwners = new Map<string, string>();
const probeWaiters: Array<() => void> = [];
const unavailableLeaseCodes = new Set([
	"ELOCKED", "EACCES", "EPERM", "EROFS", "ENOSPC", "EDQUOT", "EMFILE", "ENFILE",
	"EBUSY", "EIO", "ENOENT", "ENOTDIR",
]);

/** Reject resets during work so live waiters cannot lose their concurrency slots. */
export function resetPersistedApiCapabilitiesForTests(): void {
	if (activeEnrichments || activeProbes || probeWaiters.length) {
		throw Error("Cannot reset active API capability checks");
	}
	retainedProbes.clear();
	retainedOwners.clear();
}
async function withSharedProbeSlot<T>(operation: () => Promise<T>): Promise<T> {
	if (activeProbes >= 4) await new Promise<void>((resolve) => probeWaiters.push(resolve));
	else activeProbes++;
	try {
		return await operation();
	} finally {
		const next = probeWaiters.shift();
		if (next) next();
		else activeProbes--;
	}
}

function probeCachePath(): string {
	return join(getCodexMultiAuthDir(), "api-capability-probes.json");
}
async function loadProbeCache(): Promise<Array<[string, ProbeResult]>> {
	try {
		const text = await withRetry(() => fs.readFile(probeCachePath(), "utf8"), {
			maxAttempts: 6,
			backoffMs: 25,
		});
		return probeCacheSchema.parse(JSON.parse(text)).entries;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		return [];
	}
}
// File generation rejects wipe/readd ABA and conservatively invalidates a pass
// when any route is edited. Never carry retained evidence across generations.
type StoredOwner = { route: ApiRouteCredential; model: string; generation: string };
async function storedGeneration(route: ApiRouteCredential, model: string): Promise<string | undefined> {
	const path = getApiRoutesPath();
	const before = await fs.stat(path).catch(() => undefined);
	if (!before) return undefined;
	const current = (await loadApiRoutes()).find(value =>
		value.id === route.id && value.kind === route.kind && value.apiKey === route.apiKey &&
		value.enabled && value.probeCapabilities && value.visibleModels?.includes(model),
	);
	if (!current || JSON.stringify(current.accessPrograms) !== JSON.stringify(route.accessPrograms) || JSON.stringify(current.modelAccessPrograms) !== JSON.stringify(route.modelAccessPrograms)) return undefined;
	const after = await fs.stat(path).catch(() => undefined);
	const version = (value: typeof before) => `${value.dev}:${value.ino}:${value.mtimeMs}:${value.ctimeMs}:${value.size}`;
	return after && version(before) === version(after) ? version(after) : undefined;
}
function dropRetained(key: string): void {
	retainedProbes.delete(key);
	retainedOwners.delete(key);
}
function rememberProbe(key: string, value: ProbeResult, owner?: StoredOwner): void {
	if (retainedProbes.size >= 1000) dropRetained(retainedProbes.keys().next().value ?? "");
	retainedProbes.set(key, value);
	if (owner) retainedOwners.set(key, owner.generation);
	else retainedOwners.delete(key);
}
async function persistProbe(key: string, value: ProbeResult | undefined, now: () => number, signal?: AbortSignal, owner?: StoredOwner, memoryKey?: string): Promise<boolean> {
	if (!value) return true;
	const path = probeCachePath();
	return withFileTransactionLock(path, async () => {
		const persist = async () => {
			signal?.throwIfAborted();
			const merged = new Map(await loadProbeCache());
			const disk = merged.get(key);
			if (!disk || disk.at > now() || disk.at <= value.at) merged.set(key, value);
			const entries = [...merged].sort((a, b) => a[1].at - b[1].at).slice(-1000);
			const temp = tempPathFor(path);
			const retry = { maxAttempts: 6, backoffMs: 25 };
			try {
				await fs.writeFile(temp, JSON.stringify(probeCacheSchema.parse({ version: 1, entries })) + "\n", { mode: 0o600, flag: "wx" });
				await withRetry(() => fs.rename(temp, path), retry);
			} finally {
				await withRetry(() => fs.rm(temp, { force: true }), retry);
			}
			return true;
		};
		if (!owner) return persist();
		return withFileTransactionLock(getApiRoutesPath(), async () => {
			if (await storedGeneration(owner.route, owner.model) !== owner.generation) {
				if (memoryKey) dropRetained(memoryKey);
				return false;
			}
			try {
				return await persist();
			} catch (error) {
				signal?.throwIfAborted();
				// Keep the ownership lock through the memory decision, too.
				if (memoryKey) rememberProbe(memoryKey, value, owner);
				throw error;
			}
		}, { signal });
	}, { signal });
}

function latestProbe(key: string, retained: ProbeResult | undefined, entries: Array<[string, ProbeResult]>, now: number): ProbeResult | undefined {
	const disk = entries.find(([id]) => id === key)?.[1];
	const validDisk = disk && disk.at <= now ? disk : undefined;
	return retained && retained.at <= now && (!validDisk || retained.at > validDisk.at) ? retained : validDisk;
}

/** Reload under a credential/model lease so independent consumers share paid evidence. */
async function enrichPersistedApiCapabilitiesImpl(
	models: RouteModel[],
	refresh: boolean,
	route: ApiRouteCredential,
	force: boolean,
	options: { fetchImpl?: typeof fetch; now?: () => number; metadata?: ApiModelCapabilities; signal?: AbortSignal; programRefreshBefore?: number; requireStoredRoute?: boolean } = {},
): Promise<RouteModel[]> {
	options.signal?.throwIfAborted();
	const fetcher = options.fetchImpl ?? fetch;
	const now = options.now ?? Date.now;
	const metadata = options.metadata ?? new ApiModelCapabilities(fetcher, now);
	// Inference metadata must not wait for another consumer's paid probes.
	if (!route.enabled || !route.probeCapabilities) {
		return metadata.enrich(models, refresh, route, force);
	}
	return mapWithConcurrency(models, 3, async (model) => {
		const path = probeCachePath();
		const key = createHash("sha256").update(route.id).update("\0").update(route.apiKey).update("\0").update(model.slug).digest("hex");
		const memoryKey = `${path}\0${key}`;
		const generation = options.requireStoredRoute
			? await withFileTransactionLock(getApiRoutesPath(), () => storedGeneration(route, model.slug), {
				signal: options.signal,
			}).catch(() => undefined)
			: undefined;
		const owner = generation ? { route, model: model.slug, generation } : undefined;
		const ownsRoute = async () => !options.requireStoredRoute || Boolean(owner && await storedGeneration(route, model.slug).catch(() => undefined) === owner.generation);
		if (options.requireStoredRoute && (!owner || retainedOwners.get(memoryKey) !== owner.generation)) dropRetained(memoryKey);
		const emptyEvidence = async () => {
			options.signal?.throwIfAborted();
			const [result] = await metadata.enrich([model], refresh, route, false, []);
			options.signal?.throwIfAborted();
			return result ?? model;
		};
		if (!await ownsRoute()) return emptyEvidence();
		const fallbackWithoutProbes = async (readDisk = true): Promise<RouteModel> => {
			options.signal?.throwIfAborted();
			let entries: Array<[string, ProbeResult]> = [];
			// Atomic cache reads are safe without the paid-probe lease; never write here.
			if (readDisk) {
				try { entries = await loadProbeCache(); } catch { /* Retained evidence may still be usable. */ }
			}
			options.signal?.throwIfAborted();
			if (!await ownsRoute()) { dropRetained(memoryKey); return emptyEvidence(); }
			const retained = retainedProbes.get(memoryKey);
			const value = latestProbe(key, retained, entries, now());
			const [result] = await metadata.enrich([model], refresh, route, false, value ? [[key, value]] : []);
			options.signal?.throwIfAborted();
			// Documentation may await network I/O while a credential wipe completes.
			if (!await ownsRoute()) { dropRetained(memoryKey); return emptyEvidence(); }
			return result ?? model;
		};
		try {
			return await withFileTransactionLock(join(dirname(path), "api-probe-locks", key), async () => {
				options.signal?.throwIfAborted();
				const guardedFetch: typeof fetch = async (input, init) => {
					if (init?.method?.toUpperCase() === "POST" && !await ownsRoute()) throw Error("API route ownership changed");
					return fetcher(input, init);
				};
				const capabilities = new ApiModelCapabilities(guardedFetch, now, withSharedProbeSlot, metadata, options.programRefreshBefore);
				let entries: Array<[string, ProbeResult]>;
				try {
					entries = await loadProbeCache();
				} catch {
					logWarn("API capability cache unavailable; skipping paid probes.");
					return fallbackWithoutProbes(false);
				}
				if (!await ownsRoute()) { dropRetained(memoryKey); return emptyEvidence(); }
				const retained = retainedProbes.get(memoryKey);
				const latest = latestProbe(key, retained, entries, now());
				if (latest) capabilities.importProbes([[key, latest]]);
				options.signal?.throwIfAborted();
				const [result] = await capabilities.enrich([model], refresh, route, force);
				// Imported evidence belongs to other leases; only write our own result.
				const value = new Map(capabilities.exportProbes()).get(key);
				try {
					const saved = await persistProbe(key, value, now, options.signal, owner, memoryKey);
					if (!saved) return emptyEvidence();
					dropRetained(memoryKey);
				} catch {
					options.signal?.throwIfAborted();
					if (value && !options.requireStoredRoute) rememberProbe(memoryKey, value);
					if (!await ownsRoute()) { dropRetained(memoryKey); return emptyEvidence(); }
					logWarn("API capability results could not be persisted; keeping only ownership-validated evidence in memory.");
				}
				return result ?? model;
			}, { waitMs: 60_000, signal: options.signal });
		} catch (error) {
			if (!unavailableLeaseCodes.has((error as NodeJS.ErrnoException).code ?? "")) throw error;
			logWarn("API capability lease unavailable; skipping paid probes.");
			return fallbackWithoutProbes();
		}
	});
}

export async function enrichPersistedApiCapabilities(...args: Parameters<typeof enrichPersistedApiCapabilitiesImpl>): Promise<RouteModel[]> {
	activeEnrichments++;
	try {
		return await enrichPersistedApiCapabilitiesImpl(...args);
	} finally {
		activeEnrichments--;
	}
}
