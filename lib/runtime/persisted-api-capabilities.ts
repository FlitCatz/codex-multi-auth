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
import type { ApiRouteCredential } from "../api-route-store.js";
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
const probeWaiters: Array<() => void> = [];

/** Reject resets during work so live waiters cannot lose their concurrency slots. */
export function resetPersistedApiCapabilitiesForTests(): void {
	if (activeEnrichments || activeProbes || probeWaiters.length) {
		throw Error("Cannot reset active API capability checks");
	}
	retainedProbes.clear();
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
async function persistProbe(key: string, value: ProbeResult | undefined): Promise<void> {
	if (!value) return;
	const path = probeCachePath();
	await withFileTransactionLock(path, async () => {
		const merged = new Map(await loadProbeCache());
		const disk = merged.get(key);
		if (!disk || disk.at <= value.at) merged.set(key, value);
		const entries = [...merged].sort((a, b) => a[1].at - b[1].at).slice(-1000);
		const temp = tempPathFor(path);
		const retry = { maxAttempts: 6, backoffMs: 25 };
		try {
			await fs.writeFile(temp, JSON.stringify(probeCacheSchema.parse({ version: 1, entries })) + "\n", { mode: 0o600, flag: "wx" });
			await withRetry(() => fs.rename(temp, path), retry);
		} finally {
			await withRetry(() => fs.rm(temp, { force: true }), retry);
		}
	});
}

/** Reload under a credential/model lease so independent consumers share paid evidence. */
async function enrichPersistedApiCapabilitiesImpl(
	models: RouteModel[],
	refresh: boolean,
	route: ApiRouteCredential,
	force: boolean,
	options: { fetchImpl?: typeof fetch; now?: () => number; metadata?: ApiModelCapabilities } = {},
): Promise<RouteModel[]> {
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
		try {
			return await withFileTransactionLock(join(dirname(path), "api-probe-locks", key), async () => {
				const capabilities = new ApiModelCapabilities(fetcher, now, withSharedProbeSlot);
				let entries: Array<[string, ProbeResult]>;
				try {
					entries = await loadProbeCache();
				} catch {
					logWarn("API capability cache unavailable; skipping paid probes.");
					const [result] = await metadata.enrich([model], refresh, { ...route, probeCapabilities: false }, false);
					return result ?? model;
				}
				const memoryKey = `${path}\0${key}`;
				const retained = retainedProbes.get(memoryKey);
				const disk = entries.find(([id]) => id === key)?.[1];
				if (retained && (!disk || retained.at > disk.at)) capabilities.importProbes([[key, retained]]);
				capabilities.importProbes(entries);
				const [result] = await capabilities.enrich([model], refresh, route, force);
				// Imported evidence belongs to other leases; only write our own result.
				const value = new Map(capabilities.exportProbes()).get(key);
				try {
					await persistProbe(key, value);
					retainedProbes.delete(memoryKey);
				} catch {
					if (value) {
						if (retainedProbes.size >= 1000) retainedProbes.delete(retainedProbes.keys().next().value ?? "");
						retainedProbes.set(memoryKey, value);
					}
					logWarn("API capability results could not be persisted; retaining completed evidence in memory.");
				}
				return result ?? model;
			}, { waitMs: 60_000 });
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ELOCKED") throw error;
			const [result] = await metadata.enrich([model], refresh, { ...route, probeCapabilities: false }, false);
			return result ?? model;
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
