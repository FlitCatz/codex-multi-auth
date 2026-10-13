import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import { dirname, join } from "node:path";
import { z } from "zod";
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
const probeWaiters: Array<() => void> = [];
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
		return probeCacheSchema.parse(JSON.parse(await fs.readFile(probeCachePath(), "utf8"))).entries;
	} catch {
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
export async function enrichPersistedApiCapabilities(
	models: RouteModel[],
	refresh: boolean,
	route: ApiRouteCredential,
	force: boolean,
	options: { fetchImpl?: typeof fetch; now?: () => number } = {},
): Promise<RouteModel[]> {
	const fetcher = options.fetchImpl ?? fetch;
	const now = options.now ?? Date.now;
	// Inference metadata must not wait for another consumer's paid probes.
	if (!route.enabled || !route.probeCapabilities) {
		return new ApiModelCapabilities(fetcher, now).enrich(models, refresh, route, force);
	}
	return mapWithConcurrency(models, 3, async (model) => {
		const path = probeCachePath();
		const key = createHash("sha256").update(route.id).update("\0").update(route.apiKey).update("\0").update(model.slug).digest("hex");
		try {
			return await withFileTransactionLock(join(dirname(path), "api-probe-locks", key), async () => {
				const capabilities = new ApiModelCapabilities(fetcher, now, withSharedProbeSlot);
				capabilities.importProbes(await loadProbeCache());
				const [result] = await capabilities.enrich([model], refresh, route, force);
				// Imported evidence belongs to other leases; only write our own result.
				await persistProbe(key, new Map(capabilities.exportProbes()).get(key));
				return result ?? model;
			}, { waitMs: 60_000 });
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ELOCKED") throw error;
			const capabilities = new ApiModelCapabilities(fetcher, now);
			const [result] = await capabilities.enrich([model], refresh, { ...route, probeCapabilities: false }, false);
			return result ?? model;
		}
	});
}
