import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AccountManager } from "../lib/accounts.js";
import { PreemptiveQuotaScheduler } from "../lib/preemptive-quota-scheduler.js";
import type { RotationProxyStateInit } from "../lib/runtime/rotation-proxy-state.js";
import type { AccountStorageV3 } from "../lib/storage.js";

const { recordRuntimeResetMock, recordRuntimeReloadMock } = vi.hoisted(() => ({
	recordRuntimeResetMock: vi.fn(),
	recordRuntimeReloadMock: vi.fn(),
}));

vi.mock("../lib/runtime/runtime-observability.js", async (importOriginal) => {
	const actual = await importOriginal<
		typeof import("../lib/runtime/runtime-observability.js")
	>();
	return {
		...actual,
		recordRuntimeReset: recordRuntimeResetMock,
		recordRuntimeReload: recordRuntimeReloadMock,
	};
});

const { createRotationProxyState, recoverStaleRuntimeState, reloadRuntimeCredentials } = await import(
	"../lib/runtime/rotation-proxy-state.js"
);

const NOW = Date.now();

function storageWith(count: number): AccountStorageV3 {
	return {
		version: 3,
		activeIndex: 0,
		activeIndexByFamily: {},
		accounts: Array.from({ length: count }, (_unused, index) => ({
			email: `account-${index + 1}@example.com`,
			accountId: `acc_${index + 1}`,
			refreshToken: `refresh-${index + 1}`,
			accessToken: `access-${index + 1}`,
			expiresAt: NOW + 3_600_000,
			addedAt: NOW - 60_000,
			lastUsed: NOW - 60_000,
			enabled: true,
		})),
	};
}

function stateInit(): RotationProxyStateInit {
	return {
		activeAccountManager: new AccountManager(undefined, storageWith(1)),
		routingMutexMode: "enabled",
		schedulingStrategy: "hybrid",
		fetchImpl: fetch,
		upstreamBaseUrl: "https://upstream.example",
		clientApiKey: "key",
		now: () => NOW,
		tokenRefreshSkewMs: 30_000,
		networkErrorCooldownMs: 10_000,
		serverErrorCooldownMs: 10_000,
		tokenInvalidationCooldownMs: 300_000,
		minRotationIntervalMs: 0,
		pidOffsetEnabled: false,
		fetchTimeoutMs: 30_000,
		streamStallTimeoutMs: 30_000,
		maxRuntimeAccountAttempts: 3,
		maxRequestBodyBytes: 1024,
		quotaRemainingPercentThreshold: 0,
		preemptiveQuotaScheduler: new PreemptiveQuotaScheduler(),
		sessionAffinityStore: null,
		lastObservedAffinityGeneration: 0,
		forcedAccountIndex: null,
	};
}

let loadFromDisk: ReturnType<typeof vi.spyOn>;

beforeEach(() => {
	vi.clearAllMocks();
	loadFromDisk = vi.spyOn(AccountManager, "loadFromDisk");
});

afterEach(() => {
	loadFromDisk.mockRestore();
});

describe("createRotationProxyState", () => {
	it("seeds the known-manager set and a zeroed status from the injected clock", () => {
		const init = stateInit();

		const state = createRotationProxyState(init);

		expect([...state.knownAccountManagers]).toEqual([
			init.activeAccountManager,
		]);
		expect(state.status).toStrictEqual({
			startedAt: NOW,
			totalRequests: 0,
			upstreamRequests: 0,
			retries: 0,
			rotations: 0,
			streamsStarted: 0,
			lastError: null,
			lastAccountIndex: null,
			lastAccountLabel: null,
			lastAccountId: null,
			lastAccountUpdatedAt: null,
		});
		expect(state.lastGlobalAccountIndex).toBeNull();
		expect(state.lastStaleRuntimeReloadAt).toBe(0);
	});
});

describe("reloadRuntimeCredentials", () => {
	it("carries a token rotation that lands while disk loading is pending", async () => {
		const state = createRotationProxyState(stateInit());
		const previous = state.activeAccountManager.getAccountByIndex(0);
		if (!previous) throw new Error("missing previous account");
		previous.accountIdSource = "token";
		const reloadedStorage = storageWith(1);
		reloadedStorage.accounts[0]!.accountIdSource = "token";
		reloadedStorage.accounts[0]!.workspaces = [{ id: "workspace-preserved", enabled: true }];
		reloadedStorage.accounts[0]!.currentWorkspaceIndex = 0;
		const reloaded = new AccountManager(undefined, reloadedStorage);
		let signalLoadStarted!: () => void;
		const loadStarted = new Promise<void>((resolve) => { signalLoadStarted = resolve; });
		let releaseLoad!: () => void;
		const loadGate = new Promise<void>((resolve) => { releaseLoad = resolve; });
		loadFromDisk.mockImplementationOnce(async () => {
			signalLoadStarted();
			await loadGate;
			return reloaded;
		});

		const pending = reloadRuntimeCredentials(state);
		await loadStarted;
		previous.access = "rotated-access";
		previous.refreshToken = "rotated-refresh";
		previous.expires = NOW + 7_200_000;
		previous.accountId = "acc_rotated";
		previous.accountIdSource = "token";
		previous.email = "rotated@example.com";
		releaseLoad();

		const result = await pending;
		const active = result?.getAccountByIndex(0);
		expect(active?.access).toBe("rotated-access");
		expect(active?.refreshToken).toBe("rotated-refresh");
		expect(active?.expires).toBe(NOW + 7_200_000);
		expect(active?.accountId).toBe("acc_rotated");
		expect(active?.accountIdSource).toBe("token");
		expect(active?.email).toBe("rotated@example.com");
		expect(active?.workspaces).toEqual([{ id: "workspace-preserved", enabled: true }]);
		expect(active?.currentWorkspaceIndex).toBe(0);
		expect(state.activeAccountManager).toBe(reloaded);
	});

	it("keeps an explicit disk workspace binding while carrying rotated tokens", async () => {
		const state = createRotationProxyState(stateInit());
		const previous = state.activeAccountManager.getAccountByIndex(0);
		if (!previous) throw new Error("missing previous account");
		previous.accountIdSource = "token";
		const reloadedStorage = storageWith(1);
		reloadedStorage.accounts[0]!.accountId = "workspace-manual";
		reloadedStorage.accounts[0]!.accountIdSource = "manual";
		reloadedStorage.accounts[0]!.workspaces = [{ id: "workspace-manual", enabled: true }];
		const reloaded = new AccountManager(undefined, reloadedStorage);
		let signalLoadStarted!: () => void;
		const loadStarted = new Promise<void>((resolve) => { signalLoadStarted = resolve; });
		let releaseLoad!: () => void;
		const loadGate = new Promise<void>((resolve) => { releaseLoad = resolve; });
		loadFromDisk.mockImplementationOnce(async () => {
			signalLoadStarted();
			await loadGate;
			return reloaded;
		});

		const pending = reloadRuntimeCredentials(state);
		await loadStarted;
		previous.access = "rotated-access";
		previous.refreshToken = "rotated-refresh";
		previous.expires = NOW + 7_200_000;
		previous.accountId = "token-workspace-new";
		previous.accountIdSource = "token";
		releaseLoad();

		const active = (await pending)?.getAccountByIndex(0);
		expect(active?.access).toBe("rotated-access");
		expect(active?.refreshToken).toBe("rotated-refresh");
		expect(active?.accountId).toBe("workspace-manual");
		expect(active?.accountIdSource).toBe("manual");
		expect(active?.workspaces).toEqual([{ id: "workspace-manual", enabled: true }]);
	});
});

describe("recoverStaleRuntimeState", () => {
	it("reloads from disk, swaps the active manager, and records observability", async () => {
		const state = createRotationProxyState(stateInit());
		const previousManager = state.activeAccountManager;
		const reloaded = new AccountManager(undefined, storageWith(2));
		loadFromDisk.mockResolvedValue(reloaded);

		const result = await recoverStaleRuntimeState(state);

		expect(result).toBe(reloaded);
		expect(state.activeAccountManager).toBe(reloaded);
		// The previous manager stays known so in-flight requests can finish.
		expect(state.knownAccountManagers.has(previousManager)).toBe(true);
		expect(state.knownAccountManagers.has(reloaded)).toBe(true);
		// The configured mutex mode carries over to the reloaded pool.
		expect(reloaded.getRoutingMutexMode()).toBe("enabled");
		expect(state.lastStaleRuntimeReloadAt).toBeGreaterThan(0);
		expect(recordRuntimeResetMock).toHaveBeenCalledExactlyOnceWith(
			"pool-exhausted-no-account",
		);
		expect(recordRuntimeReloadMock).toHaveBeenCalledExactlyOnceWith(
			"pool-exhausted-no-account",
		);
	});

	it("dedupes within the 1s window and reloads again once it expires", async () => {
		// The dedupe guard runs on the real wall clock (deliberately not the
		// injected now()), so fake timers make the window deterministic.
		vi.useFakeTimers();
		try {
			const state = createRotationProxyState(stateInit());
			const reloaded = new AccountManager(undefined, storageWith(2));
			loadFromDisk.mockResolvedValue(reloaded);

			await recoverStaleRuntimeState(state);
			const second = await recoverStaleRuntimeState(state);

			// The second call lands inside the 1s dedupe window: no second reload.
			expect(second).toBe(reloaded);
			expect(loadFromDisk).toHaveBeenCalledTimes(1);

			// Once the window expires, the next call reloads from disk again.
			vi.advanceTimersByTime(1_001);
			const freshest = new AccountManager(undefined, storageWith(3));
			loadFromDisk.mockResolvedValue(freshest);
			await expect(recoverStaleRuntimeState(state)).resolves.toBe(freshest);
			expect(loadFromDisk).toHaveBeenCalledTimes(2);
		} finally {
			vi.useRealTimers();
		}
	});

	it("shares one in-flight reload between concurrent callers", async () => {
		const state = createRotationProxyState(stateInit());
		const reloaded = new AccountManager(undefined, storageWith(2));
		let releaseReload!: () => void;
		const gate = new Promise<void>((resolve) => {
			releaseReload = resolve;
		});
		loadFromDisk.mockImplementation(async () => {
			await gate;
			return reloaded;
		});

		const firstPending = recoverStaleRuntimeState(state);
		const secondPending = recoverStaleRuntimeState(state);
		releaseReload();
		const [first, second] = await Promise.all([firstPending, secondPending]);

		expect(first).toBe(reloaded);
		expect(second).toBe(reloaded);
		expect(loadFromDisk).toHaveBeenCalledTimes(1);
	});

	it("shares an in-flight stale recovery with credential-only reloads", async () => {
		const state = createRotationProxyState(stateInit());
		const recovered = new AccountManager(undefined, storageWith(2));
		let release!: () => void;
		const gate = new Promise<void>((resolve) => { release = resolve; });
		loadFromDisk.mockImplementationOnce(async () => {
			await gate;
			return recovered;
		});

		const stalePending = recoverStaleRuntimeState(state);
		const credentialPending = reloadRuntimeCredentials(state);
		release();
		const [staleResult, credentialResult] = await Promise.all([stalePending, credentialPending]);

		expect(staleResult).toBe(recovered);
		expect(credentialResult).toBe(recovered);
		expect(state.activeAccountManager).toBe(recovered);
		expect(loadFromDisk).toHaveBeenCalledTimes(1);
	});

	it("runs stale recovery after an earlier credential reload finishes", async () => {
		const state = createRotationProxyState(stateInit());
		const credentialReloaded = new AccountManager(undefined, storageWith(2));
		const staleRecovered = new AccountManager(undefined, storageWith(3));
		let releaseCredential!: () => void;
		const credentialGate = new Promise<void>((resolve) => { releaseCredential = resolve; });
		loadFromDisk
			.mockImplementationOnce(async () => {
				await credentialGate;
				return credentialReloaded;
			})
			.mockResolvedValueOnce(staleRecovered);

		const credentialPending = reloadRuntimeCredentials(state);
		const stalePending = recoverStaleRuntimeState(state);
		releaseCredential();
		const [credentialResult, staleResult] = await Promise.all([credentialPending, stalePending]);

		expect(credentialResult).toBe(credentialReloaded);
		expect(staleResult).toBe(staleRecovered);
		expect(state.activeAccountManager).toBe(staleRecovered);
		expect(loadFromDisk).toHaveBeenCalledTimes(2);
	});

	it("reports a failed reload and allows the next attempt to retry", async () => {
		const state = createRotationProxyState(stateInit());
		loadFromDisk.mockRejectedValueOnce(new Error("disk exploded"));

		const failed = await recoverStaleRuntimeState(state);

		expect(failed).toBeNull();
		expect(state.status.lastError).toBe("disk exploded");
		// The failure must not arm the dedupe window.
		expect(state.lastStaleRuntimeReloadAt).toBe(0);
		// The failure does not arm the dedupe window or leak a stale promise:
		// the next call retries the reload.
		const reloaded = new AccountManager(undefined, storageWith(2));
		loadFromDisk.mockResolvedValue(reloaded);
		await expect(recoverStaleRuntimeState(state)).resolves.toBe(reloaded);
		expect(loadFromDisk).toHaveBeenCalledTimes(2);
	});
});
