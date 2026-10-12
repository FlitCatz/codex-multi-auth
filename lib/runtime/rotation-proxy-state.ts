import type { NativeAccountSnapshot } from "./native-account-storage.js";
import type { QuotaCacheData, QuotaCacheEntry } from "../quota-cache.js";
import type {RuntimeCapabilityFailures} from "./runtime-capability-failures.js";
import type {RouteModel} from "../model-route-policy.js";
import type {ModelInventory} from "./model-discovery-status.js";
import type { ApiModelCapabilities } from "./api-model-capabilities.js";
import type { ApiModelRuntime } from "./api-model-runtime.js";
import type { ApiRouteCredential } from "../api-route-store.js";
import type { AccountModelCatalog } from "./account-model-catalog.js";
import { AccountManager, sanitizeEmail } from "../accounts.js";
import type { ContextBudgetGuard } from "../context-budget-guard.js";
import type { PreemptiveQuotaScheduler } from "../preemptive-quota-scheduler.js";
import {
	recordRuntimeReload,
	recordRuntimeReset,
} from "./runtime-observability.js";
import type { RuntimeRotationProxyStatus } from "./rotation-server-types.js";
import type { SessionAffinityStore } from "../session-affinity.js";

/**
 * Per-instance configuration resolved once in `startRuntimeRotationProxy`,
 * captured as explicit fields instead of closure variables so the request
 * handler and the stale-reload recovery can be plain functions.
 *
 * @internal
 */
export interface RotationProxyStateInit {
	nativeOpenai?: boolean;
	readNativeAccountStorage?: () => Promise<NativeAccountSnapshot>;
	readApiRoutes?: () => Promise<ApiRouteCredential[]>;
	readSubscriptionQuota?: () => Promise<QuotaCacheData | null>;
	catalogAccount?: { email: string; accountId: string; };
	activeAccountManager: AccountManager;
	routingMutexMode: "enabled" | "legacy";
	schedulingStrategy: "hybrid" | "sequential";
	fetchImpl: typeof fetch;
	upstreamBaseUrl: string;
	clientApiKey: string;
	now: () => number;
	tokenRefreshSkewMs: number;
	networkErrorCooldownMs: number;
	serverErrorCooldownMs: number;
	/**
	 * Wall-clock ceiling on how long ONE request may wait out a model-capacity
	 * response before giving up. `0` restores the pre-#689 behaviour of rotating
	 * the pool and returning the exhaustion 503.
	 */
	modelCapacityRetryMs: number;
	tokenInvalidationCooldownMs: number;
	minRotationIntervalMs: number;
	pidOffsetEnabled: boolean;
	fetchTimeoutMs: number;
	streamStallTimeoutMs: number;
	maxRuntimeAccountAttempts: number;
	maxRequestBodyBytes: number;
	quotaRemainingPercentThreshold: number;
	preemptiveQuotaScheduler: PreemptiveQuotaScheduler;
	contextBudgetGuard: ContextBudgetGuard;
	sessionAffinityStore: SessionAffinityStore | null;
	lastObservedAffinityGeneration: number;
	/**
	 * Ephemeral per-invocation account pin (0-based) or null. When a number,
	 * the request handler treats it exactly like a manual `switch` pin — a
	 * deterministic, fail-hard selection — but it is never written to disk, so
	 * concurrent invocations with different pins never interfere (issue #623).
	 */
	forcedAccountIndex: number | null;
}

/**
 * Mutable rotation-loop state shared by every request of one proxy instance.
 * `activeAccountManager` and `lastObservedAffinityGeneration` are reassigned
 * at runtime (stale-state recovery and affinity-generation tracking), so the
 * whole container is passed by reference and never copied.
 *
 * @internal
 */
export interface RotationProxyState extends RotationProxyStateInit {
	readonly knownAccountManagers: Set<AccountManager>;
	readonly status: RuntimeRotationProxyStatus;
	modelCatalog?: AccountModelCatalog;
	modelCatalogs?: Map<string, AccountModelCatalog>;
    catalogBackoff?: Map<string, number>;
    /** Short-lived per-request file reads (quota cache, reset credits), keyed by source. */
    readCache?: Map<string, { at: number; value: Promise<unknown> }>;
 capabilityFailures?: RuntimeCapabilityFailures;
	apiModelRuntime?: ApiModelRuntime;
 apiModelCapabilities?:ApiModelCapabilities;
	catalogEtag?: string;
 catalogOAuthModels?:RouteModel[];
 catalogApiRoutes?:ApiRouteCredential[];
 catalogInventory?:ModelInventory;
 subscriptionQuotaObservations?: Map<string, QuotaCacheEntry>;
	readonly threadGoalFallbacks: Map<string, string | null>;
	lastGlobalAccountIndex: number | null;
	lastGlobalSwitchAt: number;
	staleRuntimeReloadPromise: Promise<AccountManager | null> | null;
	lastStaleRuntimeReloadAt: number;
	credentialReloadPromise: Promise<AccountManager | null> | null;
	lastCredentialReloadAt: number;
	transcriptionNextAccountIndex: number;
	readonly transcriptionRateLimitUntilByRecordId: Map<string, number>;
}

const STALE_RUNTIME_RELOAD_DEDUPE_MS = 1_000;

/** @internal */
export function createRotationProxyState(
	init: RotationProxyStateInit,
): RotationProxyState {
	return {
		...init,
		knownAccountManagers: new Set<AccountManager>([init.activeAccountManager]),
		status: {
			startedAt: init.now(),
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
		},
		threadGoalFallbacks: new Map<string, string | null>(),
		lastGlobalAccountIndex: null,
		lastGlobalSwitchAt: 0,
		staleRuntimeReloadPromise: null,
		lastStaleRuntimeReloadAt: 0,
		credentialReloadPromise: null,
		lastCredentialReloadAt: 0,
		transcriptionNextAccountIndex: 0,
		transcriptionRateLimitUntilByRecordId: new Map<string, number>(),
	};
}

/**
 * Reload account credentials without resetting inference runtime state.
 *
 * This is intentionally narrower than {@link recoverStaleRuntimeState}: it is
 * used when a non-inference operation discovers that its in-memory credential
 * inventory is stale after an external login/enable action. Fresh credential,
 * workspace, enabled and auth-invalidation fields come from disk, while live
 * inference rate-limit windows and non-auth cooldowns are carried forward by
 * stable account identity. An auth-failure cooldown is carried only while the
 * access token is unchanged, so a genuine re-login can repair the credential.
 * Process-global health/token trackers and circuit breakers are never reset.
 *
 * @internal
 */
export async function reloadRuntimeCredentials(
	state: RotationProxyState,
): Promise<AccountManager | null> {
	// Inference stale recovery is destructive by design and takes precedence over
	// the credential-only refresh. Sharing the in-flight result prevents two
	// asynchronous loaders from racing to replace activeAccountManager.
	if (state.staleRuntimeReloadPromise) return state.staleRuntimeReloadPromise;
	if (Date.now() - state.lastCredentialReloadAt <= STALE_RUNTIME_RELOAD_DEDUPE_MS) {
		return state.activeAccountManager;
	}
	if (state.credentialReloadPromise) return state.credentialReloadPromise;

	state.credentialReloadPromise = (async () => {
		const previousManager = state.activeAccountManager;
		/** Stable matching key for carrying only runtime observations between credential snapshots. */
		const identityOf = (account: { accountId?: string; email?: string; refreshToken?: string }) => {
			const accountId = account.accountId?.trim();
			const email = sanitizeEmail(account.email);
			return accountId || email
				? JSON.stringify([accountId ?? null, email ?? null])
				: account.refreshToken?.trim()
					? `refresh:${account.refreshToken.trim()}`
					: null;
		};
		// Capture auth before I/O so a token rotation that lands while loadFromDisk
		// is suspended can be distinguished from credentials intentionally changed
		// on disk by another login. Only the former is carried into the replacement.
		const authBeforeLoad = previousManager.getAccountsSnapshot();
		const reloaded = await AccountManager.loadFromDisk();
		reloaded.setRoutingMutexMode(state.routingMutexMode);
		// Snapshot only after the disk read resolves. Inference can still update
		// the old live manager while I/O is pending, and those observations must
		// be carried into the replacement manager before it becomes active.
		const previousAccounts = previousManager.getAccountsSnapshot();
		const freshAccounts = reloaded.getAccountsSnapshot();
		const claimed = new Set<number>();
		for (const previous of previousAccounts) {
			const candidates = freshAccounts.filter((account) => !claimed.has(account.index));
			const baseline = authBeforeLoad.find((account) =>
				(account.recordId && account.recordId === previous.recordId) ||
				account.index === previous.index,
			);
			const identities = new Set(
				[identityOf(previous), baseline ? identityOf(baseline) : null].filter(
					(identity): identity is string => identity !== null,
				),
			);
			const identityMatched = candidates.find((account) => {
				const identity = identityOf(account);
				return identity !== null && identities.has(identity);
			});
			const stableEmail = sanitizeEmail(previous.email) ?? sanitizeEmail(baseline?.email);
			const emailMatches = stableEmail
				? candidates.filter((account) => sanitizeEmail(account.email) === stableEmail)
				: [];
			const refreshMatches = candidates.filter((account) =>
				account.refreshToken === previous.refreshToken ||
				(baseline !== undefined && account.refreshToken === baseline.refreshToken),
			);
			const matched = candidates.find((account) =>
				account.recordId && account.recordId === previous.recordId,
			) ?? identityMatched
				?? (emailMatches.length === 1 ? emailMatches[0] : undefined)
				?? (refreshMatches.length === 1 ? refreshMatches[0] : undefined);
			if (!matched) continue;
			claimed.add(matched.index);
			const live = reloaded.getAccountByIndex(matched.index);
			if (!live) continue;
			const authRotatedDuringLoad = baseline !== undefined && (
				baseline.access !== previous.access ||
				baseline.refreshToken !== previous.refreshToken ||
				baseline.expires !== previous.expires
			);
			if (authRotatedDuringLoad) {
				live.access = previous.access;
				live.refreshToken = previous.refreshToken;
				live.expires = previous.expires;
				const tokenBoundIdentityChanged = baseline !== undefined && (
					baseline.accountId !== previous.accountId ||
					baseline.accountIdSource !== previous.accountIdSource
				);
				const liveHasExplicitBinding =
					live.accountIdSource === "manual" || live.accountIdSource === "org";
				const previousHasTokenBinding =
					previous.accountIdSource === "token" || previous.accountIdSource === "id_token";
				if (tokenBoundIdentityChanged && previousHasTokenBinding && !liveHasExplicitBinding) {
					live.accountId = previous.accountId;
					live.accountIdSource = previous.accountIdSource;
				}
				if (baseline !== undefined && baseline.email !== previous.email) {
					live.email = previous.email;
				}
			}
			for (const [key, until] of Object.entries(previous.rateLimitResetTimes)) {
				if (typeof until === "number" && until > state.now()) {
					live.rateLimitResetTimes[key] = Math.max(live.rateLimitResetTimes[key] ?? 0, until);
				}
			}
			if ((previous.coolingDownUntil ?? 0) > (live.coolingDownUntil ?? 0)) {
				const repairedAuth =
					previous.cooldownReason === "auth-failure" && previous.access !== live.access;
				if (!repairedAuth) {
					live.coolingDownUntil = previous.coolingDownUntil;
					live.cooldownReason = previous.cooldownReason;
				}
			}
		}
		state.activeAccountManager = reloaded;
		state.knownAccountManagers.add(reloaded);
		state.lastCredentialReloadAt = Date.now();
		recordRuntimeReload("credential-inventory-refresh");
		return reloaded;
	})()
		.catch((error) => {
			state.status.lastError = error instanceof Error ? error.message : String(error);
			return null;
		})
		.finally(() => {
			state.credentialReloadPromise = null;
		});
	return state.credentialReloadPromise;
}

/** @internal */
export async function recoverStaleRuntimeState(
	state: RotationProxyState,
): Promise<AccountManager | null> {
	if (state.staleRuntimeReloadPromise) return state.staleRuntimeReloadPromise;
	// If a non-destructive credential refresh started first, let it publish its
	// inventory before performing the authoritative stale-state reset. Re-check
	// the stale promise afterward so concurrent recovery callers still dedupe.
	if (state.credentialReloadPromise) await state.credentialReloadPromise;
	if (state.staleRuntimeReloadPromise) return state.staleRuntimeReloadPromise;
	if (Date.now() - state.lastStaleRuntimeReloadAt <= STALE_RUNTIME_RELOAD_DEDUPE_MS) {
		return state.activeAccountManager;
	}
	state.staleRuntimeReloadPromise = (async () => {
		AccountManager.resetVolatileRuntimeState();
		recordRuntimeReset("pool-exhausted-no-account");
		const reloaded = await AccountManager.loadFromDisk();
		reloaded.setRoutingMutexMode(state.routingMutexMode);
		// Wipe per-account cooldowns and rate-limit windows on the freshly
		// reloaded pool. `resetVolatileRuntimeState` above only cleared global
		// singletons (trackers, circuit breakers); the per-account transient
		// state is serialized to disk, so loadFromDisk restores the same state
		// that wedged the pool. Clearing it here gives recovery a real clean
		// slate before any request can pick up the manager (issue #606). Runs
		// before the `state.activeAccountManager` assignment so no concurrent
		// request can observe the reloaded manager with stale state.
		//
		// This also drops still-future rate-limit windows from genuine upstream
		// 429s, not just stale ones from a dead prior process. That is the
		// intended trade-off: recovery only runs at full pool exhaustion, where
		// the alternative is a hard 503 anyway, and the 1s reload dedupe bounds
		// re-probing to ~1/sec — the upstream simply re-429s and re-populates the
		// window until the next exhaustion. Availability is preferred over
		// honoring backoff in this already-degraded state.
		reloaded.clearAccountTransientState();
		// Force the cleared snapshot to disk now rather than waiting out the
		// debounce window inside clearAccountTransientState. If the process
		// exited during that window the next startup would reload the wedged
		// snapshot; flushing here makes the "next reload starts clean" guarantee
		// durable across a restart, not just best-effort. Recovery is rare
		// (full-pool exhaustion), so the extra synchronous write is cheap.
		await reloaded.flushPendingSave();
		state.activeAccountManager = reloaded;
		state.knownAccountManagers.add(reloaded);
		state.lastStaleRuntimeReloadAt = Date.now();
		recordRuntimeReload("pool-exhausted-no-account");
		return reloaded;
	})()
		.catch((error) => {
			state.status.lastError = error instanceof Error ? error.message : String(error);
			return null;
		})
		.finally(() => {
			state.staleRuntimeReloadPromise = null;
		});
	return state.staleRuntimeReloadPromise;
}
