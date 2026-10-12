import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AccountManager } from "../lib/accounts.js";
import { clearCircuitBreakers } from "../lib/circuit-breaker.js";
import { HTTP_STATUS, OPENAI_HEADERS } from "../lib/constants.js";
import * as runtimePolicy from "../lib/policy/runtime-policy.js";
import { resetRefreshQueue } from "../lib/refresh-queue.js";
import { resetTrackers } from "../lib/rotation.js";
import {
	startRuntimeRotationProxy,
	type RuntimeRotationProxyServer,
} from "../lib/runtime-rotation-proxy.js";
import type { AccountStorageV3 } from "../lib/storage.js";

const CLIENT_KEY = "runtime-secret";
const openServers: RuntimeRotationProxyServer[] = [];
const openManagers: AccountManager[] = [];

interface FetchCall {
	url: string;
	headers: Headers;
	bodyBytes: Buffer;
}

function createStorage(now: number, count = 2): AccountStorageV3 {
	return {
		version: 3,
		activeIndex: 0,
		activeIndexByFamily: { codex: 0 },
		accounts: Array.from({ length: count }, (_unused, index) => ({
			email: `account-${index + 1}@example.com`,
			accountId: `acc_${index + 1}`,
			refreshToken: `refresh-${index + 1}`,
			accessToken: `access-${index + 1}`,
			expiresAt: now + 3_600_000,
			addedAt: now - 60_000,
			lastUsed: now - (count - index) * 60_000,
			enabled: true,
		})),
	};
}

function bodyBytes(init: RequestInit | undefined): Buffer {
	const body = init?.body;
	if (typeof body === "string") return Buffer.from(body);
	if (body instanceof Uint8Array) return Buffer.from(body);
	return Buffer.alloc(0);
}

function createRecordingFetch(
	handler: (call: FetchCall, attempt: number) => Response | Promise<Response>,
): { calls: FetchCall[]; fetchImpl: typeof fetch } {
	const calls: FetchCall[] = [];
	const fetchImpl: typeof fetch = async (input, init) => {
		const call = {
			url: String(input),
			headers: new Headers(init?.headers),
			bodyBytes: bodyBytes(init),
		};
		calls.push(call);
		return handler(call, calls.length);
	};
	return { calls, fetchImpl };
}

async function startProxy(
	accountManager: AccountManager,
	fetchImpl: typeof fetch,
): Promise<RuntimeRotationProxyServer> {
	openManagers.push(accountManager);
	const proxy = await startRuntimeRotationProxy({
		accountManager,
		fetchImpl,
		upstreamBaseUrl: "https://example.test/backend-api",
		clientApiKey: CLIENT_KEY,
		quotaRemainingPercentThreshold: 10,
	});
	openServers.push(proxy);
	return proxy;
}

async function postTranscribe(
	proxy: RuntimeRotationProxyServer,
	body: Buffer,
	contentType = "multipart/form-data; boundary=x",
): Promise<Response> {
	return fetch(`${proxy.baseUrl}/transcribe`, {
		method: "POST",
		headers: {
			authorization: `Bearer ${CLIENT_KEY}`,
			"content-type": contentType,
			"x-api-key": "caller-key",
			cookie: "session=caller-cookie",
		},
		body,
	});
}

beforeEach(() => {
	resetTrackers();
	clearCircuitBreakers();
	resetRefreshQueue();
});

afterEach(async () => {
	for (const proxy of openServers.splice(0)) await proxy.close();
	for (const manager of openManagers.splice(0)) await manager.flushPendingSave();
	resetTrackers();
	clearCircuitBreakers();
	resetRefreshQueue();
	vi.restoreAllMocks();
});

describe("runtime transcription proxy", () => {
	it("forwards multipart bytes unchanged with managed OAuth", async () => {
		const manager = new AccountManager(undefined, createStorage(Date.now(), 1));
		const boundary = "----codex-dictation-boundary";
		const contentType = `multipart/form-data; boundary=${boundary}`;
		const body = Buffer.concat([
			Buffer.from(
				`--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="dictation.wav"\r\nContent-Type: audio/wav\r\n\r\n`,
				"utf8",
			),
			Buffer.from([0x52, 0x49, 0x46, 0x46, 0x00, 0xff, 0x7f, 0x80]),
			Buffer.from(`\r\n--${boundary}--\r\n`, "utf8"),
		]);
		const record = vi.fn(async () => undefined);
		const recorder = vi.spyOn(runtimePolicy, "createRuntimeUsageRecorder")
			.mockReturnValue({ hasRecorded: () => record.mock.calls.length > 0, record });
		const recordSuccess = vi.spyOn(manager, "recordSuccess");
		const { calls, fetchImpl } = createRecordingFetch(() =>
			new Response(JSON.stringify({ text: "hello from dictation" }), {
				status: HTTP_STATUS.OK,
				headers: { "content-type": "application/json" },
			}),
		);
		const proxy = await startProxy(manager, fetchImpl);

		const response = await postTranscribe(proxy, body, contentType);

		expect(response.status).toBe(HTTP_STATUS.OK);
		expect(await response.json()).toEqual({ text: "hello from dictation" });
		expect(calls).toHaveLength(1);
		expect(calls[0]?.url).toBe("https://example.test/backend-api/transcribe");
		expect(calls[0]?.bodyBytes.equals(body)).toBe(true);
		expect(calls[0]?.headers.get("content-type")).toBe(contentType);
		expect(calls[0]?.headers.get("authorization")).toBe("Bearer access-1");
		expect(calls[0]?.headers.get(OPENAI_HEADERS.ACCOUNT_ID)).toBe("acc_1");
		expect(calls[0]?.headers.has("x-api-key")).toBe(false);
		expect(calls[0]?.headers.has("cookie")).toBe(false);
		expect(recorder).toHaveBeenCalledWith(expect.objectContaining({
			operation: "transcribe",
			model: null,
		}));
		expect(record).toHaveBeenCalledWith(expect.objectContaining({
			outcome: "success",
			statusCode: HTTP_STATUS.OK,
		}));
		expect(recordSuccess).not.toHaveBeenCalled();
	});

	it("requires local proxy authentication", async () => {
		const manager = new AccountManager(undefined, createStorage(Date.now(), 1));
		const { calls, fetchImpl } = createRecordingFetch(() =>
			new Response(JSON.stringify({ text: "unexpected" })),
		);
		const proxy = await startProxy(manager, fetchImpl);
		const response = await fetch(`${proxy.baseUrl}/transcribe`, {
			method: "POST",
			headers: { "content-type": "multipart/form-data; boundary=x" },
			body: Buffer.from("--x--\r\n"),
		});

		expect(response.status).toBe(HTTP_STATUS.UNAUTHORIZED);
		await response.text();
		expect(calls).toHaveLength(0);
	});

	it("forwards 429 once without poisoning inference quota", async () => {
		const manager = new AccountManager(undefined, createStorage(Date.now(), 2));
		const recordRateLimit = vi.spyOn(manager, "recordRateLimit");
		const markRateLimited = vi.spyOn(manager, "markRateLimitedWithReason");
		const { calls, fetchImpl } = createRecordingFetch(() =>
			new Response(
				JSON.stringify({ error: { code: "usage_limit_reached", message: "transcription limit" } }),
				{
					status: HTTP_STATUS.TOO_MANY_REQUESTS,
					headers: { "content-type": "application/json", "retry-after": "60" },
				},
			),
		);
		const proxy = await startProxy(manager, fetchImpl);

		const response = await postTranscribe(proxy, Buffer.from("--x--\r\n"));

		expect(response.status).toBe(HTTP_STATUS.TOO_MANY_REQUESTS);
		expect(calls).toHaveLength(1);
		expect(recordRateLimit).not.toHaveBeenCalled();
		expect(markRateLimited).not.toHaveBeenCalled();
		expect(manager.getAccountsSnapshot().every((account) =>
			Object.keys(account.rateLimitResetTimes).length === 0,
		)).toBe(true);
		expect(proxy.getStatus().lunaReserveFallbacks ?? 0).toBe(0);
	});

	it("forwards Cloudflare 403 once without rotating or disabling accounts", async () => {
		const manager = new AccountManager(undefined, createStorage(Date.now(), 2));
		const { calls, fetchImpl } = createRecordingFetch(() =>
			new Response(
				JSON.stringify({ error: { code: "cloudflare_challenge", message: "challenge" } }),
				{
					status: HTTP_STATUS.FORBIDDEN,
					headers: { "content-type": "application/json", "cf-mitigated": "challenge" },
				},
			),
		);
		const proxy = await startProxy(manager, fetchImpl);

		const response = await postTranscribe(proxy, Buffer.from("--x--\r\n"));

		expect(response.status).toBe(HTTP_STATUS.FORBIDDEN);
		expect(calls).toHaveLength(1);
		expect(proxy.getStatus().rotations).toBe(0);
		expect(manager.getAccountsSnapshot().every((account) => account.enabled !== false)).toBe(true);
	});

	it("forwards 5xx once without inference health cooldown", async () => {
		const manager = new AccountManager(undefined, createStorage(Date.now(), 2));
		const recordFailure = vi.spyOn(manager, "recordFailure");
		const markCooldown = vi.spyOn(manager, "markAccountCoolingDown");
		const { calls, fetchImpl } = createRecordingFetch(() =>
			new Response("upstream unavailable", { status: HTTP_STATUS.SERVICE_UNAVAILABLE }),
		);
		const proxy = await startProxy(manager, fetchImpl);

		const response = await postTranscribe(proxy, Buffer.from("--x--\r\n"));

		expect(response.status).toBe(HTTP_STATUS.SERVICE_UNAVAILABLE);
		expect(await response.text()).toBe("upstream unavailable");
		expect(calls).toHaveLength(1);
		expect(recordFailure).not.toHaveBeenCalled();
		expect(markCooldown).not.toHaveBeenCalled();
		expect(proxy.getStatus().rotations).toBe(0);
	});

	it("does not replay after an ambiguous transport failure", async () => {
		const manager = new AccountManager(undefined, createStorage(Date.now(), 2));
		const recordFailure = vi.spyOn(manager, "recordFailure");
		const markCooldown = vi.spyOn(manager, "markAccountCoolingDown");
		const { calls, fetchImpl } = createRecordingFetch(() => {
			throw new Error("socket closed after upload");
		});
		const proxy = await startProxy(manager, fetchImpl);

		const response = await postTranscribe(proxy, Buffer.from("--x--\r\n"));
		const payload = (await response.json()) as { error: { code: string } };

		expect(response.status).toBe(HTTP_STATUS.BAD_GATEWAY);
		expect(payload.error.code).toBe("transcribe_upstream_transport_error");
		expect(calls).toHaveLength(1);
		expect(recordFailure).not.toHaveBeenCalled();
		expect(markCooldown).not.toHaveBeenCalled();
		expect(proxy.getStatus().rotations).toBe(0);
	});

	it("ignores inference rate limits and preserves them after transcription", async () => {
		const manager = new AccountManager(undefined, createStorage(Date.now(), 1));
		const account = manager.getAccountByIndex(0);
		if (!account) throw new Error("missing test account");
		manager.markRateLimitedWithReason(account, 60_000, "codex", "quota", null);
		expect(manager.getAccountRuntimeSkipReason(0, "codex", null)).toBe("rate-limited");
		const consumeToken = vi.spyOn(manager, "consumeTokenWithReason");
		const { calls, fetchImpl } = createRecordingFetch(() =>
			new Response(JSON.stringify({ text: "still works" }), {
				status: HTTP_STATUS.OK,
				headers: { "content-type": "application/json" },
			}),
		);
		const proxy = await startProxy(manager, fetchImpl);

		const response = await postTranscribe(proxy, Buffer.from("--x--\r\n"));

		expect(response.status).toBe(HTTP_STATUS.OK);
		expect(calls).toHaveLength(1);
		expect(calls[0]?.headers.get("authorization")).toBe("Bearer access-1");
		expect(consumeToken).not.toHaveBeenCalled();
		expect(manager.getAccountRuntimeSkipReason(0, "codex", null)).toBe("rate-limited");
	});

	it("does not claim the half-open inference recovery probe", async () => {
		const now = Date.now();
		let clock = now;
		vi.spyOn(Date, "now").mockImplementation(() => clock);
		const manager = new AccountManager(undefined, createStorage(now, 1));
		const account = manager.getAccountByIndex(0);
		if (!account) throw new Error("missing test account");
		manager.recordFailure(account, "codex", null);
		manager.recordFailure(account, "codex", null);
		manager.recordFailure(account, "codex", null);
		clock += 30_001;
		const { calls, fetchImpl } = createRecordingFetch(() =>
			new Response(JSON.stringify({ text: "probe untouched" }), {
				status: HTTP_STATUS.OK,
				headers: { "content-type": "application/json" },
			}),
		);
		const proxy = await startProxy(manager, fetchImpl);

		const response = await postTranscribe(proxy, Buffer.from("--x--\r\n"));

		expect(response.status).toBe(HTTP_STATUS.OK);
		expect(calls).toHaveLength(1);
		expect(manager.consumeTokenWithReason(account, "codex", null, { bypassTokenBucket: true })).toEqual({ ok: true });
	});

	it("does not advance the inference cursor when transcription selects another credential", async () => {
		const manager = new AccountManager(undefined, createStorage(Date.now(), 2));
		const first = manager.getAccountByIndex(0);
		if (!first) throw new Error("missing first test account");
		manager.markAuthInvalidated(first, "test_invalidated");
		const markSwitched = vi.spyOn(manager, "markSwitched");
		const markSwitchedLocked = vi.spyOn(manager, "markSwitchedLocked");
		const { calls, fetchImpl } = createRecordingFetch(() =>
			new Response(JSON.stringify({ text: "second account" }), {
				status: HTTP_STATUS.OK,
				headers: { "content-type": "application/json" },
			}),
		);
		const proxy = await startProxy(manager, fetchImpl);

		expect(manager.getCurrentAccountForFamily("codex")?.index).toBe(0);
		const response = await postTranscribe(proxy, Buffer.from("--x--\r\n"));

		expect(response.status).toBe(HTTP_STATUS.OK);
		expect(calls).toHaveLength(1);
		expect(calls[0]?.headers.get("authorization")).toBe("Bearer access-2");
		expect(manager.getCurrentAccountForFamily("codex")?.index).toBe(0);
		expect(markSwitched).not.toHaveBeenCalled();
		expect(markSwitchedLocked).not.toHaveBeenCalled();
	});

	it("forwards an ordinary transcription 401 once without inference failure or rotation", async () => {
		const manager = new AccountManager(undefined, createStorage(Date.now(), 2));
		const recordFailure = vi.spyOn(manager, "recordFailure");
		const markCooldown = vi.spyOn(manager, "markAccountCoolingDown");
		const { calls, fetchImpl } = createRecordingFetch(() =>
			new Response(JSON.stringify({ error: { code: "unauthorized", message: "not authorized" } }), {
				status: HTTP_STATUS.UNAUTHORIZED,
				headers: { "content-type": "application/json" },
			}),
		);
		const proxy = await startProxy(manager, fetchImpl);

		const response = await postTranscribe(proxy, Buffer.from("--x--\r\n"));
		const payload = (await response.json()) as { error: { code: string } };

		expect(response.status).toBe(HTTP_STATUS.UNAUTHORIZED);
		expect(payload.error.code).toBe("unauthorized");
		expect(calls).toHaveLength(1);
		expect(recordFailure).not.toHaveBeenCalled();
		expect(markCooldown).not.toHaveBeenCalled();
		expect(proxy.getStatus().rotations).toBe(0);
	});

	it("keeps explicit token invalidation semantics for 401", async () => {
		const manager = new AccountManager(undefined, createStorage(Date.now(), 2));
		const invalidationBody = JSON.stringify({
			error: { message: "Encountered invalidated oauth token for user, failing request" },
		});
		const { calls, fetchImpl } = createRecordingFetch(() =>
			new Response(invalidationBody, {
				status: HTTP_STATUS.UNAUTHORIZED,
				headers: { "content-type": "application/json" },
			}),
		);
		const proxy = await startProxy(manager, fetchImpl);

		const response = await postTranscribe(proxy, Buffer.from("--x--\r\n"));
		const payload = (await response.json()) as { error: { code: string } };

		expect(response.status).toBe(HTTP_STATUS.UNAUTHORIZED);
		expect(payload.error.code).toBe("token_invalidated");
		expect(calls).toHaveLength(1);
		expect(manager.getAccountByIndex(0)).toMatchObject({
			authInvalidatedAt: expect.any(Number),
		});
		expect(proxy.getStatus().rotations).toBe(0);
	});
});
