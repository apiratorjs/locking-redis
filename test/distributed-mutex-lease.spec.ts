import { after, before, beforeEach, describe, it } from "node:test";
import * as assert from "node:assert";
import { LockNotFoundError, types } from "@apiratorjs/locking";
import { sleep } from "../src/utils";
import { DEFAULT_TTL_MS } from "../src/constants";
import { RedisDistributedLockManager, RedisDistributedMutex } from "../src";

const MUTEX_NAME = "lease-mutex";
const MUTEX_KEY = `mutex:${MUTEX_NAME}`;
const REDIS_URL = "redis://localhost:6379/0";

describe("RedisDistributedMutex locks by token and TTL", () => {
  let locks: RedisDistributedLockManager;
  let peers: RedisDistributedMutex[] = [];

  before(async () => {
    locks = await RedisDistributedLockManager.create({ url: REDIS_URL });
  });

  after(async () => {
    await locks.destroyAll();
    await locks.disconnect();
  });

  beforeEach(async () => {
    for (const peer of peers) {
      await peer.destroy();
    }
    peers = [];

    await locks.destroyAll();
    await locks.getRedisClient().flushDb();
  });

  function peerMutexes(): [RedisDistributedMutex, RedisDistributedMutex] {
    const redisClient = locks.getRedisClient();
    const created: [RedisDistributedMutex, RedisDistributedMutex] = [
      new RedisDistributedMutex({ name: MUTEX_NAME, redisClient }),
      new RedisDistributedMutex({ name: MUTEX_NAME, redisClient }),
    ];
    peers.push(...created);
    return created;
  }

  function assertRemaining(remaining: number | null, min: number, max: number): void {
    assert.ok(remaining !== null && remaining > min && remaining <= max, `remaining was ${remaining}`);
  }

  it("should release the lock restored from its token in another instance", async () => {
    const [producer, worker] = peerMutexes();
    const releaser = await producer.acquire({ ttlMs: 60_000 });

    const restored = worker.restoreReleaser(releaser.getToken());
    assert.strictEqual(await restored.isHeld(), true);

    await restored.release();
    assert.strictEqual(await producer.isLocked(), false);
    assert.strictEqual(await releaser.isHeld(), false);
  });

  it("should ignore tokens that never held the lock", async () => {
    const mutex = locks.mutex(MUTEX_NAME);
    await mutex.acquire();

    const unknown = mutex.restoreReleaser("unknown" as types.TMutexToken);
    await unknown.release();

    assert.strictEqual(await mutex.isLocked(), true);
    assert.strictEqual(await unknown.remainingTtl(), null);
    assert.strictEqual(await unknown.extend(1000), false);
  });

  it("should default to a finite TTL, independent of timeoutMs", async () => {
    const releaser = await locks.mutex(MUTEX_NAME).acquire({ timeoutMs: 100 });

    assertRemaining(await releaser.remainingTtl(), DEFAULT_TTL_MS - 1000, DEFAULT_TTL_MS);
  });

  it("should hold the lock with an Infinity TTL", async () => {
    const releaser = await locks.mutex(MUTEX_NAME).acquire({ ttlMs: Infinity });

    assert.strictEqual(await releaser.remainingTtl(), Infinity);
    assert.strictEqual(await locks.getRedisClient().pTTL(MUTEX_KEY), -1);
  });

  it("should hand an expired lock to a queued acquirer without any release", async () => {
    const [holderSide, waiterSide] = peerMutexes();
    const expiring = await holderSide.acquire({ ttlMs: 200 });

    const startedAt = Date.now();
    const waiter = await waiterSide.acquire({ timeoutMs: 3000 });
    const waitedMs = Date.now() - startedAt;

    assert.ok(waitedMs < 1000, `Should be woken by the expiry, not the timeout; waited ${waitedMs}ms`);

    await expiring.release();
    assert.strictEqual(await waiter.isHeld(), true, "The late holder must not unlock the waiter's lock");
  });

  it("should resolve waitForUnlock when the lock expires", async () => {
    const mutex = locks.mutex(MUTEX_NAME);
    await mutex.acquire({ ttlMs: 200 });

    const startedAt = Date.now();
    await mutex.waitForUnlock();

    assert.ok(Date.now() - startedAt < 1000);
  });

  it("should extend the lock from any instance and remove its TTL with Infinity", async () => {
    const [holderSide, workerSide] = peerMutexes();
    const releaser = await holderSide.tryAcquire({ ttlMs: 150 });
    assert.ok(releaser);

    const restored = workerSide.restoreReleaser(releaser.getToken());
    assert.strictEqual(await restored.extend(5000), true);
    assertRemaining(await releaser.remainingTtl(), 4000, 5000);

    assert.strictEqual(await restored.extend(Infinity), true);
    await sleep(200);

    assert.strictEqual(await releaser.remainingTtl(), Infinity);
  });

  it("should not extend an expired lock", async () => {
    const releaser = await locks.mutex(MUTEX_NAME).acquire({ ttlMs: 100 });
    await sleep(150);

    assert.strictEqual(await releaser.extend(1000), false);
    assert.strictEqual(await releaser.isHeld(), false);
  });

  it("should reject invalid TTLs without taking the lock", async () => {
    const mutex = locks.mutex(MUTEX_NAME);

    await assert.rejects(mutex.acquire({ ttlMs: 0 }));
    await assert.rejects(mutex.acquire({ ttlMs: 2 ** 31 }));
    assert.strictEqual(await mutex.isLocked(), false);
  });

  it("should turn releasers into no-ops and refuse to restore tokens once destroyed", async () => {
    const mutex = locks.mutex(MUTEX_NAME);
    const releaser = await mutex.acquire({ ttlMs: 60_000 });

    await mutex.destroy();

    assert.strictEqual(await releaser.isHeld(), false);
    await releaser.release();
    assert.throws(() => mutex.restoreReleaser(releaser.getToken()), LockNotFoundError);
  });
});
