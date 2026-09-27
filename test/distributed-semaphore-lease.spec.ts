import { after, before, beforeEach, describe, it } from "node:test";
import * as assert from "node:assert";
import { LockNotFoundError, types } from "@apiratorjs/locking";
import { sleep } from "../src/utils";
import { DEFAULT_TTL_MS } from "../src/constants";
import { RedisDistributedLockManager, RedisDistributedSemaphore } from "../src";

const SEMAPHORE_NAME = "lease-semaphore";
const SEMAPHORE_KEY = `semaphore:${SEMAPHORE_NAME}`;
const REDIS_URL = "redis://localhost:6379/0";

describe("RedisDistributedSemaphore permits by token and TTL", () => {
  let locks: RedisDistributedLockManager;
  let peers: RedisDistributedSemaphore[] = [];

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

  /** Independent handles over the same Redis key (simulates separate processes). */
  function peerSemaphores(maxCount: number, count: number = 2): RedisDistributedSemaphore[] {
    const redisClient = locks.getRedisClient();
    const created = Array.from({ length: count }, () =>
      new RedisDistributedSemaphore({ name: SEMAPHORE_NAME, maxCount, redisClient }),
    );
    peers.push(...created);
    return created;
  }

  function assertRemaining(remaining: number | null, min: number, max: number): void {
    assert.ok(remaining !== null && remaining > min && remaining <= max, `remaining was ${remaining}`);
  }

  describe("tokens", () => {
    it("should release a permit restored from its token in another instance", async () => {
      const [producer, worker] = peerSemaphores(2);
      const releaser = await producer.acquire({ ttlMs: 60_000 });

      const restored = worker.restoreReleaser(releaser.getToken());
      assert.strictEqual(restored.getToken(), releaser.getToken());
      assert.strictEqual(await restored.isHeld(), true);

      await restored.release();
      assert.strictEqual(await producer.freeCount(), 2);
      assert.strictEqual(await releaser.isHeld(), false);
    });

    it("should release once no matter how many releasers share the token", async () => {
      const [first, second] = peerSemaphores(2);
      const held = await first.acquire();
      const other = await first.acquire();

      await second.restoreReleaser(held.getToken()).release();
      await first.restoreReleaser(held.getToken()).release();
      await held.release();

      assert.strictEqual(await first.freeCount(), 1, "Only the first permit comes back");
      assert.strictEqual(await other.isHeld(), true);
    });

    it("should ignore tokens that never held a permit", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);
      await s.acquire();

      const unknown = s.restoreReleaser("unknown" as types.TSemaphoreToken);
      await unknown.release();

      assert.strictEqual(await s.isLocked(), true);
      assert.strictEqual(await unknown.isHeld(), false);
      assert.strictEqual(await unknown.remainingTtl(), null);
      assert.strictEqual(await unknown.extend(1000), false);
    });

    it("should wake a queued acquirer in another instance when a restored releaser releases", async () => {
      const [holderSide, waiterSide] = peerSemaphores(1);
      const holder = await holderSide.acquire();

      const pending = waiterSide.acquire({ timeoutMs: 2000 });
      await sleep(50);

      await waiterSide.restoreReleaser(holder.getToken()).release();

      const waiter = await pending;
      assert.strictEqual(await waiter.isHeld(), true);
    });
  });

  describe("TTL", () => {
    it("should default to a finite TTL, independent of timeoutMs", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);

      const releaser = await s.acquire({ timeoutMs: 100 });
      assertRemaining(await releaser.remainingTtl(), DEFAULT_TTL_MS - 1000, DEFAULT_TTL_MS);
    });

    it("should report the remaining TTL", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);
      const releaser = await s.acquire({ ttlMs: 5000 });

      assertRemaining(await releaser.remainingTtl(), 4000, 5000);
    });

    it("should hold a permit with an Infinity TTL and keep the key without expiry", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);
      const releaser = await s.acquire({ ttlMs: Infinity });

      assert.strictEqual(await releaser.remainingTtl(), Infinity);
      assert.strictEqual(await locks.getRedisClient().pTTL(SEMAPHORE_KEY), -1);

      await releaser.release();
      assert.strictEqual(await locks.getRedisClient().exists(SEMAPHORE_KEY), 0);
    });

    it("should give the permit back when the TTL runs out", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);
      const releaser = await s.acquire({ ttlMs: 100 });

      assert.strictEqual(await s.isLocked(), true);
      await sleep(150);

      assert.strictEqual(await s.isLocked(), false);
      assert.strictEqual(await releaser.isHeld(), false);
    });

    it("should hand an expired permit to a queued acquirer without any release", async () => {
      const [holderSide, waiterSide] = peerSemaphores(1);
      await holderSide.acquire({ ttlMs: 200 });

      const startedAt = Date.now();
      const waiter = await waiterSide.acquire({ timeoutMs: 3000 });
      const waitedMs = Date.now() - startedAt;

      assert.ok(waitedMs < 1000, `Should be woken by the expiry, not the timeout; waited ${waitedMs}ms`);
      assert.strictEqual(await waiter.isHeld(), true);
    });

    it("should not let a late holder release the permit of whoever got it next", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);
      const expiring = await s.acquire({ ttlMs: 100 });

      const waiter = await s.acquire({ timeoutMs: 3000 });
      await expiring.release();

      assert.strictEqual(await s.isLocked(), true, "The waiter still holds the permit");
      assert.strictEqual(await waiter.isHeld(), true);
    });

    it("should resolve waitForAnyUnlock when a permit expires", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);
      await s.acquire({ ttlMs: 200 });

      const startedAt = Date.now();
      await s.waitForAnyUnlock();

      assert.ok(Date.now() - startedAt < 1000);
      assert.strictEqual(await s.isLocked(), false);
    });

    it("should start the TTL when the permit is granted, not when it is requested", async () => {
      const [holderSide, waiterSide] = peerSemaphores(1);
      const holder = await holderSide.acquire();

      const pending = waiterSide.acquire({ ttlMs: 1000, timeoutMs: 3000 });
      await sleep(500);
      await holder.release();

      const waiter = await pending;
      assertRemaining(await waiter.remainingTtl(), 800, 1000);
    });

    it("should extend a held permit, from any instance", async () => {
      const [holderSide, workerSide] = peerSemaphores(1);
      const releaser = await holderSide.acquire({ ttlMs: 150 });

      await sleep(100);
      assert.strictEqual(await workerSide.restoreReleaser(releaser.getToken()).extend(400), true);
      await sleep(150);

      assert.strictEqual(await releaser.isHeld(), true, "Would have expired without extend");

      await sleep(350);
      assert.strictEqual(await releaser.isHeld(), false);
      assert.strictEqual(await releaser.extend(400), false, "An expired permit cannot be extended");
    });

    it("should keep the key alive for as long as the longest permit", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 2);
      const short = await s.acquire({ ttlMs: 1000 });
      await s.acquire({ ttlMs: 5000 });

      const client = locks.getRedisClient();
      assertRemaining(await client.pTTL(SEMAPHORE_KEY), 4000, 5000);

      await short.extend(10_000);
      assertRemaining(await client.pTTL(SEMAPHORE_KEY), 9000, 10_000);
    });

    it("should remove the TTL when extended by Infinity", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);
      const releaser = await s.acquire({ ttlMs: 100 });

      assert.strictEqual(await releaser.extend(Infinity), true);
      await sleep(150);

      assert.strictEqual(await releaser.remainingTtl(), Infinity);
      assert.strictEqual(await locks.getRedisClient().pTTL(SEMAPHORE_KEY), -1);
    });

    it("should pass the TTL through tryAcquire", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);
      const releaser = await s.tryAcquire({ ttlMs: 5000 });

      assert.ok(releaser);
      assertRemaining(await releaser.remainingTtl(), 4000, 5000);
    });

    it("should reject invalid TTLs without taking a permit", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);

      await assert.rejects(s.acquire({ ttlMs: 0 }));
      await assert.rejects(s.acquire({ ttlMs: -1 }));
      await assert.rejects(s.acquire({ ttlMs: NaN }));
      await assert.rejects(s.acquire({ ttlMs: 2 ** 31 }));
      assert.strictEqual(await s.freeCount(), 1);

      const releaser = await s.acquire();
      await assert.rejects(releaser.extend(0));
    });
  });

  describe("destroy", () => {
    it("should turn releasers into no-ops and refuse to restore tokens", async () => {
      const s = locks.semaphore(SEMAPHORE_NAME, 1);
      const releaser = await s.acquire({ ttlMs: 60_000 });

      await s.destroy();

      assert.strictEqual(await releaser.isHeld(), false);
      assert.strictEqual(await releaser.extend(1000), false);
      await releaser.release();
      assert.throws(() => s.restoreReleaser(releaser.getToken()), LockNotFoundError);
    });

    it("should drop the permits for other instances too", async () => {
      const [first, second] = peerSemaphores(1);
      const releaser = await first.acquire({ ttlMs: 60_000 });

      await first.destroy();

      assert.strictEqual(await second.restoreReleaser(releaser.getToken()).isHeld(), false);
    });
  });
});
