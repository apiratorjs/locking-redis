import { after, before, beforeEach, describe, it } from "node:test";
import * as assert from "node:assert";
import {
  CancelledLockingError,
  LockNotFoundError,
  TimeoutLockingError,
  types,
} from "@apiratorjs/locking";
import { sleep } from "../src/utils";
import {
  RedisDistributedLockManager,
  RedisDistributedMutex,
} from "../src";

const DISTRIBUTED_MUTEX_NAME = "shared-mutex";
const REDIS_URL = "redis://localhost:6379/0";

describe("RedisDistributedMutex", () => {
  let locks: RedisDistributedLockManager;

  before(async () => {
    locks = await RedisDistributedLockManager.create({ url: REDIS_URL });
  });

  after(async () => {
    await locks.destroyAll();
    await locks.disconnect();
  });

  beforeEach(async () => {
    await locks.destroyAll();
    await locks.getRedisClient().flushDb();
  });

  function mutex(name: string = DISTRIBUTED_MUTEX_NAME): types.IDistributedMutex {
    return locks.mutex(name);
  }

  /** Two independent handles over the same Redis key (simulates two processes). */
  function peerMutexes(name: string = DISTRIBUTED_MUTEX_NAME): [RedisDistributedMutex, RedisDistributedMutex] {
    const redisClient = locks.getRedisClient();
    return [
      new RedisDistributedMutex({ name, redisClient }),
      new RedisDistributedMutex({ name, redisClient }),
    ];
  }

  it("should immediately acquire and release", async () => {
    const m = mutex();
    assert.strictEqual(await m.isLocked(), false);

    const releaser = await m.acquire();
    await sleep(100);
    assert.strictEqual(await m.isLocked(), true);

    await releaser.release();
    assert.strictEqual(await m.isLocked(), false);
  });

  it("should wait for mutex to be available", async () => {
    const m = mutex();
    const releaser = await m.acquire();

    let acquired = false;
    const acquirePromise = m.acquire().then(() => {
      acquired = true;
    });

    await sleep(300);
    assert.strictEqual(acquired, false, "Second acquire should be waiting");

    await releaser.release();
    await acquirePromise;
    assert.strictEqual(acquired, true, "Second acquire should succeed after release");
  });

  it("should time out on acquire if mutex is not released", async () => {
    const m = mutex();
    await m.acquire();

    let error: Error | undefined;
    try {
      await m.acquire({ timeoutMs: 1_000 });
    } catch (err: any) {
      error = err;
    }

    assert.ok(error instanceof TimeoutLockingError, "Error should be TimeoutLockingError");
    assert.strictEqual(error!.message, "Timeout acquiring");
  });

  it("should cancel pending acquisitions", async () => {
    const m = mutex();
    await m.acquire();

    let error1: Error | undefined;
    let error2: Error | undefined;
    const p1 = m.acquire().catch((err: Error) => {
      error1 = err;
    });
    const p2 = m.acquire().catch((err: Error) => {
      error2 = err;
    });

    await sleep(400);
    await m.cancel();

    await Promise.allSettled([p1, p2]);

    assert.ok(error1 instanceof CancelledLockingError);
    assert.ok(error2 instanceof CancelledLockingError);
    assert.strictEqual(error1!.message, "Mutex cancelled");
    assert.strictEqual(error2!.message, "Mutex cancelled");
  });

  it("should gracefully handle multiple consecutive release calls", async () => {
    const m = mutex();
    const releaser = await m.acquire();

    await releaser.release();
    await releaser.release();

    assert.strictEqual(await m.isLocked(), false);
  });

  it("should limit concurrent access", async () => {
    const m = mutex();
    let concurrent = 0;
    let maxConcurrent = 0;

    const tasks = Array.from({ length: 10 }).map(async () => {
      const releaser = await m.acquire();
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await sleep(400);
      concurrent--;
      await releaser.release();
    });

    await Promise.all(tasks);
    assert.strictEqual(maxConcurrent, 1, "Max concurrent tasks should not exceed 1");
  });

  it("should share state between two instances with the same name", async () => {
    const [mutex1, mutex2] = peerMutexes("sharedMutex");

    assert.strictEqual(await mutex1.isLocked(), false, "mutex1 should initially be unlocked");
    assert.strictEqual(await mutex2.isLocked(), false, "mutex2 should initially be unlocked");

    const releaser1 = await mutex1.acquire();
    assert.strictEqual(await mutex1.isLocked(), true, "After mutex1 acquire, mutex1 should be locked");
    assert.strictEqual(await mutex2.isLocked(), true, "After mutex1 acquire, mutex2 should be locked");

    let mutex2Acquired = false;
    let releaser2: types.IReleaser | undefined;
    const acquirePromise = mutex2.acquire().then((releaser: types.IReleaser) => {
      releaser2 = releaser;
      mutex2Acquired = true;
    });

    await sleep(100);
    assert.strictEqual(mutex2Acquired, false, "mutex2 acquire should be pending");

    await releaser1.release();
    await acquirePromise;
    assert.strictEqual(mutex2Acquired, true, "mutex2 should acquire after mutex1 releases");

    assert.strictEqual(await mutex1.isLocked(), true, "After mutex2 acquired, mutex1 should be locked");
    assert.strictEqual(await mutex2.isLocked(), true, "After mutex2 acquired, mutex2 should be locked");

    await releaser2!.release();
    assert.strictEqual(await mutex1.isLocked(), false, "After release, mutex1 should be unlocked");
    assert.strictEqual(await mutex2.isLocked(), false, "After release, mutex2 should be unlocked");

    await mutex1.destroy();
    await mutex2.destroy();
  });

  it("should cancel pending acquisitions across instances", async () => {
    const [mutex1, mutex2] = peerMutexes();

    const releaser1 = await mutex1.acquire();

    let errorFromMutex2: Error | undefined;
    const pending = mutex2.acquire().catch((err: Error) => {
      errorFromMutex2 = err;
    });

    await sleep(100);
    await mutex1.cancel();

    await pending;
    assert.ok(errorFromMutex2 instanceof CancelledLockingError);
    assert.strictEqual(errorFromMutex2!.message, "Mutex cancelled");

    await releaser1.release();
    assert.strictEqual(await mutex1.isLocked(), false, "Mutex should be unlocked after release");

    await mutex1.destroy();
    await mutex2.destroy();
  });

  it("should correctly acquire and release using runExclusive", async () => {
    const m = mutex();

    assert.strictEqual(await m.isLocked(), false);

    let sideEffect = false;
    await m.runExclusive(async () => {
      assert.strictEqual(await m.isLocked(), true);
      sideEffect = true;
    });

    assert.strictEqual(await m.isLocked(), false);
    assert.strictEqual(sideEffect, true);
  });

  it("should release the lock even if the runExclusive callback throws", async () => {
    const m = mutex();
    let errorThrown = false;

    try {
      await m.runExclusive(async () => {
        throw new Error("Something went wrong inside runExclusive callback");
      });
    } catch {
      errorThrown = true;
    }

    assert.strictEqual(await m.isLocked(), false);
    assert.strictEqual(errorThrown, true);
  });

  it("should not allow the same instance to acquire twice without releasing", async () => {
    const m = mutex();
    const releaser = await m.acquire();

    let secondAcquireTimedOut = false;
    try {
      await m.acquire({ timeoutMs: 500 });
    } catch (err: any) {
      assert.ok(err instanceof TimeoutLockingError);
      assert.strictEqual(err.message, "Timeout acquiring");
      secondAcquireTimedOut = true;
    }

    assert.strictEqual(secondAcquireTimedOut, true);

    await releaser.release();
  });

  it("should allow acquisition by another instance after the lock expires naturally in Redis", async () => {
    const [mutex1, mutex2] = peerMutexes();

    const releaser = await mutex1.acquire({ timeoutMs: 500 });

    await sleep(1000);

    let acquired = false;
    try {
      await mutex2.acquire({ timeoutMs: 5000 });
      acquired = true;
    } finally {
      await releaser.release().catch(() => undefined);
    }

    assert.strictEqual(acquired, true, "Should acquire after original lock's TTL expires");

    await mutex1.destroy();
    await mutex2.destroy();
  });

  it("should remove the lock and reject waiters when destroy is called while locked", async () => {
    const [mutex1, mutex2] = peerMutexes();
    await mutex1.acquire();

    let mutex2Acquired = false;
    const p = mutex2.acquire().then(() => {
      mutex2Acquired = true;
    });

    await sleep(200);

    await mutex1.destroy();

    let pError: Error | undefined;
    try {
      await p;
    } catch (err: any) {
      pError = err;
    }

    assert.ok(pError instanceof CancelledLockingError, "Second mutex should be rejected");
    assert.strictEqual(pError!.message, "Mutex destroyed");
    assert.strictEqual(mutex2Acquired, false);

    await mutex2.destroy();
  });

  it("should handle multiple waiters in the correct order", async () => {
    const m = mutex();

    const releaser = await m.acquire();
    const acquiredOrder: number[] = [];

    const p1 = (async () => {
      const releaser2 = await m.acquire();
      acquiredOrder.push(1);
      await releaser2.release();
    })();

    const p2 = (async () => {
      const releaser3 = await m.acquire();
      acquiredOrder.push(2);
      await releaser3.release();
    })();

    const p3 = (async () => {
      const releaser4 = await m.acquire();
      acquiredOrder.push(3);
      await releaser4.release();
    })();

    await sleep(300);

    await releaser.release();

    await Promise.all([p1, p2, p3]);
    assert.deepStrictEqual(acquiredOrder, [1, 2, 3], "Queue should acquire in FIFO order");
  });

  it("should be safe to call destroy multiple times", async () => {
    const m = mutex();
    await m.acquire();

    await m.destroy();
    await m.destroy();

    assert.strictEqual(m.isDestroyed, true);
  });

  it("should fail immediately if timeoutMs is 0 and mutex is locked", async () => {
    const m = mutex();
    const releaser = await m.acquire();

    let error: Error | undefined;
    try {
      await m.acquire({ timeoutMs: 0 });
    } catch (e: any) {
      error = e;
    }

    assert.ok(error instanceof TimeoutLockingError, "Should throw immediately if already locked");
    assert.strictEqual(error!.message, "Timeout acquiring");

    await releaser.release();
  });

  it("should wait for the mutex to be unlocked", async () => {
    const m = mutex();
    const releaser = await m.acquire();

    assert.strictEqual(await m.isLocked(), true, "Mutex should be locked");

    setTimeout(async () => {
      await releaser.release();
    }, 200);

    assert.strictEqual(await m.isLocked(), true, "Mutex should be locked");

    await m.waitForUnlock();

    assert.strictEqual(await m.isLocked(), false, "Mutex should be unlocked");
  });

  it("should still wake queued acquirers after waitForUnlock has resolved", async () => {
    const m = mutex();

    const first = await m.acquire();
    const unlockPromise = m.waitForUnlock();
    await first.release();
    await unlockPromise;

    const second = await m.acquire();
    const queued = m.acquire({ timeoutMs: 2000 });

    setTimeout(async () => {
      await second.release();
    }, 100);

    const releaser = await queued;
    await releaser.release();
  });

  it("should resolve pending waitForUnlock when the mutex is destroyed", async () => {
    const m = mutex();

    await m.acquire();
    const unlockPromise = m.waitForUnlock();

    setTimeout(async () => {
      await m.destroy();
    }, 100);

    await unlockPromise;
  });

  it("should resolve waitForUnlock when an in-flight release notify races with destroy", async () => {
    class HarnessMutex extends RedisDistributedMutex {
      public markDestroyed(): void {
        this.destroyed = true;
      }

      public async callNotifyUnlockWaiters(): Promise<void> {
        await this.notifyUnlockWaiters();
      }

      public async cleanupSubscriber(): Promise<void> {
        // destroy() no-ops once destroyed is set; tear down the subscriber here.
        if (this.redisSubscriber) {
          await this.redisSubscriber.unsubscribe(`${this.name}:cancel`);
          await this.redisSubscriber.unsubscribe(`${this.name}:release`);
          await this.redisSubscriber.unsubscribe(`${this.name}:destroy`);
          await this.redisSubscriber.disconnect();
          this.redisSubscriber = undefined;
        }

        await this.redisClient.del(this.name);
      }
    }

    const m = new HarnessMutex({
      name: DISTRIBUTED_MUTEX_NAME,
      redisClient: locks.getRedisClient(),
    });

    try {
      await m.acquire();
      const unlockPromise = m.waitForUnlock();

      // Simulate destroy flipping the flag before resolveUnlockWaiters runs, while
      // a `:release` handler is still notifying waiters.
      m.markDestroyed();
      await m.callNotifyUnlockWaiters();

      await unlockPromise;
    } finally {
      await m.cleanupSubscriber();
    }
  });

  it("should resolve waitForUnlock when release and destroy run concurrently", async () => {
    for (let i = 0; i < 20; i++) {
      const m = mutex(`release-destroy-race-${i}`);
      const releaser = await m.acquire();
      const unlockPromise = m.waitForUnlock();

      await Promise.all([releaser.release(), m.destroy()]);
      await unlockPromise;
    }
  });

  it("should keep working after the server script cache is flushed", async () => {
    const m = mutex();

    const first = await m.acquire();
    await first.release();
    await locks.getRedisClient().scriptFlush();

    const second = await m.acquire();
    assert.strictEqual(await m.isLocked(), true, "Mutex should be locked");

    await second.release();
    assert.strictEqual(await m.isLocked(), false, "Mutex should be unlocked");
  });

  it("should not settle waitForUnlock when acquisitions are cancelled", async () => {
    const m = mutex();

    const releaser = await m.acquire();
    const unlockPromise = m.waitForUnlock();

    await sleep(50);
    await m.cancel("Cancelled by test");

    let settled = false;
    void unlockPromise.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );

    await sleep(100);
    assert.strictEqual(settled, false, "cancel() must not settle waitForUnlock - the mutex is still held");

    await releaser.release();
    await unlockPromise;
  });

  describe("tryAcquire", () => {
    it("should acquire a free mutex and return a working releaser", async () => {
      const m = mutex();

      const releaser = await m.tryAcquire();
      assert.ok(releaser);
      assert.strictEqual(await m.isLocked(), true);

      await releaser.release();
      assert.strictEqual(await m.isLocked(), false);
    });

    it("should return null right away when the mutex is locked", async () => {
      const [a, b] = peerMutexes();

      try {
        const held = await a.acquire();

        const start = Date.now();
        const releaser = await b.tryAcquire();
        assert.strictEqual(releaser, null);
        assert.ok(Date.now() - start < 500, "tryAcquire should not wait by default");

        await held.release();
      } finally {
        await a.destroy();
        await b.destroy();
      }
    });

    it("should wait up to timeoutMs and acquire once the mutex is released", async () => {
      const m = mutex();
      const held = await m.acquire();

      const tryPromise = m.tryAcquire({ timeoutMs: 2000 });
      await sleep(200);
      await held.release();

      const releaser = await tryPromise;
      assert.ok(releaser);
      await releaser.release();
    });

    it("should return null when timeoutMs elapses", async () => {
      const m = mutex();
      await m.acquire();

      assert.strictEqual(await m.tryAcquire({ timeoutMs: 200 }), null);
    });

    it("should still throw on cancellation", async () => {
      const m = mutex();
      await m.acquire();

      const rejection = assert.rejects(m.tryAcquire({ timeoutMs: 5000 }), CancelledLockingError);
      await sleep(100);
      await m.cancel();

      await rejection;
    });

    it("should throw LockNotFoundError on a destroyed mutex", async () => {
      const m = mutex();
      await m.destroy();

      await assert.rejects(m.tryAcquire(), LockNotFoundError);
    });
  });
});
