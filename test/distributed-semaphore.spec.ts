import { after, before, beforeEach, describe, it } from "node:test";
import * as assert from "node:assert";
import {
  CancelledLockingError,
  TimeoutLockingError,
  types,
} from "@apiratorjs/locking";
import { sleep } from "../src/utils";
import {
  RedisDistributedLockManager,
  RedisDistributedSemaphore,
} from "../src";

const DISTRIBUTED_SEMAPHORE_NAME = "shared-semaphore";
const REDIS_URL = "redis://localhost:6379/0";

describe("RedisDistributedSemaphore", () => {
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

  function semaphore(
    maxCount: number = 1,
    name: string = DISTRIBUTED_SEMAPHORE_NAME,
  ): types.IDistributedSemaphore {
    return locks.semaphore(name, maxCount);
  }

  /** Two independent handles over the same Redis key (simulates two processes). */
  function peerSemaphores(
    maxCount: number = 1,
    name: string = DISTRIBUTED_SEMAPHORE_NAME,
  ): [RedisDistributedSemaphore, RedisDistributedSemaphore] {
    const redisClient = locks.getRedisClient();
    return [
      new RedisDistributedSemaphore({ name, maxCount, redisClient }),
      new RedisDistributedSemaphore({ name, maxCount, redisClient }),
    ];
  }

  it("should immediately acquire and release", async () => {
    const s = semaphore();
    assert.strictEqual(await s.isLocked(), false);
    assert.strictEqual(await s.freeCount(), 1);

    const releaser = await s.acquire();
    assert.strictEqual(await s.isLocked(), true);
    assert.strictEqual(await s.freeCount(), 0);

    await releaser.release();
    assert.strictEqual(await s.isLocked(), false);
    assert.strictEqual(await s.freeCount(), 1);
  });

  it("should wait for semaphore to be available", async () => {
    const s = semaphore();
    const releaser = await s.acquire();

    let acquired = false;
    const acquirePromise = s.acquire().then((r: types.IReleaser) => {
      acquired = true;
      return r;
    });

    await sleep(50);
    assert.strictEqual(acquired, false, "Second acquire should be waiting");

    await releaser.release();
    await acquirePromise;
    assert.strictEqual(acquired, true, "Second acquire should succeed after release");
  });

  it("should time out on acquire if semaphore is not released", async () => {
    const s = semaphore();
    await s.acquire();

    let error: Error | undefined;
    try {
      await s.acquire({ timeoutMs: 100 });
    } catch (err: any) {
      error = err;
    }

    assert.ok(error instanceof TimeoutLockingError, "Error should be TimeoutLockingError");
    assert.strictEqual(error!.message, "Timeout acquiring");
  });

  it("should cancel all pending acquisitions", async () => {
    const s = semaphore();
    await s.acquire();

    let error1: Error | undefined;
    let error2: Error | undefined;
    const p1 = s.acquire().catch((err: Error) => {
      error1 = err;
    });
    const p2 = s.acquire().catch((err: Error) => {
      error2 = err;
    });

    await sleep(50);
    await s.cancelAll();

    await Promise.allSettled([p1, p2]);

    assert.ok(error1 instanceof CancelledLockingError);
    assert.ok(error2 instanceof CancelledLockingError);
    assert.strictEqual(error1!.message, "Semaphore cancelled");
    assert.strictEqual(error2!.message, "Semaphore cancelled");
  });

  it("should not increase freeCount beyond maxCount on over-release", async () => {
    const s = semaphore(2);

    const releaser1 = await s.acquire();
    const releaser2 = await s.acquire();

    await releaser1.release();
    await releaser2.release();

    assert.strictEqual(await s.isLocked(), false);

    await releaser1.release();
    assert.strictEqual(await s.isLocked(), false);
    assert.strictEqual(await s.freeCount(), 2);
  });

  it("should limit concurrent access according to semaphore count", async () => {
    const s = semaphore(3);
    let concurrent = 0;
    let maxConcurrent = 0;

    const tasks = Array.from({ length: 10 }).map(async () => {
      const releaser = await s.acquire();
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await sleep(50);
      concurrent--;
      await releaser.release();
    });

    await Promise.all(tasks);

    const freeCount = await s.freeCount();
    assert.ok(freeCount === 3, "Semaphore should be free after all releases");
    assert.ok(maxConcurrent <= 3, "Max concurrent tasks should not exceed semaphore limit");
  });

  it("should share state between two instances with the same name", async () => {
    const [sem1, sem2] = peerSemaphores(1, "sharedSemaphore");

    assert.strictEqual(await sem1.freeCount(), 1, "sem1 initial freeCount should be 1");
    assert.strictEqual(await sem2.freeCount(), 1, "sem2 initial freeCount should be 1");

    const releaser1 = await sem1.acquire();
    assert.strictEqual(await sem1.freeCount(), 0, "After sem1 acquire, freeCount should be 0");
    assert.strictEqual(await sem2.freeCount(), 0, "After sem1 acquire, sem2 freeCount should be 0");

    let sem2Acquired = false;
    const acquirePromise = sem2.acquire().then((releaser: types.IReleaser) => {
      sem2Acquired = true;
      return releaser;
    });

    await sleep(50);
    assert.strictEqual(sem2Acquired, false, "sem2 acquire should be pending");

    await releaser1.release();
    const releaser2 = await acquirePromise;
    assert.strictEqual(sem2Acquired, true, "sem2 should acquire after sem1 releases");

    assert.strictEqual(await sem1.freeCount(), 0, "After sem2 acquired, freeCount should be 0");
    assert.strictEqual(await sem2.freeCount(), 0, "After sem2 acquired, freeCount should be 0");

    await releaser2.release();
    assert.strictEqual(await sem1.freeCount(), 1, "After release, freeCount should be back to 1 (sem1)");
    assert.strictEqual(await sem2.freeCount(), 1, "After release, freeCount should be back to 1 (sem2)");

    await sem1.destroy();
    await sem2.destroy();
  });

  it("should cancel pending acquisitions across instances", async () => {
    const [sem1, sem2] = peerSemaphores(1, "sharedSemaphoreCancel");

    const releaser1 = await sem1.acquire();

    let errorFromSem2: Error | undefined;
    const pending = sem2.acquire().catch((err: Error) => {
      errorFromSem2 = err;
    });

    await sleep(50);
    await sem1.cancelAll();

    await pending;
    assert.ok(errorFromSem2 instanceof CancelledLockingError);
    assert.strictEqual(errorFromSem2!.message, "Semaphore cancelled");

    await releaser1.release();
    assert.strictEqual(await sem1.freeCount(), 1, "Semaphore should be free after release");

    await sem1.destroy();
    await sem2.destroy();
  });

  it("should return acquired distributed token after successful acquire", async () => {
    const s = semaphore();

    const releaser = await s.acquire();
    assert.ok(releaser);
    assert.ok(releaser.getToken().includes(s.name));
  });

  it("should be safe to call destroy multiple times", async () => {
    const s = semaphore();
    await s.acquire();

    await s.destroy();
    await s.destroy();

    assert.strictEqual(s.isDestroyed, true);
  });

  it("should remove the lock and reject waiters when destroy is called while locked", async () => {
    const [semaphore1, semaphore2] = peerSemaphores();
    await semaphore1.acquire();

    let semaphore2Acquired = false;
    const p = semaphore2.acquire().then(() => {
      semaphore2Acquired = true;
    });

    await sleep(50);

    await semaphore1.destroy();

    let pError: Error | undefined;
    try {
      await p;
    } catch (err: any) {
      pError = err;
    }

    assert.ok(pError instanceof CancelledLockingError, "Second semaphore should be rejected");
    assert.strictEqual(pError!.message, "Semaphore destroyed");
    assert.strictEqual(semaphore2Acquired, false);

    await semaphore2.destroy();
  });

  it("should wait for the semaphore to be unlocked", async () => {
    const s = semaphore();
    const releaser = await s.acquire();

    assert.strictEqual(await s.isLocked(), true, "Semaphore should be locked");

    setTimeout(async () => {
      await releaser.release();
    }, 200);

    assert.strictEqual(await s.isLocked(), true, "Semaphore should be locked");

    await s.waitForAnyUnlock();

    assert.strictEqual(await s.isLocked(), false, "Semaphore should be unlocked");
  });

  it("should wait for the semaphore to be unlocked of first 3 slots of 5", async () => {
    const s = semaphore(5);
    const releaser = await s.acquire();
    const releaser2 = await s.acquire();
    const releaser3 = await s.acquire();
    const releaser4 = await s.acquire();
    const releaser5 = await s.acquire();

    assert.strictEqual(await s.isLocked(), true, "Semaphore should be locked");
    assert.strictEqual(await s.freeCount(), 0, "Semaphore should have no free slots");

    const releases = (async () => {
      await sleep(100);
      await releaser.release();
      await releaser2.release();
      await releaser3.release();
    })();

    await s.waitForAnyUnlock();
    // waitForAnyUnlock may settle after the first free slot; wait for the batch.
    await releases;

    assert.strictEqual(await s.freeCount(), 3, "Semaphore should have 3 slots free");

    await releaser4.release();
    await releaser5.release();
  });

  it("should wait for the semaphore to be fully unlocked", async () => {
    const s = semaphore(5);
    const releaser = await s.acquire();
    const releaser2 = await s.acquire();
    const releaser3 = await s.acquire();
    const releaser4 = await s.acquire();
    const releaser5 = await s.acquire();

    assert.strictEqual(await s.isLocked(), true, "Semaphore should be locked");
    assert.strictEqual(await s.freeCount(), 0, "Semaphore should have no free slots");

    setTimeout(async () => {
      await releaser.release();
      await releaser2.release();
      await releaser3.release();
    }, 100);

    setTimeout(async () => {
      await releaser4.release();
      await releaser5.release();
    }, 200);

    await s.waitForFullyUnlock();

    assert.strictEqual(await s.freeCount(), 5, "Semaphore should be fully unlocked");
  });

  it("should still wake queued acquirers after waitForAnyUnlock has resolved", async () => {
    const s = semaphore();

    const first = await s.acquire();
    const unlockPromise = s.waitForAnyUnlock();
    await first.release();
    await unlockPromise;

    const second = await s.acquire();
    const queued = s.acquire({ timeoutMs: 2000 });

    setTimeout(async () => {
      await second.release();
    }, 100);

    const releaser = await queued;
    await releaser.release();
  });

  it("should keep working after the server script cache is flushed", async () => {
    const s = semaphore(2);

    const first = await s.acquire();
    await first.release();
    await locks.getRedisClient().scriptFlush();

    const second = await s.acquire();
    assert.strictEqual(await s.freeCount(), 1, "Semaphore should have 1 slot free");

    await second.release();
    assert.strictEqual(await s.freeCount(), 2, "Semaphore should have 2 slots free");
  });

  it("should resolve pending waitForAnyUnlock when the semaphore is destroyed", async () => {
    const s = semaphore();

    await s.acquire();
    const unlockPromise = s.waitForAnyUnlock();

    setTimeout(async () => {
      await s.destroy();
    }, 100);

    await unlockPromise;
  });

  it("should resolve waitForAnyUnlock when an in-flight release notify races with destroy", async () => {
    class HarnessSemaphore extends RedisDistributedSemaphore {
      public markDestroyed(): void {
        this.destroyed = true;
      }

      public async callNotifyUnlockWaiters(): Promise<void> {
        await this.notifyUnlockWaiters();
      }

      public async cleanupSubscriber(): Promise<void> {
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

    const s = new HarnessSemaphore({
      name: DISTRIBUTED_SEMAPHORE_NAME,
      maxCount: 1,
      redisClient: locks.getRedisClient(),
    });

    try {
      await s.acquire();
      const unlockPromise = s.waitForAnyUnlock();

      s.markDestroyed();
      await s.callNotifyUnlockWaiters();

      await unlockPromise;
    } finally {
      await s.cleanupSubscriber();
    }
  });

  it("should resolve waitForAnyUnlock when release and destroy run concurrently", async () => {
    for (let i = 0; i < 20; i++) {
      const s = semaphore(1, `release-destroy-race-${i}`);
      const releaser = await s.acquire();
      const unlockPromise = s.waitForAnyUnlock();

      await Promise.all([releaser.release(), s.destroy()]);
      await unlockPromise;
    }
  });
});
