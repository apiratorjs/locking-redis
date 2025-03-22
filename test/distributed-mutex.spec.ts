import { after, before, beforeEach, describe, it } from "node:test";
import assert from "node:assert";
import { sleep } from "../src/utils";
import { DistributedMutex, types } from "@apiratorjs/locking";
import { createRedisLockFactory, IRedisLockFactory } from "../src";

const DISTRIBUTED_MUTEX_NAME = "shared-mutex";
const REDIS_URL = "redis://localhost:6379/0";

describe("DistributedMutex", () => {
  let factory: IRedisLockFactory;

  before(async () => {
    factory = await createRedisLockFactory({ url: REDIS_URL });

    DistributedMutex.factory = factory.createDistributedMutex;
  });

  after(async () => {
    await factory.getRedisClient().disconnect();
  });

  beforeEach(async () => {
    await factory.getRedisClient().flushDb();
  });

  it("should immediately acquire and release", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    assert.strictEqual(await mutex.isLocked(), false);

    const releaser = await mutex.acquire();
    assert.strictEqual(await mutex.isLocked(), true);

    await releaser.release();
    assert.strictEqual(await mutex.isLocked(), false);

    await mutex.destroy();
  });

  it("should wait for mutex to be available", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    const releaser = await mutex.acquire();

    let acquired = false;
    const acquirePromise = mutex.acquire().then(() => {
      acquired = true;
    });

    await sleep(300);
    assert.strictEqual(acquired, false, "Second acquire should be waiting");

    await releaser.release();
    await acquirePromise;
    assert.strictEqual(acquired, true, "Second acquire should succeed after release");

    await mutex.destroy();
  });

  it("should time out on acquire if mutex is not released", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    const releaser = await mutex.acquire();

    let error: Error | undefined;
    try {
      await mutex.acquire({ timeoutMs: 1_000 });
    } catch (err: any) {
      error = err;
    }

    assert.ok(error instanceof Error, "Error should be thrown on timeout");
    assert.strictEqual(error!.message, "Timeout acquiring");

    await mutex.destroy();
  });

  it("should cancel pending acquisitions", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    const releaser = await mutex.acquire();

    let error1: Error | undefined, error2: Error | undefined;
    const p1 = mutex.acquire().catch((err) => { error1 = err; });
    const p2 = mutex.acquire().catch((err) => { error2 = err; });

    await sleep(400);
    await mutex.cancel();

    await Promise.allSettled([p1, p2]);

    assert.strictEqual(error1!.message, "Mutex cancelled");
    assert.strictEqual(error2!.message, "Mutex cancelled");

    await mutex.destroy();
  });

  it("should gracefully handle multiple consecutive release calls", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    const releaser = await mutex.acquire();

    await releaser.release();
    await releaser.release();

    assert.strictEqual(await mutex.isLocked(), false);

    await mutex.destroy();
  });

  it("should limit concurrent access", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    let concurrent = 0;
    let maxConcurrent = 0;

    const tasks = Array.from({ length: 10 }).map(async () => {
      const releaser = await mutex.acquire();
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      // Simulate asynchronous work.
      await sleep(400);
      concurrent--;
      await releaser.release();
    });

    await Promise.all(tasks);
    assert.strictEqual(maxConcurrent, 1, "Max concurrent tasks should not exceed 1");

    await mutex.destroy();
  });

  it("should share state between two instances with the same name", async () => {
    const name = "sharedMutex";
    const mutex1 = new DistributedMutex({ name });
    const mutex2 = new DistributedMutex({ name });

    assert.strictEqual(await mutex1.isLocked(), false, "mutex1 should initially be unlocked");
    assert.strictEqual(await mutex2.isLocked(), false, "mutex2 should initially be unlocked");

    const releaser1 = await mutex1.acquire();
    assert.strictEqual(await mutex1.isLocked(), true, "After mutex1 acquire, mutex1 should be locked");
    assert.strictEqual(await mutex2.isLocked(), true, "After mutex1 acquire, mutex2 should be locked");

    let mutex2Acquired = false;
    let releaser2: types.IReleaser;
    const acquirePromise = mutex2.acquire().then((releaser) => {
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
    const mutex1 = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    const mutex2 = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });

    const releaser1 = await mutex1.acquire();

    let errorFromMutex2: Error | undefined;
    const pending = mutex2.acquire().catch((err) => { errorFromMutex2 = err; });

    await sleep(100);
    await mutex1.cancel();

    await pending;
    assert.ok(errorFromMutex2 instanceof Error, "Pending acquire should be cancelled with an error");
    assert.strictEqual(errorFromMutex2!.message, "Mutex cancelled");

    await releaser1.release();
    assert.strictEqual(await mutex1.isLocked(), false, "Mutex should be unlocked after release");

    await mutex1.destroy();
    await mutex2.destroy();
  });

  it("should correctly acquire and release using runExclusive", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });

    assert.strictEqual(await mutex.isLocked(), false);

    let sideEffect = false;
    await mutex.runExclusive(async () => {
      assert.strictEqual(await mutex.isLocked(), true);
      sideEffect = true;
    });

    assert.strictEqual(await mutex.isLocked(), false);
    assert.strictEqual(sideEffect, true);

    await mutex.destroy();
  });

  it("should release the lock even if the runExclusive callback throws", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    let errorThrown = false;

    try {
      await mutex.runExclusive(async () => {
        throw new Error("Something went wrong inside runExclusive callback");
      });
    } catch (err: any) {
      errorThrown = true;
    }

    assert.strictEqual(await mutex.isLocked(), false);
    assert.strictEqual(errorThrown, true);

    await mutex.destroy();
  });

  it("should not allow the same instance to acquire twice without releasing", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    const releaser = await mutex.acquire();

    let secondAcquireTimedOut = false;
    try {
      await mutex.acquire({ timeoutMs: 500 });
    } catch (err: any) {
      assert.strictEqual(err.message, "Timeout acquiring");
      secondAcquireTimedOut = true;
    }

    assert.strictEqual(secondAcquireTimedOut, true);

    await releaser.release();
    await mutex.destroy();
  });

  it("should allow acquisition by another instance after the lock expires naturally in Redis", async () => {
    const mutex1 = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });

    const releaser = await mutex1.acquire({ timeoutMs: 500 });

    await sleep(1000);

    const mutex2 = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    let acquired = false;
    try {
      await mutex2.acquire({ timeoutMs: 5000 });
      acquired = true;
    } finally {
      await releaser.release();
    }

    assert.strictEqual(acquired, true, "Should acquire after original lock's TTL expires");

    await mutex1.destroy();
    await mutex2.destroy();
  });

  it("should remove the lock and reject waiters when destroy is called while locked", async () => {
    const mutex1 = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    const releaser = await mutex1.acquire();

    const mutex2 = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    let mutex2Acquired = false;
    const p = mutex2.acquire().then(() => { mutex2Acquired = true; });

    // Wait while mutex2 subscription is established
    await sleep(200);

    await mutex1.destroy();

    let pError: Error | undefined;
    try {
      await p;
    } catch (err: any) {
      pError = err;
    }

    assert.ok(pError, "Second mutex should be rejected");
    assert.strictEqual(pError!.message, "Mutex destroyed");
    assert.strictEqual(mutex2Acquired, false);

    await mutex2.destroy();
  });

  it("should handle multiple waiters in the correct order", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });

    const releaser = await mutex.acquire();
    let acquiredOrder: number[] = [];

    const p1 = (async () => {
      const releaser2 = await mutex.acquire();
      acquiredOrder.push(1);
      await releaser2.release();
    })();

    const p2 = (async () => {
      const releaser3 = await mutex.acquire();
      acquiredOrder.push(2);
      await releaser3.release();
    })();

    const p3 = (async () => {
      const releaser4 = await mutex.acquire();
      acquiredOrder.push(3);
      await releaser4.release();
    })();

    // Wait a bit to ensure they are all queued
    await sleep(300);

    await releaser.release();

    await Promise.all([p1, p2, p3]);
    assert.deepStrictEqual(acquiredOrder, [1, 2, 3], "Queue should acquire in FIFO order");

    await mutex.destroy();
  });

  it("should be safe to call destroy multiple times", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    const releaser = await mutex.acquire();

    await mutex.destroy();
    await mutex.destroy();

    assert.ok(true, "Calling destroy() multiple times did not crash or throw");
  });

  it("should fail immediately if timeoutMs is 0 and mutex is locked", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });
    const releaser = await mutex.acquire();

    let error: any;
    try {
      await mutex.acquire({ timeoutMs: 1 });
    } catch (e) {
      error = e;
    }

    assert.ok(error, "Should throw immediately if already locked");
    assert.strictEqual(error.message, "Timeout acquiring");

    await releaser.release();
    await mutex.destroy();
  });

  it("should remove the lock and reject waiters when destroy is called while locked", async () => {
    const mutex = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });

    const releaser = await mutex.acquire();

    const mutex2 = new DistributedMutex({ name: DISTRIBUTED_MUTEX_NAME });

    let semaphore2Acquired = false;
    const p = mutex2.acquire().then(() => {
      semaphore2Acquired = true;
    });

    // Wait while mutex2 subscription is established
    await sleep(50);

    await mutex.destroy();

    let pError: Error | undefined;
    try {
      await p;
    } catch (err: any) {
      pError = err;
    }

    assert.ok(pError, "Second mutex should be rejected");
    assert.ok(pError!.message === "Mutex destroyed", "Error message should be 'Mutex destroyed'");
    assert.ok(!semaphore2Acquired, "Second mutex should not be acquired");
  });
});
