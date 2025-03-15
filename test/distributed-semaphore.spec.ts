import { after, before, beforeEach, describe, it } from "node:test";
import assert from "node:assert";
import { createRedisLockFactory, IRedisLockFactory } from "../src";
import { DistributedMutex, DistributedSemaphore } from "@apiratorjs/locking";
import { sleep } from "../src/utils";

const DISTRIBUTED_SEMAPHORE_NAME = "shared-semaphore";
const REDIS_URL = "redis://localhost:6379/0";

describe("DistributedSemaphore", () => {
  let factory: IRedisLockFactory;

  before(async () => {
    factory = await createRedisLockFactory({ url: REDIS_URL });

    DistributedSemaphore.factory = factory.createDistributedSemaphore;
  });

  after(async () => {
    await factory.getRedisClient().disconnect();
  });

  beforeEach(async () => {
    await factory.getRedisClient().flushDb();
  });

  it("should immediately acquire and release", async () => {
    const semaphore = new DistributedSemaphore({ maxCount: 1, name: DISTRIBUTED_SEMAPHORE_NAME });
    assert.strictEqual(await semaphore.isLocked(), false);
    assert.strictEqual(await semaphore.freeCount(), 1);

    const releaser = await semaphore.acquire();
    assert.strictEqual(await semaphore.isLocked(), true);
    assert.strictEqual(await semaphore.freeCount(), 0);

    await releaser.release();
    assert.strictEqual(await semaphore.isLocked(), false);
    assert.strictEqual(await semaphore.freeCount(), 1);

    await semaphore.destroy();
  });

  it("should wait for semaphore to be available", async () => {
    const semaphore = new DistributedSemaphore({ maxCount: 1, name: DISTRIBUTED_SEMAPHORE_NAME });
    const releaser = await semaphore.acquire();

    let acquired = false;
    const acquirePromise = semaphore.acquire().then((releaser) => {
      acquired = true;
      return releaser;
    });

    await sleep(50);
    assert.strictEqual(acquired, false, "Second acquire should be waiting");

    await releaser.release();
    await acquirePromise;
    assert.strictEqual(acquired, true, "Second acquire should succeed after release");

    await semaphore.destroy();
  });

  it("should time out on acquire if semaphore is not released", async () => {
    const semaphore = new DistributedSemaphore({ maxCount: 1, name: DISTRIBUTED_SEMAPHORE_NAME });
    const releaser = await semaphore.acquire();

    let error: Error | undefined;
    try {
      await semaphore.acquire({ timeoutMs: 100 });
    } catch (err: any) {
      error = err;
    }

    assert.ok(error instanceof Error, "Error should be thrown on timeout");
    assert.strictEqual(error!.message, "Timeout acquiring");

    await semaphore.destroy();
  });

  it("should cancel all pending acquisitions", async () => {
    const semaphore = new DistributedSemaphore({ maxCount: 1, name: DISTRIBUTED_SEMAPHORE_NAME });
    const releaser = await semaphore.acquire();

    let error1: Error | undefined, error2: Error | undefined;
    const p1 = semaphore.acquire().catch((err) => { error1 = err; });
    const p2 = semaphore.acquire().catch((err) => { error2 = err; });

    // Allow the pending acquisitions to queue.
    await sleep(50);
    await semaphore.cancelAll();

    // Wait for both promises to settle.
    await Promise.allSettled([p1, p2]);

    assert.strictEqual(error1!.message, "Semaphore cancelled");
    assert.strictEqual(error2!.message, "Semaphore cancelled");

    await semaphore.destroy();
  });

  it("should not increase freeCount beyond maxCount on over-release", async () => {
    const semaphore = new DistributedSemaphore({ maxCount: 2, name: DISTRIBUTED_SEMAPHORE_NAME });

    const releaser1 = await semaphore.acquire();
    const releaser2 = await semaphore.acquire();

    await releaser1.release();
    await releaser2.release();

    assert.strictEqual(await semaphore.isLocked(), false);

    await releaser1.release();
    assert.strictEqual(await semaphore.isLocked(), false);

    await semaphore.destroy();
  });

  it("should limit concurrent access according to semaphore count", async () => {
    const semaphore = new DistributedSemaphore({ maxCount: 3, name: DISTRIBUTED_SEMAPHORE_NAME });
    let concurrent = 0;
    let maxConcurrent = 0;

    const tasks = Array.from({ length: 10 }).map(async () => {
      const releaser = await semaphore.acquire();
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      // Simulate asynchronous work.
      await sleep(50);
      concurrent--;
      await releaser.release();
    });

    await Promise.all(tasks);

    const freeCount = await semaphore.freeCount();
    assert.ok(freeCount === 3, "Semaphore should be free after all releases");
    assert.ok(maxConcurrent <= 3, "Max concurrent tasks should not exceed semaphore limit");

    await semaphore.destroy();
  });

  it("should share state between two instances with the same name", async () => {
    const name = "sharedSemaphore";
    const sem1 = new DistributedSemaphore({ maxCount: 1, name });
    const sem2 = new DistributedSemaphore({ maxCount: 1, name });

    assert.strictEqual(await sem1.freeCount(), 1, "sem1 initial freeCount should be 1");
    assert.strictEqual(await sem2.freeCount(), 1, "sem2 initial freeCount should be 1");

    const releaser1 = await sem1.acquire();
    assert.strictEqual(await sem1.freeCount(), 0, "After sem1 acquire, freeCount should be 0");
    assert.strictEqual(await sem2.freeCount(), 0, "After sem1 acquire, sem2 freeCount should be 0");

    let sem2Acquired = false;
    const acquirePromise = sem2.acquire().then((releaser) => {
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
    const name = "sharedSemaphoreCancel";
    const sem1 = new DistributedSemaphore({ maxCount: 1, name });
    const sem2 = new DistributedSemaphore({ maxCount: 1, name });

    const releaser1 = await sem1.acquire();

    let errorFromSem2: Error;
    const pending = sem2.acquire().catch((err) => { errorFromSem2 = err; });

    await sleep(50);
    await sem1.cancelAll();

    const releaser2 = await pending;
    assert.ok(errorFromSem2! instanceof Error, "Pending acquire should be rejected after cancelAll");
    assert.strictEqual(errorFromSem2!.message, "Semaphore cancelled");

    await releaser1.release();
    assert.strictEqual(await sem1.freeCount(), 1, "Semaphore should be free after release");

    await sem1.destroy();
    await sem2.destroy();
  });

  it("should return acquired distributed token after successful acquire", async () => {
    const semaphore = new DistributedSemaphore({ maxCount: 1, name: DISTRIBUTED_SEMAPHORE_NAME });

    const releaser = await semaphore.acquire();
    assert.ok(releaser);
    assert.ok(releaser.getToken().includes(semaphore.name));

    await semaphore.destroy();
  });

  it("should be safe to call destroy multiple times", async () => {
    const semaphore = new DistributedSemaphore({ maxCount: 1, name: DISTRIBUTED_SEMAPHORE_NAME });
    const releaser = await semaphore.acquire();

    await semaphore.destroy();
    await semaphore.destroy();

    assert.ok(true, "Calling destroy() multiple times did not crash or throw");
  });

  it("should remove the lock and reject waiters when destroy is called while locked", async () => {
    const semaphore = new DistributedSemaphore({ maxCount: 1, name: DISTRIBUTED_SEMAPHORE_NAME });
    const releaser = await semaphore.acquire();

    const semaphore2 = new DistributedSemaphore({ maxCount: 1, name: DISTRIBUTED_SEMAPHORE_NAME });
    let semaphore2Acquired = false;
    const p = semaphore2.acquire().then(() => { semaphore2Acquired = true; });

    // Wait while semaphore2 subscription is established
    await sleep(200);

    await semaphore.destroy();

    let pError: Error | undefined;
    try {
      await p;
    } catch (err: any) {
      pError = err;
    }

    assert.ok(pError, "Second semaphore should be rejected");
    assert.strictEqual(pError!.message, "Semaphore destroyed");
    assert.strictEqual(semaphore2Acquired, false);

    await semaphore2.destroy();
  });
});
