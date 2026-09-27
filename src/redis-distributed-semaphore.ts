import assert from "node:assert";
import crypto from "node:crypto";
import { RedisClientType } from "redis";
import {
  CancelledLockingError,
  ELockDisplayType,
  LockNotFoundError,
  TimeoutLockingError,
  types,
} from "@apiratorjs/locking";
import { DEFAULT_TTL_MS } from "./constants";
import { IDistributedDeferred, ILeaseOperations } from "./types";
import { BaseDistributedLockPrimitive } from "./base-distributed-lock-primitive";
import { RedisLeaseReleaser } from "./lease-releaser";
import { assertTtl, ttlArgument } from "./utils";
import {
  SEMAPHORE_ACQUIRE_SCRIPT,
  SEMAPHORE_COUNT_SCRIPT,
  SEMAPHORE_EXTEND_SCRIPT,
  SEMAPHORE_RELEASE_SCRIPT,
  SEMAPHORE_REMAINING_TTL_SCRIPT,
} from "./lua-scripts";

export class RedisDistributedSemaphore extends BaseDistributedLockPrimitive implements types.IDistributedSemaphore {
  public readonly maxCount: number;

  private readonly permits: ILeaseOperations<types.TSemaphoreToken>;

  public constructor(props: types.TDistributedSemaphoreConstructorProps & {
    redisClient: RedisClientType;
  }) {
    assert.ok(props.name, "RedisDistributedSemaphore requires a non-empty name.");
    assert.ok(props.maxCount > 0, "maxCount must be greater than 0");

    super({ ...props, name: `${ELockDisplayType.Semaphore}:${props.name}` });
    this.maxCount = props.maxCount;
    this.permits = {
      release: async (token) => this.release(token),
      extend: async (token, ttlMs) => this.extendPermit(token, ttlMs),
      remainingTtl: async (token) => this.permitRemainingTtl(token),
    };
  }

  public async waitForAnyUnlock(): Promise<void> {
    this.ensureAlive();

    return this.waitForUnlockEvent(async () => {
      // Treat destroy as unlocked so in-flight `:release` notify checks do not
      // throw LockNotFoundError via freeCount() after destroyed is set.
      if (this.destroyed) {
        return true;
      }

      return (await this.freeCount()) > 0;
    });
  }

  public async waitForFullyUnlock(): Promise<void> {
    this.ensureAlive();

    return this.waitForUnlockEvent(async () => {
      if (this.destroyed) {
        return true;
      }

      return (await this.freeCount()) === this.maxCount;
    });
  }

  public async destroy(message?: string): Promise<void> {
    if (this.destroyed) {
      return;
    }

    this.destroyed = true;
    this.clearExpiryWake();

    await this.redisClient.del(this.name);

    if (this.redisSubscriber) {
      await this.redisSubscriber.unsubscribe(`${this.name}:cancel`);
      await this.redisSubscriber.unsubscribe(`${this.name}:release`);
      await this.redisSubscriber.unsubscribe(`${this.name}:destroy`);
      await this.redisSubscriber.disconnect();
      this.redisSubscriber = undefined;
    }

    await this.redisClient.publish(`${this.name}:destroy`, "destroyed");

    this.rejectQueuedAcquirers(new CancelledLockingError(message ?? "Semaphore destroyed"));
    this.resolveUnlockWaiters();
  }

  public async freeCount(): Promise<number> {
    this.ensureAlive();

    const [heldCount, nextExpiryInMs] = await SEMAPHORE_COUNT_SCRIPT.run(this.redisClient, {
      keys: [this.name],
    }) as [number, number];

    this.scheduleExpiryWake(nextExpiryInMs);

    return this.maxCount - heldCount;
  }

  public async acquire(params?: types.TSemaphoreAcquireParams): Promise<types.ISemaphoreReleaser> {
    this.ensureAlive();

    const lockTtlMs = params?.ttlMs ?? DEFAULT_TTL_MS;
    assertTtl(lockTtlMs);

    await this.ensureSubscriber();

    // `??` and not `||`: timeoutMs 0 means "fail fast", not "use the default".
    const timeoutMs = params?.timeoutMs ?? DEFAULT_TTL_MS;

    const acquireToken = await this.acquireOnce(lockTtlMs);
    if (acquireToken) {
      return this.createReleaser(acquireToken);
    }

    if (timeoutMs === 0) {
      throw new TimeoutLockingError("Timeout acquiring");
    }

    return new Promise((resolve, reject) => {
      const deferred: IDistributedDeferred = {
        resolve,
        reject,
        ttlMs: lockTtlMs,
        timer: null,
      };

      deferred.timer = setTimeout(() => {
        const index = this.queue.indexOf(deferred);
        if (index !== -1) {
          this.queue.splice(index, 1);
        }

        reject(new TimeoutLockingError("Timeout acquiring"));
      }, timeoutMs);
      deferred.timer.unref();

      this.queue.push(deferred);
    });
  }

  public async tryAcquire(params?: types.TSemaphoreAcquireParams): Promise<types.ISemaphoreReleaser | null> {
    const timeoutMs = params?.timeoutMs ?? 0;

    try {
      return await this.acquire({ ...params, timeoutMs });
    } catch (error) {
      if (error instanceof TimeoutLockingError) {
        return null;
      }

      throw error;
    }
  }

  public restoreReleaser(token: types.TSemaphoreToken): types.ISemaphoreReleaser {
    this.ensureAlive();

    return this.createReleaser(token);
  }

  public async cancelAll(errMessage?: string): Promise<void> {
    this.ensureAlive();

    const msg = `cancel:${errMessage ?? "Semaphore cancelled"}`;
    await this.redisClient.publish(`${this.name}:cancel`, msg);
  }

  public async isLocked(): Promise<boolean> {
    this.ensureAlive();

    const free = await this.freeCount();
    return free === 0;
  }

  public async runExclusive<T>(fn: () => Promise<T> | T): Promise<T>;
  public async runExclusive<T>(params: types.TAcquireParams, fn: () => Promise<T> | T): Promise<T>;
  public async runExclusive<T>(...args: any[]): Promise<T> {
    let callback: () => Promise<T> | T;
    let params: types.TAcquireParams | undefined;

    if (args.length === 1) {
      callback = args[0];
    } else {
      params = args[0];
      callback = args[1];
    }

    const releaser = await this.acquire(params);
    try {
      return await callback();
    } finally {
      await releaser.release();
    }
  }

  protected async acquireOnce(ttlMs: number): Promise<types.TAcquireToken | undefined> {
    const token = `${this.name}:${crypto.randomUUID()}` as types.TAcquireToken;

    const [acquired, nextExpiryInMs] = await SEMAPHORE_ACQUIRE_SCRIPT.run(this.redisClient, {
      keys: [this.name],
      arguments: [this.maxCount.toString(), token, ttlArgument(ttlMs)],
    }) as [number, number?];

    if (acquired === 1) {
      return token;
    }

    this.scheduleExpiryWake(nextExpiryInMs ?? -1);
    return undefined;
  }

  protected createReleaser(token: types.TAcquireToken): types.ISemaphoreReleaser {
    return new RedisLeaseReleaser(this.permits, token as types.TSemaphoreToken);
  }

  protected async release(token: types.TAcquireToken): Promise<void> {
    if (this.destroyed) {
      return;
    }

    const released = await SEMAPHORE_RELEASE_SCRIPT.run(this.redisClient, {
      keys: [this.name],
      arguments: [token],
    });

    if (released === 1) {
      await this.redisClient.publish(`${this.name}:release`, token);
    }
  }

  private async extendPermit(token: types.TSemaphoreToken, ttlMs: number): Promise<boolean> {
    assertTtl(ttlMs);

    if (this.destroyed) {
      return false;
    }

    const extended = await SEMAPHORE_EXTEND_SCRIPT.run(this.redisClient, {
      keys: [this.name],
      arguments: [token, ttlArgument(ttlMs)],
    });

    return extended === 1;
  }

  private async permitRemainingTtl(token: types.TSemaphoreToken): Promise<number | null> {
    if (this.destroyed) {
      return null;
    }

    const remaining = await SEMAPHORE_REMAINING_TTL_SCRIPT.run(this.redisClient, {
      keys: [this.name],
      arguments: [token],
    }) as number;

    if (remaining === -2) {
      return null;
    }

    return remaining === -1 ? Infinity : remaining;
  }

  private ensureAlive(): void {
    if (this.destroyed) {
      throw new LockNotFoundError(
        `${ELockDisplayType.Semaphore} '${this.name}' does not exist`,
      );
    }
  }
}
