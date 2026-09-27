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
import { RedisLeaseReleaser } from "./lease-releaser";
import { BaseDistributedLockPrimitive } from "./base-distributed-lock-primitive";
import {
  MUTEX_ACQUIRE_SCRIPT,
  MUTEX_EXTEND_SCRIPT,
  MUTEX_RELEASE_SCRIPT,
  MUTEX_REMAINING_TTL_SCRIPT,
} from "./lua-scripts";
import { assertTtl, ttlArgument } from "./utils";

export class RedisDistributedMutex extends BaseDistributedLockPrimitive implements types.IDistributedMutex {
  private readonly lockOperations: ILeaseOperations<types.TMutexToken>;

  public constructor(props: types.TDistributedMutexConstructorProps & {
    redisClient: RedisClientType;
  }) {
    assert.ok(props.name, "RedisDistributedMutex requires a non-empty name.");
    super({ ...props, name: `${ELockDisplayType.Mutex}:${props.name}` });
    this.lockOperations = {
      release: async (token) => this.release(token),
      extend: async (token, ttlMs) => this.extendLock(token, ttlMs),
      remainingTtl: async (token) => this.lockRemainingTtl(token),
    };
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

    this.rejectQueuedAcquirers(new CancelledLockingError(message ?? "Mutex destroyed"));
    this.resolveUnlockWaiters();
  }

  public async acquire(params?: types.TMutexAcquireParams): Promise<types.IMutexReleaser> {
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

  public async tryAcquire(params?: types.TMutexAcquireParams): Promise<types.IMutexReleaser | null> {
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

  public restoreReleaser(token: types.TMutexToken): types.IMutexReleaser {
    this.ensureAlive();

    return this.createReleaser(token);
  }

  public async cancel(errMessage?: string): Promise<void> {
    this.ensureAlive();

    const msg = `cancel:${errMessage ?? "Mutex cancelled"}`;
    await this.redisClient.publish(`${this.name}:cancel`, msg);
  }

  public async isLocked(): Promise<boolean> {
    this.ensureAlive();

    const ttl = await this.redisClient.pTTL(this.name);
    // Whoever waits for the unlock has to look again once the lock expires.
    this.scheduleExpiryWake(ttl);

    return ttl !== -2;
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

  public async waitForUnlock(): Promise<void> {
    this.ensureAlive();

    return this.waitForUnlockEvent(async () => {
      // Treat destroy as unlocked so in-flight `:release` notify checks do not
      // throw LockNotFoundError via isLocked() after destroyed is set.
      if (this.destroyed) {
        return true;
      }

      return !(await this.isLocked());
    });
  }

  protected async acquireOnce(ttlMs: number): Promise<types.TAcquireToken | undefined> {
    const token = `${this.name}:${crypto.randomUUID()}` as types.TAcquireToken;

    const [acquired, lockTtlMs] = await MUTEX_ACQUIRE_SCRIPT.run(this.redisClient, {
      keys: [this.name],
      arguments: [token, ttlArgument(ttlMs)],
    }) as [number, number?];

    if (acquired === 1) {
      return token;
    }

    this.scheduleExpiryWake(lockTtlMs ?? -1);
    return undefined;
  }

  protected createReleaser(token: types.TAcquireToken): types.IMutexReleaser {
    return new RedisLeaseReleaser(this.lockOperations, token as types.TMutexToken);
  }

  protected async release(token: types.TAcquireToken): Promise<void> {
    if (this.destroyed) {
      return;
    }

    const result = await MUTEX_RELEASE_SCRIPT.run(this.redisClient, {
      keys: [this.name],
      arguments: [token],
    });

    if (result === 1) {
      await this.redisClient.publish(`${this.name}:release`, token);
    }
  }

  private async extendLock(token: types.TMutexToken, ttlMs: number): Promise<boolean> {
    assertTtl(ttlMs);

    if (this.destroyed) {
      return false;
    }

    const extended = await MUTEX_EXTEND_SCRIPT.run(this.redisClient, {
      keys: [this.name],
      arguments: [token, ttlArgument(ttlMs)],
    });

    return extended === 1;
  }

  private async lockRemainingTtl(token: types.TMutexToken): Promise<number | null> {
    if (this.destroyed) {
      return null;
    }

    const remaining = await MUTEX_REMAINING_TTL_SCRIPT.run(this.redisClient, {
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
        `${ELockDisplayType.Mutex} '${this.name}' does not exist`,
      );
    }
  }
}
