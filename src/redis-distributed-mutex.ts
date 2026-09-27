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
import { IDistributedDeferred } from "./types";
import { DistributedReleaser } from "./distributed-releaser";
import { BaseDistributedLockPrimitive } from "./base-distributed-lock-primitive";
import { RedisScript } from "./redis-script";

/**
 * Only release if the lock key's value matches our lock token.
 */
const RELEASE_SCRIPT = new RedisScript(`
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  end
  return 0
`);

export class RedisDistributedMutex extends BaseDistributedLockPrimitive implements types.IDistributedMutex {
  public constructor(props: types.TDistributedMutexConstructorProps & {
    redisClient: RedisClientType;
  }) {
    assert.ok(props.name, "RedisDistributedMutex requires a non-empty name.");
    super({ ...props, name: `${ELockDisplayType.Mutex}:${props.name}` });
  }

  public async destroy(message?: string): Promise<void> {
    if (this.destroyed) {
      return;
    }

    this.destroyed = true;

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

  public async acquire(params?: types.TAcquireParams): Promise<types.IReleaser<types.TMutexToken>> {
    this.ensureAlive();

    await this.ensureSubscriber();

    // `??` and not `||`: timeoutMs 0 means "fail fast", not "use the default".
    const timeoutMs = params?.timeoutMs ?? DEFAULT_TTL_MS;
    // Redis PX must be positive; a zero wait timeout still needs a real lock TTL.
    const lockTtlMs = timeoutMs > 0 ? timeoutMs : DEFAULT_TTL_MS;

    const acquireToken = await this.acquireOnce(lockTtlMs);
    if (acquireToken) {
      return new DistributedReleaser<types.TMutexToken>(
        () => this.release(acquireToken),
        acquireToken as types.TMutexToken,
      );
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

  public async tryAcquire(params?: types.TAcquireParams): Promise<types.IReleaser<types.TMutexToken> | null> {
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

  public async cancel(errMessage?: string): Promise<void> {
    this.ensureAlive();

    const msg = `cancel:${errMessage ?? "Mutex cancelled"}`;
    await this.redisClient.publish(`${this.name}:cancel`, msg);
  }

  public async isLocked(): Promise<boolean> {
    this.ensureAlive();

    const val = await this.redisClient.get(this.name);
    return val !== null;
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

  protected async acquireOnce(timeoutMs: number): Promise<types.TAcquireToken | undefined> {
    const token = `${this.name}:${crypto.randomUUID()}` as types.TAcquireToken;

    const result = await this.redisClient.set(this.name, token, {
      NX: true,
      PX: timeoutMs,
    });

    if (result === "OK") {
      return token;
    }

    return undefined;
  }

  protected async release(token: types.TAcquireToken): Promise<void> {
    if (this.destroyed) {
      return;
    }

    const result = await RELEASE_SCRIPT.run(this.redisClient, {
      keys: [this.name],
      arguments: [token],
    });

    if (result === 1) {
      await this.redisClient.publish(`${this.name}:release`, token);
    }
  }

  private ensureAlive(): void {
    if (this.destroyed) {
      throw new LockNotFoundError(
        `${ELockDisplayType.Mutex} '${this.name}' does not exist`,
      );
    }
  }
}
