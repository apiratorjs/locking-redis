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

const RELEASE_SCRIPT = new RedisScript(`
  local removed = redis.call('zrem', KEYS[1], ARGV[1])
  return removed
`);

const ACQUIRE_SCRIPT = new RedisScript(`
  -- Remove expired locks
  redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1])

  -- Check if there are free slots and add the lock in one atomic operation
  local currentCount = redis.call('zcard', KEYS[1])
  if currentCount < tonumber(ARGV[2]) then
      redis.call('zadd', KEYS[1], ARGV[3], ARGV[4])

      -- Set the key to expire if it is not already set to expire sooner
      local keyTtl = redis.call('pttl', KEYS[1])
        if keyTtl < tonumber(ARGV[5]) then
            redis.call('pexpire', KEYS[1], ARGV[5])
        end
      return 1
  end
  return 0
`);

export class RedisDistributedSemaphore extends BaseDistributedLockPrimitive implements types.IDistributedSemaphore {
  public readonly maxCount: number;

  public constructor(props: types.TDistributedSemaphoreConstructorProps & {
    redisClient: RedisClientType;
  }) {
    assert.ok(props.name, "RedisDistributedSemaphore requires a non-empty name.");
    assert.ok(props.maxCount > 0, "maxCount must be greater than 0");

    super({ ...props, name: `${ELockDisplayType.Semaphore}:${props.name}` });
    this.maxCount = props.maxCount;
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

    await this.redisClient.zRemRangeByScore(this.name, "-inf", Date.now());
    const currentCount = await this.redisClient.zCard(this.name);
    return this.maxCount - currentCount;
  }

  public async acquire(params?: types.TAcquireParams): Promise<types.IReleaser<types.TSemaphoreToken>> {
    this.ensureAlive();

    await this.ensureSubscriber();

    // `??` and not `||`: timeoutMs 0 means "fail fast", not "use the default".
    const timeoutMs = params?.timeoutMs ?? DEFAULT_TTL_MS;
    // Lock member expiry must stay positive even when the wait timeout is 0.
    const lockTtlMs = timeoutMs > 0 ? timeoutMs : DEFAULT_TTL_MS;

    const acquireToken = await this.acquireOnce(lockTtlMs);
    if (acquireToken) {
      return new DistributedReleaser<types.TSemaphoreToken>(
        () => this.release(acquireToken),
        acquireToken as types.TSemaphoreToken,
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

  public async tryAcquire(params?: types.TAcquireParams): Promise<types.IReleaser<types.TSemaphoreToken> | null> {
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
    const now = Date.now();
    const expiryTimestamp = now + ttlMs;

    const result = await ACQUIRE_SCRIPT.run(this.redisClient, {
      keys: [this.name],
      arguments: [
        now.toString(),
        this.maxCount.toString(),
        expiryTimestamp.toString(),
        token,
        (ttlMs * 3).toString(),
      ],
    });

    return result === 1 ? token : undefined;
  }

  protected async release(token: types.TAcquireToken): Promise<void> {
    if (this.destroyed) {
      return;
    }

    const removed = await RELEASE_SCRIPT.run(this.redisClient, {
      keys: [this.name],
      arguments: [token],
    });

    if (removed === 1) {
      await this.redisClient.publish(`${this.name}:release`, token);
    }
  }

  private ensureAlive(): void {
    if (this.destroyed) {
      throw new LockNotFoundError(
        `${ELockDisplayType.Semaphore} '${this.name}' does not exist`,
      );
    }
  }
}
