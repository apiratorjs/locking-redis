import { AcquireParams, IDistributedSemaphore, IReleaser } from "@apiratorjs/locking/dist/src/types";
import assert from "node:assert";
import { RedisClientType } from "redis";
import { IDistributedDeferred } from "./types";
import { types } from "@apiratorjs/locking";
import { DEFAULT_TTL_MS } from "./constants";
import crypto from "node:crypto";
import { DistributedReleaser } from "./distributed-releaser";
import { BaseDistributedPrimitive } from "./base-distributed-primitive";

export class RedisDistributedSemaphore extends BaseDistributedPrimitive implements IDistributedSemaphore {
  public readonly maxCount: number;

  public constructor(props: types.DistributedSemaphoreConstructorProps & {
    redisClient: RedisClientType;
  }) {
    super({ ...props, name: `semaphore:${props.name}` });
    const { maxCount } = props;
    assert.ok(maxCount > 0, "maxCount must be greater than 0");

    this.maxCount = maxCount;
  }

  public async destroy(): Promise<void> {
    if (this._isDestroyed) {
      return;
    }

    this._isDestroyed = true;

    await this._redisClient.del(this.name);

    if (this._redisSubscriber) {
      await this._redisSubscriber.unsubscribe(`${this.name}:cancel`);
      await this._redisSubscriber.unsubscribe(`${this.name}:release`);
      await this._redisSubscriber.unsubscribe(`${this.name}:destroy`);
      await this._redisSubscriber.disconnect();
      this._redisSubscriber = undefined;
    }

    await this._redisClient.publish(`${this.name}:destroy`, "destroyed");

    while (this._queue.length > 0) {
      const deferred = this._queue.shift()!;

      if (deferred.timer) {
        clearTimeout(deferred.timer);
        deferred.timer = null;
      }

      deferred.reject(new Error("Semaphore destroyed"));
    }
  }

  public async freeCount(): Promise<number> {
    await this._redisClient.zRemRangeByScore(this.name, "-inf", Date.now());
    const currentCount = await this._redisClient.zCard(this.name);
    return this.maxCount - currentCount;
  }

  public async acquire(params?: AcquireParams): Promise<IReleaser> {
    this.throwIfDestroyed();

    await this.ensureSubscriber();

    const { timeoutMs = DEFAULT_TTL_MS } = params ?? {};

    const acquireToken = await this.tryAcquire(timeoutMs);
    if (acquireToken) {
      return new DistributedReleaser(() => this.release(acquireToken), acquireToken);
    }

    // Return a promise that resolves once the lock is eventually acquired.
    return new Promise((resolve, reject) => {
      const deferred: IDistributedDeferred = {
        resolve,
        reject,
        ttlMs: timeoutMs,
        timer: null
      };

      deferred.timer = setTimeout(() => {
        const index = this._queue.indexOf(deferred);
        if (index !== -1) {
          this._queue.splice(index, 1);
        }

        reject(new Error("Timeout acquiring"));
      }, timeoutMs);

      this._queue.push(deferred);
    });
  }

  protected async release(token: types.AcquireToken): Promise<void> {
    this.throwIfDestroyed();

    const RELEASE_LUA = `
      local removed = redis.call('zrem', KEYS[1], ARGV[1])
      return removed
    `;

    const removed = await this._redisClient.eval(RELEASE_LUA, {
      keys: [this.name],
      arguments: [token]
    });

    // Only publish release message if a token was actually removed
    if (removed === 1) {
      await this._redisClient.publish(`${this.name}:release`, token);
    }
  }

  public async cancelAll(errMessage?: string): Promise<void> {
    this.throwIfDestroyed();

    const msg = `cancel:${errMessage ?? ""}`;
    await this._redisClient.publish(`${this.name}:cancel`, msg);
  }

  public async isLocked(): Promise<boolean> {
    const free = await this.freeCount();
    return free === 0;
  }

  public async runExclusive<T>(fn: () => Promise<T> | T): Promise<T>
  public async runExclusive<T>(params: types.AcquireParams, fn: () => Promise<T> | T): Promise<T>
  public async runExclusive<T>(...args: any[]): Promise<T> {
    let params: types.AcquireParams | undefined;
    let fn: () => Promise<T> | T;

    if (args.length === 1) {
      fn = args[0];
    } else if (args.length === 2) {
      params = args[0];
      fn = args[1];
    } else {
      throw new Error("Invalid arguments for runExclusive");
    }

    const releaser = await this.acquire(params);
    try {
      return await fn();
    } finally {
      await this.release(releaser.getToken());
    }
  }

  protected async tryAcquire(ttlMs: number): Promise<types.AcquireToken | undefined> {
    const ACQUIRE_LUA = `
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
    `;

    const token = `${this.name}:${crypto.randomUUID()}` as types.AcquireToken;
    const now = Date.now();
    const expiryTimestamp = now + ttlMs;

    const result = await this._redisClient.eval(ACQUIRE_LUA, {
      keys: [this.name],
      arguments: [
        now.toString(),                // Current time for expiry check
        this.maxCount.toString(),      // Max count of semaphore
        expiryTimestamp.toString(),    // Expiry timestamp for the new lock
        token,                         // Token for the lock
        (ttlMs * 3).toString()         // TTL for the key in Redis
      ]
    });

    return result === 1 ? token : undefined;
  }

  private throwIfDestroyed(): void {
    if (this._isDestroyed) {
      throw new Error("Semaphore has been destroyed");
    }
  }
}
