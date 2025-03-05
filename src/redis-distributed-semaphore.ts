import { AcquireParams, IDistributedSemaphore } from "@apiratorjs/locking/dist/src/types";
import assert from "node:assert";
import { RedisClientType } from "redis";
import { IDistributedDeferred } from "./types";
import { types } from "@apiratorjs/locking";
import { DEFAULT_TTL_MS } from "./constants";
import crypto from "node:crypto";

export class RedisDistributedSemaphore implements IDistributedSemaphore {
  public readonly name: string;
  public readonly implementation: string = "redis";
  public readonly maxCount: number;

  private readonly _redisClient: RedisClientType;
  private _redisSubscriber?: RedisClientType;
  private _queue: IDistributedDeferred[];
  private _isDestroyed: boolean = false;

  /**
   * We store each active token in a set named `<name>:owners`,
   * and each token's TTL is enforced by a key named `<name>:token:<uuid>`.
   */
  private readonly _ownersKey: string;       // e.g. "mySemaphore:owners"
  private readonly _tokenKeyPrefix: string;  // e.g. "mySemaphore:token:"

  public constructor(props: types.DistributedSemaphoreConstructorProps & {
    redisClient: RedisClientType;
  }) {
    const { maxCount, name, redisClient } = props;
    assert.ok(maxCount > 0, "maxCount must be greater than 0");
    assert.ok(name, "name must be provided");

    this.maxCount = maxCount;
    this.name = `semaphore:${name}`;
    this._redisClient = redisClient;
    this._queue = [];
    this._ownersKey = `${name}:owners`;
    this._tokenKeyPrefix = `${name}:token:`;
  }

  public get isDestroyed(): boolean {
    return this._isDestroyed;
  };

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
    const currentStr = await this._redisClient.get(this.name);
    const current = currentStr ? Number(currentStr) : 0;
    return this.maxCount - current;
  }

  public async acquire(params?: AcquireParams): Promise<types.AcquiredDistributedToken> {
    this.throwIfDestroyed();

    await this.ensureSubscriber();

    const { timeoutMs = DEFAULT_TTL_MS } = params ?? {};

    const acquiredToken = await this.tryAcquire(timeoutMs);
    if (acquiredToken) {
      return acquiredToken;
    }

    // Return a promise that resolves once the lock is eventually acquired.
    return new Promise((resolve, reject) => {
      const deferred: IDistributedDeferred = {
        resolve: () => resolve(acquiredToken as string),
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

  public async release(token: types.AcquiredDistributedToken): Promise<void> {
    this.throwIfDestroyed();

    const RELEASE_LUA = `
      local removed = redis.call("SREM", KEYS[1], ARGV[1])
      if removed == 1 then
        local tokenKey = ARGV[2] .. ARGV[1]
        redis.call("DEL", tokenKey)
        return 1
      else
        return 0
      end
    `;

    const result = await this._redisClient.eval(RELEASE_LUA, {
      keys: [this._ownersKey],
      arguments: [
        token,
        this._tokenKeyPrefix
      ]
    });

    if (result !== 1) {
      return;
    }

    await this._redisClient.publish(`${this.name}:release`, "release");
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

    const acquiredToken = await this.acquire(params);
    try {
      return await fn();
    } finally {
      await this.release(acquiredToken);
    }
  }

  private async ensureSubscriber(): Promise<void> {
    if (this._redisSubscriber) {
      return;
    }

    this._redisSubscriber = this._redisClient.duplicate();
    await this._redisSubscriber.connect();

    await this._redisSubscriber.subscribe(`${this.name}:cancel`, (message) => {
      if (!message.startsWith("cancel")) {
        return;
      }

      const errMessage: string | undefined = message.split(":")[1];

      while (this._queue.length > 0) {
        const deferred = this._queue.shift()!;

        if (deferred.timer) {
          clearTimeout(deferred.timer);
          deferred.timer = null;
        }

        deferred.reject(new Error(errMessage || "Mutex cancelled"));
      }
    });

    await this._redisSubscriber.subscribe(`${this.name}:release`, async () => {
      while (this._queue.length > 0) {
        const nextInQueue = this._queue[0];
        const acquiredToken = await this.tryAcquire(nextInQueue.ttlMs);
        if (!acquiredToken) {
          break;
        }

        this._queue.shift();

        if (nextInQueue.timer) {
          clearTimeout(nextInQueue.timer);
          nextInQueue.timer = null;
        }

        nextInQueue.resolve(acquiredToken);
      }
    });

    await this._redisSubscriber.subscribe(`${this.name}:destroy`, async () => {
      await this.destroy();
    });
  }

  private async tryAcquire(ttlMs: number): Promise<types.AcquiredDistributedToken | undefined> {
    // Use a Lua script to do it atomically:
    //  1. SCARD ownersKey to see how many tokens exist
    //  2. If < maxCount, add new token to set, SET the tokenKey with TTL
    //  3. Return 1 or 0 to indicate success/failure
    const ACQUIRE_LUA = `
      local currentCount = redis.call("SCARD", KEYS[1])
      local maxCount = tonumber(ARGV[1])
      local token = ARGV[2]
      local tokenKey = ARGV[3] .. token
      local ttlMs = tonumber(ARGV[4])

      if currentCount < maxCount then
        -- Add the token to the set
        redis.call("SADD", KEYS[1], token)
        -- Create a separate key with a TTL
        redis.call("SET", tokenKey, "1", "PX", ttlMs)
        return 1
      else
        return 0
      end
    `;

    const token = `${this.name}:${crypto.randomUUID()}`;

    const result = await this._redisClient.eval(ACQUIRE_LUA, {
      keys: [this._ownersKey],
      arguments: [
        this.maxCount.toString(),
        token,
        this._tokenKeyPrefix,
        ttlMs.toString()
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
