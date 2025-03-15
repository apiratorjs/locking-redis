import { types } from "@apiratorjs/locking";
import { RedisClientType } from "redis";
import crypto from "node:crypto";
import { DEFAULT_TTL_MS } from "./constants";
import { IDistributedDeferred } from "./types";
import { IReleaser } from "@apiratorjs/locking/dist/src/types";
import { DistributedReleaser } from "./distributed-releaser";
import { BaseDistributedPrimitive } from "./base-distributed-primitive";

export class RedisDistributedMutex extends BaseDistributedPrimitive implements types.IDistributedMutex {
  /**
   * Holds the random lock token if we successfully acquire it.
   * Using a random token helps ensure that only the owner can release.
   */
  private _lockValue?: types.AcquireToken;

  public constructor(props: types.DistributedMutexConstructorProps & {
    redisClient: RedisClientType;
  }) {
    super({ ...props, name: `mutex:${props.name}` });
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

      deferred.reject(new Error("Mutex destroyed"));
    }
  }

  public async acquire(params?: types.AcquireParams): Promise<IReleaser> {
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

  public async cancel(errMessage?: string): Promise<void> {
    this.throwIfDestroyed();

    const msg = `cancel:${errMessage ?? ""}`;
    await this._redisClient.publish(`${this.name}:cancel`, msg);
  }

  public async isLocked(): Promise<boolean> {
    const val = await this._redisClient.get(this.name);
    return val !== null;
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
      await releaser.release();
    }
  }

  protected async tryAcquire(timeoutMs: number): Promise<types.AcquireToken | undefined> {
    const token = `${this.name}:${crypto.randomUUID()}` as types.AcquireToken;

    const result = await this._redisClient.set(this.name, token, {
      NX: true,
      PX: timeoutMs
    });

    if (result === "OK") {
      this._lockValue = token;
      return token;
    }

    return undefined;
  }

  private throwIfDestroyed(): void {
    if (this._isDestroyed) {
      throw new Error("Mutex has been destroyed");
    }
  }

  protected async release(token: types.AcquireToken): Promise<void> {
    this.throwIfDestroyed();

    // Only release if the lock key’s value matches our lockValue
    const RELEASE_LUA = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      end
      return 0
    `;
    const result = await this._redisClient.eval(RELEASE_LUA, {
      keys: [this.name],
      arguments: [this._lockValue ?? ""]
    });

    // If we successfully released, let the next queue item know they can try
    if (result === 1) {
      await this._redisClient.publish(`${this.name}:release`, token);
      this._lockValue = undefined;
    }
  }
}
