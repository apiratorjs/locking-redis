import { types } from "@apiratorjs/locking";
import { RedisClientType } from "redis";
import assert from "node:assert";
import crypto from "node:crypto";

interface Deferred {
  resolve: () => void;
  reject: (reason?: any) => void;
  ttlMs: number;
  timer?: NodeJS.Timeout | null;
}

const DEFAULT_TTL_MS = 1000 * 60; // 1 minute

export class RedisDistributedMutex implements types.IDistributedMutex {
  public readonly name: string;
  public readonly implementation: string = "redis";

  private readonly _redisClient: RedisClientType;
  private _redisSubscriber?: RedisClientType;
  private _queue: Deferred[];
  private _isDestroyed: boolean = false;
  /**
   * Holds the random lock token if we successfully acquire it.
   * Using a random token helps ensure that only the owner can release.
   */
  private _lockValue?: string;

  public constructor(props: types.DistributedMutexConstructorProps & {
    redisClient: RedisClientType;
  }) {
    const { name, redisClient } = props;
    assert.ok(name, "name must be provided");

    this.name = `mutex:${name}`;
    this._redisClient = redisClient;
    this._queue = [];
  }

  get isDestroyed(): boolean {
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

      deferred.reject(new Error("Mutex destroyed"));
    }
  }

  public async acquire(params?: types.AcquireParams): Promise<void> {
    this.throwIfDestroyed();

    await this.ensureSubscriber();

    const { timeoutMs = DEFAULT_TTL_MS } = params ?? {};

    const acquired = await this.tryAcquire(timeoutMs);
    if (acquired) {
      return;
    }

    // Return a promise that resolves once the lock is eventually acquired.
    return new Promise((resolve, reject) => {
      const deferred: Deferred = {
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

  public async release(): Promise<void> {
    this.throwIfDestroyed();

    // Only release if the lock key’s value matches our lockValue
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      end
      return 0
    `;
    const result = await this._redisClient.eval(script, {
      keys: [this.name],
      arguments: [this._lockValue ?? ""]
    });

    // If we successfully released, let the next queue item know they can try
    if (result === 1) {
      await this._redisClient.publish(`${this.name}:release`, "released");
      this._lockValue = undefined;
    }
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

    await this.acquire(params);
    try {
      return await fn();
    } finally {
      await this.release();
    }
  }

  private async tryAcquire(ttlMs: number): Promise<boolean> {
    const token = crypto.randomBytes(16).toString("hex");

    const result = await this._redisClient.set(this.name, token, {
      NX: true,
      PX: ttlMs
    });

    if (result === "OK") {
      this._lockValue = token;
      return true;
    }

    return false;
  }

  /**
   * Ensure we have a single subscriber that listens to both `cancel` and `release`.
   * - on `cancel`, we reject all waiters.
   * - on `release`, let the first in queue attempt reacquiring the lock.
   */
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
        const gotLock = await this.tryAcquire(nextInQueue.ttlMs);
        if (!gotLock) {
          break;
        }

        this._queue.shift();

        if (nextInQueue.timer) {
          clearTimeout(nextInQueue.timer);
          nextInQueue.timer = null;
        }

        nextInQueue.resolve();
      }
    });

    await this._redisSubscriber.subscribe(`${this.name}:destroy`, async () => {
      await this.destroy();
    });
  }

  private throwIfDestroyed(): void {
    if (this._isDestroyed) {
      throw new Error("Mutex has been destroyed");
    }
  }
}
