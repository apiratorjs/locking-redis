import { AcquireParams, Deferred, IDistributedSemaphore } from "@apiratorjs/locking/dist/src/types";
import assert from "node:assert";
import { RedisClientType } from "redis";

const DEFAULT_TIMEOUT_IN_MS = 1_000 * 60;

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export class RedisDistributedSemaphore implements IDistributedSemaphore {
  public readonly name: string;
  public readonly implementation: string = "redis";
  public readonly maxCount: number;

  private readonly _redisClient: RedisClientType;
  private _redisSubscriber?: RedisClientType;
  private _queue: Deferred[];
  private _isDestroyed: boolean = false;

  public constructor(props: {
    maxCount: number;
    name: string;
    prefix?: string
    redisClient: RedisClientType;
  }) {
    const { maxCount, name, prefix = "semaphore", redisClient } = props;

    assert.ok(maxCount > 0, "maxCount must be greater than 0");
    assert.ok(name, "name must be provided");

    this.maxCount = maxCount;
    this.name = `${prefix}:${name}`;
    this._redisClient = redisClient;
    this._queue = [];
  }

  public get isDestroyed(): boolean {
    return this._isDestroyed;
  };

  public async destroy(errMessage?: string): Promise<void> {
    await this._redisClient.del(this.name);

    if (this._redisSubscriber) {
      await this._redisSubscriber.unsubscribe(`${this.name}:cancel`);
      await this._redisSubscriber.unsubscribe(`${this.name}:release`);
      await this._redisSubscriber.disconnect();
      this._redisSubscriber = undefined;
    }

    while (this._queue.length > 0) {
      const { reject } = this._queue.shift()!;
      reject(new Error(errMessage ?? "Semaphore destroyed"));
    }
  }

  public async freeCount(): Promise<number> {
    const currentStr = await this._redisClient.get(this.name);
    const current = currentStr ? Number(currentStr) : 0;
    return this.maxCount - current;
  }

  public async acquire(params?: AcquireParams): Promise<void> {
    const timeoutInMs = params?.timeoutMs ?? DEFAULT_TIMEOUT_IN_MS;
    const retryDelayMs = 100;
    const startTime = Date.now();

    // Ensure the subscriber is set up for "cancel" and "release" events.
    await this.ensureSubscriber();

    while (Date.now() - startTime < timeoutInMs) {
      if (await this.tryAcquire()) {
        return;
      }

      await sleep(retryDelayMs);
    }

    throw new Error("Timeout acquiring semaphore");
  }

  public async release(): Promise<void> {
    const result = await this._redisClient.eval(
      "local current = tonumber(redis.call('GET', KEYS[1]) or '0'); " +
      "if current > 0 then " +
      "  redis.call('DECR', KEYS[1]); " +
      "  return 1; " +
      "else " +
      "  return 0; " +
      "end",
      {
        keys: [this.name],
        arguments: []
      }
    );

    if (result !== 1) {
      throw new Error("Semaphore release failed: no permit to release");
    }

    await this._redisClient.publish(`${this.name}:release`, "release");
  }

  public async cancelAll(errMessage?: string): Promise<void> {
    await this._redisClient.del(this.name);
    const message = errMessage ? `cancel:${errMessage}` : "cancel";
    await this._redisClient.publish(`${this.name}:cancel`, message);
  }

  public async isLocked(): Promise<boolean> {
    const free = await this.freeCount();
    return free === 0;
  }

  public async runExclusive<T>(fn: () => Promise<T> | T): Promise<T>
  public async runExclusive<T>(params: AcquireParams, fn: () => Promise<T> | T): Promise<T>
  public async runExclusive<T>(...args: any[]): Promise<T> {
    let params: AcquireParams | undefined;
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

  private async ensureSubscriber(): Promise<void> {
    if (!this._redisSubscriber) {
      this._redisSubscriber = this._redisClient.duplicate();
      await this._redisSubscriber.connect();

      // Subscribe to cancellation events.
      await this._redisSubscriber.subscribe(`${this.name}:cancel`, (message) => {
        const errMessage = message.startsWith("cancel:") ? message.split("cancel:")[1] : "cancelled";
        while (this._queue.length > 0) {
          const { reject } = this._queue.shift()!;
          reject(new Error(errMessage));
        }
      });

      // Subscribe to release events.
      await this._redisSubscriber.subscribe(`${this.name}:release`, async (message) => {
        // On release, try to fulfill waiting acquisitions.
        // Iterate over the queue and try to acquire a permit for each deferred.
        for (let i = 0; i < this._queue.length; i++) {
          if (await this.tryAcquire()) {
            const def = this._queue.splice(i, 1)[0];
            def.resolve();
            i--; // adjust the index after removing an element
          }
        }
      });
    }
  }

  private async tryAcquire(): Promise<boolean> {
    const result = await this._redisClient.eval(
      "local current = tonumber(redis.call('GET', KEYS[1]) or '0'); " +
      "if current < tonumber(ARGV[1]) then " +
      "  redis.call('INCR', KEYS[1]); " +
      "  return 1; " +
      "else " +
      "  return 0; " +
      "end",
      {
        keys: [this.name],
        arguments: [this.maxCount.toString()]
      }
    );

    return result === 1;
  }
}
