import { RedisClientType } from "redis";
import { IDistributedDeferred } from "./types";
import assert from "node:assert";
import { types } from "@apiratorjs/locking";
import { DistributedReleaser } from "./distributed-releaser";

export abstract class BaseDistributedPrimitive {
  public readonly name: string;
  public readonly implementation: string = "redis";

  protected readonly _redisClient: RedisClientType;
  protected _redisSubscriber?: RedisClientType;
  protected _queue: IDistributedDeferred[];
  protected _isDestroyed: boolean = false;

  protected constructor(props: {
    name: string;
    redisClient: RedisClientType;
  }) {
    const { name, redisClient } = props;
    assert.ok(name, "name must be provided");

    this.name = name;
    this._redisClient = redisClient;
    this._queue = [];
  }

  public get isDestroyed(): boolean {
    return this._isDestroyed;
  };

  protected async ensureSubscriber(): Promise<void> {
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

    await this._redisSubscriber.subscribe(`${this.name}:release`, async (token) => {
      while (this._queue.length > 0) {
        const nextInQueue = this._queue.shift() as IDistributedDeferred;
        const acquireToken = await this.tryAcquire(nextInQueue.ttlMs);
        if (!acquireToken) {
          this._queue.unshift(nextInQueue);
          break;
        }

        if (nextInQueue.timer) {
          clearTimeout(nextInQueue.timer);
          nextInQueue.timer = null;
        }

        const releaser = new DistributedReleaser(() => this.release(acquireToken), acquireToken);

        nextInQueue.resolve(releaser);
      }
    });

    await this._redisSubscriber.subscribe(`${this.name}:destroy`, async () => {
      await this.destroy();
    });
  }

  protected abstract tryAcquire(ttlMs: number): Promise<types.AcquireToken | undefined>;

  protected abstract destroy(): Promise<void>;

  protected abstract release(token: types.AcquireToken): Promise<void>;
}
