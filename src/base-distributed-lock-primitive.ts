import assert from "node:assert";
import { RedisClientType } from "redis";
import { CancelledLockingError, types } from "@apiratorjs/locking";
import { IDistributedDeferred, IUnlockWaiter } from "./types";
import { DistributedReleaser } from "./distributed-releaser";

export abstract class BaseDistributedLockPrimitive {
  public readonly name: string;
  public readonly implementation: string = "redis";

  protected readonly redisClient: RedisClientType;
  protected redisSubscriber?: RedisClientType;
  protected queue: IDistributedDeferred[];
  protected unlockWaiters: Set<IUnlockWaiter> = new Set();
  protected destroyed: boolean = false;

  protected constructor(props: {
    name: string;
    redisClient: RedisClientType;
  }) {
    const { name, redisClient } = props;
    assert.ok(name, "name must be provided");

    this.name = name;
    this.redisClient = redisClient;
    this.queue = [];
  }

  public get isDestroyed(): boolean {
    return this.destroyed;
  }

  protected async ensureSubscriber(): Promise<void> {
    if (this.redisSubscriber) {
      return;
    }

    this.redisSubscriber = this.redisClient.duplicate();
    await this.redisSubscriber.connect();

    await this.redisSubscriber.subscribe(`${this.name}:cancel`, (message) => {
      if (!message.startsWith("cancel:")) {
        return;
      }

      const errMessage = message.slice("cancel:".length);
      this.rejectQueuedAcquirers(new CancelledLockingError(errMessage || "Cancelled"));

      // Unlock waiters keep waiting: cancel only drops pending acquisitions.
      // Held locks stay held, so waitForUnlock* is still meaningful.
    });

    await this.redisSubscriber.subscribe(`${this.name}:release`, async () => {
      while (this.queue.length > 0) {
        const nextInQueue = this.queue.shift() as IDistributedDeferred;
        const acquireToken = await this.tryAcquire(nextInQueue.ttlMs);
        if (!acquireToken) {
          this.queue.unshift(nextInQueue);
          break;
        }

        if (nextInQueue.timer) {
          clearTimeout(nextInQueue.timer);
          nextInQueue.timer = null;
        }

        const releaser = new DistributedReleaser(() => this.release(acquireToken), acquireToken);

        nextInQueue.resolve(releaser);
      }

      // Queued acquirers get first refusal; only what is left over frees up
      // the waiters of waitForUnlock / waitForAnyUnlock / waitForFullyUnlock.
      await this.notifyUnlockWaiters();
    });

    await this.redisSubscriber.subscribe(`${this.name}:destroy`, async () => {
      await this.destroy();
    });
  }

  /**
   * Waits until `isSatisfied` holds after a release event.
   *
   * Waiters are kept locally and driven by the single `:release` subscription
   * opened in `ensureSubscriber`. They must never subscribe to that channel
   * themselves: node-redis appends listeners per `subscribe` call, and
   * `unsubscribe(channel)` without a listener drops *all* of them - including
   * the one that drains `queue`.
   */
  protected async waitForUnlockEvent(isSatisfied: () => Promise<boolean>): Promise<void> {
    if (await isSatisfied()) {
      return;
    }

    await this.ensureSubscriber();

    return new Promise<void>((resolve, reject) => {
      const waiter: IUnlockWaiter = {
        isSatisfied,
        resolve: () => {
          this.unlockWaiters.delete(waiter);
          resolve();
        },
        reject: (error: Error) => {
          this.unlockWaiters.delete(waiter);
          reject(error);
        },
      };

      this.unlockWaiters.add(waiter);

      // A release may have landed while the subscriber was being set up.
      isSatisfied().then(
        (satisfied) => {
          if (satisfied) {
            waiter.resolve();
          }
        },
        (error) => {
          // destroy() flips `destroyed` before settling waiters; isLocked /
          // freeCount then throw. Prefer resolve so we don't race ahead of
          // resolveUnlockWaiters() and leave waitForUnlock* rejected.
          if (this.destroyed) {
            waiter.resolve();
            return;
          }

          waiter.reject(error as Error);
        },
      );
    });
  }

  protected async notifyUnlockWaiters(): Promise<void> {
    if (this.destroyed) {
      // In-flight `:release` handling can outlive destroy()'s unsubscribe.
      // Satisfaction checks throw once destroyed; settle the same way destroy does.
      this.resolveUnlockWaiters();
      return;
    }

    for (const waiter of [...this.unlockWaiters]) {
      try {
        if (await waiter.isSatisfied()) {
          waiter.resolve();
        }
      } catch (error) {
        if (this.destroyed) {
          waiter.resolve();
          continue;
        }

        waiter.reject(error as Error);
      }
    }
  }

  protected rejectQueuedAcquirers(error: Error): void {
    while (this.queue.length > 0) {
      const deferred = this.queue.shift()!;

      if (deferred.timer) {
        clearTimeout(deferred.timer);
        deferred.timer = null;
      }

      deferred.reject(error);
    }
  }

  /**
   * A destroyed lock cannot be held: settle unlock waiters successfully so a
   * fire-and-forget `void lock.waitForUnlock()` does not become an unhandled
   * rejection.
   */
  protected resolveUnlockWaiters(): void {
    for (const waiter of [...this.unlockWaiters]) {
      waiter.resolve();
    }
  }

  protected abstract tryAcquire(ttlMs: number): Promise<types.TAcquireToken | undefined>;

  protected abstract destroy(): Promise<void>;

  protected abstract release(token: types.TAcquireToken): Promise<void>;
}
