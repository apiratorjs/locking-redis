import assert from "node:assert";
import { createClient, RedisClientType } from "redis";
import {
  ELockDisplayType,
  LockConfigMismatchError,
  LockingError,
  LockNotFoundError,
  types,
} from "@apiratorjs/locking";
import { RedisDistributedMutex } from "./redis-distributed-mutex";
import { RedisDistributedSemaphore } from "./redis-distributed-semaphore";

export type TRedisDistributedLockManagerProps = {
  redisClient: RedisClientType;
};

/**
 * Redis-backed implementation of IDistributedLockManager.
 *
 * Owns a set of named distributed locks keyed in Redis: hands out the same
 * instance for the same name while that lock is alive, and knows what it handed
 * out so an application can list, cancel, or tear everything down on shutdown.
 *
 * ```ts
 * const locks = await RedisDistributedLockManager.create({ url: "redis://localhost:6379" });
 * await locks.mutex("orders").runExclusive(() => shipOrder());
 *
 * process.on("SIGTERM", async () => {
 *   await locks.destroyAll("Shutting down");
 *   await locks.disconnect();
 * });
 * ```
 */
export class RedisDistributedLockManager implements types.IDistributedLockManager {
  private readonly redisClient: RedisClientType;
  private readonly managedLocks: Map<string, types.TManagedLock> = new Map();
  private readonly ownsClient: boolean;

  public constructor(
    props: TRedisDistributedLockManagerProps,
    options?: { ownsClient?: boolean },
  ) {
    assert.ok(props.redisClient, "RedisDistributedLockManager requires a redisClient.");

    this.redisClient = props.redisClient;
    this.ownsClient = options?.ownsClient ?? false;
  }

  /**
   * Connects to Redis and returns a manager that owns the client lifecycle.
   */
  public static async create(options: { url: string }): Promise<RedisDistributedLockManager> {
    const redisClient: RedisClientType = createClient({ url: options.url });
    await redisClient.connect();

    return new RedisDistributedLockManager({ redisClient }, { ownsClient: true });
  }

  public getRedisClient(): RedisClientType {
    return this.redisClient;
  }

  /**
   * Disconnects the Redis client when this manager created it via {@link create}.
   * No-op when the client was injected from outside.
   */
  public async disconnect(): Promise<void> {
    if (!this.ownsClient) {
      return;
    }

    if (this.redisClient.isOpen) {
      await this.redisClient.disconnect();
    }
  }

  public mutex(name: string): types.IDistributedMutex {
    assert.ok(name, "RedisDistributedLockManager requires a non-empty lock name.");

    const existing = this.takeAlive(ELockDisplayType.Mutex, name);
    if (existing?.kind === ELockDisplayType.Mutex) {
      return existing.lock;
    }

    const lock = new RedisDistributedMutex({
      name,
      redisClient: this.redisClient,
    });
    this.managedLocks.set(this.keyOf(ELockDisplayType.Mutex, name), {
      kind: ELockDisplayType.Mutex,
      name,
      lock,
    });

    return lock;
  }

  public semaphore(name: string, maxCount: number): types.IDistributedSemaphore {
    assert.ok(name, "RedisDistributedLockManager requires a non-empty lock name.");
    assert.ok(maxCount > 0, "maxCount must be greater than 0");

    const existing = this.takeAlive(ELockDisplayType.Semaphore, name);
    if (existing?.kind === ELockDisplayType.Semaphore) {
      if (existing.maxCount !== maxCount) {
        throw new LockConfigMismatchError(
          `${ELockDisplayType.Semaphore} '${name}' is already registered with maxCount ${existing.maxCount}, requested ${maxCount}`,
        );
      }

      return existing.lock;
    }

    const lock = new RedisDistributedSemaphore({
      name,
      maxCount,
      redisClient: this.redisClient,
    });
    this.managedLocks.set(this.keyOf(ELockDisplayType.Semaphore, name), {
      kind: ELockDisplayType.Semaphore,
      name,
      lock,
      maxCount,
    });

    return lock;
  }

  public readWriteLock(name: string, maxReaders?: number): types.IDistributedRWLock {
    void name;
    void maxReaders;
    throw new LockingError(
      "RedisDistributedLockManager.readWriteLock is not implemented yet",
    );
  }

  public hasMutex(name: string): boolean {
    return this.takeAlive(ELockDisplayType.Mutex, name) !== undefined;
  }

  public hasSemaphore(name: string): boolean {
    return this.takeAlive(ELockDisplayType.Semaphore, name) !== undefined;
  }

  public hasRWLock(name: string): boolean {
    return this.takeAlive(ELockDisplayType.RWLock, name) !== undefined;
  }

  public list(): types.TDistributedLockInfo[] {
    this.dropDestroyed();

    return [...this.managedLocks.values()].map((managed) => this.describe(managed));
  }

  public count(kind?: ELockDisplayType): number {
    this.dropDestroyed();

    if (kind === undefined) {
      return this.managedLocks.size;
    }

    let total = 0;
    for (const managed of this.managedLocks.values()) {
      if (managed.kind === kind) {
        total++;
      }
    }

    return total;
  }

  public async snapshot(): Promise<types.TDistributedLockSnapshot[]> {
    this.dropDestroyed();

    return Promise.all(
      [...this.managedLocks.values()].map(async (managed) => {
        const info = this.describe(managed);

        try {
          if (managed.kind === ELockDisplayType.Mutex) {
            return { ...info, isLocked: await managed.lock.isLocked() };
          }

          if (managed.kind === ELockDisplayType.Semaphore) {
            return {
              ...info,
              isLocked: await managed.lock.isLocked(),
              freeCount: await managed.lock.freeCount(),
            };
          }

          return {
            ...info,
            isWriteLocked: await managed.lock.isWriteLocked(),
            isReadLocked: await managed.lock.isReadLocked(),
            activeReaders: await managed.lock.activeReaders(),
          };
        } catch (error) {
          if (error instanceof LockNotFoundError) {
            return info;
          }

          throw error;
        }
      }),
    );
  }

  public async cancelAll(errMessage?: string): Promise<void> {
    this.dropDestroyed();

    const results = await Promise.allSettled(
      [...this.managedLocks.values()].map((managed) => this.cancelOne(managed, errMessage)),
    );

    this.throwIfAnyFailed(results, "Failed to cancel some locks");
  }

  public async destroyAll(errMessage?: string): Promise<void> {
    const managedLocks = [...this.managedLocks.values()];
    this.managedLocks.clear();

    const results = await Promise.allSettled(
      managedLocks.map(async (managed) => {
        if (managed.lock.isDestroyed) {
          return;
        }

        if (errMessage !== undefined) {
          await this.cancelOne(managed, errMessage);
        }

        await managed.lock.destroy();
      }),
    );

    this.throwIfAnyFailed(results, "Failed to destroy some locks");
  }

  private cancelOne(managed: types.TManagedLock, errMessage?: string): Promise<void> {
    return managed.kind === ELockDisplayType.Mutex
      ? managed.lock.cancel(errMessage)
      : managed.lock.cancelAll(errMessage);
  }

  private describe(managed: types.TManagedLock): types.TDistributedLockInfo {
    const info: types.TDistributedLockInfo = {
      kind: managed.kind,
      name: managed.name,
      implementation: managed.lock.implementation,
      isDestroyed: managed.lock.isDestroyed,
    };

    if (managed.kind === ELockDisplayType.Semaphore) {
      info.maxCount = managed.maxCount;
    }

    if (managed.kind === ELockDisplayType.RWLock && managed.maxReaders !== undefined) {
      info.maxReaders = managed.maxReaders;
    }

    return info;
  }

  private keyOf(kind: ELockDisplayType, name: string): string {
    return `${kind}:${name}`;
  }

  private takeAlive(kind: ELockDisplayType, name: string): types.TManagedLock | undefined {
    const key = this.keyOf(kind, name);
    const managed = this.managedLocks.get(key);

    if (!managed) {
      return undefined;
    }

    if (managed.lock.isDestroyed) {
      this.managedLocks.delete(key);
      return undefined;
    }

    return managed;
  }

  private dropDestroyed(): void {
    for (const [key, managed] of this.managedLocks) {
      if (managed.lock.isDestroyed) {
        this.managedLocks.delete(key);
      }
    }
  }

  private throwIfAnyFailed(
    results: PromiseSettledResult<unknown>[],
    message: string,
  ): void {
    const errors = results
      .filter(
        (result): result is PromiseRejectedResult => result.status === "rejected",
      )
      .map((result) => result.reason)
      .filter((reason) => !(reason instanceof LockNotFoundError));

    if (errors.length > 0) {
      throw new AggregateError(errors, message);
    }
  }
}
