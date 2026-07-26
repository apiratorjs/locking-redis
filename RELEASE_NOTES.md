# Release notes — @apiratorjs/locking-redis 2.0.0

Major rewrite to match [@apiratorjs/locking](https://github.com/apiratorjs/locking) **5.0.0**. Redis locks are no longer plugged in via static `.factory` hooks; you construct a `RedisDistributedLockManager` and create named mutexes / semaphores from it.

## Highlights

- **`RedisDistributedLockManager`** replaces `createRedisLockFactory` / `IRedisLockFactory`.
- Implements **`IDistributedLockManager`**: same contract as `InMemoryDistributedLockManager` in the core package.
- **`timeoutMs: 0`** fails immediately with `TimeoutLockingError` when the lock is busy (lock TTL still uses a positive default).
- Idempotent **`release()`**; unlock waiters no longer use a separate Redis subscription that could break the acquire queue.
- Peer dependency: **`@apiratorjs/locking` ^5.0.0**.
- Distributed read-write locks are still not supported (`readWriteLock()` throws).

## Upgrade in one glance

```typescript
// 1.x
import { DistributedMutex } from "@apiratorjs/locking";
import { createRedisLockFactory } from "@apiratorjs/locking-redis";

const lockFactory = await createRedisLockFactory({ url: "redis://localhost:6379" });
DistributedMutex.factory = lockFactory.createDistributedMutex;
const mutex = new DistributedMutex({ name: "orders" });

// 2.x
import { RedisDistributedLockManager } from "@apiratorjs/locking-redis";

const locks = await RedisDistributedLockManager.create({ url: "redis://localhost:6379" });
const mutex = locks.mutex("orders");
```

Full migration notes and behavior details: [CHANGELOG.md](./CHANGELOG.md).
