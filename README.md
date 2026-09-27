# @apiratorjs/locking-redis

[![NPM version](https://img.shields.io/npm/v/@apiratorjs/locking-redis.svg)](https://www.npmjs.com/package/@apiratorjs/locking-redis)
[![License: MIT](https://img.shields.io/npm/l/@apiratorjs/locking-redis.svg)](https://github.com/apiratorjs/locking-redis/blob/main/LICENSE)

An extension to the core [@apiratorjs/locking](https://github.com/apiratorjs/locking) library, providing a Redis-backed
`IDistributedLockManager` with distributed mutexes and semaphores for true cross-process concurrency control in Node.js.

> **Note:** Requires Node.js version **>=16.4.0**, [@apiratorjs/locking](https://github.com/apiratorjs/locking) **^6.0.0**,
> and a running Redis instance (version 5+ recommended).
>
> Upgrading from 1.x? See [CHANGELOG](./CHANGELOG.md) and [2.0.0 release notes](./RELEASE_NOTES.md).

---

## Why Use Redis for Distributed Locking?

- **Multi-instance deployments**: If you have multiple Node.js processes or servers behind a load balancer, an in-memory
  lock is insufficient. Redis provides a single, centralized coordination point.
- **Fault tolerance**: Configurable timeouts (TTLs) prevent indefinite locks if a process crashes.
- **Scalability**: Redis can handle many simultaneous locking requests at scale.

---

## Features

- **`RedisDistributedLockManager`** — implements `types.IDistributedLockManager` from `@apiratorjs/locking`.
- **Distributed Mutex and Semaphore** — same acquire / release / cancel / wait-for-unlock API as the core distributed
  primitives, coordinated through Redis.
- **Named locks** — the same name returns the same live instance while it is alive; `list()`, `count()`, `snapshot()`,
  `cancelAll()`, and `destroyAll()` for inspection and shutdown.
- **Time-limited locks (TTL)** — prevents deadlocks if processes crash without releasing.
- **Cancellation, timeouts, and FIFO waiters** — cancel blocked acquisitions, fail fast with `timeoutMs: 0`, queue
  waiters in order.
- **Non-throwing `tryAcquire()`** — returns a releaser, or `null` when the lock is busy (no wait by default).
- **Read-write locks** — not implemented yet; `readWriteLock()` throws `LockingError`.

---

## Installation

Install with npm:

```bash
npm install @apiratorjs/locking @apiratorjs/locking-redis
```

Or with yarn:

```bash
yarn add @apiratorjs/locking @apiratorjs/locking-redis
```

---

## Usage

> Default acquire timeout is 1 minute (same as the core library). Pass `timeoutMs: 0` to fail fast with a
> `TimeoutLockingError` when the lock is not immediately available.
>
> `cancel()` / `cancelAll()` reject pending acquisitions only: held locks stay held, and
> `waitForUnlock()` / `waitForAnyUnlock()` / `waitForFullyUnlock()` keep waiting until holders release (or until
> `destroy()` / `destroyAll()`, which resolves those waiters).

### Quick start

```typescript
import { RedisDistributedLockManager } from "@apiratorjs/locking-redis";

const locks = await RedisDistributedLockManager.create({
  url: "redis://localhost:6379",
});

async function example() {
  const mutex = locks.mutex("shared-resource");

  const releaser = await mutex.acquire({ timeoutMs: 5000 });
  try {
    console.log("Distributed mutex acquired");
  } finally {
    await releaser.release();
  }

  await locks.semaphore("api-rate-limiter", 5).runExclusive(async () => {
    console.log("Distributed semaphore slot acquired");
  });
}

process.on("SIGTERM", async () => {
  await locks.destroyAll("Shutting down");
  await locks.disconnect();
});
```

### Inject an existing Redis client

When you already own a `redis` client, pass it into the constructor. In that case `disconnect()` is a no-op — you close
the client yourself.

```typescript
import { createClient } from "redis";
import { RedisDistributedLockManager } from "@apiratorjs/locking-redis";

const redisClient = createClient({ url: "redis://localhost:6379" });
await redisClient.connect();

const locks = new RedisDistributedLockManager({ redisClient });
```

### Distributed Mutex

```typescript
const mutex = locks.mutex("orders");

const releaser = await mutex.acquire({ timeoutMs: 5000 });
try {
  // Critical section — exclusive across all processes sharing this Redis
} finally {
  await releaser.release();
}

await mutex.runExclusive(async () => {
  // Acquired and released automatically
});

// Returns null instead of throwing when the mutex is busy; doesn't wait unless timeoutMs is given
const maybeReleaser = await mutex.tryAcquire();
if (maybeReleaser) {
  try {
    // Critical section
  } finally {
    await maybeReleaser.release();
  }
}

await mutex.cancel("Operation cancelled");
await mutex.waitForUnlock();
```

### Distributed Semaphore

```typescript
const semaphore = locks.semaphore("uploads", 5);

const releaser = await semaphore.acquire({ timeoutMs: 5000 });
try {
  // Up to 5 concurrent holders across processes
} finally {
  await releaser.release();
}

await semaphore.runExclusive(async () => {
  // Acquired and released automatically
});

const maybePermit = await semaphore.tryAcquire({ timeoutMs: 1000 }); // null if no permit within 1s
await maybePermit?.release();

await semaphore.cancelAll("Operation cancelled");
await semaphore.waitForAnyUnlock();
await semaphore.waitForFullyUnlock();
```

### Managing locks

```typescript
import { ELockDisplayType } from "@apiratorjs/locking";

locks.hasMutex("orders");
locks.hasSemaphore("uploads");
locks.count();
locks.count(ELockDisplayType.Semaphore);
locks.list();
await locks.snapshot();

await locks.cancelAll("Draining before deploy");
await locks.destroyAll("Shutting down");
```

Requesting the same semaphore name with a different `maxCount` throws `LockConfigMismatchError`.

### Swapping backends

Because both managers implement `IDistributedLockManager`, application code can depend on the interface and receive
either an in-memory or Redis manager:

```typescript
import { types, InMemoryDistributedLockManager } from "@apiratorjs/locking";
import { RedisDistributedLockManager } from "@apiratorjs/locking-redis";

export const locks: types.IDistributedLockManager =
  process.env.REDIS_URL
    ? await RedisDistributedLockManager.create({ url: process.env.REDIS_URL })
    : new InMemoryDistributedLockManager();
```

Nothing is global, so a Redis-backed manager and an in-memory one can coexist — useful when only part of the system needs
cross-process coordination, and in tests.

### Low-level classes

`RedisDistributedMutex` and `RedisDistributedSemaphore` are also exported for advanced use (for example custom
managers). Prefer `RedisDistributedLockManager` in application code so named instances, listing, and shutdown stay
consistent.

---

## Error handling

Errors come from `@apiratorjs/locking`:

| Error Class | When thrown |
|-------------|-------------|
| `TimeoutLockingError` | `acquire()` exceeds `timeoutMs` (`tryAcquire()` returns `null` instead) |
| `CancelledLockingError` | `cancel()` / `cancelAll()` or destroy |
| `LockNotFoundError` | Lock was destroyed |
| `LockConfigMismatchError` | Same name requested with conflicting `maxCount` |
| `LockingError` | Base class; also thrown by unimplemented `readWriteLock()` |

---

## Contributing

Contributions, issues, and feature requests are welcome!
Please open an issue or submit a pull request on [GitHub](https://github.com/apiratorjs/locking-redis).
