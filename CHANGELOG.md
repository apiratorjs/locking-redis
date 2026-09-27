# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [4.0.0] - 2026-09-27

Compared to **3.0.0**. Requires [@apiratorjs/locking](https://github.com/apiratorjs/locking) **^8.0.0** and Redis **5+**.

### Breaking Changes

- Peer dependency `@apiratorjs/locking` bumped from **^6.0.0** to **^8.0.0**. Locking 7 and 8 add `restoreReleaser()` and `ISemaphoreReleaser` / `IMutexReleaser` to the semaphore and mutex contracts, so this package no longer works with locking 6.x or 7.x.
- A lock's lifetime no longer follows `timeoutMs`, for semaphores and mutexes alike. Before, `acquire({ timeoutMs: 5000 })` silently gave a permit or lock that expired after 5 seconds, and `tryAcquire()` one that expired after 1 minute. Now it lives for `ttlMs`, or 1 minute without it, however long the acquisition was allowed to wait. Code that relied on a long `timeoutMs` keeping the lock for as long must pass `ttlMs` instead.
- `BaseDistributedLockPrimitive` declares a new protected abstract `createReleaser(token)`, and the queue draining moved from the `:release` subscription into the protected `drainQueue()`. Only affects code that subclasses the base class.

### Added

- `ttlMs` when acquiring a semaphore permit or a mutex lock (`acquire()` / `tryAcquire()`), counted from the moment it is granted. `Infinity` means no TTL.
- Releasers are `ISemaphoreReleaser` / `IMutexReleaser`: `extend(ttlMs)`, `remainingTtl()`, `isHeld()` on top of `release()` / `getToken()`.
- `restoreReleaser(token)` on `RedisDistributedSemaphore` and `RedisDistributedMutex`: rebuilds the releaser from its token, in any process.

### Behavior

- Permits and locks are addressed by token. `release()` is idempotent per token across releasers and processes; releasing an expired one is a no-op and never frees what was granted to somebody else since. The internal `DistributedReleaser` is replaced by a releaser without local state.
- Queued acquirers and `waitForUnlock()` / `waitForAnyUnlock()` / `waitForFullyUnlock()` are woken when a permit or lock expires, not only when one is released. Before, they waited for the next release or their own timeout.
- Semaphore scripts use the Redis server clock (`TIME`) instead of each client's `Date.now()`; mutex expiry is the key's own `PX`.
- The semaphore key expires together with its longest-lived permit, and has no expiry while it holds a permit without a TTL.
- `freeCount()` / `isLocked()` of the semaphore run as a single script instead of two commands.

### Migration checklist

1. Upgrade `@apiratorjs/locking` to **^8.0.0** alongside `@apiratorjs/locking-redis` **^4.0.0**.
2. Where a permit or lock may be held longer than 1 minute, pass `ttlMs` (or `Infinity`) instead of relying on `timeoutMs`.
3. If you subclass `BaseDistributedLockPrimitive`, implement `createReleaser(token)`.

## [3.0.0] - 2026-09-27

Compared to **2.0.0**. Requires [@apiratorjs/locking](https://github.com/apiratorjs/locking) **^6.0.0**.

### Breaking Changes

- Peer dependency `@apiratorjs/locking` bumped from **^5.0.0** to **^6.0.0**. Locking 6 adds required `tryAcquire()` methods to `IMutex` / `ISemaphore` (and therefore to `IDistributedMutex` / `IDistributedSemaphore`), so this package no longer works with locking 5.x.
- `BaseDistributedLockPrimitive`: the protected abstract `tryAcquire(ttlMs)` is renamed to `acquireOnce(ttlMs)` to free the name for the public interface method. Only affects code that subclasses `RedisDistributedMutex`, `RedisDistributedSemaphore`, or the base class and overrides / calls that method.

### Added

- `tryAcquire(params?)` on `RedisDistributedMutex` and `RedisDistributedSemaphore`. Resolves to a releaser, or to `null` when the lock could not be acquired within `timeoutMs`, instead of throwing `TimeoutLockingError`.

```typescript
const releaser = await locks.mutex("orders").tryAcquire();
if (!releaser) {
  return; // lock is busy
}

try {
  // ... critical section ...
} finally {
  await releaser.release();
}
```

### Behavior (aligned with locking 6.x)

- `timeoutMs` defaults to `0` for `tryAcquire()` (the `acquire()` default stays at 1 minute): a busy lock yields `null` right away. Pass `timeoutMs` to wait a bounded time first.
- Only a timeout becomes `null`. Cancellation (`CancelledLockingError`) and destroyed locks (`LockNotFoundError`) still throw.
- With `timeoutMs: 0` the check and the acquisition are one atomic Redis operation (`SET NX` for the mutex, the acquire Lua script for the semaphore); a failed attempt leaves nothing queued.
- `tryAcquire()` grants the lock under exactly the same conditions as `acquire()`.
- Distributed read-write locks are still not supported (`readWriteLock()` throws), so there are no `tryAcquireRead()` / `tryAcquireWrite()` yet.

### Migration checklist

1. Upgrade `@apiratorjs/locking` to **^6.0.0** alongside `@apiratorjs/locking-redis` **^3.0.0**.
2. If you subclass the Redis primitives and override or call the protected `tryAcquire(ttlMs)`, rename it to `acquireOnce(ttlMs)`.
3. Optionally replace `try { await lock.acquire({ timeoutMs: 0 }) } catch (TimeoutLockingError)` patterns with `await lock.tryAcquire()`.

## [2.0.0] - 2026-07-26

Compared to **1.0.x** (`1.0.5`). Requires [@apiratorjs/locking](https://github.com/apiratorjs/locking) **^5.0.0**.

### Breaking Changes

#### Factory API removed

- Removed `createRedisLockFactory` and `IRedisLockFactory`.
- Removed the pattern of assigning `DistributedMutex.factory` / `DistributedSemaphore.factory`.
- Distributed Redis locks are obtained through a `RedisDistributedLockManager` that implements `types.IDistributedLockManager` from `@apiratorjs/locking`.

**Before (1.x):**

```typescript
import { DistributedMutex, DistributedSemaphore } from "@apiratorjs/locking";
import { createRedisLockFactory } from "@apiratorjs/locking-redis";

const lockFactory = await createRedisLockFactory({ url: "redis://localhost:6379" });
DistributedMutex.factory = lockFactory.createDistributedMutex;
DistributedSemaphore.factory = lockFactory.createDistributedSemaphore;

const mutex = new DistributedMutex({ name: "orders" });
const semaphore = new DistributedSemaphore({ name: "uploads", maxCount: 5 });
```

**After (2.x):**

```typescript
import { types } from "@apiratorjs/locking";
import { RedisDistributedLockManager } from "@apiratorjs/locking-redis";

const locks: types.IDistributedLockManager = await RedisDistributedLockManager.create({
  url: "redis://localhost:6379",
});

const mutex = locks.mutex("orders");
const semaphore = locks.semaphore("uploads", 5);
```

#### Peer dependency

- Declares peer dependency `@apiratorjs/locking` **^5.0.0** (1.x depended on locking **^4** as a devDependency only).
- Aligns with the core package’s manager-based distributed API (`IDistributedLockManager`, `T…` type names, cancel / timeout semantics).

#### Constructor props / types

- Mutex and semaphore constructors now expect `types.TDistributedMutexConstructorProps` / `types.TDistributedSemaphoreConstructorProps` (the `T…` renames from locking 5.x), plus an injected `redisClient`.
- Prefer creating locks via the manager rather than constructing `RedisDistributedMutex` / `RedisDistributedSemaphore` directly.

### Added

- `RedisDistributedLockManager` — Redis-backed `IDistributedLockManager`:
  - `mutex(name)` / `semaphore(name, maxCount)` hand out the same live instance per name.
  - `hasMutex` / `hasSemaphore` / `hasRWLock`, `list()`, `count()`, `snapshot()`.
  - `cancelAll()` and `destroyAll()` for draining and shutdown.
  - `LockConfigMismatchError` when the same semaphore name is requested with a conflicting `maxCount`.
- `RedisDistributedLockManager.create({ url })` — connects a client and returns a manager that owns its lifecycle.
- Inject an existing `redisClient` via the constructor when you already manage Redis yourself; call `disconnect()` only when the manager owns the client.
- Shared `BaseDistributedLockPrimitive` and `RedisScript` helpers for Lua scripts and pub/sub waiters.

### Changed

- Runtime dependency `redis` bumped from **^4** to **^6**.
- `timeoutMs: 0` now fails immediately with `TimeoutLockingError` when the lock is not free. The Redis key still gets a positive default PX (`DEFAULT_TTL_MS`); 1.x passed `PX: 0` into Redis.
- `release()` is idempotent on `DistributedReleaser` (mutex and semaphore); double-release no longer hits Redis again.
- Unlock waiters are tracked locally and driven by the shared `:release` subscription (fixes the 1.x pattern of a second `subscribe` / `unsubscribe` that could drop the queue listener).
- Cancel / destroy use typed errors from `@apiratorjs/locking` (`CancelledLockingError`, `LockNotFoundError`, …) instead of plain `Error`.
- `readWriteLock()` is present on the manager for interface compatibility but throws `LockingError` (not implemented yet).

### Behavior (aligned with locking 5.x)

These match the core package contract; Redis 1.x already only cancelled the acquire queue (it did not force-release holders):

- `cancel()` / `cancelAll()` reject pending acquirers only; held locks stay held.
- Unlock waiters (`waitForUnlock` / `waitForAnyUnlock` / `waitForFullyUnlock`) are not rejected by cancel; they settle on a real unlock or on `destroy()`.

### Migration checklist

1. Replace `createRedisLockFactory` + `Distributed*.factory = …` with `RedisDistributedLockManager.create({ url })` (or `new RedisDistributedLockManager({ redisClient })`).
2. Replace `new DistributedMutex({ name })` / `new DistributedSemaphore({ name, maxCount })` with `locks.mutex(name)` / `locks.semaphore(name, maxCount)`.
3. Upgrade `@apiratorjs/locking` to **^5.0.0** and update `types.*` imports to the `T…` names if you use them directly.
4. On shutdown, call `await locks.destroyAll()` and, if you used `.create()`, `await locks.disconnect()`.
5. If you pass `timeoutMs: 0`, expect an immediate `TimeoutLockingError` when the lock is busy (not a Redis `PX: 0` attempt).

## [1.0.5] - 2025-03-18

See Git history and npm for 1.0.x patch notes. This changelog starts detailed entries at 2.0.0.

[3.0.0]: https://github.com/apiratorjs/locking-redis/releases/tag/v3.0.0
[2.0.0]: https://github.com/apiratorjs/locking-redis/releases/tag/v2.0.0
[1.0.5]: https://github.com/apiratorjs/locking-redis/releases/tag/v1.0.5
