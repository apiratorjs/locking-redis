# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[2.0.0]: https://github.com/apiratorjs/locking-redis/releases/tag/v2.0.0
[1.0.5]: https://github.com/apiratorjs/locking-redis/releases/tag/v1.0.5
