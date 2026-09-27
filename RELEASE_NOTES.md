# Release notes — @apiratorjs/locking-redis 3.0.0

Follows [@apiratorjs/locking](https://github.com/apiratorjs/locking) **6.0.0**, which adds non-throwing `tryAcquire()` to the mutex and semaphore contracts. Redis mutexes and semaphores now implement it.

## Highlights

- **`tryAcquire(params?)`** on `RedisDistributedMutex` and `RedisDistributedSemaphore`: returns a releaser, or `null` when the lock is busy.
- **No waiting by default**: `timeoutMs` defaults to `0` for `tryAcquire()`; pass `timeoutMs` to wait a bounded time. `acquire()` keeps its 1-minute default.
- **Only a timeout becomes `null`**: cancellation (`CancelledLockingError`) and destroyed locks (`LockNotFoundError`) still throw.
- **Atomic fast path**: with `timeoutMs: 0` the check and acquisition are one Redis operation (`SET NX` / Lua script), not `isLocked()` + `acquire()`.
- Peer dependency: **`@apiratorjs/locking` ^6.0.0** (breaking: 5.x is no longer supported).
- Protected `tryAcquire(ttlMs)` in `BaseDistributedLockPrimitive` renamed to `acquireOnce(ttlMs)` (affects subclasses only).
- Distributed read-write locks are still not supported (`readWriteLock()` throws).

## Upgrade in one glance

```bash
npm install @apiratorjs/locking@^6 @apiratorjs/locking-redis@^3
```

```typescript
// 2.x
try {
  const releaser = await mutex.acquire({ timeoutMs: 0 });
  // ...
} catch (error) {
  if (!(error instanceof TimeoutLockingError)) throw error;
  // busy
}

// 3.x
const releaser = await mutex.tryAcquire();
if (releaser) {
  // ...
}
```

Full migration notes and behavior details: [CHANGELOG.md](./CHANGELOG.md).
