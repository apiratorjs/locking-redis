# Release notes — @apiratorjs/locking-redis 4.0.0

Follows [@apiratorjs/locking](https://github.com/apiratorjs/locking) **7.0.0** and **8.0.0**: Redis semaphore permits and mutex locks can be handed over between processes by their token and have their own TTL.

## Highlights

- **`restoreReleaser(token)`** on `RedisDistributedSemaphore` and `RedisDistributedMutex`: release, extend or inspect a permit or lock from any process - for example a job queue worker that got the token in the job payload.
- **`ttlMs`** per permit or lock, counted from the moment it is granted. Without it they expire after 1 minute; `Infinity` opts out of expiry.
- **`extend(ttlMs)`**, **`remainingTtl()`**, **`isHeld()`** on the releasers.
- **Release is idempotent per token** across processes; a holder whose permit or lock expired cannot release somebody else's.
- **Expiry wakes waiters**: queued acquirers and `waitForUnlock()` / `waitForAnyUnlock()` / `waitForFullyUnlock()` no longer wait for the next release or their own timeout when something expires.
- **Server clock**: expiry is measured by Redis, not by each host's clock.

## Breaking changes

- Peer dependency: **`@apiratorjs/locking` ^8.0.0**, and Redis **5+**.
- **Lifetime no longer follows `timeoutMs`.** Before, `acquire({ timeoutMs: 5000 })` gave a permit or lock that expired after 5 seconds. Now it lives for `ttlMs`, or 1 minute by default. Pass `ttlMs` where they are held longer.
- Subclasses of `BaseDistributedLockPrimitive` must implement `createReleaser(token)`.

## Upgrade in one glance

```bash
npm install @apiratorjs/locking@^8 @apiratorjs/locking-redis@^4
```

```typescript
// 3.x: the lock silently expired after timeoutMs
const releaser = await mutex.acquire({ timeoutMs: 10 * 60_000 });

// 4.x: wait and lifetime are separate
const releaser = await mutex.acquire({ timeoutMs: 5_000, ttlMs: 10 * 60_000 });
```

Full migration notes and behavior details: [CHANGELOG.md](./CHANGELOG.md).
