# Release notes — @apiratorjs/locking-redis 4.0.1

Works with [@apiratorjs/locking](https://github.com/apiratorjs/locking) **9.0.0** as well as **8.x**: the peer
dependency range is now `^8.0.0 || ^9.0.0`. No code changes.

With locking 9 you can keep a Redis mutex lock or semaphore permit alive while slow work runs:

```typescript
import { LeaseKeeper } from "@apiratorjs/locking";

const releaser = await manager.mutex("taxonomy").acquire({ ttlMs: 60_000 });

await LeaseKeeper.hold(releaser, { ttlMs: 60_000, extendIntervalMs: 10_000 }, async ({ signal, throwIfLost }) => {
  // slow work ...
  throwIfLost();
});
```

Locking 9 requires Node.js 17.2.0 or newer; with locking 8 this package still runs on 16.4.0.

Previous release: [4.0.0](./CHANGELOG.md#400---2026-09-27).
