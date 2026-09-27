import { types } from "@apiratorjs/locking";

export interface IDistributedDeferred extends types.IDeferred {
  ttlMs: number;
  timer: NodeJS.Timeout | null;
}

export interface IUnlockWaiter {
  isSatisfied: () => Promise<boolean>;
  resolve: () => void;
  reject: (error: Error) => void;
}

export interface ILeaseOperations<T extends types.TAcquireToken> {
  release(token: T): Promise<void>;

  extend(token: T, ttlMs: number): Promise<boolean>;

  remainingTtl(token: T): Promise<number | null>;
}
