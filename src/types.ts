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
