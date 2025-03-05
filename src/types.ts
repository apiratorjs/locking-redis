import { types } from "@apiratorjs/locking";

export interface IDistributedDeferred extends types.IDeferred {
  ttlMs: number;
  releaser: types.IReleaser;
}
