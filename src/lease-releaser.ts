import { types } from "@apiratorjs/locking";
import { ILeaseOperations } from "./types";

export class RedisLeaseReleaser<T extends types.TAcquireToken> implements types.ILeaseReleaser<T> {
  public constructor(
    private readonly operations: ILeaseOperations<T>,
    private readonly token: T,
  ) {}

  public async release(): Promise<void> {
    await this.operations.release(this.token);
  }

  public async extend(ttlMs: number): Promise<boolean> {
    return this.operations.extend(this.token, ttlMs);
  }

  public async remainingTtl(): Promise<number | null> {
    return this.operations.remainingTtl(this.token);
  }

  public async isHeld(): Promise<boolean> {
    return (await this.operations.remainingTtl(this.token)) !== null;
  }

  public getToken(): T {
    return this.token;
  }
}
