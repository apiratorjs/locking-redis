import { types } from "@apiratorjs/locking";

export class DistributedReleaser<T extends types.AcquireToken = types.AcquireToken> implements types.IReleaser<T> {
  constructor(
    private readonly _onRelease: () => Promise<void>,
    private readonly _token: T
  ) {}

  public async release(): Promise<void> {
    await this._onRelease();
  }

  public getToken(): T {
    return this._token;
  }
}
