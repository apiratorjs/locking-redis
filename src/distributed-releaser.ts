import { types } from "@apiratorjs/locking";

export class DistributedReleaser implements types.IReleaser {
  constructor(
    private readonly _onRelease: () => Promise<void>,
    private readonly _token: types.AcquireToken
  ) {}

  public async release(): Promise<void> {
    await this._onRelease();
  }

  public getToken(): types.AcquireToken {
    return this._token;
  }
}
