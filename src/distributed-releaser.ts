import { types } from "@apiratorjs/locking";

export class DistributedReleaser<T extends types.TAcquireToken = types.TAcquireToken> implements types.IReleaser<T> {
  private isReleased: boolean = false;
  private releasePromise: Promise<void> | undefined;

  public constructor(
    private readonly onRelease: () => Promise<void>,
    private readonly token: T,
  ) {}

  /**
   * Releasing is idempotent: one releaser owns exactly one acquisition, so
   * repeated calls must not unlock again (or hand a semaphore permit back twice).
   *
   * A failed `onRelease` does not stick the releaser in a released state, so
   * the caller can retry after a transient Redis/network error.
   */
  public async release(): Promise<void> {
    if (this.isReleased) {
      return;
    }

    if (!this.releasePromise) {
      this.releasePromise = (async () => {
        try {
          await this.onRelease();
          this.isReleased = true;
        } finally {
          this.releasePromise = undefined;
        }
      })();
    }

    return this.releasePromise;
  }

  public getToken(): T {
    return this.token;
  }
}
