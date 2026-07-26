import { describe, it } from "node:test";
import * as assert from "node:assert";
import { types } from "@apiratorjs/locking";
import { DistributedReleaser } from "../src/distributed-releaser";

describe("DistributedReleaser", () => {
  it("should allow retry when onRelease fails", async () => {
    let attempts = 0;
    const releaser = new DistributedReleaser(async () => {
      attempts += 1;
      if (attempts === 1) {
        throw new Error("transient redis error");
      }
    }, "token" as types.TAcquireToken);

    await assert.rejects(() => releaser.release(), /transient redis error/);
    assert.strictEqual(attempts, 1);

    await releaser.release();
    assert.strictEqual(attempts, 2);

    // Successful release sticks: further calls must not invoke onRelease again.
    await releaser.release();
    assert.strictEqual(attempts, 2);
  });

  it("should coalesce concurrent release calls into a single onRelease", async () => {
    let attempts = 0;
    let releaseOnRelease!: () => void;
    const gate = new Promise<void>((resolve) => {
      releaseOnRelease = resolve;
    });

    const releaser = new DistributedReleaser(async () => {
      attempts += 1;
      await gate;
    }, "token" as types.TAcquireToken);

    const first = releaser.release();
    const second = releaser.release();

    releaseOnRelease();
    await Promise.all([first, second]);

    assert.strictEqual(attempts, 1);
  });

  it("should not coalesce a retry with a previous failed release", async () => {
    let attempts = 0;
    const releaser = new DistributedReleaser(async () => {
      attempts += 1;
      if (attempts === 1) {
        throw new Error("boom");
      }
    }, "token" as types.TAcquireToken);

    await assert.rejects(() => releaser.release(), /boom/);
    await releaser.release();
    assert.strictEqual(attempts, 2);
  });
});
