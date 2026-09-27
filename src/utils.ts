import assert from "node:assert";
import { MAX_TIMER_DELAY_IN_MS } from "./constants";

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export function assertTtl(ttlMs: number): void {
  assert.ok(
    ttlMs === Infinity || (ttlMs > 0 && ttlMs <= MAX_TIMER_DELAY_IN_MS),
    `ttlMs must be Infinity, or greater than 0 and at most ${MAX_TIMER_DELAY_IN_MS}`
  );
}

export function ttlArgument(ttlMs: number): string {
  return ttlMs === Infinity ? "inf" : String(ttlMs);
}
