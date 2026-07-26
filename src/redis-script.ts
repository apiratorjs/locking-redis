import { RedisClientType } from "redis";
import * as crypto from "node:crypto";

export type RedisScriptReply = Awaited<ReturnType<RedisClientType["eval"]>>;

export interface IRedisScriptOptions {
  keys?: string[];
  arguments?: string[];
}

/**
 * A Lua script kept in the server-side script cache and invoked by its SHA1,
 * so the source is not sent on every call.
 *
 * Falls back to EVAL whenever the server does not know the script - a fresh
 * server, SCRIPT FLUSH, a restart, a failover, or another node of a cluster.
 * That fallback re-populates the cache, so the next call is an EVALSHA again.
 */
export class RedisScript {
  private readonly source: string;
  public readonly sha1: string;

  public constructor(source: string) {
    this.source = source;
    // Redis keys its script cache by the SHA1 of the body, so the digest can be
    // computed locally instead of paying a SCRIPT LOAD round-trip up front.
    this.sha1 = crypto.createHash("sha1").update(source).digest("hex");
  }

  public async run(client: RedisClientType, options?: IRedisScriptOptions): Promise<RedisScriptReply> {
    try {
      return await client.evalSha(this.sha1, options);
    } catch (error) {
      if (!RedisScript.isNoScriptError(error)) {
        throw error;
      }

      return await client.eval(this.source, options);
    }
  }

  private static isNoScriptError(error: unknown): boolean {
    return error instanceof Error && error.message.startsWith("NOSCRIPT");
  }
}
