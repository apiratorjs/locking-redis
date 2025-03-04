import { RedisDistributedSemaphore } from "./redis-distributed-semaphore";
import { types } from "@apiratorjs/locking";
import { RedisDistributedMutex } from "./redis-distributed-mutex";
import { createClient, RedisClientType } from "redis";

export interface IRedisLockFactory {
  createDistributedSemaphore(props: types.DistributedSemaphoreConstructorProps): RedisDistributedSemaphore;

  createDistributedMutex(props: types.DistributedMutexConstructorProps): RedisDistributedMutex;

  getRedisClient(): RedisClientType;
}

export async function createRedisLockFactory(options: { url: string }): Promise<IRedisLockFactory> {
  const redisClient: RedisClientType = createClient({ url: options.url });
  await redisClient.connect();

  return {
    createDistributedSemaphore(props: types.DistributedSemaphoreConstructorProps) {
      const { name, maxCount } = props;
      return new RedisDistributedSemaphore({
        name,
        maxCount,
        redisClient
      });
    },

    createDistributedMutex(props: types.DistributedMutexConstructorProps) {
      const { name } = props;
      return new RedisDistributedMutex({
        name,
        redisClient
      });
    },

    getRedisClient() {
      return redisClient;
    }
  };
}
