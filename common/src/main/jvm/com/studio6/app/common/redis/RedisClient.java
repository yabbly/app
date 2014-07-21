package com.studio6.app.common.redis;

import com.google.common.base.Function;

import redis.clients.jedis.Jedis;

public interface RedisClient {
    <T> T work(Function<Jedis, T> f);

    <T> T workInLock(
            String lockKey,
            long maxRunTimeMs,
            Function<Jedis, T> f);
}
