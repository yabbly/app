package com.studio6.app.common.redis;

import com.google.common.base.Function;

import org.joda.time.DateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

public class RedisClientImpl implements RedisClient {
    private static final Logger log = LoggerFactory.getLogger(RedisClientImpl.class);

    private static final String UNLOCK_SCRIPT =
              "if redis.call(\"get\", KEYS[1]) == ARGV[1]\n"
            + "then\n"
            + "  redis.call(\"del\", KEYS[1])\n"
            + "  return \"OK\"\n"
            + "else\n"
            + "  return \"BROKE\"\n"
            + "end";


    private long maxConnectionPoolTimeoutMs = 0l;
    private int maxActiveConnections;
    private int db;
    private JedisPool jedisPool;
    private Random random = new Random();

    public RedisClientImpl(JedisPool jedisPool, int db, int maxActiveConnections) {
        this.maxActiveConnections = maxActiveConnections;
        this.jedisPool = jedisPool;
        this.db = db;
    }

    @Override
    public <T> T workInLock(
            final String lockKey,
            final long maxRunTimeMs,
            final Function<Jedis, T> f)
    {
        return work(new Function<Jedis, T>() {
            @Override
            public T apply(Jedis jedis) {
                // Get a lock
                String nonce = randomAlphanumeric(16);
                String status = jedis.set(lockKey, nonce, "NX", "PX", maxRunTimeMs);
                while (!"OK".equals(status)) {
                    try {
                        
                        long sleepTime = 10l + (Math.abs(random.nextInt()) % 20);
                        Thread.sleep(sleepTime);
                        status = jedis.set(lockKey, nonce, "NX", "PX", maxRunTimeMs);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                try {
                    return f.apply(jedis);
                } finally {
                    String ret = (String) jedis.eval(UNLOCK_SCRIPT, 1, lockKey, nonce);
                    if ("OK".equals(ret)) {
                        // Do nothing
                    } else if ("BROKE".equals(ret)) {
                        try {
                            throw new RuntimeException();
                        } catch (RuntimeException e) {
                            log.error("Job took too long [>{}ms] and lost lock [{}]", new Object[] {maxRunTimeMs, lockKey, e});
                        }
                    }
                }
            }
        });
    }

    /*
     * Handle timed out connections. Also must handle redis being down.
     * If redis is down, an exponential backoff will be employed if
     * maxConnectionPoolTimeoutMs is gt 0l.
     */
    @Override
    public <T> T work(Function<Jedis, T> f) {
        int tryCountdown = maxActiveConnections;
        long sleepTime = 10l;
        while (true) {
            try {
                return doWork(f);
            } catch (JedisConnectionException e) {
                if (tryCountdown < 0) {
                    if (sleepTime > maxConnectionPoolTimeoutMs) {
                        log.info("Reached max sleep time [{}]. Will not wait any longer", sleepTime);
                        throw e;
                    }
                    try {
                        log.info("Sleeping for [{}ms]", sleepTime);
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e2) {
                        throw new RuntimeException(e2);
                    }
                    sleepTime *= 2;
                }
            }
            tryCountdown--;
        }
    }

    private <T> T doWork(Function<Jedis, T> f) {
        Jedis jedis = jedisPool.getResource();

        boolean isBroken = false;
        try {
            jedis.select(db);
            return f.apply(jedis);
        } catch (JedisConnectionException e) {
            log.info("Caught JedisConnectionException");
            isBroken = true;
            throw e;
        } catch (Exception e) { // Check if the Function wrapped a JedisConnectionException
            isBroken = true;
            if (e.getCause() != null && e.getCause() instanceof JedisConnectionException) {
                log.info("Caught wrapped JedisConnectionException");
                //isBroken = true;
                throw (JedisConnectionException) e.getCause();
            }
            // TODO double check that this is correct
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            log.trace("Jedis isBroken [{}]", isBroken);
            if (jedis != null) {
                if (isBroken) {
                    log.info("Returned broken connection");
                    jedisPool.returnBrokenResource(jedis);
                } else {
                    jedisPool.returnResource(jedis);
                }
            }
        }
    }

    public void setMaxConnectionPoolTimeoutMs(long maxConnectionPoolTimeoutMs) {
        this.maxConnectionPoolTimeoutMs = maxConnectionPoolTimeoutMs;
    }

    public void setMaxActiveConnections(int maxActiveConnections) {
        this.maxActiveConnections = maxActiveConnections;
    }
}
