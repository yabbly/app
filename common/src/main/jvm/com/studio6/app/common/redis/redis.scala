package com.studio6.app.common.redis

import com.studio6.app.common.SecurityUtils.{randomAlphanumeric, randomPositiveInt}
import com.studio6.app.common.Log

import com.google.common.base.Function

import redis.clients.jedis.Jedis

trait RedisClientHelper extends Log {
  def redisClient: RedisClient

  private val UnlockScript = """
      if redis.call("get", KEYS[1]) == ARGV[1]
      then
        redis.call("del", KEYS[1])
        return "OK"
      else
        return "BROKE"
      end
      """

  def withJedis[T](fn: Jedis => T): T = redisClient.work(new Function[Jedis, T] {
    override def apply(jedis: Jedis): T = fn(jedis)
  })

  def inLock[T](jedis: Jedis, lockName: String, maxRunTimeMs: Long)(b: => T): T = {
    val nonce = randomAlphanumeric(12)

    var status = jedis.set(lockName, nonce, "NX", "PX", maxRunTimeMs)
    while (status != "OK") {
      val sleepTime = 10l + (randomPositiveInt % 20)
      Thread.sleep(sleepTime)

      status = jedis.set(lockName, nonce, "NX", "PX", maxRunTimeMs)
    }

    try {
      b
    } finally {
      // Unlock
      jedis.eval(UnlockScript, 1, lockName, nonce) match {
        case "OK" => // Do nothing
        case "BROKE" => {
          try {
            throw new RuntimeException
          } catch {
            case e: RuntimeException => {
              log.error("Job took too long [>{}ms] and lost lock [{}]", Array[Object](maxRunTimeMs.asInstanceOf[AnyRef], lockName, e))
            }
          }
        }
        case o: Object => log.error("Unexpected return value [{}]", o)
      }
    }
  }
}
