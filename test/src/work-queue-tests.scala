import com.studio6.app.common.redis._
import com.studio6.app.common.wq._

import com.studio6.app.common.Predef._
import com.studio6.app.common.wq._

import redis.clients.jedis.JedisPool

import scala.concurrent.ExecutionContext.Implicits.global

val jedisPool = new JedisPool("localhost", 6379)
val qconfig = WorkQueueConfig(maxAttempts = 4)
val qname = "test"

val redisClient = new RedisClientImpl(jedisPool, 0, 32)

val wq = new RedisWorkQueueWorker[String](qname, qconfig, redisClient) {
  override def deserialize(bytes: Array[Byte]) = new String(bytes, "utf-8")
  override def process(item: String) = {
    //if (true) sys.error("Fail")
    println("Processing \"%s\"".format(item))
  }
}


val wqClient = new RedisWorkQueueClient(redisClient)
wqClient.delete(qname)

//val delayedWorker = new DelayedItemWorker(wqClient)
//delayedWorker.start()

assert(wqClient.size(qname) == 0)
assert(wqClient.inProgressSize(qname) == 0)
assert(wqClient.failSize(qname) == 0)

val itemCount = 10

0.until(itemCount) foreach { i => {
  wqClient.submit(qname, i.toString.toUtf8Bytes, 10)
}}

assert(wqClient.failSize(qname) == 0)

wq.start()

Thread.sleep(1000 + (itemCount * 6))

//delayedWorker.stop()

assert(wqClient.size(qname) == 0)
assert(wqClient.inProgressSize(qname) == 0)
assert(wqClient.failSize(qname) == 0)

wqClient.stop()

println("done")
