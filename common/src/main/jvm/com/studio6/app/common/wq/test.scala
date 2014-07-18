package com.studio6.app.common.wq

import com.studio6.app.common.Predef._
import com.studio6.app.common.wq._

import redis.clients.jedis.JedisPool

import scala.concurrent.ExecutionContext.Implicits.global

object Test {
  def main(args: Array[String]): Unit = {
    val jedisPool = new JedisPool("localhost", 6379)
    val qconfig = WorkQueueConfig(maxAttempts = 4)
    val qname = "test"

    val wq = new RedisWorkQueueWorker[String](qname, qconfig, jedisPool, 0) {
      override def deserialize(bytes: Array[Byte]) = new String(bytes, "utf-8")
      override def process(item: String) = {
        if (true) sys.error("Fail")
        println("Processing \"%s\"".format(item))
      }
    }


    val wqClient = new RedisWorkQueueClient(jedisPool, 0)
    wqClient.delete(qname)

    val delayedWorker = new DelayedItemWorker(wqClient)
    delayedWorker.start()

    assert(wqClient.size(qname) == 0)
    assert(wqClient.inProgressSize(qname) == 0)
    assert(wqClient.failSize(qname) == 0)

    val itemCount = 1000

    0.until(itemCount) foreach { i => {
      wqClient.submit(qname, i.toString.toUtf8Bytes)
    }}

    assert(wqClient.size(qname) == itemCount)
    assert(wqClient.inProgressSize(qname) == 0)
    assert(wqClient.failSize(qname) == 0)

    wq.start()

    Thread.sleep(1000 + (itemCount * 6))

    delayedWorker.stop()

    assert(wqClient.size(qname) == 0)
    assert(wqClient.inProgressSize(qname) == 0)
    assert(wqClient.failSize(qname) == itemCount)

    println("done")
  }
}

object Stats {
  def main(args: Array[String]): Unit = {
    val jedisPool = new JedisPool("localhost", 6379)
    val qconfig = WorkQueueConfig()
    val qname = "test"
    val wqClient = new RedisWorkQueueClient(jedisPool, 0)
    println("Size: " + wqClient.size(qname))
    println("In Progress: " + wqClient.inProgressSize(qname))
    println("Fail: " + wqClient.failSize(qname))
  }
}
