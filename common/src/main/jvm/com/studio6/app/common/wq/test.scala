package com.studio6.app.common.wq

import com.studio6.app.common.Predef._
import com.studio6.app.common.wq._

import redis.clients.jedis.JedisPool

import scala.concurrent.ExecutionContext.Implicits.global

object Test {
  def main(args: Array[String]): Unit = {
    val jedisPool = new JedisPool("localhost", 6379)
    val qconfig = WorkQueueConfig("ian")

    val wq = new RedisWorkQueueWorker[String](qconfig, jedisPool, 0) {
      override def deserialize(bytes: Array[Byte]) = new String(bytes, "utf-8")
      override def process(item: String) = {
        if (true) sys.error("Fail")
        println("Processing \"%s\"".format(item))
      }
    }

    val wqClient = new RedisWorkQueueClient(qconfig, jedisPool, 0)
    wqClient.delete()

    assert(wqClient.size() == 0)
    assert(wqClient.inProgressSize() == 0)
    assert(wqClient.failSize() == 0)

    val itemCount = 1000

    0.until(itemCount) foreach { i => {
      wqClient.submit(i.toString.toUtf8Bytes)
    }}

    assert(wqClient.size() == itemCount)
    assert(wqClient.inProgressSize() == 0)
    assert(wqClient.failSize() == 0)

    wq.start()

    Thread.sleep(1000 + (itemCount * 6))

    assert(wqClient.size() == 0)
    assert(wqClient.inProgressSize() == 0)
    assert(wqClient.failSize() == itemCount)
  }
}

object Stats {
  def main(args: Array[String]): Unit = {
    val jedisPool = new JedisPool("localhost", 6379)
    val qconfig = WorkQueueConfig("ian")
    val wqClient = new RedisWorkQueueClient(qconfig, jedisPool, 0)
    println("Size: " + wqClient.size())
    println("In Progress: " + wqClient.inProgressSize())
    println("Fail: " + wqClient.failSize())
  }
}
