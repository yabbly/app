package com.studio6.app.common.wq

import com.studio6.app.common.Predef._
import com.studio6.app.common.CommonProtos.WorkQueue.Item

import com.google.protobuf.ByteString

import org.joda.time.DateTime

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.exceptions.JedisConnectionException

import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit._

trait RedisClient {
  def pool: JedisPool
  def db: Option[Int] = None

  def inRedis[T](fn: Jedis => T) {
    val jedis = pool.getResource()
    var isBroken = false
    try {
      db.foreach(db => jedis.select(db))
      fn(jedis)
    } finally {
      if (isBroken) pool.returnBrokenResource(jedis)
      else pool.returnResource(jedis)
    }
  }
}

trait WorkQueue[T] {
  def submit(item: T): Unit
  def process(item: T): Unit
}

abstract class RedisWorkQueue[T](
    name: String,
    client: RedisClient,
    maxProcessMs: Long)
  extends WorkQueue[T]
{
  case class QName(name: String) {
    def seq = "%s.seq".format(name).toUtf8Bytes
    def items = "%s.items".format(name).toUtf8Bytes
    def pending = "%s.pending".format(name).toUtf8Bytes
  }

  private val qname = QName(name)

  override def submit(item: T): Unit = {
    val b = Item.newBuilder
    b.setCreationDate(DateTime.now().toString)
        .setMaxProcessMs(maxProcessMs)
        .setItem(ByteString.copyFrom(serialize(item)))

    client.inRedis { jedis => {
      val id = jedis.incr(qname.seq).toString
      b.setId(id.toString)
      jedis.hset(qname.items, id.toUtf8Bytes, b.build.toByteArray)
      jedis.rpush(qname.pending, id.toUtf8Bytes)
    }}
  }

  override def process(item: T) = ()

  protected def serialize(item: T): Array[Byte]

  protected def deserialize(bytes: Array[Byte]): T
}

trait SingleThreadExecutorWorkQueueProcessor extends Runnable {
  def threadName: String

  val executor = Executors.newSingleThreadExecutor(new ThreadFactory {
    override def newThread(r: Runnable) = {
      val t = new Thread(r, threadName)
      t.setDaemon(true)
      t
    }
  })

  executor.submit(this)

  override def run(): Unit = {
    // Pick items off the work queue and process them
  }
}
