package com.studio6.app.common.wq

import com.studio6.app.common.Log
import com.studio6.app.common.Predef._
import com.studio6.app.common.SecurityUtils
import com.studio6.app.common.proto.CommonProtos.WorkQueue.Delayed
import com.studio6.app.common.proto.CommonProtos.WorkQueue.{Item => WQItem}

import com.google.protobuf.ByteString
import com.google.protobuf.GeneratedMessage

import org.joda.time.DateTime

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.exceptions.JedisConnectionException

import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit._

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext

trait RedisClient extends Log {
  def pool: JedisPool
  def db: Int

  private val UnlockScript = """
      if redis.call("get", KEYS[1]) == ARGV[1]
      then
        redis.call("del", KEYS[1])
        return "OK"
      else
        return "BROKE"
      end
      """

  def inRedisLock[T](lockName: String, maxRunTimeMs: Long)(fn: Jedis => T): T = inRedis { jedis => {
    val nonce = SecurityUtils.randomAlphanumeric(12)

    var status = jedis.set(lockName, nonce, "NX", "PX", maxRunTimeMs)
    while (status != "OK") {
      val sleepTime = 10l + (SecurityUtils.randomPositiveInt % 20)
      Thread.sleep(sleepTime)

      status = jedis.set(lockName, nonce, "NX", "PX", maxRunTimeMs)
    }

    try {
      fn(jedis)
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
  }}

  def inRedis[T](fn: Jedis => T): T = {
    val jedis = pool.getResource()
    var isBroken = false
    try {
      jedis.select(db)
      fn(jedis)
    } catch {
      case e: Exception => {
        isBroken = true
        throw e
      }
    } finally {
      if (null != jedis) {
        if (isBroken) pool.returnBrokenResource(jedis)
        else pool.returnResource(jedis)
      }
    }
  }
}

case class WorkQueueConfig(
    maxProcessMs: Int = 4000,
    maxAttempts: Int = 2,
    delayBetweenAttemptsS: Int = 10)

object Item {
  case class Attempt(creationDate: DateTime, message: Option[String])
}

case class Item(id: String, creationDate: DateTime, item: Array[Byte], attempts: Seq[Item.Attempt])

trait WorkQueueClient {
  def submit(qname: String, bytes: Array[Byte]): Unit
  def submit(qname: String, bytes: Array[Byte], delayMs: Int): Unit
  def retry(qname: String, item: Item): Unit
  def delete(qname: String): Unit
  def delete(qname: String, item: Item): Boolean
  def size(qname: String): Long
  def inProgressSize(qname: String): Long
  def failSize(qname: String): Long
  def getItems(qname: String, offset: Int, limit: Int): Seq[Item]
  def getInProgressItems(qname: String, offset: Int, limit: Int): Seq[Item]
  def getFailedItems(qname: String, offset: Int, limit: Int): Seq[Item]

  def findAllDelayedItems(): Seq[Delayed]
  def undelay(id: String): Unit
  def undelayAllReadyToSubmit(): Unit
  def getAllQueueNames(): Seq[String]
}

object predef {
  implicit class QString(name: String) {
    def idseq = "__wq.%s.idseq".format(name).toUtf8Bytes
    def items = "__wq.%s.items".format(name).toUtf8Bytes
    def pending = "__wq.%s.pending".format(name).toUtf8Bytes
    def inProgress = "__wq.%s.in-progress".format(name).toUtf8Bytes
    def fails = "__wq.%s.fails".format(name).toUtf8Bytes
  }
}

import predef._

class RedisWorkQueueClient(jedisPool: JedisPool, redisDb: Int)
  extends WorkQueueClient
{
  private val DelayItems = "__queue.delay-items".toUtf8Bytes
  private val DelayItemsSeq = "__queue.delay-idseq".toUtf8Bytes

  private val client = new RedisClient {
    val pool = jedisPool
    val db = redisDb
  }

  override def submit(qname: String, bytes: Array[Byte]) = {
    val b = WQItem.newBuilder
    b.setCreationDate(DateTime.now().toString)
        .setItem(ByteString.copyFrom(bytes))

    client.inRedis { jedis => {
      val id = jedis.incr(qname.idseq).toString
      b.setId(id.toString)
      jedis.hset(qname.items, id.toUtf8Bytes, b.build.toByteArray)
      jedis.rpush(qname.pending, id.toUtf8Bytes)
    }}
  }

  override def submit(qname: String, bytes: Array[Byte], delayMs: Int) = {
    val submitTime = DateTime.now().plusMillis(delayMs)
    val b = Delayed.newBuilder
        .setDatetimeToSubmit(submitTime.toString)
        .setItem(ByteString.copyFrom(bytes))
        .setQname(qname)
    client.inRedis { jedis => {
      val id = jedis.incr(DelayItemsSeq).toString
      b.setId(id.toString)
      jedis.hset(DelayItems, id.toUtf8Bytes, b.build.toByteArray)
    }}
  }

  override def retry(qname: String, item: Item) = {
    val i = item.asInstanceOf[WQItem]
    val id = i.getId.toUtf8Bytes
    client.inRedis { jedis => {
      jedis.lrem(qname.fails, 0, id)
      jedis.rpush(qname.pending, id)
    }}
  }

  override def delete(qname: String) = {
    client.inRedis { jedis => {
      jedis.del(qname.items)
      jedis.del(qname.inProgress)
      jedis.del(qname.idseq)
      jedis.del(qname.fails)
      jedis.del(qname.pending)
    }}
  }

  override def delete(qname: String, item: Item) = {
    val i = item.asInstanceOf[WQItem]
    val id = i.getId.toUtf8Bytes
    client.inRedis { jedis => {
      jedis.lrem(qname.inProgress, 0, id)
      jedis.lrem(qname.fails, 0, id)
      jedis.lrem(qname.pending, 0, id)
      jedis.hdel(qname.items, id) == 1
    }}
  }

  override def size(qname: String) = {
    client.inRedis { jedis => {
      jedis.llen(qname.pending)
    }}
  }

  override def inProgressSize(qname: String) = {
    client.inRedis { jedis => {
      jedis.llen(qname.inProgress)
    }}
  }

  override def failSize(qname: String) = {
    client.inRedis { jedis => {
      jedis.llen(qname.fails)
    }}
  }

  override def getItems(qname: String, offset: Int, limit: Int) = getItems(qname, qname.pending, offset, limit)
  override def getInProgressItems(qname: String, offset: Int, limit: Int) = getItems(qname, qname.inProgress, offset, limit)
  override def getFailedItems(qname: String, offset: Int, limit: Int) = getItems(qname, qname.fails, offset, limit)

  override def getAllQueueNames() = {
    client.inRedis { jedis => {
      jedis.keys("__wq.*-idseq").map(n => n.substring(5, n.length - 4)).toSeq
    }}
  }

  override def findAllDelayedItems() = {
    client.inRedis { jedis => {
      findAllDelayedItems(jedis)
    }}
  }

  private def findAllDelayedItems(jedis: Jedis) = {
    jedis.hkeys(DelayItems).map(id => {
      Delayed.parseFrom(jedis.hget(DelayItems, id))
    }).toSeq
  }

  override def undelay(id: String) = {
    client.inRedis { jedis => {
      undelay(jedis, id)
    }}
  }

  private def undelay(jedis: Jedis, id: String) = {
    val delayed = Delayed.parseFrom(jedis.hget(DelayItems, id.toUtf8Bytes))
    jedis.hdel(DelayItems, id.toUtf8Bytes)
    submit(delayed.getQname, delayed.getItem.toByteArray)
  }

  override def undelayAllReadyToSubmit() = {
    client.inRedisLock("__queue.delay-lock", 4000l) { jedis => {
      findAllDelayedItems(jedis).foreach(d => {
        val now = DateTime.now
        val timeToSubmit = DateTime.parse(d.getDatetimeToSubmit)
        if (now.isAfter(timeToSubmit)) {
          undelay(jedis, d.getId)
        }
      })
    }}
  }

  private def getItems(qname: String, name: Array[Byte], offset: Int, limit: Int): Seq[Item] = {
    client.inRedis { jedis => {
      val ids = jedis.lrange(name, offset, (limit-1))
      if (ids != null) {
        ids.map(id => toItem(WQItem.parseFrom(jedis.hget(qname.items, id))))
      } else {
        Nil
      }
    }}
  }

  private def toItem(wi: WQItem): Item = {
    val attempts = wi.getAttemptList.map(a => {
      val message = if (a.hasMessage) Some(a.getMessage) else None
      Item.Attempt(DateTime.parse(wi.getCreationDate), message)
    })
    Item(wi.getId.toString, DateTime.parse(wi.getCreationDate), wi.getItem.toByteArray, attempts)
  }
}

class DelayedItemWorker(q: WorkQueueClient) extends Runnable with Log {
  private val executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
    override def newThread(r: Runnable) = new Thread(r, "delayed-item-worker")
  })

  def start(): Unit = executorService.scheduleWithFixedDelay(this, 10l, 10l, SECONDS)

  def stop(): Unit = executorService.shutdown()

  override def run(): Unit = {
    try {
      q.undelayAllReadyToSubmit()
    } catch {
      case e: Exception => log.error(e.getMessage, e)
    }
  }
}

abstract class RedisWorkQueueWorker[T](
    qname: String,
    config: WorkQueueConfig,
    jedisPool: JedisPool,
    redisDb: Int)
    (implicit exeCtx: ExecutionContext)
  extends Runnable
  with Log
{
  val client = new RedisClient {
    val pool = jedisPool
    val db = redisDb
  }

  def start(): Unit = {
    exeCtx.execute(this)
  }

  def blockingProcessNext() = {
    client.inRedis { jedis => {
      jedis.blpop(1024, qname.pending).toList match {
        case keyName :: id :: Nil => {
          val itemBuilder = WQItem.parseFrom(jedis.hget(qname.items, id)).toBuilder

          // Set default values
          if (!itemBuilder.hasMaxProcessMs) {
            itemBuilder.setMaxProcessMs(config.maxProcessMs)
          }

          if (!itemBuilder.hasMaxAttempts) {
            itemBuilder.setMaxAttempts(config.maxAttempts)
          }

          if (!itemBuilder.hasDelayBetweenAttemptsS) {
            itemBuilder.setDelayBetweenAttemptsS(config.delayBetweenAttemptsS)
          }
          // END Set default values

          val item = itemBuilder
              .setInProgressAttempt(WQItem.Attempt.newBuilder.setCreationDate(DateTime.now().toString))
              .build

          jedis.hset(qname.items, id, item.toByteArray)
          jedis.rpush(qname.inProgress, id)

          // Process the item
          var error: Throwable = null
          val t = deserialize(item.getItem.toByteArray)
          try {
            process(t)
          } catch {
            case e: Exception => { error = e }
          } finally {
            try {
              // Remove from in-progress
              jedis.lrem(qname.inProgress, 0, id) // Should return 1

              if (null == error) {
                jedis.hdel(qname.items, id) // Should return 1
              } else {
                val errorMessage = "%s: [%s]".format(error.getClass.getSimpleName, error.getMessage)
                log.info("errorMessage [{}}", errorMessage)
                
                val attempt = item.getInProgressAttempt
                val newItem = item.toBuilder
                    .clearInProgressAttempt()
                    .addAttempt(attempt.toBuilder.setMessage(errorMessage))
                    .build
                jedis.hset(qname.items, id, newItem.toByteArray)

                val attemptCount = item.getAttemptList.size + 1
                if (attemptCount >= item.getMaxAttempts) {
                  log.error("Work queue failure [{}] [{}]", Array(qname, errorMessage): _*)
                  exeCtx.reportFailure(error)
                  // Move to fails
                  jedis.rpush(qname.fails, id)
                } else {
                  // TODO Delay the retry
                  // Add back to pending
                  log.info("Rolling back item [{}] [{}]", Array(qname, errorMessage): _*)
                  jedis.rpush(qname.pending, id)
                }
              }
            } catch {
              case e: Exception => {
                log.error("Unexpected exception [{}]", e.getMessage)
                //log.error(e.getMessage, e)
              }
            }
          }
        }
        case null => // Do nothing
        case _ => sys.error("Unexpected result")
      }
    }}
  }

  override def run(): Unit = {
    val MaxBackoffSleepTime = 20l

    var backoffSleepTime = 1l
    while (true) {
      try {
        blockingProcessNext()
        backoffSleepTime = 1l
      } catch {
        case e: Exception => {
          try {
            log.warn(e.getMessage(), e)
            val t = MaxBackoffSleepTime.min(backoffSleepTime)
            log.info("Sleeping for [{}] seconds", t)
            Thread.sleep(SECONDS.toMillis(t))
          } catch {
            case e: InterruptedException => log.warn(e.getMessage, e)
          }
          backoffSleepTime = backoffSleepTime * 2
        }
      }
    }
  }

  protected def process(item: T) = ()

  protected def deserialize(bytes: Array[Byte]): T
}
