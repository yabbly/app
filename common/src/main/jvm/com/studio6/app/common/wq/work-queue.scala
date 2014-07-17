package com.studio6.app.common.wq

import com.studio6.app.common.Log
import com.studio6.app.common.Predef._
import com.studio6.app.common.SecurityUtils
import com.studio6.app.common.proto.CommonProtos.WorkItem

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

trait RedisClient {
  def pool: JedisPool
  def db: Int

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
    name: String,
    maxProcessMs: Int = 4000,
    maxAttempts: Int = 2,
    waitBetweenAttemptsMs: Int = 1000)
{
  def seq = "__wq.%s.seq".format(name).toUtf8Bytes
  def items = "__wq.%s.items".format(name).toUtf8Bytes
  def pending = "__wq.%s.pending".format(name).toUtf8Bytes
  def inProgress = "__wq.%s.in-progress".format(name).toUtf8Bytes
  def fails = "__wq.%s.fails".format(name).toUtf8Bytes
}

object Item {
  case class Attempt(creationDate: DateTime, message: Option[String])
}

case class Item(id: String, creationDate: DateTime, item: Array[Byte], attempts: Seq[Item.Attempt])

trait WorkQueueClient {
  def submit(bytes: Array[Byte]): Unit
  def retry(item: Item): Unit
  def delete(): Unit
  def delete(item: Item): Boolean
  def size(): Long
  def inProgressSize(): Long
  def failSize(): Long
  def getItems(offset: Int, limit: Int): Seq[Item]
  def getInProgressItems(offset: Int, limit: Int): Seq[Item]
  def getFailedItems(offset: Int, limit: Int): Seq[Item]
  def getAllQueueNames(): Seq[String]
}

class RedisWorkQueueClient(config: WorkQueueConfig, jedisPool: JedisPool, redisDb: Int)
  extends WorkQueueClient
{
  private val client = new RedisClient {
    val pool = jedisPool
    val db = redisDb
  }

  override def submit(bytes: Array[Byte]) = {
    val b = WorkItem.newBuilder
    b.setCreationDate(DateTime.now().toString)
        .setMaxProcessMs(config.maxProcessMs)
        .setMaxAttempts(config.maxAttempts)
        .setWaitBetweenAttemptsMs(config.waitBetweenAttemptsMs)
        .setItem(ByteString.copyFrom(bytes))

    client.inRedis { jedis => {
      val id = jedis.incr(config.seq).toString
      b.setId(id.toString)
      jedis.hset(config.items, id.toUtf8Bytes, b.build.toByteArray)
      jedis.rpush(config.pending, id.toUtf8Bytes)
    }}
  }

  override def retry(item: Item) = {
    val i = item.asInstanceOf[WorkItem]
    val id = i.getId.toUtf8Bytes
    client.inRedis { jedis => {
      jedis.lrem(config.fails, 0, id)
      jedis.rpush(config.pending, id)
    }}
  }

  override def delete() = {
    client.inRedis { jedis => {
      jedis.del(config.items)
      jedis.del(config.inProgress)
      jedis.del(config.seq)
      jedis.del(config.fails)
      jedis.del(config.pending)
    }}
  }

  override def delete(item: Item) = {
    val i = item.asInstanceOf[WorkItem]
    val id = i.getId.toUtf8Bytes
    client.inRedis { jedis => {
      jedis.lrem(config.inProgress, 0, id)
      jedis.lrem(config.fails, 0, id)
      jedis.lrem(config.pending, 0, id)
      jedis.hdel(config.items, id) == 1
    }}
  }

  override def size() = {
    client.inRedis { jedis => {
      jedis.llen(config.pending)
    }}
  }

  override def inProgressSize() = {
    client.inRedis { jedis => {
      jedis.llen(config.inProgress)
    }}
  }

  override def failSize() = {
    client.inRedis { jedis => {
      jedis.llen(config.fails)
    }}
  }

  override def getItems(offset: Int, limit: Int) = getItems(config.pending, offset, limit)
  override def getInProgressItems(offset: Int, limit: Int) = getItems(config.inProgress, offset, limit)
  override def getFailedItems(offset: Int, limit: Int) = getItems(config.fails, offset, limit)

  override def getAllQueueNames() = {
    client.inRedis { jedis => {
      jedis.keys("__wq.*-seq").map(n => n.substring(5, n.length - 4)).toSeq
    }}
  }

  private def getItems(name: Array[Byte], offset: Int, limit: Int): Seq[Item] = {
    client.inRedis { jedis => {
      val ids = jedis.lrange(name, offset, (limit-1))
      if (ids != null) {
        ids.map(id => toItem(WorkItem.parseFrom(jedis.hget(config.items, id))))
      } else {
        Nil
      }
    }}
  }

  private def toItem(wi: WorkItem): Item = {
    val attempts = wi.getAttemptList.map(a => {
      val message = if (a.hasMessage) Some(a.getMessage) else None
      Item.Attempt(DateTime.parse(wi.getCreationDate), message)
    })
    Item(wi.getId.toString, DateTime.parse(wi.getCreationDate), wi.getItem.toByteArray, attempts)
  }

/*
    @Override
    public List<DelayedItem> findAllDelayedItems(final int offset, final int limit) {
        return client.work(new Function<Jedis, List<DelayedItem>>() {
            public List<DelayedItem> apply(Jedis jedis) {
                Set<String> vals = jedis.zrange("delayed-job-ids", offset, limit);
                List<DelayedItem> djs = Lists.newArrayList();
                for (String s : vals) {
                    try {
                        byte[] val = jedis.hget(u("delayed-jobs"), u(s));
                        djs.add(DelayedItem.parseFrom(val));
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
                return djs;
            }
        });
    }

    @Override
    public void undelayAllJobs() {
        int offset = 0;
        int limit = 256;

        List<DelayedItem> dis = findAllDelayedItems(0, limit);
        while (dis.size() > 0) {
            for (DelayedItem i : dis) {
                undelayJob(i.getId());
            }
            offset += limit;
            dis = findAllDelayedItems(0, limit);
        }
    }

    @Override
    public void undelayJob(final long id) {
        DelayedItem di = client.work(new Function<Jedis, DelayedItem>() {
            @Override
            public DelayedItem apply(Jedis jedis) {
                try {
                    byte[] v = jedis.hget(u("delayed-jobs"), u(String.valueOf(id)));
                    return DelayedItem.parseFrom(v);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // TODO this should be done within a txn
        deleteDelayedItem(id);
        submit(di.getQname(), di.getItemId());
    }
*/
}

abstract class RedisWorkQueueWorker[T](
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
      jedis.blpop(1024, config.pending).toList match {
        case keyName :: id :: Nil => {
          val item = WorkItem.parseFrom(jedis.hget(config.items, id)).toBuilder
              .setInProgressAttempt(WorkItem.Attempt.newBuilder.setCreationDate(DateTime.now().toString))
              .build

          jedis.hset(config.items, id, item.toByteArray)
          jedis.rpush(config.inProgress, id)

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
              jedis.lrem(config.inProgress, 0, id) // Should return 1

              if (null == error) {
                jedis.hdel(config.items, id) // Should return 1
              } else {
                val errorMessage = "%s: [%s]".format(error.getClass.getSimpleName, error.getMessage)
                log.info("errorMessage [{}}", errorMessage)
                
                val attempt = item.getInProgressAttempt
                val newItem = item.toBuilder
                    .clearInProgressAttempt()
                    .addAttempt(attempt.toBuilder.setMessage(errorMessage))
                    .build
                jedis.hset(config.items, id, newItem.toByteArray)

                val attemptCount = item.getAttemptList.size + 1
                if (attemptCount >= item.getMaxAttempts) {
                  log.error("Work queue failure [{}] [{}]", Array(config.name, errorMessage): _*)
                  exeCtx.reportFailure(error)
                  // Move to fails
                  jedis.rpush(config.fails, id)
                } else {
                  // TODO Delay the retry
                  // Add back to pending
                  log.info("Rolling back item [{}] [{}]", Array(config.name, errorMessage): _*)
                  jedis.rpush(config.pending, id)
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
