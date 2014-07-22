import com.studio6.app.common.redis._
import com.studio6.app.common.wq._

import com.studio6.app.common.Predef._
import com.studio6.app.common.wq._

import redis.clients.jedis.JedisPool

import java.util.concurrent._
import java.util.concurrent.TimeUnit._

val jedisPool = new JedisPool("localhost", 6379)
val redisClient = new RedisClientImpl(jedisPool, 0, 32)

class StringWorker(qname: String, redisClient: RedisClient)
  extends RedisWorkQueueWorker[String](qname, redisClient, 1000, 2, 1000)
{
  override def deserialize(bytes: Array[Byte]) = new String(bytes, "utf-8")

  override def process(item: String) = {
    val l = item.length
  }
}

class FailStringWorker(qname: String, redisClient: RedisClient)
  extends RedisWorkQueueWorker[String](qname, redisClient, 1000, 2, 1000)
{
  override def deserialize(bytes: Array[Byte]) = new String(bytes, "utf-8")

  override def process(item: String) = {
    log.info(s"processing $item")
    if (true) sys.error("fail")
    val l = item.length
  }
}

object Test {
  val ThreadCount = 4
  val IterationCount = 128

  def main(args: Array[String]): Unit = {
    val qname = "test"
    val executor = Executors.newFixedThreadPool(ThreadCount)
    val wqClient = new RedisWorkQueueClient(redisClient)
    wqClient.delete(qname)

    assert(wqClient.size(qname) == 0)
    assert(wqClient.inProgressSize(qname) == 0)
    assert(wqClient.failSize(qname) == 0)

    val worker = new StringWorker(qname, redisClient)

    worker.start()

    try {
      0.until(ThreadCount).foreach(i => {
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              0.until(IterationCount).foreach(j => {
                print('.')
                wqClient.submit(qname, "item".toUtf8Bytes)
              })
            } catch {
              case e: Exception => e.printStackTrace
            }
          }
        })
      })

      executor.shutdown()
      executor.awaitTermination(1l, MINUTES)
      println
    } catch {
      case e: Exception => e.printStackTrace
    }

    var pending = wqClient.size(qname)
    while (pending > 0) {
      Thread.sleep(100)
      val newPending = wqClient.size(qname)
      if (newPending >= pending) {
        sys.error(s"Pending count grew [$pending] [$newPending]")
      }
      pending = newPending
    }

    assert(wqClient.size(qname) == 0)
    assert(wqClient.inProgressSize(qname) == 0)
    assert(wqClient.failSize(qname) == 0)

    worker.stop()
    wqClient.stop()
  }
}

object TestFails {
  def main(args: Array[String]): Unit = {
    val qname = "test2"
    val wqClient = new RedisWorkQueueClient(redisClient)
    wqClient.delete(qname)

    assert(wqClient.size(qname) == 0)
    assert(wqClient.inProgressSize(qname) == 0)
    assert(wqClient.failSize(qname) == 0)

    val worker = new FailStringWorker(qname, redisClient)

    println("worker started")

    wqClient.submit(qname, "item".toUtf8Bytes)

    assert(wqClient.size(qname) == 1)
    worker.start()
    Thread.sleep(500l)


    assert(wqClient.size(qname) == 0)
    assert(wqClient.inProgressSize(qname) == 0)
    assert(wqClient.failSize(qname) == 1)

    worker.stop()
    wqClient.stop()
  }
}

Test.main(args)
TestFails.main(args)

//val delayedWorker = new DelayedItemWorker(wqClient)
//delayedWorker.start()
