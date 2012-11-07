package cassie_hector

import com.twitter.cassie.{Column, Cluster}
import com.twitter.cassie.codecs.{LongCodec, IntCodec, ByteArrayCodec, Utf8Codec}
import com.twitter.util.TimeConversions._
import com.twitter.cassie.types.LexicalUUID
import com.twitter.cassie.clocks.MicrosecondEpochClock
import com.twitter.util.{Promise, Time, Future}
import java.util.concurrent.atomic.AtomicInteger

object CassieClient extends App {

  override def main(args: Array[String]) {
    // Command-line args
    val host          = args(0)
    val concurrency   = args(1).toInt
    val totalRequests = args(2).toInt

    // ENV Settings
    val cluster = new Cluster(host, 9160)
    val keyspace = cluster.mapHostsEvery(10.minutes).keyspace("test").connect()
    val batchInfo = keyspace.columnFamily("testcf", Utf8Codec, Utf8Codec, ByteArrayCodec)
    val ttl = 30.minutes

    val completedRequests = new AtomicInteger(0)
    val completedLoops = new AtomicInteger(0)

    println("Press ENTER to start test")
    val BLOCK = readLine()
    // Stat tracking
    val start = Time.now

    def complete() {
      if (completedLoops.incrementAndGet == concurrency) {
        val duration = start.untilNow
        println("================")
        println("%d writes completed in %dms\r\n%f requests per second\r\n%fms average".format(
          completedRequests.get, duration.inMilliseconds,
          completedRequests.get.toFloat / duration.inMillis.toFloat * 1000,
          duration.inMillis.toFloat / completedRequests.get.toFloat
        ))
        println("=====")

        System.exit(0)
      }
    }

    (1 to concurrency).map { _ =>
      def loop(): Future[Unit] = {
        val bi = LexicalUUID(MicrosecondEpochClock).toString()
        batchInfo.batch()
          .insert(bi, Column("test1", IntCodec.encode(5)).ttl(ttl))
          .insert(bi, Column("test2", LongCodec.encode(MicrosecondEpochClock.timestamp)).ttl(ttl))
          .execute()
          .flatMap { _ =>
            if (completedRequests.getAndIncrement < totalRequests) {
              // fire another request
              loop()
            } else {
              // we're done
              complete()
              Future.Unit
            }
          }
      }
      // fire an async loop per `concurrency`
      loop()
    }

    Thread.sleep(Long.MaxValue)    
  }
}