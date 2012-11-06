package cassie_hector

import com.twitter.cassie.clocks.MicrosecondEpochClock
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicInteger
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate
import me.prettyprint.cassandra.serializers.{LongSerializer, IntegerSerializer, StringSerializer}
import com.twitter.cassie.types.LexicalUUID
import me.prettyprint.hector.api.beans.HColumn

object HectorClient extends App {


  override def main(args: Array[String]) {
    // Command-line args
    val host          = args(0)
    val concurrency   = args(1).toInt
    val totalRequests = args(2).toInt

    // ENV Settings
    val cluster = HFactory.getOrCreateCluster("TestCluster", host)
    val keyspace = HFactory.createKeyspace("test", cluster)
    val batchInfoTemplate = new ThriftColumnFamilyTemplate[String, String](keyspace, "testcf", StringSerializer.get, StringSerializer.get)
    val ttl = 300

    val completedRequests = new AtomicInteger(0)
    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(concurrency)

    println("Press ENTER to start test")
    val BLOCK = readLine()
    // Stat tracking
    val start = Time.now

    try
    {
      (1 to totalRequests).par.foreach { _ =>
        val bi = LexicalUUID(MicrosecondEpochClock).toString()

        val mutator = batchInfoTemplate.createMutator()

        val batchCountCol: HColumn[String, Int] = HFactory.createColumn("test1", 5, StringSerializer.get, IntegerSerializer.get.asInstanceOf[Serializer[Int]])
        batchCountCol.setTtl(ttl)
        mutator.addInsertion(bi, batchInfoTemplate.getColumnFamily, batchCountCol)

        val startTimeCol: HColumn[String, Long] = HFactory.createColumn("test2", MicrosecondEpochClock.timestamp, StringSerializer.get, LongSerializer.get.asInstanceOf[Serializer[Long]])
        startTimeCol.setTtl(ttl)
        mutator.addInsertion(bi, batchInfoTemplate.getColumnFamily, startTimeCol)

        mutator.execute()

        completedRequests.incrementAndGet
      }
    }
    finally
    {
      cluster.getConnectionManager.shutdown
    }

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

