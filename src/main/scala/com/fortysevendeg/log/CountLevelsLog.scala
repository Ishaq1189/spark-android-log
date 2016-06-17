package com.fortysevendeg.log

import com.fortysevendeg.log.models._
import com.fortysevendeg.log.utils.Regex._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps

object CountLevelsLog {

  def main(args: Array[String]) = {

    // run:
    // $ adb logcat -v time | nc -lk 9999

    // Spark configuration

    val conf = new SparkConf().setMaster("local[2]").setAppName("CountLevelsLog")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("/tmp")

    val logLines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val logs = logLines.flatMap { line =>
      for {
        typePlusAppPlusPid <- typePlusAppPlusPid.findFirstIn(line)
        data = extractTypeAppPid(typePlusAppPlusPid)
        logType = data._1
        app <- data._2
        pid <- data._3
        date <- date.findFirstIn(line)
        message <- message.findFirstIn(line)
      } yield {
        LogLine(LogInfo(app, pid, logType, date), message.substring(2))
      }
    }

//    logs.cache()

//    val exceptionWindow = logs.window(Seconds(2))

    val levels = logs.map { logLine =>
      (logLine.info.logType.toString, 1)
    }

    val counters: DStream[(String, Int)] = levels.reduceByKeyAndWindow(_ + _, _ - _, Seconds(5), Seconds(1))

    val globalCounters: DStream[(String, Int)] = counters.updateStateByKey {
      (counts: Seq[Int], oldState: Option[Int]) => Option(oldState.fold(counts.sum)(_ + counts.sum))
    }

    val info = counters.join(globalCounters)

    counters.foreachRDD(rdd =>
      println("\nTop 10 levels:\n" + rdd.take(10).mkString("\n")))

    info.foreachRDD(rdd =>
      println("\nTop 10 counters:\n" + rdd.take(10).mkString("\n")))

    ssc.start()
    ssc.awaitTermination()
  }
}



