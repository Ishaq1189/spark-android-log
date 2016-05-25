package com.fortysevendeg.log

import com.fortysevendeg.log.models._
import com.fortysevendeg.log.utils.Regex._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps

object SimpleDataStream {

  def main(args: Array[String]) = {

    // run:
    // $ adb logcat -v time | nc -lk 9999

    // Spark configuration

    val conf = new SparkConf().setMaster("local[2]").setAppName("SimpleDataStream")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Milliseconds(100))
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

    logs foreachRDD (_.foreach { log =>
      println(s"${log.info.logType}: ${log.info.app}: ${log.message}")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}



