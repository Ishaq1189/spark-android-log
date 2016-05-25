package com.fortysevendeg.log

import com.fortysevendeg.log.models._
import com.fortysevendeg.log.utils.Config._
import com.fortysevendeg.log.utils.Regex._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps

object SaveToParquet {

  def main(args: Array[String]) = {

    // run:
    // $ adb logcat -v time | nc -lk 9999

    // Spark configuration

    val conf = new SparkConf().setMaster("local[2]").setAppName("SaveToParquet")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Milliseconds(100))
    ssc.checkpoint("/tmp")

    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

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

    // Log Exception

    val exceptionWindow = logs.filter(ll => ll.info.logType == Error || ll.info.logType == Warning).window(Seconds(2))

    val exceptions = exceptionWindow.flatMap { logLine =>
      exception.findFirstIn(logLine.message) map (LogException(_, logLine.message, logLine.info.date))
    }

    exceptions.foreachRDD { exception =>
      exception.toDF().write.mode(SaveMode.Append).parquet(exceptionPathParquet)
      exception foreach { log =>
        println(s"name: ${log.name}: -- ${log.message}")
      }

    }

    ssc.start()
    ssc.awaitTermination()
  }
}



