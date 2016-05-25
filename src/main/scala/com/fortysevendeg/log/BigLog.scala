package com.fortysevendeg.log

import akka.actor.ActorSystem
import com.fortysevendeg.log.models._
import com.fortysevendeg.log.utils.Config
import com.fortysevendeg.log.utils.Config._
import com.fortysevendeg.log.utils.Regex._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.http4s.client.blaze.PooledHttp1Client
import org.http4s.util.UrlCodingUtils._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

object BigLog {

  def main(args: Array[String]) = {

    // run:
    // $ adb logcat -v time | nc -lk 9999

    // Spark configuration

    val conf = new SparkConf().setMaster("local[2]").setAppName("BigLog")
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

    logs.cache()

    // Real time

//    logs foreachRDD (_.foreach { log =>
//      println(s"${log.info.logType}: ${log.info.app}: ${log.message}")
//    })

    // Log Exception

    val exceptionWindow = logs.filter(ll => ll.info.logType == Error || ll.info.logType == Warning).window(Seconds(2))

    val exceptions = exceptionWindow.flatMap { logLine =>
      exception.findFirstIn(logLine.message) map (LogException(_, logLine.message, logLine.info.date))
    }

    exceptions.foreachRDD { exception =>
      exception.toDF().write.mode(SaveMode.Append).parquet(exceptionPathParquet)
      exception.foreachPartition { iterLog =>

        val system = ActorSystem.create("Scheduler")
        val client = PooledHttp1Client()

        iterLog foreach { log =>
          println(s"FOUNDED: ==>> ${log.name} ${log.message}")
          val url = s"${Config.urlSearchStackOverflow}${urlEncode(log.message)}"
          println("URL: ==>> " + url)
          system.scheduler.scheduleOnce(0 seconds) {
            val url = s"${Config.urlSearchStackOverflow}${urlEncode(log.message)}"
            println("URL: ==>> " + url)
            client.getAs[String](url)
              .runAsync(_.fold(
                l => println(s"Error: ${log.message} => ${l.getMessage}"),
                r => println(s"JSON => $r")//println(s"${r.items.length} ${r.items.map(_.title).mkString("\n")}")
              ))
          }
        }

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}



