package com.fortysevendeg.log

import com.fortysevendeg.log.utils.Config._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object ShowExceptionsFromParquet {

  def main(args: Array[String]) = {

    // Spark configuration

    val conf = new SparkConf().setMaster("local[2]").setAppName("ShowExceptionsFromParquet")
    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    val parquetFile = sqlContext.read.parquet(exceptionPathParquet)

    parquetFile.registerTempTable("Exceptions")
    val countExceptions = sqlContext.sql("SELECT name, message FROM Exceptions")

    countExceptions.map(row => s"${row.getString(0)} (${row.getString(1)})").collect().foreach(println)

  }

}
