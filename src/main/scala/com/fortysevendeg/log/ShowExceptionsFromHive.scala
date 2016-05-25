package com.fortysevendeg.log

import com.fortysevendeg.log.utils.Config._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ShowExceptionsFromHive {

  def main(args: Array[String]) = {

    // Spark configuration

    val conf = new SparkConf().setMaster("local[2]").setAppName("ShowExceptionsFromHive")
    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    val dataFrame = sqlContext.table(tableHive)

    val dfName = dataFrame.select("name", "date").where(s"name = 'ErrnoException'")

    dfName.map(row => s"${row.getString(0)} (${row.getString(1)})").collect().foreach(println)

  }

}
