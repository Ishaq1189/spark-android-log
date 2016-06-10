package com.fortysevendeg.log

import com.fortysevendeg.log.utils.Config
import org.http4s.client.blaze.PooledHttp1Client

import scala.language.postfixOps

object StackExchangeTestUrl {

  def main(args: Array[String]) = {

    val client = PooledHttp1Client()

    val url = s"${Config.urlSearchStackOverflow}linux"

    client.getAs[String](url)
      .runAsync(_.fold(
        l => println(s"Error: ${l.getMessage}"),
        r => println(s"JSON => $r")//println(s"${r.items.length} ${r.items.map(_.title).mkString("\n")}")
      ))
  }
}



