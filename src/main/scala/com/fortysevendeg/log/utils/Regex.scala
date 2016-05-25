package com.fortysevendeg.log.utils

import com.fortysevendeg.log.models.{Debug, Unknown, _}

import scala.util.Try

object Regex {
  val exception = "\\w+\\.?Exception".r

  val date = "([0-9]|:)+\\.\\d*".r

  val typePlusAppPlusPid = "[DWIEV]/.*\\s*\\(\\s*\\d+\\)".r

  val message = "\\):.*$".r

  def extractTypeAppPid(chain: String): (LogType, Option[String], Option[Int]) = {
    val typeException = chain.substring(0, 1) match {
      case "V" => Verbose
      case "I" => Info
      case "E" => Error
      case "W" => Warning
      case "D" => Debug
      case _ => Unknown
    }
    val app = Try(chain.substring(chain.indexOf("/") + 1, chain.indexOf("(")).trim).toOption
    val pid = Try(chain.substring(chain.indexOf("(") + 1, chain.indexOf(")")).trim.toInt).toOption
    (typeException, app, pid)
  }

}
