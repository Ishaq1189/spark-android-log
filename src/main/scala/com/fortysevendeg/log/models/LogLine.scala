package com.fortysevendeg.log.models

case class LogLine(info: LogInfo, message: String)

case class LogInfo(app: String, pid: Int, logType: LogType, date: String)

case class LogException(name: String, message: String, date: String)

sealed trait LogType

case object Verbose extends LogType

case object Warning extends LogType

case object Info extends LogType

case object Error extends LogType

case object Debug extends LogType

case object Unknown extends LogType
