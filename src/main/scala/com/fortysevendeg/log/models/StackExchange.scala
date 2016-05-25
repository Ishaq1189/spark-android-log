package com.fortysevendeg.log.models

case class StackExchange(items: Seq[StackExchangeItem])

case class StackExchangeItem(link: String, title: String, score: Int, view_count: Int)
