package com.fortysevendeg.log.utils

import com.fortysevendeg.log.models.StackExchange
import io.circe.Decoder
import org.http4s.EntityDecoder

trait StackExchangeDecoder {
  import org.http4s.circe._

  implicit val stackExchangeDecoder: Decoder[StackExchange] = Decoder[StackExchange]

  implicit val stackExchangeEntityDecoder: EntityDecoder[StackExchange] = jsonOf[StackExchange]
}