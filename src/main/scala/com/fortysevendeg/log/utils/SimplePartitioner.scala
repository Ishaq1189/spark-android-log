package com.fortysevendeg.log.utils

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

class SimplePartitioner(props: VerifiableProperties) extends Partitioner {

  override def partition(key: Any, numPartitions: Int): Int = {
    key.hashCode % numPartitions
  }

}
