package com.fortysevendeg.log.utils

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkConnection
import org.apache.kafka.clients.consumer.KafkaConsumer

object KafkaUtilities {

  def createKafkaProducer(): Producer[String, String] = {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
//    props.put("partitioner.class", "com.fortysevendeg.biglog.SimplePartitioner")
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("producer.type", "async")
    props.put("request.required.acks", "1")

    val config = new ProducerConfig(props)
    new Producer[String, String](config)
  }

  def createKafkaConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    new KafkaConsumer[String, String](props)
  }

  def createTopicIntoKafka(topic: String, numPartitions: Int, replicationFactor: Int): Unit = {
    val zookeeperConnect = "localhost:2181"
    val sessionTimeoutMs = 10 * 1000
    val connectionTimeoutMs = 8 * 1000

    val zkClient = ZkUtils.createZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs)
    val zkUtils = new ZkUtils(zkClient, zkConnection = new ZkConnection(zookeeperConnect), isSecure = false)
    AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, new Properties)
    zkClient.close()
  }

  def d(kafkaProducer: Producer[String, String], topic: String, message: String) = {
    kafkaProducer.send(new KeyedMessage[String, String](topic, message))
  }

}
