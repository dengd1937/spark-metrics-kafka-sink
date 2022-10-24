package org.apache.spark.droidhang.metrics.sink

import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.{SecurityManager, SparkConf}
import org.junit.{After, Before, Test}


class KafkaSinkSuite {

  private val sinkClassPropertyName = "spark.metrics.conf.*.sink.kafka.class"
  private val sinkClassPropertyValue = "org.apache.spark.metrics.sink.KafkaSink"

  private val kafkaBrokersName = "spark.metrics.conf.*.sink.kafka.brokers"
  private val kafkaBrokersValue = "localhost:9092"

  private val kafkaTopicName = "spark.metrics.conf.*.sink.kafka.topic"
  private val kafkaTopicValue = "test"

  @Test
  def testThatKafkaSinkCanBeLoaded(): Unit = {
    val instance = "driver"
    val conf = new SparkConf(true)
    val sm = new SecurityManager(conf)
    val ms = MetricsSystem.createMetricsSystem(instance, conf, sm)
    ms.start()
    ms.stop()
  }

  @Before
  def tearDown(): Unit = {
    System.setProperty(sinkClassPropertyName, sinkClassPropertyValue)
    System.setProperty(kafkaBrokersName, kafkaBrokersValue)
    System.setProperty(kafkaTopicName, kafkaTopicValue)
  }

  @After
  def setUp(): Unit = {
    System.clearProperty(sinkClassPropertyName)
    System.clearProperty(kafkaBrokersName)
    System.clearProperty(kafkaTopicName)
  }

}
