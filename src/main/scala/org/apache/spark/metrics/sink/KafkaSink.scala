package org.apache.spark.metrics.sink

import java.util.concurrent.TimeUnit
import java.util.{Locale, Properties}

import com.codahale.metrics.MetricRegistry
import com.droidhang.metrics.KafkaReporter
import org.apache.spark.SecurityManager
import org.slf4j.{Logger, LoggerFactory}

class KafkaSink(val properties: Properties, val registry: MetricRegistry,
                securityMgr: SecurityManager) extends Sink {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val KAFKA_KEY_BROKERS = "brokers"
  val KAFKA_KEY_TOPIC = "topic"
  val KAFKA_KEY_FILTER = "filter"

  def propertyToOption(prop: String): Option[String] = Option(properties.getProperty(prop))

  if (propertyToOption(KAFKA_KEY_BROKERS).isEmpty) {
    throw new Exception("Kafka sink requires 'brokers' property.")
  }

  if (propertyToOption(KAFKA_KEY_TOPIC).isEmpty) {
    throw new Exception("Kafka sink requires 'topic' property.")
  }

  val brokers: String = propertyToOption(KAFKA_KEY_BROKERS).get
  val topic: String = propertyToOption(KAFKA_KEY_TOPIC).get
  val filter: Array[String] = propertyToOption(KAFKA_KEY_FILTER) match {
    case Some(o) => o.split(",")
    case None => Array.empty[String]
  }

  lazy val reporter = new KafkaReporter(registry, brokers, topic, filter, properties)

  override def start(): Unit = {
    logger.info(s"Starting Kafka metric reporter at $brokers, topic $topic")

    val period = propertyToOption("period").getOrElse("10").toLong
    val unit = TimeUnit.valueOf(propertyToOption("unit").getOrElse("seconds").toUpperCase(Locale.ROOT))

    reporter.start(period, unit)
  }

  override def stop(): Unit = {
    logger.info(s"Stopping Kafka metric reporter at $brokers, topic $topic")
    reporter.stop()
  }

  override def report(): Unit = {
    logger.info(s"Reporting metrics to Kafka reporter at $brokers, topic $topic")
    reporter.report()
  }

}
