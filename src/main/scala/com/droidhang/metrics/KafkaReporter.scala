package com.droidhang.metrics

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSONObject
import com.codahale.metrics.{MetricAttribute, _}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class KafkaReporter(registry: MetricRegistry,
                    kafkaBootstrapServers: String,
                    kafkaTopic: String,
                    metricsFilter: Array[String],
                    properties: Properties)
  extends ScheduledReporter(
    registry,
    "kafka-reporter",
    MetricFilter.ALL,
    TimeUnit.SECONDS,
    TimeUnit.MILLISECONDS) {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer"
  private val VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer"


  override def start(initialDelay: Long, period: Long, unit: TimeUnit): Unit = {
    super.start(initialDelay, period, unit)
  }

  override def stop(): Unit = {
    super.stop()
  }

  override def report(gauges: util.SortedMap[String, Gauge[_]],
                      counters: util.SortedMap[String, Counter],
                      histograms: util.SortedMap[String, Histogram],
                      meters: util.SortedMap[String, Meter],
                      timers: util.SortedMap[String, Timer]): Unit = {
    val jsonObj = new JSONObject()
    val createTs = (System.currentTimeMillis() * 1000000L + System.nanoTime() % 1000000L).toString
    jsonObj.put("create_ts", createTs)

    var client: KafkaProducer[String, String] = null
    try {
      for (entry <- gauges.entrySet().asScala) {
        reportGauges(entry.getKey, entry.getValue, jsonObj)
      }

      for (entry <- counters.entrySet().asScala) {
        reportCounters(entry.getKey, entry.getValue, jsonObj)
      }

      for (entry <- histograms.entrySet().asScala) {
        reportHistograms(entry.getKey, entry.getValue, jsonObj)
      }

      for (entry <- meters.entrySet().asScala) {
        reportMeters(entry.getKey, entry.getValue, jsonObj)
      }

      for (entry <- timers.entrySet().asScala) {
        reportTimers(entry.getKey, entry.getValue, jsonObj)
      }

      client = initKafkaProducer()
      if (client != null) {
        client.send(new ProducerRecord[String, String](kafkaTopic, "", jsonObj.toJSONString))
      }
    } catch {
      case e: Exception => logger.warn("Unable to report to kafka", e)
    } finally {
      if (client != null) {
        client.flush()
        client.close()
      }
    }

  }

  def reportTimers(name: String, timer: Timer, jsonObj: JSONObject): Unit = {
    if (filterMetricsNamespace(name)) {
      val snapshot = timer.getSnapshot

      jsonObj.put(prefix(name, MetricAttribute.MAX), format(convertDuration(snapshot.getMax)))
      jsonObj.put(prefix(name, MetricAttribute.MEAN), format(convertDuration(snapshot.getMean)))
      jsonObj.put(prefix(name, MetricAttribute.MIN), format(convertDuration(snapshot.getMin)))
      jsonObj.put(prefix(name, MetricAttribute.STDDEV), format(convertDuration(snapshot.getStdDev)))
      jsonObj.put(prefix(name, MetricAttribute.P50), format(convertDuration(snapshot.getMedian)))
      jsonObj.put(prefix(name, MetricAttribute.P75), format(convertDuration(snapshot.get75thPercentile())))
      jsonObj.put(prefix(name, MetricAttribute.P95), format(convertDuration(snapshot.get95thPercentile())))
      jsonObj.put(prefix(name, MetricAttribute.P98), format(convertDuration(snapshot.get98thPercentile())))
      jsonObj.put(prefix(name, MetricAttribute.P99), format(convertDuration(snapshot.get99thPercentile())))

      reportMeters(name, timer, jsonObj)
    }
  }


  def reportMeters(name: String, meter: Metered, jsonObj: JSONObject): Unit = {
    if (filterMetricsNamespace(name)) {
      jsonObj.put(prefix(name, MetricAttribute.COUNT), format(meter.getCount))
      jsonObj.put(prefix(name, MetricAttribute.MEAN_RATE), format(convertRate(meter.getMeanRate)))
      jsonObj.put(prefix(name, MetricAttribute.M1_RATE), format(convertRate(meter.getOneMinuteRate)))
      jsonObj.put(prefix(name, MetricAttribute.M5_RATE), format(convertRate(meter.getFiveMinuteRate)))
      jsonObj.put(prefix(name, MetricAttribute.M15_RATE), format(convertRate(meter.getFifteenMinuteRate)))
    }
  }


  def reportHistograms(name: String, histogram: Histogram, jsonObj: JSONObject): Unit = {
    if (filterMetricsNamespace(name)) {
      val snapshot = histogram.getSnapshot
      jsonObj.put(prefix(name, MetricAttribute.COUNT), format(histogram.getCount))
      jsonObj.put(prefix(name, MetricAttribute.MAX), format(snapshot.getMax))
      jsonObj.put(prefix(name, MetricAttribute.MIN), format(snapshot.getMin))
      jsonObj.put(prefix(name, MetricAttribute.MEAN), format(snapshot.getMean))
      jsonObj.put(prefix(name, MetricAttribute.STDDEV), format(snapshot.getStdDev))
      jsonObj.put(prefix(name, MetricAttribute.P50), format(snapshot.getMedian))
      jsonObj.put(prefix(name, MetricAttribute.P75), format(snapshot.get75thPercentile()))
      jsonObj.put(prefix(name, MetricAttribute.P95), format(snapshot.get95thPercentile()))
      jsonObj.put(prefix(name, MetricAttribute.P98), format(snapshot.get98thPercentile()))
      jsonObj.put(prefix(name, MetricAttribute.P99), format(snapshot.get99thPercentile()))
    }
  }

  def reportCounters(name: String, counter: Counter, jsonObj: JSONObject): Unit = {
    if (filterMetricsNamespace(name)) {
      val value = counter.getCount.longValue().toString
      jsonObj.put(name, value)
    }
  }

  def reportGauges(name: String, gauge: Gauge[_], jsonObj: JSONObject): Unit = {
    val value = gauge.getValue match {
      case v: Float => format(v.toDouble)
      case v: Double => format(v)
      case v: Byte => format(v.toLong)
      case v: Short => format(v.toLong)
      case v: Int => format(v.toLong)
      case v: Long => format(v)
      case _ => null
    }

    if (value != null && filterMetricsNamespace(name)) {
      jsonObj.put(name, value)
    }
  }

  private def initKafkaProducer(): KafkaProducer[String, String] = {
    logger.info("initialize kafka producer with {bootstrap.servers:{}}", kafkaBootstrapServers)

    try {
      val properties = new Properties
      properties.put("bootstrap.servers", kafkaBootstrapServers)
      properties.put("ack", "1")
      properties.put("key.serializer", KEY_SERIALIZER)
      properties.put("value.serializer", VALUE_SERIALIZER)
      new KafkaProducer[String, String](properties)
    } catch {
      case e: Exception =>
        logger.error(s"Failure opening Kafka endpoint $kafkaBootstrapServers:\n$e")
        null
    }

  }

  private def filterMetricsNamespace(name: String): Boolean = {
    if (metricsFilter.isEmpty) {
      return true
    }
    metricsFilter.foreach(m => {
      if (name.contains(m)) return true
    })
    false
  }

  private def prefix(name: String, metricType: MetricAttribute): String = {
    MetricRegistry.name(name, metricType.getCode)
  }

  private def format(n: Long): String = {
    n.longValue().toString
  }

  private def format(n: Double): String = {
    n.doubleValue().formatted("%2.2f")
  }


}
