package atech.guide.consumer

import java.time.Duration

import atech.guide.commons.Utils
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._

object MyKafkaComsumer {

  private val logger = LoggerFactory.getLogger(getClass)
  private val group = "MyGroup"
  private val offset = "earliest"
  private val kafkaTopic = "atechguide_first_topic_partitioned"

  def main(args: Array[String]): Unit = {

    val props = Utils.getConsumerProperties(group, offset)

    // Creating the Consumer
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Seq(kafkaTopic).asJava)

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100)) // new in Kafka

      records.asScala.foreach { record =>
        logger.info(s"Received key = ${record.key()}, Value = ${record.value()}, Partition = ${record.partition()}, offset = ${record.offset()}")
      }
    }
  }

}
