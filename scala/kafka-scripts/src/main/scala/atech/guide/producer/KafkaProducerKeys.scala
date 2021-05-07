package atech.guide.producer

import atech.guide.commons.Utils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory
import scala.concurrent.Promise

object KafkaProducerKeys {

  private val kafkaTopic = "atechguide_first_topic_partitioned"

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val properties = Utils.getProducerProperties
    val producer = new KafkaProducer[String, String](properties)

    (6 to 10).foreach { value =>

      // Creating new promise for each message
      val promise = Promise[RecordMetadata]()
      val key = "Key_" + value
      val message = s"Second Message with value = $value"

      logger.info(s"Key = $key and message = $message")
      val record = new ProducerRecord[String, String](kafkaTopic, key, message)
      producer.send(record, Utils.createCallback(promise))

      // Printing Metadata from the message
      Utils.fetchMetadataFromPromise(promise)
    }

    // Wait for the data to be sent
    producer.flush() // Flush Data
    producer.close() // Flush and Close Producer
  }
}
