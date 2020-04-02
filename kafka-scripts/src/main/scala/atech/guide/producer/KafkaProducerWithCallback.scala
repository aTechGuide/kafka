package atech.guide.producer

import atech.guide.commons.Utils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import scala.concurrent.Promise

object KafkaProducerWithCallback {

  private val kafkaTopic = "atechguide_first_topic"

  def main(args: Array[String]): Unit = {

    val properties = Utils.getProducerProperties
    val producer = new KafkaProducer[String, String](properties)
    val record = new ProducerRecord[String, String](kafkaTopic, "Second Message")

    val promise = Promise[RecordMetadata]()

    producer.send(record, Utils.createCallback(promise))

    // Wait for the data to be sent
    producer.flush() // Flush Data
    producer.close() // Flush and Close Producer

    Utils.fetchMetadataFromPromise(promise)
  }

}
