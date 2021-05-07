package atech.guide.producer

import atech.guide.commons.Utils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {

  val kafkaTopic = "atechguide_first_topic"

  def main(args: Array[String]): Unit = {

    // Create Producer Properties
    val properties = Utils.getProducerProperties

    // Create Producer
    val producer = new KafkaProducer[String, String](properties)

    // Create a Producer Record
    val record = new ProducerRecord[String, String](kafkaTopic, "First Message")

    // Send Data
    // Remember send() is Async
    producer.send(record)

    // Wait for the data to be sent
    producer.flush() // Flush Data
    producer.close() // Flush and Close Producer
  }

}
