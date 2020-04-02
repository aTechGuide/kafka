package atech.guide.commons

import java.util.Properties

import atech.guide.producer.KafkaProducerKeys.logger
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import scala.concurrent.Promise
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object Utils {

  private val kafkaBootstrapServer = "localhost:9092"

  private val logger = LoggerFactory.getLogger(getClass)

  def getProducerProperties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props
  }

  def fetchMetadataFromPromise(promise: Promise[RecordMetadata]): Unit = {
    val future = promise.future
    future.onComplete({
      case Success(metadata) => logger.info(s"Topic = ${metadata.topic()}, Partition = ${metadata.partition}, Offset = ${metadata.offset}")
      case Failure(exception) => logger.error(s"Received error ${exception.getMessage}")
    })
  }

  def createCallback(promise: Promise[RecordMetadata]) = {
    new Callback() {
      // Executes every time record is successfully sent or an exception is thrown
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {

        Option(exception) match {
          case Some(err) => promise.failure(err)
          case None => promise.success(metadata)
        }
      }
    }
  }

}
